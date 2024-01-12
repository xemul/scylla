#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from uuid import UUID

from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, HTTPError
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts, read_barrier
from test.topology.conftest import skip_mode
from test.topology.util import reconnect_driver

import pytest
import asyncio
import logging
import time


logger = logging.getLogger(__name__)


async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)


async def get_tablet_replicas(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str, token: int):
    """
    Gets tablet replicas of the tablet which owns a given token of a given table.
    This call is guaranteed to see all prior changes applied to group0 tables.

    :param server: server to query. Can be any live node.
    """

    host = manager.cql.cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.cql, host)

    table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.cql.run_async(f"SELECT last_token, replicas FROM system.tablets where "
                                       f"keyspace_name = '{keyspace_name}' and "
                                       f"table_id = {table_id}", host=host)
    for row in rows:
        if row.last_token >= token:
            return row.replicas


async def get_tablet_replica(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str, token: int):
    """
    Get the first replica of the tablet which owns a given token of a given table.
    This call is guaranteed to see all prior changes applied to group0 tables.

    :param server: server to query. Can be any live node.
    """
    replicas = await get_tablet_replicas(manager, server, keyspace_name, table_name, token)
    return replicas[0]


@pytest.mark.asyncio
async def test_tablet_metadata_propagates_with_schema_changes_in_snapshot_mode(manager: ManagerClient):
    """Test that you can create a table and insert and query data"""

    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_proxy=trace',
        '--logger-log-level', 'cql_server=trace',
        '--logger-log-level', 'query_processor=trace',
        '--logger-log-level', 'gossip=trace',
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'messaging_service=trace',
        '--logger-log-level', 'rpc=trace',
        ]
    servers = await manager.servers_add(3, cmdline=cmdline)

    s0 = servers[0].server_id
    not_s0 = servers[1:]

    # s0 should miss schema and tablet changes
    await manager.server_stop_gracefully(s0)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 100};")

    # force s0 to catch up later from the snapshot and not the raft log
    await inject_error_one_shot_on(manager, 'raft_server_force_snapshot', not_s0)
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(10)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 1);") for k in keys])

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(list(rows)) == len(keys)
    for r in rows:
        assert r.c == 1

    manager.driver_close()
    await manager.server_start(s0, wait_others=2)
    await manager.driver_connect(server=servers[0])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

    # Trigger a schema change to invoke schema agreement waiting to make sure that s0 has the latest schema
    await cql.run_async("CREATE KEYSPACE test_dummy WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")

    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 2);", execution_profile='whitelist')
                           for k in keys])

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == 2

    conn_logger = logging.getLogger("conn_messages")
    conn_logger.setLevel(logging.DEBUG)
    try:
        # Check that after rolling restart the tablet metadata is still there
        await manager.rolling_restart(servers)

        cql = await reconnect_driver(manager)

        await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

        await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 3);", execution_profile='whitelist')
                               for k in keys])

        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == 3
    finally:
        conn_logger.setLevel(logging.INFO)

    await cql.run_async("DROP KEYSPACE test;")
    await cql.run_async("DROP KEYSPACE test_dummy;")


@pytest.mark.asyncio
async def test_scans(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(100)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    rows = await cql.run_async("SELECT count(*) FROM test.test;")
    assert rows[0].count == len(keys)

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == r.pk

    await cql.run_async("DROP KEYSPACE test;")


@pytest.mark.asyncio
async def test_table_drop_with_auto_snapshot(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = { 'auto_snapshot': True }
    servers = await manager.servers_add(3, config = cfg)

    cql = manager.get_cql()

    # Increases the chance of tablet migration concurrent with schema change
    await inject_error_on(manager, "tablet_allocator_shuffle", servers)

    for i in range(3):
        await cql.run_async("DROP KEYSPACE IF EXISTS test;")
        await cql.run_async("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8 };")
        await cql.run_async("CREATE TABLE IF NOT EXISTS test.tbl_sample_kv (id int, value text, PRIMARY KEY (id));")
        await cql.run_async("INSERT INTO test.tbl_sample_kv (id, value) VALUES (1, 'ala');")

    await cql.run_async("DROP KEYSPACE test;")


@pytest.mark.asyncio
async def test_topology_changes(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        logger.info("Checking table")
        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await inject_error_on(manager, "tablet_allocator_shuffle", servers)

    logger.info("Adding new server")
    await manager.server_add()

    await check()

    logger.info("Adding new server")
    await manager.server_add()

    await check()
    time.sleep(5) # Give load balancer some time to do work
    await check()

    await manager.decommission_node(servers[0].server_id)

    await check()

    await cql.run_async("DROP KEYSPACE test;")


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_streaming_is_guarded_by_topology_guard(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    servers.append(await manager.server_add(cmdline=cmdline))

    key = 7 # Whatever
    tablet_token = 0 # Doesn't matter since there is one tablet
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, 0)")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1

    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)
    dst_shard = 0

    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
    s1_log = await manager.server_open_log(servers[1].server_id)
    s1_mark = await s1_log.mark()

    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

    # Wait for the replica-side writer of streaming to reach a place where it already
    # received writes from the leaving replica but haven't applied them yet.
    # Once the writer reaches this place, it will wait for the message_injection() call below before proceeding.
    # The place we block the writer in should not hold to erm or topology_guard because that will block the migration
    # below and prevent test from proceeding.
    await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)
    s1_mark = await s1_log.mark()

    # Should cause streaming to fail and be retried while leaving behind the replica-side writer.
    await manager.api.inject_disconnect(servers[0].ip_addr, servers[1].ip_addr)

    logger.info("Waiting for migration to finish")
    await migration_task
    logger.info("Migration done")

    # Sanity test
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1

    await cql.run_async("TRUNCATE test.test")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 0

    # Release abandoned streaming
    await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
    await s1_log.wait_for('stream_mutation_fragments: done', from_mark=s1_mark)

    # Verify that there is no data resurrection
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 0

    # Verify that moving the tablet back works
    await manager.api.move_tablet(servers[0].ip_addr, "test", "test", s1_host_id, dst_shard, replica[0], replica[1], tablet_token)
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 0


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_table_dropped_during_streaming(manager: ManagerClient):
    """
    Verifies that load balancing recovers when table is dropped during streaming phase of tablet migration.
    Recovering means that state machine is not stuck and later migrations can proceed.
    """

    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add()]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    await cql.run_async("CREATE TABLE test.test2 (pk int PRIMARY KEY, c int);")

    servers.append(await manager.server_add())

    logger.info("Populating tables")
    key = 7 # Whatever
    value = 3 # Whatever
    tablet_token = 0 # Doesn't matter since there is one tablet
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, {value})")
    await cql.run_async(f"INSERT INTO test.test2 (pk, c) VALUES ({key}, {value})")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1
    rows = await cql.run_async("SELECT pk from test.test2")
    assert len(list(rows)) == 1

    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
    s1_log = await manager.server_open_log(servers[1].server_id)
    s1_mark = await s1_log.mark()

    logger.info("Starting tablet migration")
    s1_host_id = await manager.get_host_id(servers[1].server_id)
    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica[0], replica[1], s1_host_id, 0, tablet_token))

    # Wait for the replica-side writer of streaming to reach a place where it already
    # received writes from the leaving replica but haven't applied them yet.
    # Once the writer reaches this place, it will wait for the message_injection() call below before proceeding.
    # We want to drop the table while streaming is deep in the process, where it will attempt to apply writes
    # to the dropped table.
    await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)

    # Streaming blocks table drop, so we can't wait here.
    drop_task = cql.run_async("DROP TABLE test.test")

    # Release streaming as late as possible to increase probability of drop causing problems.
    await s1_log.wait_for('Dropping', from_mark=s1_mark)

    # Unblock streaming
    await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
    await drop_task

    logger.info("Waiting for migration to finish")
    try:
        await migration_task
    except HTTPError as e:
        assert 'Tablet map not found' in e.message

    logger.info("Verifying that moving the other tablet works")
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test2', tablet_token)
    s0_host_id = await manager.get_host_id(servers[0].server_id)
    assert replica[0] == UUID(s0_host_id)
    await manager.api.move_tablet(servers[0].ip_addr, "test", "test2", replica[0], replica[1], s1_host_id, 0, tablet_token)

    logger.info("Verifying tablet replica")
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test2', tablet_token)
    assert replica == (UUID(s1_host_id), 0)
