#!/usr/bin/env python3

import asyncio
import os
import requests
import pytest
import logging

from test.pylib.manager_client import ManagerClient
from test.object_store.conftest import format_tuples
from test.object_store.conftest import get_s3_resource
from test.topology.conftest import skip_mode

logger = logging.getLogger(__name__)

def create_ks_and_cf(cql):
    ks = 'test_ks'
    cf = 'test_cf'

    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy', 'replication_factor': '1'})
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(f"CREATE TABLE {ks}.{cf} ( name text primary key, value text );")

    rows = [('0', 'zero'),
            ('1', 'one'),
            ('2', 'two')]
    for row in rows:
        cql_fmt = "INSERT INTO {}.{} ( name, value ) VALUES ('{}', '{}');"
        cql.execute(cql_fmt.format(ks, cf, *row))

    return ks, cf

async def prepare_snapshot_for_backup(manager: ManagerClient, server):
    cql = manager.get_cql()
    workdir = await manager.server_get_workdir(server.server_id)
    print(f'Create keyspace')
    ks, cf = create_ks_and_cf(cql)
    print('Flush keyspace')
    await manager.api.flush_keyspace(server.ip_addr, ks)
    print('Take keyspace snapshot')
    await manager.api.take_snapshot(server.ip_addr, ks, 'backup')

    return ks, cf

@pytest.mark.asyncio
async def test_simple_backup(manager: ManagerClient, s3_server):
    '''check that backing up a snapshot for a keyspace works'''

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = [ '--logger-log-level', 'snapshots=trace:task_manager=trace' ]
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    print('Backup snapshot')
    tid = await manager.api.backup(server.ip_addr, ks, 'backup', s3_server.address, s3_server.bucket_name)
    print(f'Started task {tid}')
    status = await manager.api.get_task_status(server.ip_addr, tid)
    print(f'Status: {status}, waiting to finish')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')

    objects = set([ o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.all() ])
    for f in files:
        print(f'Check {f} is in backup')
        assert f'{cf}/backup/{f}' in objects

    # Check that task runs in the streaming sched group
    log = await manager.server_open_log(server.server_id)
    res = await log.grep(r'INFO.*\[shard [0-9]:([a-z]+)\] .* Backup sstables from .* to')
    assert len(res) == 1 and res[0][1].group(1) == 'strm'


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_backup_is_abortable(manager: ManagerClient, s3_server):
    '''check that backing up a snapshot for a keyspace works'''

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = [ '--logger-log-level', 'snapshots=trace:task_manager=trace' ]
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 1

    await manager.api.enable_injection(server.ip_addr, "backup_task_pause", one_shot=True)
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    print('Backup snapshot')
    tid = await manager.api.backup(server.ip_addr, ks, 'backup', s3_server.address, s3_server.bucket_name)

    print(f'Started task {tid}, aborting it early')
    await log.wait_for('backup task: waiting', from_mark=mark)
    await manager.api.abort_task(server.ip_addr, tid)
    await manager.api.message_injection(server.ip_addr, "backup_task_pause")
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert (status is not None) and (status['state'] == 'failed')

    objects = set([ o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.all() ])
    uploaded_count = 0
    for f in files:
        print(f'Check {f} is in backup')
        if f'{cf}/backup/{f}' in objects:
            uploaded_count += 1
    assert uploaded_count > 0 and uploaded_count < len(files)
