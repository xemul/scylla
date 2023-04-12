/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/client.hh>

using namespace seastar;
class memory_data_sink_buffers;

namespace s3 {

struct range {
    uint64_t off;
    size_t len;
};

class client : public enable_shared_from_this<client> {
    class upload_sink;
    class readable_file;
    std::string _host;
    int _port;
    http::experimental::client _http;

    struct private_tag {};

public:
    explicit client(std::string host, int port, private_tag);
    static shared_ptr<client> make(std::string endpoint);

    future<uint64_t> get_object_size(sstring object_name);
    future<temporary_buffer<char>> get_object_contiguous(sstring object_name, std::optional<range> range = {});
    future<> put_object(sstring object_name, temporary_buffer<char> buf);
    future<> put_object(sstring object_name, ::memory_data_sink_buffers bufs);
    future<> delete_object(sstring object_name);

    file make_readable_file(sstring object_name);
    data_sink make_upload_sink(sstring object_name);

    future<> close();
};

} // s3 namespace
