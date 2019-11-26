/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#include "tcp_connection.hh"

namespace seastar {

namespace kafka {

future<lw_shared_ptr<tcp_connection>> tcp_connection::connect(const std::string& host, uint16_t port) {
    net::inet_address target_host = net::inet_address{host};
    sa_family_t family = target_host.is_ipv4() ? sa_family_t(AF_INET) : sa_family_t(AF_INET6);
    socket_address socket = socket_address(::sockaddr_in{family, INADDR_ANY, {0}});
    auto f = target_host.is_ipv4()
            ? engine().net().connect(ipv4_addr{target_host, port}, socket, transport::TCP)
            : engine().net().connect(ipv6_addr{target_host, port}, socket, transport::TCP);
    return f.then([target_host = std::move(target_host), port] (connected_socket fd) {
                return make_lw_shared<tcp_connection>(target_host, port, std::move(fd));
            }
    );
}

future<temporary_buffer<char>> tcp_connection::read(size_t bytes) {
    return _read_buf.read_exactly(bytes);
}

future<> tcp_connection::write(temporary_buffer<char> buff) {
    return _write_buf.write(std::move(buff)).then([this] {
        return _write_buf.flush();
    });
}

future<> tcp_connection::close() {
    return when_all(_read_buf.close(), _write_buf.close()).discard_result();
}

}

}