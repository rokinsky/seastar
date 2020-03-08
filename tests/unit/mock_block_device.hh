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
 * Copyright (C) 2020 ScyllaDB Ltd.
 */

#pragma once

#include <cstring>
#include <seastar/fs/block_device.hh>
#include <seastar/testing/test_case.hh>

namespace seastar::fs {

class mock_block_device_impl : public block_device_impl {
public:
    using buf_type = basic_sstring<uint8_t, size_t, 32, false>;
    buf_type buf;
    ~mock_block_device_impl() override = default;

    future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class&) override {
        if (buf.size() < pos + len)
            buf.resize(pos + len);
        std::memcpy(buf.data() + pos, buffer, len);
        return make_ready_future<size_t>(len);
    }

    future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class&) noexcept override {
        if (buf.size() < pos + len)
            buf.resize(pos + len);
        std::memcpy(buffer, buf.c_str() + pos, len);
        return make_ready_future<size_t>(len);
    }

    future<> flush() noexcept override {
        return make_ready_future<>();
    }

    future<> close() noexcept override {
        return make_ready_future<>();
    }
};

} // seastar::fs
