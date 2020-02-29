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
 * Copyright (C) 2020 ScyllaDB
 */

#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs_mock_metadata_to_disk_buffer.hh"

#include <seastar/core/print.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/units.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>
#include <stdint.h>

using namespace seastar;
using namespace seastar::fs;

using append = mock_metadata_to_disk_buffer::action::append;
using flush_to_disk = mock_metadata_to_disk_buffer::action::flush_to_disk;

namespace seastar::fs {

class empty_block_device_impl : public block_device_impl {
public:
    ~empty_block_device_impl() override = default;

    future<size_t> write([[maybe_unused]] uint64_t pos, [[maybe_unused]] const void* buffer,
            size_t len, [[maybe_unused]] const io_priority_class&) override {
        return make_ready_future<size_t>(len);
    }

    future<size_t> read([[maybe_unused]] uint64_t pos, void* buffer, size_t len,
            [[maybe_unused]] const io_priority_class&) noexcept override {
        std::memset(buffer, 0, len);
        return make_ready_future<size_t>(len);
    }

    future<> flush() noexcept override {
        return make_ready_future<>();
    }

    future<disk_offset_t> size() noexcept override {
        return make_ready_future<disk_offset_t>(0);
    }

    future<> close() noexcept override {
        return make_ready_future<>();
    }
};

} // seastar::fs

SEASTAR_THREAD_TEST_CASE(mock_metadata_to_disk_buffer_test) {
    constexpr unit_size_t cluster_size = 1 * MB;
    constexpr unit_size_t alignment = 4096;
    shared_ptr<metadata_to_disk_buffer> tmp_buff = make_shared<mock_metadata_to_disk_buffer>(cluster_size, alignment);
    auto dev_impl = make_shared<empty_block_device_impl>();
    block_device dev(dev_impl);

    metadata_log log(dev, cluster_size, alignment, std::move(tmp_buff));

    log.bootstrap(0, 3, {3, 10}, 4, 0).get();
}
