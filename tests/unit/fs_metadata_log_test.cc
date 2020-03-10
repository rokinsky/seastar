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
#include "mock_block_device.hh"
#include "seastar/fs/temporary_file.hh"

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

namespace {

// Returns copy of given value. Needed to solve misalignment iiss
template<typename T>
T copy_value(T x) {
    return x;
}

std::string to_string(const temporary_buffer<uint8_t>& a) {
    return std::string(a.get(), a.get() + a.size());
}

constexpr unit_size_t default_cluster_size = 1 * MB;
constexpr unit_size_t default_alignment = 4096;
constexpr cluster_range default_cluster_range = {1, 10};
constexpr cluster_id_t default_metadata_log_cluster = 1;

template<typename BlockDevice = mock_block_device_impl, typename MetadataToDiskBuffer = mock_metadata_to_disk_buffer>
auto init_structs() {
    auto dev_impl = make_shared<BlockDevice>();
    metadata_log log(block_device(dev_impl), default_cluster_size, default_alignment, make_shared<MetadataToDiskBuffer>());
    log.bootstrap(0, default_metadata_log_cluster, default_cluster_range, 1, 0).get();

    return std::pair{std::move(dev_impl), std::move(log)};
}

} // namespace

SEASTAR_THREAD_TEST_CASE(some_tests) {
    auto [dev, log] = init_structs();

    log.create_directory("/test/", file_permissions::default_dir_permissions).get();
    log.create_file("/test/test", file_permissions::default_dir_permissions).get();

    auto& created_buffers = mock_metadata_to_disk_buffer::virtually_constructed_buffers;
    auto& buff = created_buffers.back();
    BOOST_REQUIRE_EQUAL(buff->actions.size(), 2);
    BOOST_REQUIRE(buff->is_append_type<ondisk_create_inode_as_dir_entry>(0));
    BOOST_REQUIRE(buff->is_append_type<ondisk_create_inode_as_dir_entry>(1));
    auto& ondisk_file_header = buff->get_by_append_type<ondisk_create_inode_as_dir_entry>(1).header;

    inode_t file_inode = log.open_file("/test/test").get0();
    BOOST_REQUIRE_EQUAL(file_inode, ondisk_file_header.entry_inode.inode);

    std::string str_write = "123456";
    constexpr uint64_t offset = 10;
    size_t wrote = log.write(file_inode, offset, str_write.data(), str_write.size()).get0();
    BOOST_REQUIRE_EQUAL(wrote, str_write.size());
    BOOST_REQUIRE_EQUAL(buff->actions.size(), 3);
    BOOST_REQUIRE(buff->is_append_type<ondisk_small_write>(2));
    BOOST_REQUIRE_EQUAL(to_string(buff->get_by_append_type<ondisk_small_write>(2).data), str_write);
    BOOST_REQUIRE_EQUAL(copy_value(buff->get_by_append_type<ondisk_small_write>(2).header.offset), offset);

    log.close_file(file_inode).get();
    log.flush_log().get();
    BOOST_REQUIRE_EQUAL(buff->actions.size(), 4);
    BOOST_REQUIRE(buff->is_type<flush_to_disk>(3));
}
