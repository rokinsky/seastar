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
#include "fs/bitwise.hh"
#include "fs/metadata_log_operations/write.hh"
#include "fs_mock_cluster_writer.hh"
#include "fs_mock_metadata_to_disk_buffer.hh"
#include "fs_mock_block_device.hh"
#include "seastar/fs/temporary_file.hh"

#include <cstdint>
#include <random>
#include <seastar/core/print.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/units.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;
using namespace seastar::fs;

using std::cerr;
using std::endl;

using append = mock_metadata_to_disk_buffer::action::append;
using flush_to_disk = mock_metadata_to_disk_buffer::action::flush_to_disk;

namespace {

// Returns copy of the given value. Needed to solve misalignment problems.
template<typename T>
T copy_value(T x) {
    return x;
}

constexpr unit_size_t default_cluster_size = 1 * MB;
constexpr unit_size_t default_alignment = 4096;
constexpr cluster_range default_cluster_range = {1, 10};
constexpr cluster_id_t default_metadata_log_cluster = 1;

template<typename BlockDevice = mock_block_device_impl,
        typename MetadataToDiskBuffer = mock_metadata_to_disk_buffer,
        typename ClusterWriter = mock_cluster_writer>
auto init_structs() {
    auto dev_impl = make_shared<BlockDevice>();
    metadata_log log(block_device(dev_impl), default_cluster_size, default_alignment,
            make_shared<MetadataToDiskBuffer>(), make_shared<ClusterWriter>());
    log.bootstrap(0, default_metadata_log_cluster, default_cluster_range, 1, 0).get();

    return std::pair{std::move(dev_impl), std::move(log)};
}

auto& get_current_metadata_buffer() {
    auto& created_buffers = mock_metadata_to_disk_buffer::virtually_constructed_buffers;
    BOOST_REQUIRE(created_buffers.size() > 0);
    return created_buffers.back();
}

auto& get_current_cluster_writer() {
    auto& created_writers = mock_cluster_writer::virtually_constructed_writers;
    BOOST_REQUIRE(created_writers.size() > 0);
    return created_writers.back();
}

inode_t create_and_open_file(metadata_log& log, std::string name = "tmp") {
    return log.create_file("/" + name, file_permissions::default_dir_permissions).get0();
}

std::default_random_engine random_engine(0);

temporary_buffer<uint8_t> gen_buffer(size_t len, unit_size_t alignment, bool aligned) {
    BOOST_WARN(alignment >= 4096);
    BOOST_WARN(is_power_of_2(alignment));
    BOOST_WARN(not aligned or len % alignment == 0);

    temporary_buffer<uint8_t> buff;
    if (not aligned) {
        // Make buff unaligned
        buff = temporary_buffer<uint8_t>::aligned(alignment, round_up_to_multiple_of_power_of_2(len, alignment) + alignment);
        size_t offset = std::uniform_int_distribution<>(1, alignment - 1)(random_engine);
        buff.trim_front(offset);
        buff.trim(len);
    } else {
        buff = temporary_buffer<uint8_t>::aligned(alignment, len);
    }

    for (auto [i, char_dis] = std::tuple((size_t)0, std::uniform_int_distribution<>(0, 256)); i < buff.size(); i++) {
        buff.get_write()[i] = char_dis(random_engine);
    }

    return buff;
}

uint64_t get_current_time_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

// TODO: send patch to fix temporary_buffer == operator
template<typename C>
bool operator==(const temporary_buffer<C>& a, const temporary_buffer<C>& b) {
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

} // namespace

SEASTAR_THREAD_TEST_CASE(ondisk_data_small_write_test) {
    for (size_t len : std::vector<size_t>{10, write_operation::SMALL_WRITE_THRESHOLD, 1}) {
        BOOST_WARN(len <= write_operation::SMALL_WRITE_THRESHOLD);

        BOOST_TEST_MESSAGE("write len: " << len);

        auto [dev, log] = init_structs();
        inode_t inode = create_and_open_file(log);
        auto meta_buff = get_current_metadata_buffer();
        meta_buff->actions.clear();

        temporary_buffer<uint8_t> buff = gen_buffer(len, default_alignment, false);
        constexpr size_t file_offset = 1236;

        uint64_t mtime_ns_start = get_current_time_ns();
        size_t wrote = log.write(inode, file_offset, buff.get(), buff.size()).get0();
        uint64_t mtime_ns_end = get_current_time_ns();

        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        BOOST_REQUIRE_EQUAL(wrote, buff.size());

        // Check metadata to disk buffer entries
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 1);
        BOOST_REQUIRE(meta_buff->is_append_type<ondisk_small_write>(0));
        auto& small_write_action = meta_buff->get_by_append_type<ondisk_small_write>(0);
        BOOST_REQUIRE(small_write_action.data == buff);
        BOOST_REQUIRE_EQUAL(copy_value(small_write_action.header.inode), inode);
        BOOST_REQUIRE_EQUAL(copy_value(small_write_action.header.offset), file_offset);
        BOOST_REQUIRE_EQUAL(copy_value(small_write_action.header.length), len);
        BOOST_REQUIRE_GE(copy_value(small_write_action.header.mtime_ns), mtime_ns_start);
        BOOST_REQUIRE_LE(copy_value(small_write_action.header.mtime_ns), mtime_ns_end);
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_medium_write_test) {
    for (size_t len : std::vector<size_t>{
            round_up_to_multiple_of_power_of_2(write_operation::SMALL_WRITE_THRESHOLD + 1, default_alignment),
            round_down_to_multiple_of_power_of_2(default_cluster_size - 1, default_alignment),
            round_up_to_multiple_of_power_of_2(default_cluster_size / 3 + 10, default_alignment)}) {
        BOOST_WARN(len % default_alignment == 0);
        BOOST_WARN(len > write_operation::SMALL_WRITE_THRESHOLD);
        BOOST_WARN(len < default_cluster_size);

        BOOST_TEST_MESSAGE("write len: " << len);

        auto [dev, log] = init_structs();
        inode_t inode = create_and_open_file(log);
        auto meta_buff = get_current_metadata_buffer();
        auto clst_writer = get_current_cluster_writer();
        meta_buff->actions.clear();

        temporary_buffer<uint8_t> buff = gen_buffer(len, default_alignment, true);
        constexpr size_t file_offset = 1337;

        uint64_t mtime_ns_start = get_current_time_ns();
        size_t wrote = log.write(inode, file_offset, buff.get(), buff.size()).get0();
        uint64_t mtime_ns_end = get_current_time_ns();

        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        BOOST_REQUIRE_EQUAL(wrote, buff.size());

        // Check written data
        BOOST_REQUIRE_EQUAL(clst_writer->writes.size(), 1);
        auto& write_ops = clst_writer->writes;
        BOOST_REQUIRE(write_ops[0].data == buff);

        // Check metadata to disk buffer entries
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 1);
        BOOST_REQUIRE(meta_buff->is_append_type<ondisk_medium_write>(0));
        auto& medium_write_action = meta_buff->get_by_append_type<ondisk_medium_write>(0);
        BOOST_REQUIRE_EQUAL(copy_value(medium_write_action.inode), inode);
        BOOST_REQUIRE_EQUAL(copy_value(medium_write_action.offset), file_offset);
        BOOST_REQUIRE_EQUAL(copy_value(medium_write_action.length), len);
        BOOST_REQUIRE_EQUAL(copy_value(medium_write_action.disk_offset), write_ops[0].disk_offset);
        BOOST_REQUIRE_GE(copy_value(medium_write_action.mtime_ns), mtime_ns_start);
        BOOST_REQUIRE_LE(copy_value(medium_write_action.mtime_ns), mtime_ns_end);
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_split_medium_write_with_small_write_test) {
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_split_medium_write_with_medium_write_test) {
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_large_write_test) {
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_unaligned_beggining_write_test) {
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_unaligned_ending_write_test) {
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_write_splitting_test) {
}

SEASTAR_THREAD_TEST_CASE(from_memory_read) {
    // small writes
    // holes
    // holes + small writes
}

SEASTAR_THREAD_TEST_CASE(from_disk_read) {
}

SEASTAR_THREAD_TEST_CASE(mixed_read) {
}

SEASTAR_THREAD_TEST_CASE(unaligned_mixed_read) {
}

SEASTAR_THREAD_TEST_CASE(write_no_space_test) {
}

SEASTAR_THREAD_TEST_CASE(read_overwritten_test) {
}
