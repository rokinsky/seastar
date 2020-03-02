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

#include <seastar/core/print.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/units.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>
#include <cstdint>

using namespace seastar;
using namespace seastar::fs;

namespace {

using append = mock_metadata_to_disk_buffer::action::append;
using flush_to_disk = mock_metadata_to_disk_buffer::action::flush_to_disk;
constexpr mock_metadata_to_disk_buffer::append_result APPENDED = mock_metadata_to_disk_buffer::APPENDED;
constexpr mock_metadata_to_disk_buffer::append_result TOO_BIG = mock_metadata_to_disk_buffer::TOO_BIG;

constexpr size_t default_buff_size = 1 * MB;
constexpr size_t default_alignment = 4 * KB;
constexpr size_t checkpoint_size = sizeof(ondisk_type) + sizeof(ondisk_checkpoint);

mock_metadata_to_disk_buffer create_default_mock_buffer() {
    mock_metadata_to_disk_buffer buf(default_buff_size, default_alignment);
    buf.init(0);
    return buf;
}

// Returns copy of given value. Needed to solve misalignment iiss
template<typename T>
T copy_value(T x) {
    return x;
}

}

// The following test checks if exceeding append is properly handled
SEASTAR_THREAD_TEST_CASE(size_exceeded_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();
    constexpr size_t delete_inode_size = sizeof(ondisk_type) + sizeof(ondisk_delete_inode);
    constexpr size_t next_metadata_cluster_size = sizeof(ondisk_type) + sizeof(ondisk_next_metadata_cluster);

    size_t remaining_space = default_buff_size - checkpoint_size - next_metadata_cluster_size;
    for (;;) {
        if (delete_inode_size > remaining_space) {
            BOOST_REQUIRE_EQUAL(buf.append(ondisk_delete_inode {1}), TOO_BIG);
            break;
        } else {
            BOOST_REQUIRE_EQUAL(buf.append(ondisk_delete_inode {1}), APPENDED);
        }
        remaining_space -= delete_inode_size;
    }
}

// The following test checks if multiple actions data are correctly added to actions vector
SEASTAR_THREAD_TEST_CASE(actions_index_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);

    BOOST_REQUIRE_EQUAL(buf.append(ondisk_next_metadata_cluster {1}), APPENDED);
    buf.flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_create_inode {4, 1, {5, 2, 6, 8, 4}}), APPENDED);
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_update_metadata {4, {5, 2, 6, 8, 4}}), APPENDED);
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_delete_inode {1}), APPENDED);
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_medium_write {1, 8, 4, 6, 9}), APPENDED);
    buf.flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_large_write {6, 8, 2, 5}), APPENDED);
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_large_write_without_mtime {6, 8, 1}), APPENDED);
    buf.flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_truncate {64, 28, 62}), APPENDED);
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_mtime_update {4, 26}), APPENDED);
    buf.flush_to_disk(dev).get();

    BOOST_REQUIRE(buf.is_append_type<ondisk_next_metadata_cluster>(0));
    BOOST_REQUIRE(buf.is_type<flush_to_disk>(1));
    BOOST_REQUIRE(buf.is_append_type<ondisk_create_inode>(2));
    BOOST_REQUIRE(buf.is_append_type<ondisk_update_metadata>(3));
    BOOST_REQUIRE(buf.is_append_type<ondisk_delete_inode>(4));
    BOOST_REQUIRE(buf.is_append_type<ondisk_medium_write>(5));
    BOOST_REQUIRE(buf.is_type<flush_to_disk>(6));
    BOOST_REQUIRE(buf.is_append_type<ondisk_large_write>(7));
    BOOST_REQUIRE(buf.is_append_type<ondisk_large_write_without_mtime>(8));
    BOOST_REQUIRE(buf.is_type<flush_to_disk>(9));
    BOOST_REQUIRE(buf.is_append_type<ondisk_truncate>(10));
    BOOST_REQUIRE(buf.is_append_type<ondisk_mtime_update>(11));
    BOOST_REQUIRE(buf.is_type<flush_to_disk>(12));
}

// The folowing test checks that constructed buffers are distinct and correctly added to
// virtually_constructed_buffers vector
SEASTAR_THREAD_TEST_CASE(virtual_constructor_test) {
    auto& created_buffers = mock_metadata_to_disk_buffer::virtually_constructed_buffers;

    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();
    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size);
    BOOST_REQUIRE_EQUAL(buf.append(ondisk_delete_inode {1}), APPENDED);

    auto buf2 = buf.virtual_constructor(default_buff_size, default_alignment);
    buf2->init(default_alignment);

    BOOST_REQUIRE_EQUAL(created_buffers.size(), 1);
    BOOST_REQUIRE(created_buffers[0] == buf2);
    BOOST_REQUIRE_EQUAL(created_buffers[0]->actions.size(), 0);
    buf2->append(ondisk_delete_inode {1});
    BOOST_REQUIRE_EQUAL(created_buffers[0]->actions.size(), 1);

    auto buf3 = buf2->virtual_constructor(default_buff_size, default_alignment);
    buf3->init(default_alignment * 2);

    BOOST_REQUIRE_EQUAL(created_buffers.size(), 2);
    BOOST_REQUIRE(created_buffers[1] == buf3);
    BOOST_REQUIRE_EQUAL(created_buffers[1]->actions.size(), 0);
}

// Tests below check that actions add correct info to actions vector

SEASTAR_THREAD_TEST_CASE(flush_action_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);

    buf.flush_to_disk(dev).get();

    BOOST_REQUIRE(buf.is_type<flush_to_disk>(0));
}

SEASTAR_THREAD_TEST_CASE(next_metadata_cluster_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_next_metadata_cluster next_metadata_cluster_op {6};
    BOOST_REQUIRE_EQUAL(buf.append(next_metadata_cluster_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(next_metadata_cluster_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_next_metadata_cluster>(0));
    auto elem = buf.get_by_append_type<ondisk_next_metadata_cluster>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.cluster_id), copy_value(next_metadata_cluster_op.cluster_id));
}

SEASTAR_THREAD_TEST_CASE(create_inode_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_create_inode create_inode_op {42, 1, {5, 2, 6, 8, 4}};
    BOOST_REQUIRE_EQUAL(buf.append(create_inode_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(create_inode_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_create_inode>(0));
    auto elem = buf.get_by_append_type<ondisk_create_inode>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(create_inode_op.inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem.is_directory), copy_value(create_inode_op.is_directory));
    BOOST_REQUIRE(std::memcmp(&elem.metadata, &create_inode_op.metadata, sizeof(elem.metadata)) == 0);
}

SEASTAR_THREAD_TEST_CASE(update_metadata_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_update_metadata update_metadata_op {42, {5, 2, 6, 8, 4}};
    BOOST_REQUIRE_EQUAL(buf.append(update_metadata_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(update_metadata_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_update_metadata>(0));
    auto elem = buf.get_by_append_type<ondisk_update_metadata>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(update_metadata_op.inode));
    BOOST_REQUIRE(std::memcmp(&elem.metadata, &update_metadata_op.metadata, sizeof(elem.metadata)) == 0);
}

SEASTAR_THREAD_TEST_CASE(delete_inode_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_delete_inode delete_inode_op {1};
    BOOST_REQUIRE_EQUAL(buf.append(delete_inode_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(delete_inode_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_delete_inode>(0));
    auto elem = buf.get_by_append_type<ondisk_delete_inode>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(delete_inode_op.inode));
}

SEASTAR_THREAD_TEST_CASE(small_write_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    std::string small_write_str = "12345";
    ondisk_small_write_header small_write_op {2, 7, static_cast<uint16_t>(small_write_str.size()), 17};
    BOOST_REQUIRE_EQUAL(buf.append(small_write_op, small_write_str.data()), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(small_write_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_small_write>(0));
    auto& elem = buf.get_by_append_type<ondisk_small_write>(0);
    auto elem_header = elem.header;
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.inode), copy_value(small_write_op.inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.offset), copy_value(small_write_op.offset));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.length), copy_value(small_write_op.length));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.mtime_ns), copy_value(small_write_op.mtime_ns));
    BOOST_REQUIRE_EQUAL(std::string(elem.data.get(), elem.data.end()), small_write_str);
}

SEASTAR_THREAD_TEST_CASE(medium_write_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_medium_write medium_write_op {1, 8, 4, 6, 9};
    BOOST_REQUIRE_EQUAL(buf.append(medium_write_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(medium_write_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_medium_write>(0));
    auto elem = buf.get_by_append_type<ondisk_medium_write>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(medium_write_op.inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem.offset), copy_value(medium_write_op.offset));
    BOOST_REQUIRE_EQUAL(copy_value(elem.disk_offset), copy_value(medium_write_op.disk_offset));
    BOOST_REQUIRE_EQUAL(copy_value(elem.length), copy_value(medium_write_op.length));
    BOOST_REQUIRE_EQUAL(copy_value(elem.mtime_ns), copy_value(medium_write_op.mtime_ns));
}

SEASTAR_THREAD_TEST_CASE(large_write_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_large_write large_write_op {6, 8, 2, 5};
    BOOST_REQUIRE_EQUAL(buf.append(large_write_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(large_write_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_large_write>(0));
    auto elem = buf.get_by_append_type<ondisk_large_write>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(large_write_op.inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem.offset), copy_value(large_write_op.offset));
    BOOST_REQUIRE_EQUAL(copy_value(elem.data_cluster), copy_value(large_write_op.data_cluster));
    BOOST_REQUIRE_EQUAL(copy_value(elem.mtime_ns), copy_value(large_write_op.mtime_ns));
}

SEASTAR_THREAD_TEST_CASE(large_write_without_mtime_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_large_write_without_mtime large_write_op {256, 88, 11};
    BOOST_REQUIRE_EQUAL(buf.append(large_write_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(large_write_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_large_write_without_mtime>(0));
    auto elem = buf.get_by_append_type<ondisk_large_write_without_mtime>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(large_write_op.inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem.offset), copy_value(large_write_op.offset));
    BOOST_REQUIRE_EQUAL(copy_value(elem.data_cluster), copy_value(large_write_op.data_cluster));
}

SEASTAR_THREAD_TEST_CASE(truncate_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_truncate truncate_op {64, 28, 62};
    BOOST_REQUIRE_EQUAL(buf.append(truncate_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(truncate_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_truncate>(0));
    auto elem = buf.get_by_append_type<ondisk_truncate>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(truncate_op.inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem.size), copy_value(truncate_op.size));
    BOOST_REQUIRE_EQUAL(copy_value(elem.mtime_ns), copy_value(truncate_op.mtime_ns));
}

SEASTAR_THREAD_TEST_CASE(mtime_update_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    ondisk_mtime_update mtime_update_op {4, 26};
    BOOST_REQUIRE_EQUAL(buf.append(mtime_update_op), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(mtime_update_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_mtime_update>(0));
    auto elem = buf.get_by_append_type<ondisk_mtime_update>(0);
    BOOST_REQUIRE_EQUAL(copy_value(elem.inode), copy_value(mtime_update_op.inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem.mtime_ns), copy_value(mtime_update_op.mtime_ns));
}

SEASTAR_THREAD_TEST_CASE(add_dir_entry_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    std::string add_dir_entry_str = "120345";
    ondisk_add_dir_entry_header add_dir_entry_op {2, 7, static_cast<uint16_t>(add_dir_entry_str.size())};
    BOOST_REQUIRE_EQUAL(buf.append(add_dir_entry_op, add_dir_entry_str.data()), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(add_dir_entry_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_add_dir_entry>(0));
    auto& elem = buf.get_by_append_type<ondisk_add_dir_entry>(0);
    auto elem_header = elem.header;
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.dir_inode), copy_value(add_dir_entry_op.dir_inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.entry_inode), copy_value(add_dir_entry_op.entry_inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.entry_name_length), copy_value(add_dir_entry_op.entry_name_length));
    BOOST_REQUIRE_EQUAL(std::string(elem.entry_name.get(), elem.entry_name.end()), add_dir_entry_str);
}

SEASTAR_THREAD_TEST_CASE(create_inode_as_dir_entry_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    std::string create_inode_str = "120345";
    ondisk_create_inode_as_dir_entry_header create_inode_op {
        {42, 1, {5, 2, 6, 8, 4}}, 7, static_cast<uint16_t>(create_inode_str.size())
    };
    BOOST_REQUIRE_EQUAL(buf.append(create_inode_op, create_inode_str.data()), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(create_inode_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_create_inode_as_dir_entry>(0));
    auto& elem = buf.get_by_append_type<ondisk_create_inode_as_dir_entry>(0);
    auto elem_header = elem.header;
    BOOST_REQUIRE(std::memcmp(&elem_header.entry_inode, &create_inode_op.entry_inode,
            sizeof(elem_header.entry_inode)) == 0);
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.dir_inode), copy_value(create_inode_op.dir_inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.entry_name_length), copy_value(create_inode_op.entry_name_length));
    BOOST_REQUIRE_EQUAL(std::string(elem.entry_name.get(), elem.entry_name.end()), create_inode_str);
}

SEASTAR_THREAD_TEST_CASE(delete_dir_entry_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    std::string delete_entry_str = "120345";
    ondisk_delete_dir_entry_header delete_entry_op {42, static_cast<uint16_t>(delete_entry_str.size())};
    BOOST_REQUIRE_EQUAL(buf.append(delete_entry_op, delete_entry_str.data()), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(delete_entry_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_delete_dir_entry>(0));
    auto& elem = buf.get_by_append_type<ondisk_delete_dir_entry>(0);
    auto elem_header = elem.header;
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.dir_inode), copy_value(delete_entry_op.dir_inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.entry_name_length), copy_value(delete_entry_op.entry_name_length));
    BOOST_REQUIRE_EQUAL(std::string(elem.entry_name.get(), elem.entry_name.end()), delete_entry_str);
}

SEASTAR_THREAD_TEST_CASE(rename_dir_entry_test) {
    mock_metadata_to_disk_buffer buf = create_default_mock_buffer();

    std::string old_name_str = "120345";
    std::string new_name_str = "541631";
    ondisk_rename_dir_entry_header rename_op {
        42,
        24,
        static_cast<uint16_t>(old_name_str.size()),
        static_cast<uint16_t>(new_name_str.size())
    };
    BOOST_REQUIRE_EQUAL(buf.append(rename_op, old_name_str.data(), new_name_str.data()), APPENDED);

    BOOST_REQUIRE_EQUAL(buf.bytes_left(), default_buff_size - checkpoint_size -
            ondisk_entry_size(rename_op));

    BOOST_REQUIRE(buf.is_append_type<ondisk_rename_dir_entry>(0));
    auto& elem = buf.get_by_append_type<ondisk_rename_dir_entry>(0);
    auto elem_header = elem.header;
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.dir_inode), copy_value(rename_op.dir_inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.new_dir_inode), copy_value(rename_op.new_dir_inode));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.entry_old_name_length), copy_value(rename_op.entry_old_name_length));
    BOOST_REQUIRE_EQUAL(copy_value(elem_header.entry_new_name_length), copy_value(rename_op.entry_new_name_length));
    BOOST_REQUIRE_EQUAL(std::string(elem.old_name.get(), elem.old_name.end()), old_name_str);
    BOOST_REQUIRE_EQUAL(std::string(elem.new_name.get(), elem.new_name.end()), new_name_str);
}
