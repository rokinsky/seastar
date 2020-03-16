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


#include "fs/bitwise.hh"
#include "fs/cluster.hh"
#include "fs/metadata_log_operations/write.hh"
#include "fs/units.hh"
#include "fs_metadata_common.hh"
#include "fs_mock_block_device.hh"
#include "fs_mock_cluster_writer.hh"
#include "fs_mock_metadata_to_disk_buffer.hh"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/units.hh>
#include <seastar/testing/thread_test_case.hh>

#include <assert.h>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

using namespace seastar;
using namespace seastar::fs;

namespace {

constexpr unit_size_t default_cluster_size = 1 * MB;
constexpr unit_size_t default_alignment = 4096;
constexpr cluster_range default_cluster_range = {1, 10};
constexpr cluster_id_t default_metadata_log_cluster = 1;
constexpr medium_write_len_t min_medium_write_len =
        round_up_to_multiple_of_power_of_2(SMALL_WRITE_THRESHOLD + 1, default_alignment);

enum class write_type {
    SMALL,
    MEDIUM,
    LARGE
};

constexpr write_type get_write_type(size_t len) noexcept {
    return len <= SMALL_WRITE_THRESHOLD ? write_type::SMALL :
            (len >= default_cluster_size ? write_type::LARGE : write_type::MEDIUM);
}

auto default_init_metadata_log() {
    return init_metadata_log(default_cluster_size, default_alignment, default_metadata_log_cluster, default_cluster_range);
}

auto default_gen_buffer(size_t len, bool aligned) {
    return gen_buffer(len, aligned, default_alignment);
}

auto default_aligned_write(metadata_log& log, inode_t inode, size_t bytes_num) {
    return aligned_write(log, inode, bytes_num, default_alignment);
}

} // namespace

SEASTAR_THREAD_TEST_CASE(ondisk_data_small_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 7312;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    for (auto write_len : std::vector<small_write_len_t> {
            1,
            10,
            SMALL_WRITE_THRESHOLD}) {
        assert(get_write_type(write_len) == write_type::SMALL);

        auto [blockdev, log] = default_init_metadata_log();
        inode_t inode = create_and_open_file(log);

        BOOST_TEST_MESSAGE("write_len: " << write_len);
        temporary_buffer<uint8_t> buff = default_gen_buffer(write_len, false);
        BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());

        auto meta_buff = get_current_metadata_buffer();
        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        // Check metadata
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 2);
        ondisk_small_write expected_entry {
            ondisk_small_write_header {
                inode,
                write_offset,
                write_len,
                mtime_ns_start
            },
            buff.share()
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[1], expected_entry));

        BOOST_TEST_MESSAGE("");
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_medium_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 7331;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    for (auto write_len : std::vector<medium_write_len_t> {
            min_medium_write_len,
            default_cluster_size - default_alignment,
            round_up_to_multiple_of_power_of_2(default_cluster_size / 3 + 10, default_alignment)}) {
        assert(write_len % default_alignment == 0);
        assert(get_write_type(write_len) == write_type::MEDIUM);

        auto [blockdev, log] = default_init_metadata_log();
        inode_t inode = create_and_open_file(log);

        BOOST_TEST_MESSAGE("write_len: " << write_len);
        temporary_buffer<uint8_t> buff = default_gen_buffer(write_len, true);
        BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());

        auto meta_buff = get_current_metadata_buffer();
        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        // Check data
        auto clst_writer = get_current_cluster_writer();
        BOOST_REQUIRE_EQUAL(clst_writer->writes.size(), 1);
        auto& clst_writer_ops = clst_writer->writes;
        BOOST_REQUIRE_EQUAL(clst_writer_ops[0].data, buff);

        // Check metadata
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 2);
        ondisk_medium_write expected_entry {
            inode,
            write_offset,
            clst_writer_ops[0].disk_offset,
            write_len,
            mtime_ns_start
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[1], expected_entry));

        BOOST_TEST_MESSAGE("");
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_second_medium_write_without_new_data_cluster_allocation_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 1337;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    constexpr medium_write_len_t second_write_len =
            round_up_to_multiple_of_power_of_2(2 * SMALL_WRITE_THRESHOLD + 1, default_alignment);
    static_assert(get_write_type(second_write_len) == write_type::MEDIUM);
    for (auto first_write_len : std::vector<medium_write_len_t> {
            default_cluster_size - second_write_len - default_alignment,
            default_cluster_size - second_write_len}) {
        assert(first_write_len % default_alignment == 0);
        assert(get_write_type(first_write_len) == write_type::MEDIUM);
        medium_write_len_t remaining_space_in_cluster = default_cluster_size - first_write_len;
        assert(remaining_space_in_cluster >= SMALL_WRITE_THRESHOLD);
        assert(remaining_space_in_cluster >= second_write_len);

        auto [blockdev, log] = default_init_metadata_log();
        inode_t inode = create_and_open_file(log);

        // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
        BOOST_TEST_MESSAGE("first write len: " << first_write_len);
        CHECK_CALL(default_aligned_write(log, inode, first_write_len));

        BOOST_TEST_MESSAGE("second write len: " << second_write_len);
        size_t nb_of_cluster_writers_before = mock_cluster_writer::virtually_constructed_writers.size();
        temporary_buffer<uint8_t> buff = default_gen_buffer(second_write_len, true);
        BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());
        size_t nb_of_cluster_writers_after = mock_cluster_writer::virtually_constructed_writers.size();

        auto meta_buff = get_current_metadata_buffer();
        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        // Check cluster writer data
        BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before, nb_of_cluster_writers_after);
        auto clst_writer = get_current_cluster_writer();
        auto& clst_writer_ops = clst_writer->writes;
        BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 2);
        BOOST_REQUIRE_EQUAL(clst_writer_ops[1].data, buff);
        BOOST_REQUIRE_EQUAL(clst_writer_ops[1].disk_offset, clst_writer_ops[0].disk_offset + first_write_len);

        // Check metadata to disk buffer entries
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 3);
        ondisk_medium_write expected_entry {
            inode,
            write_offset,
            clst_writer_ops[1].disk_offset,
            second_write_len,
            mtime_ns_start
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry));

        BOOST_TEST_MESSAGE("");
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_second_medium_write_with_new_data_cluster_allocation_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 1337;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    constexpr medium_write_len_t second_write_len = min_medium_write_len;
    static_assert(get_write_type(second_write_len) == write_type::MEDIUM);
    for (auto first_write_len : std::vector<medium_write_len_t> {
            default_cluster_size - default_alignment,
            default_cluster_size - round_down_to_multiple_of_power_of_2(SMALL_WRITE_THRESHOLD, default_alignment)}) {
        assert(first_write_len % default_alignment == 0);
        assert(get_write_type(first_write_len) == write_type::MEDIUM);
        medium_write_len_t remaining_space_in_cluster = default_cluster_size - first_write_len;
        assert(remaining_space_in_cluster <= SMALL_WRITE_THRESHOLD);

        auto [blockdev, log] = default_init_metadata_log();
        inode_t inode = create_and_open_file(log);

        // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
        BOOST_TEST_MESSAGE("first write len: " << first_write_len);
        CHECK_CALL(default_aligned_write(log, inode, first_write_len));

        BOOST_TEST_MESSAGE("second write len: " << second_write_len);
        size_t nb_of_cluster_writers_before = mock_cluster_writer::virtually_constructed_writers.size();
        temporary_buffer<uint8_t> buff = default_gen_buffer(second_write_len, true);
        BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());
        size_t nb_of_cluster_writers_after = mock_cluster_writer::virtually_constructed_writers.size();

        auto meta_buff = get_current_metadata_buffer();
        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        // Check data
        BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before + 1, nb_of_cluster_writers_after);
        auto clst_writer = get_current_cluster_writer();
        auto& clst_writer_ops = clst_writer->writes;
        BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 1);
        BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data, buff);

        // Check metadata
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 3);
        ondisk_medium_write expected_entry {
            inode,
            write_offset,
            clst_writer_ops[0].disk_offset,
            second_write_len,
            mtime_ns_start
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry));

        BOOST_TEST_MESSAGE("");
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_split_medium_write_with_small_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 1337;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    for (auto [first_write_len, second_write_len] : std::vector<std::pair<medium_write_len_t, medium_write_len_t>> {
            {
                default_cluster_size - min_medium_write_len,
                min_medium_write_len + SMALL_WRITE_THRESHOLD
            },
            {
                default_cluster_size - 3 * min_medium_write_len,
                3 * min_medium_write_len + 1
            }}) {
        assert(first_write_len % default_alignment == 0);
        assert(get_write_type(first_write_len) == write_type::MEDIUM);
        assert(get_write_type(second_write_len) == write_type::MEDIUM);
        medium_write_len_t remaining_space_in_cluster = default_cluster_size - first_write_len;
        assert(remaining_space_in_cluster > SMALL_WRITE_THRESHOLD);
        assert(remaining_space_in_cluster < second_write_len);
        assert(get_write_type(second_write_len - remaining_space_in_cluster) == write_type::SMALL);

        auto [blockdev, log] = default_init_metadata_log();
        inode_t inode = create_and_open_file(log);

        // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
        BOOST_TEST_MESSAGE("first write len: " << first_write_len);
        CHECK_CALL(default_aligned_write(log, inode, first_write_len));

        BOOST_TEST_MESSAGE("second write len: " << second_write_len);
        size_t nb_of_cluster_writers_before = mock_cluster_writer::virtually_constructed_writers.size();
        temporary_buffer<uint8_t> buff = default_gen_buffer(second_write_len, true);
        BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());
        size_t nb_of_cluster_writers_after = mock_cluster_writer::virtually_constructed_writers.size();

        auto meta_buff = get_current_metadata_buffer();
        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        // Check data
        BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before, nb_of_cluster_writers_after);
        auto clst_writer = get_current_cluster_writer();
        auto& clst_writer_ops = clst_writer->writes;
        BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 2);
        BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data, buff.share(0, remaining_space_in_cluster));

        // Check metadata
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 4);
        ondisk_medium_write expected_entry1 {
            inode,
            write_offset,
            clst_writer_ops[1].disk_offset,
            remaining_space_in_cluster,
            mtime_ns_start
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry1));
        ondisk_small_write expected_entry2 {
            ondisk_small_write_header {
                inode,
                write_offset + remaining_space_in_cluster,
                static_cast<small_write_len_t>(second_write_len - remaining_space_in_cluster),
                mtime_ns_start
            },
            buff.share(remaining_space_in_cluster, second_write_len - remaining_space_in_cluster)
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[3], expected_entry2));

        BOOST_TEST_MESSAGE("");
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_split_medium_write_with_medium_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 1337;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    for (auto [first_write_len, second_write_len] : std::vector<std::pair<medium_write_len_t, medium_write_len_t>> {
            {
                default_cluster_size - min_medium_write_len,
                2 * min_medium_write_len
            },
            {
                default_cluster_size - min_medium_write_len,
                default_cluster_size - default_alignment
            }}) {
        assert(first_write_len % default_alignment == 0);
        assert(get_write_type(first_write_len) == write_type::MEDIUM);
        assert(get_write_type(second_write_len) == write_type::MEDIUM);
        medium_write_len_t remaining_space_in_cluster = default_cluster_size - first_write_len;
        assert(remaining_space_in_cluster > SMALL_WRITE_THRESHOLD);
        assert(remaining_space_in_cluster < second_write_len);
        assert(get_write_type(second_write_len - remaining_space_in_cluster) == write_type::MEDIUM);

        auto [blockdev, log] = default_init_metadata_log();
        inode_t inode = create_and_open_file(log);

        // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
        BOOST_TEST_MESSAGE("first write len: " << first_write_len);
        CHECK_CALL(default_aligned_write(log, inode, first_write_len));

        BOOST_TEST_MESSAGE("second write len: " << second_write_len);
        size_t nb_of_cluster_writers_before = mock_cluster_writer::virtually_constructed_writers.size();
        temporary_buffer<uint8_t> buff = default_gen_buffer(second_write_len, true);
        BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());
        size_t nb_of_cluster_writers_after = mock_cluster_writer::virtually_constructed_writers.size();

        auto meta_buff = get_current_metadata_buffer();
        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        // Check data
        BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before + 1, nb_of_cluster_writers_after);
        auto clst_writer = get_current_cluster_writer();
        auto& prev_clst_writer =
                mock_cluster_writer::virtually_constructed_writers[mock_cluster_writer::virtually_constructed_writers.size() - 2];
        auto& clst_writer_ops = clst_writer->writes;
        auto& prev_clst_writer_ops = prev_clst_writer->writes;
        BOOST_REQUIRE_EQUAL(prev_clst_writer_ops.size(), 2);
        BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 1);
        BOOST_REQUIRE_EQUAL(prev_clst_writer_ops.back().data, buff.share(0, remaining_space_in_cluster));
        BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data,
                buff.share(remaining_space_in_cluster, second_write_len - remaining_space_in_cluster));

        // Check metadata
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 4);
        ondisk_medium_write expected_entry1 {
            inode,
            write_offset,
            prev_clst_writer_ops[1].disk_offset,
            remaining_space_in_cluster,
            mtime_ns_start
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry1));
        ondisk_medium_write expected_entry2 {
            inode,
            write_offset + remaining_space_in_cluster,
            clst_writer_ops[0].disk_offset,
            second_write_len - remaining_space_in_cluster,
            mtime_ns_start
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[3], expected_entry2));

        BOOST_TEST_MESSAGE("");
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_large_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 7331;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    constexpr uint64_t write_len = default_cluster_size * 2;
    // TODO: asserts

    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);

    BOOST_TEST_MESSAGE("write_len: " << write_len);
    temporary_buffer<uint8_t> buff = default_gen_buffer(write_len, true);
    BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());

    auto meta_buff = get_current_metadata_buffer();
    BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

    // Check data
    auto& blockdev_ops = blockdev->writes;
    BOOST_REQUIRE_EQUAL(blockdev_ops.size(), 2);
    BOOST_REQUIRE_EQUAL(blockdev_ops[0].disk_offset % default_cluster_size, 0);
    BOOST_REQUIRE_EQUAL(blockdev_ops[1].disk_offset % default_cluster_size, 0);
    cluster_id_t part1_cluster_id = blockdev_ops[0].disk_offset / default_cluster_size;
    cluster_id_t part2_cluster_id = blockdev_ops[1].disk_offset / default_cluster_size;
    BOOST_REQUIRE_EQUAL(blockdev_ops[0].data, buff.share(0, default_cluster_size));
    BOOST_REQUIRE_EQUAL(blockdev_ops[1].data, buff.share(default_cluster_size, default_cluster_size));

    // Check metadata
    BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 3);
    ondisk_large_write expected_entry1 {
        inode,
        write_offset,
        part1_cluster_id,
        mtime_ns_start
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[1], expected_entry1));
    ondisk_large_write_without_mtime expected_entry2 {
        inode,
        write_offset + default_cluster_size,
        part2_cluster_id
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry2));

    BOOST_TEST_MESSAGE("");
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_unaligned_write_split_into_two_small_writes_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 7331;
    const unix_time_t mtime_ns_start = get_current_time_ns();

    // medium write split into two small writes
    constexpr medium_write_len_t write_len = SMALL_WRITE_THRESHOLD + 1;
    // TODO: asserts

    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);
    BOOST_TEST_MESSAGE("write_len: " << write_len);
    temporary_buffer<uint8_t> buff = default_gen_buffer(write_len, false);
    BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());

    auto meta_buff = get_current_metadata_buffer();
    BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

    size_t misalignment = reinterpret_cast<size_t>(buff.get()) % default_alignment;
    small_write_len_t part1_write_len = default_alignment - misalignment;
    small_write_len_t part2_write_len = write_len - part1_write_len;

    // Check metadata
    BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 3);
    ondisk_small_write expected_entry1 {
        ondisk_small_write_header {
            inode,
            write_offset,
            part1_write_len,
            mtime_ns_start
        },
        buff.share(0, part1_write_len)
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[1], expected_entry1));
    ondisk_small_write expected_entry2 {
        ondisk_small_write_header {
            inode,
            write_offset + part1_write_len,
            part2_write_len,
            mtime_ns_start
        },
        buff.share(part1_write_len, part2_write_len)
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry2));

    BOOST_TEST_MESSAGE("");
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_unaligned_write_split_into_small_medium_and_small_writes_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 7331;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    for (auto write_len : std::vector<unit_size_t> {
            default_cluster_size - default_alignment,
            default_cluster_size}) {
        // TODO: asserts

        auto [blockdev, log] = default_init_metadata_log();
        inode_t inode = create_and_open_file(log);
        BOOST_TEST_MESSAGE("write_len: " << write_len);
        temporary_buffer<uint8_t> buff = default_gen_buffer(write_len, false);
        BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());

        auto meta_buff = get_current_metadata_buffer();
        BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

        size_t misalignment = reinterpret_cast<size_t>(buff.get()) % default_alignment;
        small_write_len_t part1_write_len = default_alignment - misalignment;
        medium_write_len_t part2_write_len = round_down_to_multiple_of_power_of_2(write_len - part1_write_len, default_alignment);
        small_write_len_t part3_write_len = write_len - part1_write_len - part2_write_len;

        // Check data
        auto clst_writer = get_current_cluster_writer();
        auto& clst_writer_ops = clst_writer->writes;
        BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 1);
        BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data, buff.share(part1_write_len, part2_write_len));

        // Check metadata
        BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 4);
        ondisk_small_write expected_entry1 {
            ondisk_small_write_header {
                inode,
                write_offset,
                part1_write_len,
                mtime_ns_start
            },
            buff.share(0, part1_write_len)
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[1], expected_entry1));
        ondisk_medium_write expected_entry2 {
            inode,
            write_offset + part1_write_len,
            clst_writer_ops.back().disk_offset,
            part2_write_len,
            mtime_ns_start
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry2));
        ondisk_small_write expected_entry3 {
            ondisk_small_write_header {
                inode,
                write_offset + part1_write_len + part2_write_len,
                part3_write_len,
                mtime_ns_start
            },
            buff.share(part1_write_len + part2_write_len, part3_write_len)
        };
        CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[3], expected_entry3));

        BOOST_TEST_MESSAGE("");
    }
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_unaligned_write_split_into_small_large_and_small_writes_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 7331;
    const unix_time_t mtime_ns_start = get_current_time_ns();
    constexpr uint64_t write_len = default_cluster_size + 2 * default_alignment;
    // TODO: asserts

    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);
    BOOST_TEST_MESSAGE("write_len: " << write_len);
    temporary_buffer<uint8_t> buff = default_gen_buffer(write_len, false);
    BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());

    auto meta_buff = get_current_metadata_buffer();
    BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

    size_t misalignment = reinterpret_cast<size_t>(buff.get()) % default_alignment;
    small_write_len_t part1_write_len = default_alignment - misalignment;
    small_write_len_t part3_write_len = write_len - part1_write_len - default_cluster_size;

    // Check data
    auto& blockdev_ops = blockdev->writes;
    BOOST_REQUIRE_EQUAL(blockdev_ops.size(), 1);
    BOOST_REQUIRE_EQUAL(blockdev_ops.back().disk_offset % default_cluster_size, 0);
    cluster_id_t part2_cluster_id = blockdev_ops.back().disk_offset / default_cluster_size;
    BOOST_REQUIRE_EQUAL(blockdev_ops.back().data, buff.share(part1_write_len, default_cluster_size));

    // Check metadata
    BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 4);
    ondisk_small_write expected_entry1 {
        ondisk_small_write_header {
            inode,
            write_offset,
            part1_write_len,
            mtime_ns_start
        },
        buff.share(0, part1_write_len)
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[1], expected_entry1));
    ondisk_large_write_without_mtime expected_entry2 {
        inode,
        write_offset + part1_write_len,
        part2_cluster_id
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[2], expected_entry2));
    ondisk_small_write expected_entry3 {
        ondisk_small_write_header {
            inode,
            write_offset + part1_write_len + default_cluster_size,
            part3_write_len,
            mtime_ns_start
        },
        buff.share(part1_write_len + default_cluster_size, part3_write_len)
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[3], expected_entry3));

    BOOST_TEST_MESSAGE("");
}

SEASTAR_THREAD_TEST_CASE(ondisk_data_big_single_write_splitting_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 7331;
    constexpr uint64_t write_len = default_cluster_size * 3 + min_medium_write_len + default_alignment + 10;

    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);
    BOOST_TEST_MESSAGE("write_len: " << write_len);
    temporary_buffer<uint8_t> buff = default_gen_buffer(write_len, true);
    BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());

    auto meta_buff = get_current_metadata_buffer();
    BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

    auto& meta_actions = meta_buff->actions;
    BOOST_REQUIRE_EQUAL(meta_actions.size(), 6);
    BOOST_REQUIRE(is_append_type<ondisk_large_write>(meta_actions[1]));
    BOOST_REQUIRE(is_append_type<ondisk_large_write_without_mtime>(meta_actions[2]));
    BOOST_REQUIRE(is_append_type<ondisk_large_write_without_mtime>(meta_actions[3]));
    BOOST_REQUIRE(is_append_type<ondisk_medium_write>(meta_actions[4]));
    BOOST_REQUIRE(is_append_type<ondisk_small_write>(meta_actions[5]));
}
