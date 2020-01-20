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
#include "fs/to_disk_buffer.hh"
#include "fs/units.hh"
#include "mock_block_device.hh"

#include <cstring>
#include <seastar/core/units.hh>
#include <seastar/fs/block_device.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;
using namespace seastar::fs;

constexpr size_t alignment = 4*KB;
constexpr size_t max_siz = 32*MB; // reasonably larger than alignment

BOOST_TEST_DONT_PRINT_LOG_VALUE(to_disk_buffer)

SEASTAR_THREAD_TEST_CASE(initially_empty_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}

SEASTAR_THREAD_TEST_CASE(simple_unaligned_write_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    char buf[max_siz];
    disk_offset_t len_aligned;
    disk_buf.append_bytes("asdfasdf", 6);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-6);
    disk_buf.append_bytes("xyzxyzxyz", 9);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-15);
    disk_buf.flush_to_disk(dev, false).get();
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)15, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-15);
    dev.read<char>(0, buf, len_aligned).get();
    BOOST_REQUIRE_EQUAL(strncmp("asdfasxyzxyzxyz", buf, 15), 0);
}

SEASTAR_THREAD_TEST_CASE(simple_aligned_write_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    char buf[max_siz], expected_buf[max_siz];
    disk_offset_t len_aligned;
    disk_buf.append_bytes("asdfasdf", 6);
    disk_buf.append_bytes("xyzxyzxyz", 9);
    disk_buf.flush_to_disk(dev, true).get();
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)15, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned);
    dev.read<char>(0, buf, len_aligned).get();
    strncpy(expected_buf, "asdfasxyzxyzxyz", max_siz);
    BOOST_REQUIRE_EQUAL(strncmp(expected_buf, buf, len_aligned), 0);
}

SEASTAR_THREAD_TEST_CASE(unaligned_aligned_write_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    char buf[max_siz], expected_buf[max_siz];
    disk_offset_t len_aligned;
    disk_buf.append_bytes("asdfasdf", 6);
    disk_buf.append_bytes("xyzxyzxyz", 9);
    disk_buf.flush_to_disk(dev, false).get();
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)15, alignment);
    dev.read<char>(0, buf, len_aligned).get();
    BOOST_REQUIRE_EQUAL(strncmp("asdfasxyzxyzxyz", buf, 15), 0);

    disk_buf.append_bytes("1234", 4);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-19);
    disk_buf.append_bytes("aaaaaaaaa", 9);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-28);
    disk_buf.flush_to_disk(dev, true).get();
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)28, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned);
    dev.read<char>(0, buf, len_aligned).get();
    strncpy(expected_buf, "asdfasxyzxyzxyz1234aaaaaaaaa", max_siz);
    BOOST_REQUIRE_EQUAL(strncmp(expected_buf, buf, len_aligned), 0);
}

SEASTAR_THREAD_TEST_CASE(aligned_unaligned_write_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    char buf[max_siz], expected_buf[max_siz];
    disk_offset_t len_aligned, len_aligned2;
    disk_buf.append_bytes("1234", 4);
    disk_buf.append_bytes("aaaaaaaaa", 9);
    disk_buf.flush_to_disk(dev, true).get();
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)13, alignment);
    dev.read<char>(0, buf, len_aligned).get();
    strncpy(expected_buf, "1234aaaaaaaaa", max_siz);
    BOOST_REQUIRE_EQUAL(strncmp(expected_buf, buf, len_aligned), 0);
    
    disk_buf.append_bytes("asdfasdf", 6);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned-6);
    disk_buf.append_bytes("xyzxyzxyz", 9);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned-15);
    disk_buf.flush_to_disk(dev, false).get();
    memcpy(expected_buf+len_aligned, "asdfasxyzxyzxyz", 15);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned-15);
    len_aligned2 = round_up_to_multiple_of_power_of_2(len_aligned+15, alignment);
    dev.read<char>(0, buf, len_aligned2).get();
    BOOST_REQUIRE_EQUAL(strncmp(expected_buf, buf, len_aligned+15), 0);
}

SEASTAR_THREAD_TEST_CASE(empty_aligned_write_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    disk_buf.flush_to_disk(dev, true).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}

SEASTAR_THREAD_TEST_CASE(empty_unaligned_write_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    disk_buf.flush_to_disk(dev, false).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}

SEASTAR_THREAD_TEST_CASE(empty_append_bytes_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    disk_buf.append_bytes("qwerty", 0);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
    disk_buf.flush_to_disk(dev, false).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}

SEASTAR_THREAD_TEST_CASE(reset_same_offset_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    char buf[max_siz];
    disk_offset_t len_aligned;
    disk_buf.append_bytes("123450", 6);
    disk_buf.flush_to_disk(dev,false).get();
    disk_buf.reset(0);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
    disk_buf.append_bytes("overwrite", 9);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-9);
    disk_buf.flush_to_disk(dev,false).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-9);
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)9, alignment);
    dev.read<char>(0, buf, len_aligned).get();
    BOOST_REQUIRE_EQUAL(strncmp("overwrite", buf, 9), 0);

}

SEASTAR_THREAD_TEST_CASE(reset_new_offset_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    char buf[max_siz];
    disk_offset_t len_aligned;
    disk_buf.append_bytes("123450", 6);
    disk_buf.flush_to_disk(dev,false).get();
    disk_buf.reset(6*alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
    disk_buf.append_bytes("overwrite", 9);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-9);
    disk_buf.flush_to_disk(dev,false).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-9);
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)6, alignment);
    dev.read<char>(0, buf, len_aligned).get();
    BOOST_REQUIRE_EQUAL(strncmp("123450", buf, 6), 0);
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)9, alignment);
    dev.read<char>(6*alignment, buf, len_aligned).get();
    BOOST_REQUIRE_EQUAL(strncmp("overwrite", buf, 9), 0);
}

SEASTAR_THREAD_TEST_CASE(combined_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer(max_siz, alignment, 0);
    char buf[max_siz], inp[max_siz];
    strncpy(inp, "abcdefghiju8y7t6r5XXq1q2q3YYYY", max_siz); // fills to max_siz with null characters
    disk_offset_t beg = 0, beg_aligned = 0, end = 0, end_aligned;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.append_bytes(inp, 10);
    end += 10;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.flush_to_disk(dev, false).get();
    end_aligned = round_up_to_multiple_of_power_of_2(end, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    dev.read<char>(beg_aligned, buf, end_aligned-beg_aligned).get();
    BOOST_REQUIRE_EQUAL(strncmp(inp, buf+beg-beg_aligned, end-beg), 0);

    beg = end;
    beg_aligned = round_down_to_multiple_of_power_of_2(beg, alignment);
    disk_buf.append_bytes(inp+10, 8);
    end += 8;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.append_bytes(inp+20, 6);
    end += 6;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.flush_to_disk(dev, true).get();
    end_aligned = round_up_to_multiple_of_power_of_2(end, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end_aligned);

    dev.read<char>(beg_aligned, buf, end_aligned-beg_aligned).get();
    BOOST_REQUIRE_EQUAL(strncmp(inp+10, buf+beg-beg_aligned, 8), 0);
    BOOST_REQUIRE_EQUAL(strncmp(inp+20, buf+beg-beg_aligned+8, 6), 0);
    BOOST_REQUIRE_EQUAL(strncmp(inp+30, buf+beg-beg_aligned+14, end_aligned-end), 0);
    
    end = end_aligned;
    disk_buf.append_bytes(inp+26, 4);
    end += 4;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.reset(alignment*3);
    end = beg = alignment*3;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);

    disk_buf.flush_to_disk(dev, false).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}
