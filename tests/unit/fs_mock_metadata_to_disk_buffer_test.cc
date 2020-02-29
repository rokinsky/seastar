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
#include <seastar/core/units.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>
#include <stdint.h>

using namespace seastar;
using namespace seastar::fs;

using append = mock_metadata_to_disk_buffer::action::append;
using flush_to_disk = mock_metadata_to_disk_buffer::action::flush_to_disk;

SEASTAR_THREAD_TEST_CASE(mock_metadata_to_disk_buffer_test) {
    constexpr size_t buff_size = 1 * MB;
    constexpr size_t alignment = 4 * KB;

    mock_metadata_to_disk_buffer buf(buff_size, alignment);
    buf.init(0);
    BOOST_REQUIRE_EQUAL(buf.bytes_left(), buff_size - sizeof(ondisk_type) - sizeof(ondisk_checkpoint));

    ondisk_delete_inode del_op {1};
    buf.append(del_op);
    BOOST_REQUIRE_EQUAL(buf.is_append_type<ondisk_delete_inode>(0), true);
    BOOST_REQUIRE_EQUAL(buf.get_by_append_type<ondisk_delete_inode>(0).inode, del_op.inode);

    ondisk_small_write_header write_op {2, 7, 5, 17};
    std::string write_str = "12345";
    buf.append(write_op, write_str.c_str());
    BOOST_REQUIRE_EQUAL(buf.is_append_type<ondisk_small_write>(1), true);
    const ondisk_small_write_header &write_header = buf.get_by_append_type<ondisk_small_write>(1).header;
    const std::vector<uint8_t> &write_data = buf.get_by_append_type<ondisk_small_write>(1).data;
    BOOST_REQUIRE_EQUAL(write_header.inode, write_op.inode);
    BOOST_REQUIRE_EQUAL(write_header.offset, write_op.offset);
    BOOST_REQUIRE_EQUAL(write_header.length, write_op.length);
    BOOST_REQUIRE_EQUAL(write_header.mtime_ns, write_op.mtime_ns);
    BOOST_REQUIRE_EQUAL(std::string(write_data.data(), write_data.data() + write_str.size()), write_str);

    auto buf2 = buf.virtual_constructor(buff_size, alignment);
    buf2->init(alignment);
    auto buf3 = buf2->virtual_constructor(buff_size, alignment);
    buf3->init(alignment * 2);
    BOOST_REQUIRE_EQUAL(mock_metadata_to_disk_buffer::created_buffers.size(), 2);
    BOOST_CHECK(mock_metadata_to_disk_buffer::created_buffers[0] == buf2);
    BOOST_CHECK(mock_metadata_to_disk_buffer::created_buffers[1] == buf3);
}