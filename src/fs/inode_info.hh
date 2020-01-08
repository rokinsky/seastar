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
 * Copyright (C) 2019 ScyllaDB
 */

#pragma once

#include "inode.hh"
#include "seastar/core/sstring.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/overloaded.hh"
#include "unix_metadata.hh"

#include <map>
#include <variant>

namespace seastar::fs {

struct inode_data_vec {
    file_range data_range; // data spans [beg, end) range of the file

    struct in_mem_data {
        temporary_buffer<uint8_t> data;
    };

    struct on_disk_data {
        file_offset_t device_offset;
    };

    struct hole_data { };

    std::variant<in_mem_data, on_disk_data, hole_data> data_location;
};

struct inode_info {
    uint32_t opened_files_count = 0; // Number of open files referencing inode
    uint32_t directories_containing_file = 0;
    unix_metadata metadata;

    struct directory {
        // TODO: directory entry cannot contain '/' character --> add checks for that
        std::map<sstring, inode_t> entries; // entry name => inode
    };

    struct file {
        std::map<file_offset_t, inode_data_vec> data; // file offset => data vector that begins there (data vectors do not overlap)

        file_offset_t size() const noexcept {
            return (data.empty() ? 0 : (--data.end())->second.data_range.end);
        }
    };

    std::variant<directory, file> contents;
};

} // namespace seastar::fs
