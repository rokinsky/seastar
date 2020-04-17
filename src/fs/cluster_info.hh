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

#pragma once

#include "fs/cluster.hh"
#include "fs/inode.hh"
#include "fs/units.hh"
#include <cassert>
#include <map>

namespace seastar::fs {

// Each cluster_data_vec corresponds to an inode_data_vec
struct cluster_data_vec {
    inode_t data_owner;
    file_range data_range;
};

struct cluster_info {
    size_t valid_data_size = 0;
    bool read_only = false;
    std::map<disk_offset_t, cluster_data_vec> data;

    bool is_empty() const noexcept {
        return valid_data_size == 0;
    }
    void finished_writing() noexcept {
        read_only = true;
    }
    void add_data(disk_offset_t disk_offset, inode_t inode, file_range data_range) noexcept {
        data.emplace(disk_offset, cluster_data_vec {inode, data_range});
        valid_data_size += data_range.size();
    }
    void remove_data(disk_offset_t disk_offset, file_range remove_range) {
        auto data_vec = std::move(data.extract(--data.upper_bound(disk_offset)).mapped());
        auto inode = data_vec.data_owner;
        auto left = data_vec.data_range.beg;
        auto right = data_vec.data_range.end;

        if (left < remove_range.beg) {
            data.emplace(disk_offset + (left - remove_range.beg), cluster_data_vec {inode, {left, remove_range.beg}});
        }
        if (remove_range.end < right) {
            data.emplace(disk_offset + (remove_range.end - remove_range.beg), cluster_data_vec {inode, {remove_range.end, right}});
        }
        valid_data_size -= remove_range.size();

    }
};

} // namespace seastar::fs
