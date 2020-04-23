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

class cluster_info {
public:
    // Each cluster_data_vec corresponds to an inode_data_vec
    struct cluster_data_vec {
        inode_t data_owner;
        file_range data_range;
    };

private:
    size_t _valid_data_size = 0;
    bool _read_only = false;
    std::map<disk_offset_t, cluster_data_vec> _data;

public:
    constexpr const decltype(_data)& get_data() const & noexcept {
        return _data;
    }

    bool is_empty() const noexcept {
        return _valid_data_size == 0;
    }

    void finished_writing() noexcept {
        _read_only = true;
    }

    bool can_compact() const noexcept {
        return _read_only;
    }

    void add_data(disk_offset_t disk_offset, inode_t inode, file_range data_range) noexcept {
        _data.emplace(disk_offset, cluster_data_vec {inode, data_range});
        _valid_data_size += data_range.size();
    }

    void remove_data(disk_offset_t disk_offset, file_range remove_range) {
        auto data_vec = std::move(_data.extract(--_data.upper_bound(disk_offset)).mapped());
        auto inode = data_vec.data_owner;
        auto [left, right] = data_vec.data_range;
        if (left < remove_range.beg) {
            _data.emplace(disk_offset + left - remove_range.beg, cluster_data_vec {inode, {left, remove_range.beg}});
        }
        if (remove_range.end < right) {
            _data.emplace(disk_offset + remove_range.size(), cluster_data_vec {inode, {remove_range.end, right}});
        }
        _valid_data_size -= remove_range.size();
    }
};

} // namespace seastar::fs
