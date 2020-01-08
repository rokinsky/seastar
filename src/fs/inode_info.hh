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

        // Deletes data vectors that are subset of @p data_range and cuts overlapping data vectors to make them not overlap.
        // @p cut_data_vec_processor is called on each inode_data_vec (including parts of overlapped data vectors) that will be deleted
        template<class Func>
        void cut_out_data_range(file_range range, Func&& cut_data_vec_processor) {
            static_assert(std::is_invocable_v<Func, const inode_data_vec&>);
            // Cut all vectors intersecting with range
            auto it = data.lower_bound(range.beg);
            if (it != data.begin() and are_intersecting(range, prev(it)->second.data_range)) {
                --it;
            }

            while (it != data.end() and are_intersecting(range, it->second.data_range)) {
                auto data_vec = std::move(it->second);
                data.erase(it++);
                const auto cap = intersection(range, data_vec.data_range);
                if (cap == data_vec.data_range) {
                    // Fully intersects => remove it
                    cut_data_vec_processor(data_vec);
                    continue;
                }

                // Overlaps => cut it, possibly into two parts:
                // |       data_vec      |
                //         | cap |
                // | left  | mid | right |
                // left and right remain, but mid is deleted
                inode_data_vec left, mid, right;
                left.data_range = {data_vec.data_range.beg, cap.beg};
                mid.data_range = cap;
                right.data_range = {cap.end, data_vec.data_range.end};
                auto right_beg_shift = right.data_range.beg - data_vec.data_range.beg;
                auto mid_beg_shift = mid.data_range.beg - data_vec.data_range.beg;
                std::visit(overloaded {
                    [&](inode_data_vec::in_mem_data& mem) {
                        left.data_location = inode_data_vec::in_mem_data {mem.data.share(0, left.data_range.size())};
                        mid.data_location = inode_data_vec::in_mem_data {mem.data.share(mid_beg_shift, mid.data_range.size())};
                        right.data_location = inode_data_vec::in_mem_data {mem.data.share(right_beg_shift, right.data_range.size())};
                    },
                    [&](inode_data_vec::on_disk_data& data) {
                        left.data_location = data;
                        mid.data_location = inode_data_vec::on_disk_data {data.device_offset + mid_beg_shift};
                        right.data_location = inode_data_vec::on_disk_data {data.device_offset + right_beg_shift};
                    },
                    [&](inode_data_vec::hole_data&) {
                        left.data_location = inode_data_vec::hole_data {};
                        mid.data_location = inode_data_vec::hole_data {};
                        right.data_location = inode_data_vec::hole_data {};
                    },
                }, data_vec.data_location);

                // Save new data vectors
                if (not left.data_range.is_empty()) {
                    data.emplace(left.data_range.beg, std::move(left));
                }
                if (not right.data_range.is_empty()) {
                    data.emplace(right.data_range.beg, std::move(right));
                }

                // Process deleted vector
                cut_data_vec_processor(mid);
            }
        }
    };

    std::variant<directory, file> contents;
};

} // namespace seastar::fs
