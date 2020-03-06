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

#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"

namespace seastar::fs {

class read_operation {
    metadata_log& _metadata_log;
    inode_t _inode;
    file_offset_t _file_offset;
    uint8_t* _buffer;
    size_t _read_len;
    const io_priority_class& _pc;

    read_operation(metadata_log& metadata_log, inode_t inode, file_offset_t file_offset, uint8_t* buffer,
            size_t read_len, const io_priority_class& pc)
        : _metadata_log(metadata_log), _inode(inode), _file_offset(file_offset), _buffer(buffer), _read_len(read_len)
        , _pc(pc) {}

    future<size_t> read() {
        auto inode_it = _metadata_log._inodes.find(_inode);
        if (inode_it == _metadata_log._inodes.end()) {
            return make_exception_future<size_t>(invalid_inode_exception());
        }
        if (inode_it->second.is_directory()) {
            return make_exception_future<size_t>(is_directory_exception());
        }
        inode_info::file* file_info = &inode_it->second.get_file();

        return _metadata_log._locks.with_lock(metadata_log::locks::shared {_inode}, [this, file_info] {
            // TODO: do we want to keep that lock during reading? Everything should work even after file removal
            if (not _metadata_log.inode_exists(_inode)) {
                return make_exception_future<size_t>(operation_became_invalid_exception());
            }

            std::vector<inode_data_vec> data_vecs;
            // Extract data vectors from file_info
            file_info->execute_on_data_range({_file_offset, _file_offset + _read_len}, [&data_vecs](inode_data_vec data_vec) {
                // TODO: for compaction: mark that clusters shouldn't be moved to _cluster_allocator before that read ends
                data_vecs.emplace_back(std::move(data_vec));
            });

            return iterate_reads(std::move(data_vecs));
        });
    }

    future<size_t> iterate_reads(std::vector<inode_data_vec> data_vecs) {
        return do_with(std::move(data_vecs), (size_t)0, (size_t)0,
                [this](std::vector<inode_data_vec>& data_vecs, size_t& vec_idx, size_t& valid_read_size) {
            return repeat([this, &valid_read_size, &data_vecs, &vec_idx] {
                if (vec_idx == data_vecs.size()) {
                    return make_ready_future<bool_class<stop_iteration_tag>>(stop_iteration::yes);
                }

                inode_data_vec& data_vec = data_vecs[vec_idx++];
                size_t expected_read_len = data_vec.data_range.size();

                return do_read(data_vec).then(
                        [&valid_read_size, expected_read_len](size_t read_size) {
                    valid_read_size += read_size;
                    if (read_size != expected_read_len) {
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                });
            }).then([&valid_read_size] {
                return make_ready_future<size_t>(valid_read_size);
            });
        });
    }

    future<size_t> do_read(inode_data_vec& data_vec) {
        size_t expected_read_len = data_vec.data_range.size();
        size_t buffer_offset = data_vec.data_range.beg - _file_offset;
        future<size_t> disk_read = make_ready_future<size_t>(expected_read_len);

        std::visit(overloaded {
            [&](inode_data_vec::in_mem_data& mem) {
                std::memcpy(_buffer + buffer_offset, mem.data.get(), expected_read_len);
            },
            [&](inode_data_vec::on_disk_data& disk_data) {
                // TODO: we need to align those reads, we shouldn't read the same block more than one time,
                // without assuming aligned reads here we will need to save previous disk read and copy data
                // from it
                disk_read = _metadata_log._device.read(disk_data.device_offset,
                    _buffer + buffer_offset, expected_read_len, _pc);
            },
            [&](inode_data_vec::hole_data&) {
                std::memset(_buffer + buffer_offset, 0, expected_read_len);
            },
        }, data_vec.data_location);

        return disk_read;
    }

public:
    static future<size_t> perform(metadata_log& metadata_log, inode_t inode, file_offset_t pos, void* buffer,
            size_t len, const io_priority_class& pc) {
        return do_with(read_operation(metadata_log, inode, pos, static_cast<uint8_t*>(buffer), len, pc), [](auto& obj) {
            return obj.read();
        });
    }
};

} // namespace seastar::fs
