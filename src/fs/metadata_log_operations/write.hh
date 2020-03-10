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
#include "fs/units.hh"
#include "fs/cluster.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/temporary_buffer.hh"

namespace seastar::fs {

class write_operation {
    // TODO: decide about threshold for small write
    static constexpr size_t SMALL_WRITE_THRESHOLD = std::numeric_limits<decltype(ondisk_small_write_header::length)>::max();

    metadata_log& _metadata_log;
    inode_t _inode;
    const io_priority_class& _pc;

    write_operation(metadata_log& metadata_log, inode_t inode, const io_priority_class& pc)
        : _metadata_log(metadata_log), _inode(inode), _pc(pc) {
        assert(_metadata_log._alignment <= SMALL_WRITE_THRESHOLD and
                "Small write threshold should be at least as big as alignment");
    }

    future<size_t> write(const uint8_t* buffer, size_t write_len, file_offset_t file_offset) {
        auto inode_it = _metadata_log._inodes.find(_inode);
        if (inode_it == _metadata_log._inodes.end()) {
            return make_exception_future<size_t>(invalid_inode_exception());
        }
        if (inode_it->second.is_directory()) {
            return make_exception_future<size_t>(is_directory_exception());
        }

        // TODO: maybe check if there is enough free clusters before executing?
        return _metadata_log._locks.with_lock(metadata_log::locks::shared {_inode}, [this, buffer, write_len, file_offset] {
            if (not _metadata_log.inode_exists(_inode)) {
                return make_exception_future<size_t>(operation_became_invalid_exception());
            }
            return iterate_writes(buffer, write_len, file_offset);
        });
    }

    future<size_t> iterate_writes(const uint8_t* buffer, size_t write_len, file_offset_t file_offset) {
        return do_with((size_t)0, [this, buffer, write_len, file_offset](size_t& valid_write_len) {
            return repeat([this, &valid_write_len, buffer, write_len, file_offset] {
                if (valid_write_len == write_len) {
                    return make_ready_future<bool_class<stop_iteration_tag>>(stop_iteration::yes);
                }

                size_t expected_write_len;
                if (size_t buffer_alignment = mod_by_power_of_2(reinterpret_cast<size_t>(buffer) + valid_write_len, _metadata_log._alignment);
                        buffer_alignment != 0) {
                    // When buffer is not aligned then align it using one small write
                    expected_write_len = std::min(_metadata_log._alignment - buffer_alignment, write_len - valid_write_len);
                } else {
                    size_t remaining_write_len = write_len - valid_write_len;
                    if (remaining_write_len >= _metadata_log._cluster_size) {
                        expected_write_len = _metadata_log._cluster_size;
                    } else if (remaining_write_len <= SMALL_WRITE_THRESHOLD) {
                        expected_write_len = remaining_write_len;
                    } else {
                        // If the last write is medium then align write length by splitting last write into medium aligned
                        // write and small write
                        expected_write_len = round_down_to_multiple_of_power_of_2(remaining_write_len, _metadata_log._alignment);
                    }
                }

                auto new_buffer = buffer + valid_write_len;
                auto new_file_offset = file_offset + valid_write_len;
                auto write_future = make_ready_future<size_t>(0);
                if (expected_write_len <= SMALL_WRITE_THRESHOLD) {
                    write_future = do_small_write(new_buffer, expected_write_len, new_file_offset);
                } else if (expected_write_len < _metadata_log._cluster_size) {
                    write_future = medium_write(new_buffer, expected_write_len, new_file_offset);
                } else {
                    // TODO: maybe we could get rid of that updating mtime switch?
                    // Update mtime only for the first large write
                    write_future = do_large_write(new_buffer, new_file_offset, valid_write_len == 0);
                }

                return write_future.then([&valid_write_len, expected_write_len](size_t write_len) {
                    valid_write_len += write_len;
                    if (write_len != expected_write_len) {
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                });
            }).then([&valid_write_len] {
                return make_ready_future<size_t>(valid_write_len);
            });
        });
    }

    future<size_t> do_small_write(const uint8_t* buffer, size_t expected_write_len, file_offset_t file_offset) {
        using namespace std::chrono;
        uint64_t mtime_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        ondisk_small_write_header ondisk_entry {
            _inode,
            file_offset,
            static_cast<decltype(ondisk_small_write_header::length)>(expected_write_len),
            mtime_ns
        };

        switch (_metadata_log.append_ondisk_entry(ondisk_entry, buffer)) {
        case metadata_log::append_result::TOO_BIG:
            assert(false and "ondisk entry cannot be too big");
        case metadata_log::append_result::NO_SPACE:
            return make_exception_future<size_t>(no_more_space_exception());
        case metadata_log::append_result::APPENDED:
            temporary_buffer<uint8_t> tmp_buffer(buffer, expected_write_len);
            _metadata_log.memory_only_small_write(_inode, file_offset, std::move(tmp_buffer));
            _metadata_log.memory_only_update_mtime(_inode, mtime_ns);
            return make_ready_future<size_t>(expected_write_len);
        }
        __builtin_unreachable();
    }

    future<size_t> medium_write(const uint8_t* aligned_buffer, size_t aligned_expected_write_len, file_offset_t file_offset) {
        assert(reinterpret_cast<size_t>(aligned_buffer) % _metadata_log._alignment == 0);
        assert(aligned_expected_write_len % _metadata_log._alignment == 0);
        // TODO: medium write can be divided into bigger number of smaller medium writes. Maybe we should add checks
        // for that and allow only limited number of medium writes? Or we could add to to_disk_buffer option for
        // space 'reservation' to make sure that after division our write will fit into the buffer?
        // That would also limit medium write to at most two smaller writes.
        return do_with((size_t)0, [this, aligned_buffer, aligned_expected_write_len, file_offset](size_t& valid_write_len) {
            return repeat([this, &valid_write_len, aligned_buffer, aligned_expected_write_len, file_offset] {
                if (valid_write_len == aligned_expected_write_len) {
                    return make_ready_future<bool_class<stop_iteration_tag>>(stop_iteration::yes);
                }

                size_t remaining_write_len = aligned_expected_write_len - valid_write_len;
                size_t curr_expected_write_len;
                auto new_buffer = aligned_buffer + valid_write_len;
                auto new_file_offset = file_offset + valid_write_len;
                auto write_future = make_ready_future<size_t>(0);
                if (remaining_write_len <= SMALL_WRITE_THRESHOLD) {
                    // We can use small write for the remaining data
                    curr_expected_write_len = remaining_write_len;
                    write_future = do_small_write(new_buffer, curr_expected_write_len, new_file_offset);
                } else {
                    // We must use medium write
                    size_t buff_bytes_left = _metadata_log._curr_data_buff->bytes_left();
                    if (buff_bytes_left <= SMALL_WRITE_THRESHOLD) {
                        // TODO: add wasted buff_bytes_left bytes for compaction
                        // No space left in the current to_disk_buffer for medium write - allocate a new buffer
                        std::optional<cluster_id_t> cluster_opt = _metadata_log._cluster_allocator.alloc();
                        if (not cluster_opt) {
                            // TODO: maybe we should return partial write instead of exception?
                            return make_exception_future<bool_class<stop_iteration_tag>>(no_more_space_exception());
                        }

                        auto cluster_id = cluster_opt.value();
                        disk_offset_t cluster_disk_offset = cluster_id_to_offset(cluster_id, _metadata_log._cluster_size);
                        _metadata_log._curr_data_buff = make_shared<to_disk_buffer>();
                        _metadata_log._curr_data_buff->init(_metadata_log._cluster_size, _metadata_log._alignment,
                                cluster_disk_offset);
                        buff_bytes_left = _metadata_log._curr_data_buff->bytes_left();

                        curr_expected_write_len = remaining_write_len;
                    } else {
                        // There is enough space for medium write
                        curr_expected_write_len = buff_bytes_left >= remaining_write_len ? remaining_write_len : buff_bytes_left;
                    }

                    write_future = do_medium_write(new_buffer, curr_expected_write_len, new_file_offset,
                            _metadata_log._curr_data_buff);
                }

                return write_future.then([&valid_write_len, curr_expected_write_len](size_t write_len) {
                    valid_write_len += write_len;
                    if (write_len != curr_expected_write_len) {
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                });
            }).then([&valid_write_len] {
                return make_ready_future<size_t>(valid_write_len);
            });;
        });
    }

    future<size_t> do_medium_write(const uint8_t* aligned_buffer, size_t aligned_expected_write_len, file_offset_t file_offset,
            shared_ptr<to_disk_buffer> disk_buffer) {
        assert(reinterpret_cast<size_t>(aligned_buffer) % _metadata_log._alignment == 0);
        assert(aligned_expected_write_len % _metadata_log._alignment == 0);
        assert(disk_buffer->bytes_left() >= aligned_expected_write_len);

        disk_offset_t device_offset = disk_buffer->current_disk_offset();
        // TODO: we can avoid copying data because aligned_buffer is aligned and we can just dma directly into device
        disk_buffer->append_bytes(aligned_buffer, aligned_expected_write_len);
        // TODO: handle partial write
        return disk_buffer->flush_to_disk(_metadata_log._device).then(
                [this, file_offset, aligned_expected_write_len, disk_buffer = std::move(disk_buffer), device_offset] {
            using namespace std::chrono;
            uint64_t mtime_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
            ondisk_medium_write ondisk_entry {
                _inode,
                file_offset,
                device_offset,
                static_cast<decltype(ondisk_medium_write::length)>(aligned_expected_write_len),
                mtime_ns
            };

            switch (_metadata_log.append_ondisk_entry(ondisk_entry)) {
            case metadata_log::append_result::TOO_BIG:
                assert(false and "ondisk entry cannot be too big");
            case metadata_log::append_result::NO_SPACE:
                return make_exception_future<size_t>(no_more_space_exception());
            case metadata_log::append_result::APPENDED:
                _metadata_log.memory_only_disk_write(_inode, file_offset, device_offset, aligned_expected_write_len);
                _metadata_log.memory_only_update_mtime(_inode, mtime_ns);
                return make_ready_future<size_t>(aligned_expected_write_len);
            }
            __builtin_unreachable();
            return make_ready_future<size_t>(aligned_expected_write_len);
        });
    }

    future<size_t> do_large_write(const uint8_t* aligned_buffer, file_offset_t file_offset, bool update_mtime) {
        assert(reinterpret_cast<size_t>(aligned_buffer) % _metadata_log._alignment == 0);
        // aligned_expected_write_len = _metadata_log._cluster_size
        std::optional<cluster_id_t> cluster_opt = _metadata_log._cluster_allocator.alloc();
        if (not cluster_opt) {
            return make_exception_future<size_t>(no_more_space_exception());
        }
        auto cluster_id = cluster_opt.value();
        disk_offset_t cluster_disk_offset = cluster_id_to_offset(cluster_id, _metadata_log._cluster_size);

        return _metadata_log._device.write(cluster_disk_offset, aligned_buffer, _metadata_log._cluster_size, _pc).then(
                [this, file_offset, cluster_id, cluster_disk_offset, update_mtime](size_t write_len) {
            if (write_len != _metadata_log._cluster_size) {
                _metadata_log._cluster_allocator.free(cluster_id);
                return make_ready_future<size_t>(0);
            }

            metadata_log::append_result append_result;
            if (update_mtime) {
                using namespace std::chrono;
                uint64_t mtime_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
                ondisk_large_write ondisk_entry {
                    _inode,
                    file_offset,
                    cluster_id,
                    mtime_ns
                };
                append_result = _metadata_log.append_ondisk_entry(ondisk_entry);
                if (append_result == metadata_log::append_result::APPENDED) {
                    _metadata_log.memory_only_update_mtime(_inode, mtime_ns);
                }
            } else {
                ondisk_large_write_without_mtime ondisk_entry {
                    _inode,
                    file_offset,
                    cluster_id
                };
                append_result = _metadata_log.append_ondisk_entry(ondisk_entry);
            }

            switch (append_result) {
            case metadata_log::append_result::TOO_BIG:
                assert(false and "ondisk entry cannot be too big");
            case metadata_log::append_result::NO_SPACE:
                _metadata_log._cluster_allocator.free(cluster_id);
                return make_exception_future<size_t>(no_more_space_exception());
            case metadata_log::append_result::APPENDED:
                _metadata_log.memory_only_disk_write(_inode, file_offset, cluster_disk_offset, write_len);
                return make_ready_future<size_t>(write_len);
            }
            __builtin_unreachable();
        });
    }

public:
    static future<size_t> perform(metadata_log& metadata_log, inode_t inode, file_offset_t pos, const void* buffer,
            size_t len, const io_priority_class& pc) {
        return do_with(write_operation(metadata_log, inode, pc), [buffer, len, pos](auto& obj) {
            return obj.write(static_cast<const uint8_t*>(buffer), len, pos);
        });
    }
};

} // namespace seastar::fs
