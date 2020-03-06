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

class write_operation {
    metadata_log& _metadata_log;
    inode_t _inode;
    file_offset_t _file_offset;
    const uint8_t* _buffer;
    size_t _write_len;
    [[maybe_unused]] const io_priority_class& _pc;

    write_operation(metadata_log& metadata_log, inode_t inode, file_offset_t file_offset, const uint8_t* buffer,
            size_t write_len, const io_priority_class& pc)
        : _metadata_log(metadata_log), _inode(inode), _file_offset(file_offset), _buffer(buffer), _write_len(write_len)
        , _pc(pc) {}

    future<size_t> write() {
        auto inode_it = _metadata_log._inodes.find(_inode);
        if (inode_it == _metadata_log._inodes.end()) {
            return make_exception_future<size_t>(invalid_inode_exception());
        }
        if (inode_it->second.is_directory()) {
            return make_exception_future<size_t>(is_directory_exception());
        }

        return _metadata_log._locks.with_lock(metadata_log::locks::shared {_inode}, [this] {
            if (not _metadata_log.inode_exists(_inode)) {
                return make_exception_future<size_t>(operation_became_invalid_exception());
            }
            return iterate_writes();
        });
    }

    future<size_t> iterate_writes() {
        // TODO: decide about thresholds for small, medium and big writes
        if (_write_len <= std::numeric_limits<decltype(ondisk_small_write_header::length)>::max()) {
            return do_small_write(0, static_cast<decltype(ondisk_small_write_header::length)>(_write_len));
        }
        return make_exception_future<size_t>(invalid_argument_exception());
    }

    future<size_t> do_medium_write(size_t buffer_offset, decltype(ondisk_small_write_header::length) expected_write_len) {
        return make_ready_future<size_t>(0);
    }

    future<size_t> do_big_write(size_t buffer_offset, decltype(ondisk_small_write_header::length) expected_write_len) {
        return make_ready_future<size_t>(0);
    }

    future<size_t> do_small_write(size_t buffer_offset, decltype(ondisk_small_write_header::length) expected_write_len) {
        using namespace std::chrono;
        uint64_t mtime_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        ondisk_small_write_header ondisk_entry {
            _inode,
            _file_offset + buffer_offset,
            expected_write_len,
            mtime_ns
        };

        switch (_metadata_log.append_ondisk_entry(ondisk_entry, _buffer + buffer_offset)) {
        case metadata_log::append_result::TOO_BIG:
            assert(false and "ondisk entry cannot be too big");
        case metadata_log::append_result::NO_SPACE:
            return make_exception_future<size_t>(no_more_space_exception());
        case metadata_log::append_result::APPENDED:
            temporary_buffer<uint8_t> tmp_buffer(_buffer + buffer_offset, expected_write_len);
            _metadata_log.memory_only_small_write(_inode, _file_offset, std::move(tmp_buffer));
            _metadata_log.memory_only_update_mtime(_inode, mtime_ns);
            return make_ready_future<size_t>(expected_write_len);
        }
        __builtin_unreachable();
    }

public:
    static future<size_t> perform(metadata_log& metadata_log, inode_t inode, file_offset_t pos, const void* buffer,
            size_t len, const io_priority_class& pc) {
        return do_with(write_operation(metadata_log, inode, pos, static_cast<const uint8_t*>(buffer), len, pc), [](auto& obj) {
            return obj.write();
        });
    }
};

} // namespace seastar::fs
