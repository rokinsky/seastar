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

#include "bitwise.hh"
#include "seastar/core/future.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/block_device.hh"
#include "units.hh"

#include <cstring>

namespace seastar::fs {

class to_disk_buffer {
protected:
    temporary_buffer<uint8_t> _buff;
    const unit_size_t _alignment;
    disk_offset_t _disk_write_offset; // disk offset that corresponds to _buff.begin()
    range<size_t> _next_write; // range of unflushed bytes in _buff, beg is aligned to _alignment
    size_t _zero_padded_end; // Optimization to skip padding before write if it is already done

public:
    // Represents buffer that will be written to a block_device at offset @p disk_alligned_write_offset. Total number of bytes appended cannot exceed @p aligned_max_size.
    to_disk_buffer(size_t aligned_max_size, unit_size_t alignment, disk_offset_t disk_aligned_write_offset)
    : _buff(decltype(_buff)::aligned(alignment, aligned_max_size))
    , _alignment(alignment)
    , _disk_write_offset(disk_aligned_write_offset)
    , _next_write {0, 0}
    , _zero_padded_end(0) {
        assert(is_power_of_2(alignment));
        assert(mod_by_power_of_2(disk_aligned_write_offset, alignment) == 0);
        assert(aligned_max_size % alignment == 0);
        start_new_write();
    }

    virtual ~to_disk_buffer() = default;

    // Clears buffer, leaving it in state as if it was just constructed
    void reset(disk_offset_t new_disk_aligned_write_offset) noexcept {
        assert(mod_by_power_of_2(new_disk_aligned_write_offset, _alignment) == 0);
        _disk_write_offset = new_disk_aligned_write_offset;
        _next_write = {0, 0};
        _zero_padded_end = 0;
        start_new_write();
    }

    // Writes buffered data to disk
    // IMPORTANT: using this buffer before call completes is UB
    future<> flush_to_disk(block_device device) {
        prepare_new_write_for_flush();
        // Pad buffer with zeros till alignment. Layout overview:
        // | .....................|....................|00000000000000000000|
        // ^ _next_write.beg      ^ new_next_write_beg ^ _next_write.end    ^ aligned_end
        //      (aligned)               (aligned)       (maybe unaligned)      (aligned)
        //                        |<-------------- _alignment ------------->|
        size_t new_next_write_beg = _next_write.end - mod_by_power_of_2(_next_write.end, _alignment);
        size_t aligned_end = new_next_write_beg + _alignment;
        if (_zero_padded_end != aligned_end) {
            range padding = {_next_write.end, aligned_end};
            memset(_buff.get_write() + padding.beg, 0, padding.size());
            _zero_padded_end = padding.end;
        }
        range true_write = {_next_write.beg, aligned_end};

        return device.write(_disk_write_offset + true_write.beg, _buff.get_write() + true_write.beg, true_write.size()).then([this, true_write, new_next_write_beg](size_t written_bytes) {
            if (written_bytes != true_write.size()) {
                return make_exception_future<>(std::runtime_error("Partial write"));
            }

            _next_write = {new_next_write_beg, new_next_write_beg};
            start_new_write();
            return now();
        });
    }

protected:
    virtual void start_new_write() noexcept {}

    virtual void prepare_new_write_for_flush() noexcept {}

public:
    virtual void append_bytes(const void* data, size_t len) noexcept {
        assert(len <= bytes_left());
        memcpy(_buff.get_write() + _next_write.end, data, len);
        _next_write.end += len;
    }

    // Returns maximum number of bytes that may be written to buffer without calling reset()
    size_t bytes_left() const noexcept { return _buff.size() - _next_write.end; }
};

} // namespace seastar::fs
