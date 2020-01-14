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
#include "metadata_disk_entries.hh"
#include "to_disk_buffer.hh"

#include <boost/crc.hpp>

namespace seastar::fs {

class metadata_to_disk_buffer : protected to_disk_buffer {
    boost::crc_32_type _crc;

public:
    // Represents buffer that will be written to a block_device at offset @p disk_alligned_write_offset. Total number of bytes appended cannot exceed @p aligned_max_size.
    metadata_to_disk_buffer(size_t aligned_max_size, unit_size_t alignment, disk_offset_t disk_aligned_write_offset)
    : to_disk_buffer(aligned_max_size, alignment, disk_aligned_write_offset) {
        assert(aligned_max_size >= sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    }

    using to_disk_buffer::reset;

    /**
     * @brief Clears buffer, leaving it in state as if it was just constructed
     *
     * @param cluster_beg_offset disk offset of the beginning of the cluster
     * @param cluster_contents pointer to contents of the cluster
     * @param metadata_end_pos position at which valid metadata ends: valid metadata range: [0, @p metadata_end_pos)
     * @param align_before_buffering whether to align to beginning of the next unflushed data or set it to @p metadata_end_pos
     */
    void reset_from_bootstraped_cluster(disk_offset_t cluster_beg_offset, const uint8_t* cluster_contents, size_t metadata_end_pos, bool align_before_buffering) noexcept {
        assert(mod_by_power_of_2(cluster_beg_offset, _alignment) == 0);
        _disk_write_offset = cluster_beg_offset;
        _zero_padded_end = 0;

        if (align_before_buffering) {
            auto aligned_pos = round_up_to_multiple_of_power_of_2(metadata_end_pos, _alignment);
            _unflushed_data = {aligned_pos, aligned_pos};
        } else {
            _unflushed_data = {metadata_end_pos, metadata_end_pos};
            // to_disk_buffer::flush_to_disk() has to align down the begin offset before write, so we need to prepare data at this offset
            range<size_t> to_copy = {
                round_down_to_multiple_of_power_of_2(_unflushed_data.beg, _alignment),
                _unflushed_data.beg,
            };
            memcpy(_buff.get_write() + to_copy.beg, cluster_contents + to_copy.beg, to_copy.size());
        }

        if (bytes_left() > 0) {
            start_new_unflushed_data();
        }
    }

private:
    void start_new_unflushed_data() noexcept override {
        ondisk_type type = INVALID;
        ondisk_checkpoint checkpoint;
        memset(&checkpoint, 0, sizeof(checkpoint));

        assert(bytes_left() >= sizeof(type) + sizeof(checkpoint));
        append_bytes(&type, sizeof(type));
        append_bytes(&checkpoint, sizeof(checkpoint));

        _crc.reset();
    }

    void prepare_unflushed_data_for_flush() noexcept override {
        // Make checkpoint valid
        constexpr ondisk_type checkpoint_type = CHECKPOINT;
        size_t checkpoint_pos = _unflushed_data.beg + sizeof(checkpoint_type); // TODO: check
        ondisk_checkpoint checkpoint;
        checkpoint.checkpointed_data_length = _unflushed_data.end - checkpoint_pos - sizeof(checkpoint);
        _crc.process_bytes(&checkpoint.checkpointed_data_length, sizeof(checkpoint.checkpointed_data_length));
        checkpoint.crc32_code = _crc.checksum();

        memcpy(_buff.get_write() + _unflushed_data.beg, &checkpoint_type, sizeof(checkpoint_type));
        memcpy(_buff.get_write() + checkpoint_pos, &checkpoint, sizeof(checkpoint));
    }

    void append_bytes(const void* data, size_t len) noexcept override {
        to_disk_buffer::append_bytes(data, len);
        _crc.process_bytes(data, len);
    }

public:
    using to_disk_buffer::bytes_left;

    enum append_result {
        APPENDED,
        TOO_BIG,
    };

    append_result append(const ondisk_next_metadata_cluster& next_metadata_cluster) noexcept {
        ondisk_type type = NEXT_METADATA_CLUSTER;
        if (bytes_left() < sizeof(type) + sizeof(next_metadata_cluster)) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&next_metadata_cluster, sizeof(next_metadata_cluster));
        return APPENDED;
    }

private:
    bool fits_for_append(size_t bytes_no) const noexcept {
        return (bytes_left() >= bytes_no + sizeof(ondisk_next_metadata_cluster)); // We need to reserve space for the next metadata cluster entry
    }

    template<class T>
    append_result append_simple(ondisk_type type, const T& entry) noexcept {
        size_t total_size = sizeof(type) + sizeof(entry);
        if (not fits_for_append(total_size)) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&entry, sizeof(entry));
        return APPENDED;
    }

public:
    append_result append(const ondisk_create_inode& create_inode) noexcept {
        // TODO: maybe add a constexpr static field to each ondisk_* entry specifying what type it is?
        return append_simple(CREATE_INODE, create_inode);
    }

    append_result append(const ondisk_update_metadata& update_metadata) noexcept {
        return append_simple(UPDATE_METADATA, update_metadata);
    }

    append_result append(const ondisk_delete_inode& delete_inode) noexcept {
        return append_simple(DELETE_INODE, delete_inode);
    }

    append_result append(const ondisk_medium_write& medium_write) noexcept {
        return append_simple(MEDIUM_WRITE, medium_write);
    }

    append_result append(const ondisk_large_write& large_write) noexcept {
        return append_simple(LARGE_WRITE, large_write);
    }

    append_result append(const ondisk_large_write_without_mtime& large_write_without_mtime) noexcept {
        return append_simple(LARGE_WRITE_WITHOUT_MTIME, large_write_without_mtime);
    }

    append_result append(const ondisk_truncate& truncate) noexcept {
        return append_simple(TRUNCATE, truncate);
    }

    append_result append(const ondisk_mtime_update& mtime_update) noexcept {
        return append_simple(MTIME_UPDATE, mtime_update);
    }

    append_result append(const ondisk_small_write_header& small_write, const void* data) noexcept {
        ondisk_type type = SMALL_WRITE;
        size_t total_size = sizeof(type) + sizeof(small_write) + small_write.length;
        if (not fits_for_append(total_size)) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&small_write, sizeof(small_write));
        append_bytes(data, small_write.length);
        return APPENDED;
    }

    append_result append(const ondisk_add_dir_entry_header& add_dir_entry, const void* entry_name) noexcept {
        ondisk_type type = ADD_DIR_ENTRY;
        size_t total_size = sizeof(type) + sizeof(add_dir_entry) + add_dir_entry.entry_name_length;
        if (not fits_for_append(total_size)) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&add_dir_entry, sizeof(add_dir_entry));
        append_bytes(entry_name, add_dir_entry.entry_name_length);
        return APPENDED;
    }

    append_result append(const ondisk_rename_dir_entry_header& rename_dir_entry, const void* old_name, const void* new_name) noexcept {
        ondisk_type type = RENAME_DIR_ENTRY;
        size_t total_size = sizeof(type) + sizeof(rename_dir_entry) + rename_dir_entry.entry_old_name_length + rename_dir_entry.entry_new_name_length;
        if (not fits_for_append(total_size)) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&rename_dir_entry, sizeof(rename_dir_entry));
        append_bytes(old_name, rename_dir_entry.entry_old_name_length);
        append_bytes(new_name, rename_dir_entry.entry_new_name_length);
        return APPENDED;
    }

    append_result append(const ondisk_delete_dir_entry_header& delete_dir_entry, const void* entry_name) noexcept {
        ondisk_type type = DELETE_DIR_ENTRY;
        size_t total_size = sizeof(type) + sizeof(delete_dir_entry) + delete_dir_entry.entry_name_length;
        if (not fits_for_append(total_size)) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&delete_dir_entry, sizeof(delete_dir_entry));
        append_bytes(entry_name, delete_dir_entry.entry_name_length);
        return APPENDED;
    }

    using to_disk_buffer::flush_to_disk;
};

} // namespace seastar::fs
