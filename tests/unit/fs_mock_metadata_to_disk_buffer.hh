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

#include "fs/metadata_to_disk_buffer.hh"
#include <bits/stdint-uintn.h>
#include <stdint.h>
#include <variant>
#include <vector>

namespace seastar::fs {

struct ondisk_small_write {
    ondisk_small_write_header header;
    std::vector<uint8_t> data;
};

struct ondisk_add_dir_entry {
    ondisk_add_dir_entry_header header;
    std::vector<uint8_t> entry_name;
};

struct ondisk_create_inode_as_dir_entry {
    ondisk_create_inode_as_dir_entry_header header;
    std::vector<uint8_t> entry_name;
};

struct ondisk_delete_dir_entry {
    ondisk_delete_dir_entry_header header;
    std::vector<uint8_t> entry_name;
};

struct ondisk_rename_dir_entry {
    ondisk_rename_dir_entry_header header;
    std::vector<uint8_t> old_name;
    std::vector<uint8_t> new_name;
};

class mock_metadata_to_disk_buffer : public metadata_to_disk_buffer {
public:
    mock_metadata_to_disk_buffer(size_t aligned_max_size, unit_size_t alignment, disk_offset_t disk_aligned_write_offset)
        : metadata_to_disk_buffer(aligned_max_size, alignment, disk_aligned_write_offset) {}

    // keep container with all buffers created by create_new
    static std::vector<shared_ptr<metadata_to_disk_buffer>> created_buffers;

    virtual shared_ptr<metadata_to_disk_buffer> create_new(size_t aligned_max_size, unit_size_t alignment,
            disk_offset_t disk_aligned_write_offset) const override {
        auto new_buffer = make_shared<mock_metadata_to_disk_buffer>(aligned_max_size, alignment, disk_aligned_write_offset);
        mock_metadata_to_disk_buffer::created_buffers.emplace_back(new_buffer);
        return new_buffer;
    }

    struct action {
        struct append {
            using entry_data = std::variant<
                    ondisk_next_metadata_cluster,
                    ondisk_create_inode,
                    ondisk_update_metadata,
                    ondisk_delete_inode,
                    ondisk_small_write,
                    ondisk_medium_write,
                    ondisk_large_write,
                    ondisk_large_write_without_mtime,
                    ondisk_truncate,
                    ondisk_mtime_update,
                    ondisk_add_dir_entry,
                    ondisk_create_inode_as_dir_entry,
                    ondisk_delete_dir_entry,
                    ondisk_rename_dir_entry>;

            entry_data entry;
        };

        struct reset {
            disk_offset_t disk_aligned_write_offset;
        };

        struct reset_from_bootstrapped_cluster {
            disk_offset_t cluster_beg_offset;
            size_t metadata_end_pos;
        };

        struct flush_to_disk {
        };

        struct start_new_unflushed_data {
        };

        struct prepare_unflushed_data_for_flush {
        };

        using action_data = std::variant<
                append,
                reset,
                reset_from_bootstrapped_cluster,
                flush_to_disk,
                start_new_unflushed_data,
                prepare_unflushed_data_for_flush>;

        action_data data;

        action(action_data data) : data(std::move(data)) {}
    };
    std::vector<action> actions;

    template<typename T>
    bool is_type(size_t idx) {
        return std::holds_alternative<T>(actions.at(idx).data);
    }

    template<typename T>
    const T& get_by_type(size_t idx) const {
        return std::get<T>(actions.at(idx).data);
    }

    template<typename T>
    bool is_append_type(size_t idx) {
        return is_type<action::append>(idx) && std::holds_alternative<T>(get_by_type<action::append>(idx).entry);
    }

    template<typename T>
    const T& get_by_append_type(size_t idx) {
        return std::get<T>(get_by_type<action::append>(idx).entry);
    }

    const temporary_buffer<uint8_t>& get_buff() const noexcept {
        return _buff;
    }

    void reset(disk_offset_t new_disk_aligned_write_offset) noexcept override {
        actions.emplace_back(action::reset {new_disk_aligned_write_offset});
        metadata_to_disk_buffer::reset(new_disk_aligned_write_offset);
    }

    void reset_from_bootstrapped_cluster(disk_offset_t cluster_beg_offset, size_t metadata_end_pos) noexcept override {
        actions.emplace_back(action::reset_from_bootstrapped_cluster {cluster_beg_offset, metadata_end_pos});
        metadata_to_disk_buffer::reset_from_bootstrapped_cluster(cluster_beg_offset, metadata_end_pos);
    }

    future<> flush_to_disk(block_device device) override {
        actions.emplace_back(action::flush_to_disk {});
        return metadata_to_disk_buffer::flush_to_disk(std::move(device));
    }

private:
    void start_new_unflushed_data() noexcept override {
        actions.emplace_back(action::start_new_unflushed_data {});
        metadata_to_disk_buffer::start_new_unflushed_data();
    }

    void prepare_unflushed_data_for_flush() noexcept override {
        actions.emplace_back(action::prepare_unflushed_data_for_flush {});
        metadata_to_disk_buffer::prepare_unflushed_data_for_flush();
    }

public:
    using metadata_to_disk_buffer::bytes_left_after_flush_if_done_now;
    using metadata_to_disk_buffer::append_result;
    using metadata_to_disk_buffer::bytes_left;

    append_result append(const ondisk_next_metadata_cluster& next_metadata_cluster) noexcept override {
        actions.emplace_back(action::append {ondisk_next_metadata_cluster {next_metadata_cluster}});
        return metadata_to_disk_buffer::append(next_metadata_cluster);
    }

    append_result append(const ondisk_create_inode& create_inode) noexcept override {
        actions.emplace_back(action::append {ondisk_create_inode {create_inode}});
        return metadata_to_disk_buffer::append(create_inode);
    }

    append_result append(const ondisk_update_metadata& update_metadata) noexcept override {
        actions.emplace_back(action::append {ondisk_update_metadata {update_metadata}});
        return metadata_to_disk_buffer::append(update_metadata);
    }

    append_result append(const ondisk_delete_inode& delete_inode) noexcept override {
        actions.emplace_back(action::append {ondisk_delete_inode {delete_inode}});
        return metadata_to_disk_buffer::append(delete_inode);
    }

    append_result append(const ondisk_medium_write& medium_write) noexcept override {
        actions.emplace_back(action::append {ondisk_medium_write {medium_write}});
        return metadata_to_disk_buffer::append(medium_write);
    }

    append_result append(const ondisk_large_write& large_write) noexcept override {
        actions.emplace_back(action::append {ondisk_large_write {large_write}});
        return metadata_to_disk_buffer::append(large_write);
    }

    append_result append(const ondisk_large_write_without_mtime& large_write_without_mtime) noexcept override {
        actions.emplace_back(action::append {ondisk_large_write_without_mtime {large_write_without_mtime}});
        return metadata_to_disk_buffer::append(large_write_without_mtime);
    }

    append_result append(const ondisk_truncate& truncate) noexcept override {
        actions.emplace_back(action::append {ondisk_truncate {truncate}});
        return metadata_to_disk_buffer::append(truncate);
    }

    append_result append(const ondisk_mtime_update& mtime_update) noexcept override {
        actions.emplace_back(action::append {ondisk_mtime_update {mtime_update}});
        return metadata_to_disk_buffer::append(mtime_update);
    }

private:
    std::vector<uint8_t> copy_data(const void* data, size_t length) {
        const uint8_t* data_bytes = static_cast<const uint8_t*>(data);
        return std::vector<uint8_t>(data_bytes, data_bytes + length);
    }

public:
    append_result append(const ondisk_small_write_header& small_write, const void* data) noexcept override {
        actions.emplace_back(action::append {ondisk_small_write {
                small_write,
                copy_data(data, small_write.length)
            }});
        return metadata_to_disk_buffer::append(small_write, data);
    }

    append_result append(const ondisk_add_dir_entry_header& add_dir_entry, const void* entry_name) noexcept override {
        actions.emplace_back(action::append {ondisk_add_dir_entry {
                add_dir_entry,
                copy_data(entry_name, add_dir_entry.entry_name_length)
            }});
        return metadata_to_disk_buffer::append(add_dir_entry, entry_name);
    }

    append_result append(const ondisk_create_inode_as_dir_entry_header& create_inode_as_dir_entry,
            const void* entry_name) noexcept override {
        actions.emplace_back(action::append {ondisk_create_inode_as_dir_entry {
                create_inode_as_dir_entry,
                copy_data(entry_name, create_inode_as_dir_entry.entry_name_length)
            }});
        return metadata_to_disk_buffer::append(create_inode_as_dir_entry, entry_name);
    }

    append_result append(const ondisk_rename_dir_entry_header& rename_dir_entry, const void* old_name,
            const void* new_name) noexcept override {
        actions.emplace_back(action::append {ondisk_rename_dir_entry {
                rename_dir_entry,
                copy_data(old_name, rename_dir_entry.entry_old_name_length),
                copy_data(new_name, rename_dir_entry.entry_new_name_length)
            }});
        return metadata_to_disk_buffer::append(rename_dir_entry, old_name, new_name);
    }

    append_result append(const ondisk_delete_dir_entry_header& delete_dir_entry, const void* entry_name) noexcept override {
        actions.emplace_back(action::append {ondisk_delete_dir_entry {
                delete_dir_entry,
                copy_data(entry_name, delete_dir_entry.entry_name_length)
            }});
        return metadata_to_disk_buffer::append(delete_dir_entry, entry_name);
    }

};

std::vector<shared_ptr<metadata_to_disk_buffer>> mock_metadata_to_disk_buffer::created_buffers = {};

} // namespace seastar::fs
