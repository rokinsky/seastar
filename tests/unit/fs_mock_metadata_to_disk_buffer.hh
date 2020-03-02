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
#include <seastar/core/temporary_buffer.hh>
#include <stdint.h>
#include <variant>
#include <vector>

namespace seastar::fs {

struct ondisk_small_write {
    ondisk_small_write_header header;
    temporary_buffer<uint8_t> data;
};

struct ondisk_add_dir_entry {
    ondisk_add_dir_entry_header header;
    temporary_buffer<uint8_t> entry_name;
};

struct ondisk_create_inode_as_dir_entry {
    ondisk_create_inode_as_dir_entry_header header;
    temporary_buffer<uint8_t> entry_name;
};

struct ondisk_delete_dir_entry {
    ondisk_delete_dir_entry_header header;
    temporary_buffer<uint8_t> entry_name;
};

struct ondisk_rename_dir_entry {
    ondisk_rename_dir_entry_header header;
    temporary_buffer<uint8_t> old_name;
    temporary_buffer<uint8_t> new_name;
};

class mock_metadata_to_disk_buffer : public metadata_to_disk_buffer {
public:
    mock_metadata_to_disk_buffer(size_t aligned_max_size, unit_size_t alignment)
        : metadata_to_disk_buffer(aligned_max_size, alignment) {}

    // keep container with all buffers created by virtual_constructor
    inline static thread_local std::vector<shared_ptr<mock_metadata_to_disk_buffer>> created_buffers;

    virtual shared_ptr<metadata_to_disk_buffer> virtual_constructor(size_t aligned_max_size,
            unit_size_t alignment) const override {
        auto new_buffer = make_shared<mock_metadata_to_disk_buffer>(aligned_max_size, alignment);
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

        struct flush_to_disk {};

        using action_data = std::variant<append, flush_to_disk>;

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

    using metadata_to_disk_buffer::init;
    using metadata_to_disk_buffer::init_from_bootstrapped_cluster;

    future<> flush_to_disk([[maybe_unused]] block_device device) override {
        actions.emplace_back(action::flush_to_disk {});
        prepare_unflushed_data_for_flush();

        assert(mod_by_power_of_2(_unflushed_data.beg, _alignment) == 0);
        range real_write = {
            _unflushed_data.beg,
            round_up_to_multiple_of_power_of_2(_unflushed_data.end, _alignment),
        };

        // Make sure the buffer is usable before returning from this function
        _unflushed_data = {real_write.end, real_write.end};
        if (bytes_left() > 0) {
            start_new_unflushed_data();
        }

        return now();
    }

private:
    void move_bytes_count(size_t len) {
        assert(len <= bytes_left());
        _unflushed_data.end += len;
    }

    void start_new_unflushed_data() noexcept override {
        if (bytes_left() < sizeof(ondisk_type) + sizeof(ondisk_checkpoint) + sizeof(ondisk_type) +
                sizeof(ondisk_next_metadata_cluster)) {
            assert(bytes_left() == 0); // alignment has to be big enough to hold checkpoint and next_metadata_cluster
            return; // No more space
        }
        move_bytes_count(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    }

    void prepare_unflushed_data_for_flush() noexcept override {}

    append_result check_and_move_bytes_count(size_t data_len) {
        if (not fits_for_append(data_len)) {
            return TOO_BIG;
        }
        move_bytes_count(data_len);
        return APPENDED;
    }

public:
    using metadata_to_disk_buffer::bytes_left_after_flush_if_done_now;
    using metadata_to_disk_buffer::bytes_left;
    using metadata_to_disk_buffer::append_result;

    append_result append(const ondisk_next_metadata_cluster& next_metadata_cluster) noexcept override {
        size_t len = get_ondisk_entry_size(next_metadata_cluster);
        if (bytes_left() < len) {
            return TOO_BIG;
        }
        actions.emplace_back(action::append {ondisk_next_metadata_cluster {next_metadata_cluster}});
        move_bytes_count(len);
        return APPENDED;
    }

    append_result append(const ondisk_create_inode& create_inode) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(create_inode));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_create_inode {create_inode}});
        }
        return ret;
    }

    append_result append(const ondisk_update_metadata& update_metadata) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(update_metadata));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_update_metadata {update_metadata}});
        }
        return ret;
    }

    append_result append(const ondisk_delete_inode& delete_inode) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(delete_inode));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_delete_inode {delete_inode}});
        }
        return ret;
    }

    append_result append(const ondisk_medium_write& medium_write) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(medium_write));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_medium_write {medium_write}});
        }
        return ret;
    }

    append_result append(const ondisk_large_write& large_write) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(large_write));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_large_write {large_write}});
        }
        return ret;
    }

    append_result append(const ondisk_large_write_without_mtime& large_write_without_mtime) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(large_write_without_mtime));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_large_write_without_mtime {large_write_without_mtime}});
        }
        return ret;
    }

    append_result append(const ondisk_truncate& truncate) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(truncate));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_truncate {truncate}});
        }
        return ret;
    }

    append_result append(const ondisk_mtime_update& mtime_update) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(mtime_update));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_mtime_update {mtime_update}});
        }
        return ret;
    }

private:
    temporary_buffer<uint8_t> copy_data(const void* data, size_t length) {
        return temporary_buffer<uint8_t>(static_cast<const uint8_t*>(data), length);
    }

public:
    append_result append(const ondisk_small_write_header& small_write, const void* data) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(small_write));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_small_write {
                    small_write,
                    copy_data(data, small_write.length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_add_dir_entry_header& add_dir_entry, const void* entry_name) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(add_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_add_dir_entry {
                    add_dir_entry,
                    copy_data(entry_name, add_dir_entry.entry_name_length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_create_inode_as_dir_entry_header& create_inode_as_dir_entry,
            const void* entry_name) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(create_inode_as_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_create_inode_as_dir_entry {
                    create_inode_as_dir_entry,
                    copy_data(entry_name, create_inode_as_dir_entry.entry_name_length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_rename_dir_entry_header& rename_dir_entry, const void* old_name,
            const void* new_name) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(rename_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_rename_dir_entry {
                    rename_dir_entry,
                    copy_data(old_name, rename_dir_entry.entry_old_name_length),
                    copy_data(new_name, rename_dir_entry.entry_new_name_length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_delete_dir_entry_header& delete_dir_entry, const void* entry_name) noexcept override {
        append_result ret = check_and_move_bytes_count(get_ondisk_entry_size(delete_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_delete_dir_entry {
                    delete_dir_entry,
                    copy_data(entry_name, delete_dir_entry.entry_name_length)
                }});
        }
        return ret;
    }

};

} // namespace seastar::fs
