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

#include "fs/cluster.hh"
#include "fs/cluster_allocator.hh"
#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_to_disk_buffer.hh"
#include "fs/units.hh"
#include "fs/unix_metadata.hh"
#include "fs/value_shared_lock.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/sstring.hh"
#include "seastar/core/temporary_buffer.hh"

#include <chrono>
#include <cstddef>
#include <exception>
#include <type_traits>
#include <variant>

namespace seastar::fs {

struct invalid_inode_exception : public std::exception {
    const char* what() const noexcept { return "Invalid inode"; }
};

struct operation_became_invalid_exception : public std::exception {
    const char* what() const noexcept { return "Operation became invalid"; }
};

struct no_more_space_exception : public std::exception {
    const char* what() const noexcept { return "No more space on device"; }
};

struct file_already_exists_exception : public std::exception {
    const char* what() const noexcept { return "File already exists"; }
};

struct filename_too_long_exception : public std::exception {
    const char* what() const noexcept { return "Filename too long"; }
};

struct path_is_not_absolute : public std::exception {
    const char* what() const noexcept { return "Path is not absolute"; }
};

struct no_such_file_or_directory : public std::exception {
    const char* what() const noexcept { return "No such file or directory"; }
};

struct path_component_not_directory : public std::exception {
    const char* what() const noexcept { return "A component used as a directory is not a directory"; }
};

class metadata_log {
    block_device _device;
    const unit_size_t _cluster_size;
    const unit_size_t _alignment;
    // Takes care of writing current cluster of serialized metadata log entries to device
    lw_shared_ptr<metadata_to_disk_buffer> _curr_cluster_buff;
    shared_future<> _previous_flushes = now();

    // In memory metadata
    cluster_allocator _cluster_allocator;
    std::map<inode_t, inode_info> _inodes;
    value_shared_lock<inode_t> _inode_locks;
    value_shared_lock<std::pair<inode_t, sstring>> _dir_entry_locks;
    inode_t _root_dir;
    shard_inode_allocator _inode_allocator;

    // TODO: for compaction: keep some set(?) of inode_data_vec, so that we can keep track of clusters that have lowest utilization (up-to-date data)
    // TODO: for compaction: keep estimated metadata log size (that would take when written to disk) and the real size of metadata log taken on disk to allow for detecting when compaction

    friend class metadata_log_bootstrap;
    friend class create_file_operation;

public:
    metadata_log(block_device device, unit_size_t cluster_size, unit_size_t alignment);

    metadata_log(const metadata_log&) = delete;
    metadata_log& operator=(const metadata_log&) = delete;

    future<> bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters, fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id);

    void write_update(inode_info::file& file, inode_data_vec new_data_vec);

    // Deletes data vectors that are subset of @p data_range and cuts overlapping data vectors to make them not overlap
    void cut_out_data_range(inode_info::file& file, file_range range);

private:
    void memory_only_create_inode(inode_t inode, bool is_directory, unix_metadata metadata);

    void memory_only_delete_inode(inode_t inode);

    void memory_only_small_write(inode_t inode, disk_offset_t offset, temporary_buffer<uint8_t> data);

    void memory_only_update_mtime(inode_t inode, decltype(unix_metadata::mtime_ns) mtime_ns);

    void memory_only_truncate(inode_t inode, disk_offset_t size);

    void memory_only_add_dir_entry(inode_info::directory& dir, inode_t entry_inode, sstring entry_name);

    void memory_only_delete_dir_entry(inode_info::directory& dir, sstring entry_name);

    void schedule_curr_cluster_flush();

    future<> flush_curr_cluster();

    future<> flush_curr_cluster_and_change_it_to_new_one();

    template<class... Args>
    future<> append_ondisk_entry(Args&&... args) {
        return repeat([this, args...] {
            auto append_res = _curr_cluster_buff->append(args...);
            switch (append_res) {
            case metadata_to_disk_buffer::APPENDED:
                return make_ready_future<stop_iteration>(stop_iteration::yes);

            case metadata_to_disk_buffer::TOO_BIG: {
                return flush_curr_cluster_and_change_it_to_new_one().then([] {
                    return stop_iteration::no;
                });
            }
            }

            __builtin_unreachable();
        });
    }

    enum class path_lookup_error {
        NOT_ABSOLUTE, // a path is not absolute
        NO_ENTRY, // no such file or directory
        NOT_DIR, // a component used as a directory in path is not, in fact, a directory
    };

    std::variant<inode_t, path_lookup_error> path_lookup(const sstring& path) const;

    future<inode_t> futurized_path_lookup(const sstring& path) const;

public:
    template<class Func>
    future<> iterate_directory(const sstring& dir_path, Func func) {
        static_assert(std::is_invocable_r_v<future<>, Func, const sstring&> or std::is_invocable_r_v<future<stop_iteration>, Func, const sstring&>);
        auto convert_func = [&]() -> decltype(auto) {
            if constexpr (std::is_invocable_r_v<future<stop_iteration>, Func, const sstring&>) {
                return std::move(func);
            } else {
                return [func = std::move(func)]() -> future<stop_iteration> {
                    return func().then([] {
                        return stop_iteration::no;
                    });
                };
            }
        };
        return futurized_path_lookup(dir_path).then([this, func = convert_func()](inode_t dir_inode) {
            return do_with(std::move(func), sstring {}, [this, dir_inode](auto& func, auto& prev_entry) {
                auto it = _inodes.find(dir_inode);
                if (it == _inodes.end()) {
                    return now(); // Directory disappeared
                }
                if (not std::holds_alternative<inode_info::directory>(it->second.contents)) {
                    return make_exception_future(path_component_not_directory());
                }

                return repeat([this, dir_inode, &prev_entry, &func] {
                    auto it = _inodes.find(dir_inode);
                    if (it == _inodes.end()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // Directory disappeared
                    }
                    if (not std::holds_alternative<inode_info::directory>(it->second.contents)) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // Directory became a file (should not happen, but safe-check)
                    }
                    auto& dir = std::get<inode_info::directory>(it->second.contents);

                    auto entry_it = dir.entries.upper_bound(prev_entry);
                    if (entry_it == dir.entries.end()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // No more entries
                    }

                    prev_entry = entry_it->first;
                    return func(static_cast<const sstring&>(prev_entry));
                });
            });
        });
    }

    // TODO: add stat

    // Returns size of the file or throws exception iff @p inode is invalid
    file_offset_t file_size(inode_t inode) const;

    future<inode_t> create_file(sstring path, file_permissions perms);

    future<> create_directory(sstring path, file_permissions perms);

    // TODO: what about permissions, uid, gid etc.
    future<inode_t> open_file(sstring path) { return make_ready_future<inode_t>(0); }

    future<> close_file(inode_t inode) { return make_ready_future(); }

    // Creates name (@p path) for a file (@p inode)
    future<> link(inode_t inode, sstring path);

    // Creates name (@p destination) for a file (not directory) @p source
    future<> link(sstring source, sstring destination);

    future<> unlink_file(inode_t inode);

    // Removes empty directory or unlinks file
    future<> remove(sstring path);

    future<size_t> read(inode_t inode, file_offset_t pos, void* buffer, size_t len, const io_priority_class& pc = default_priority_class ()) { return make_ready_future<size_t>(0); }

    future<size_t> write(inode_t inode, file_offset_t pos, const void* buffer, size_t len, const io_priority_class& pc = default_priority_class()) { return make_ready_future<size_t>(0); }

    future<> truncate_file(inode_t inode, file_offset_t new_size) { return make_ready_future(); }

    // All disk-related errors will be exposed here // TODO: related to flush, but earlier may be exposed through other methods e.g. create_file()
    future<> flush_log() {
        return flush_curr_cluster();
    }
};

} // namespace seastar::fs
