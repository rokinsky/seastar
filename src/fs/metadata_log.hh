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
#include "seastar/core/semaphore.hh"
#include "seastar/core/shared_future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/temporary_buffer.hh"

#include <chrono>
#include <cstddef>
#include <exception>
#include <type_traits>
#include <variant>

namespace seastar::fs {

struct fs_exception : public std::exception {
    const char* what() const noexcept override = 0;
};

struct invalid_inode_exception : public fs_exception {
    const char* what() const noexcept override { return "Invalid inode"; }
};

struct operation_became_invalid_exception : public fs_exception {
    const char* what() const noexcept override { return "Operation became invalid"; }
};

struct no_more_space_exception : public fs_exception {
    const char* what() const noexcept override { return "No more space on device"; }
};

struct file_already_exists_exception : public fs_exception {
    const char* what() const noexcept override { return "File already exists"; }
};

struct filename_too_long_exception : public fs_exception {
    const char* what() const noexcept override { return "Filename too long"; }
};

struct path_lookup_exception : public fs_exception {
    const char* what() const noexcept override = 0;
};

struct path_is_not_absolute_exception : public path_lookup_exception {
    const char* what() const noexcept override { return "Path is not absolute"; }
};

struct no_such_file_or_directory_exception : public path_lookup_exception {
    const char* what() const noexcept override { return "No such file or directory"; }
};

struct path_component_not_directory_exception : public path_lookup_exception {
    const char* what() const noexcept override { return "A component used as a directory is not a directory"; }
};

class metadata_log {
    block_device _device;
    const unit_size_t _cluster_size;
    const unit_size_t _alignment;

    // Takes care of writing current cluster of serialized metadata log entries to device
    lw_shared_ptr<metadata_to_disk_buffer> _curr_cluster_buff;
    shared_future<> _previous_flushes = now();
    // After each flush we align the offset at which dumping metadata log is continued. This is very important, as it ensures
    // that consecutive flushes, as their underlying write operations to a block device, do not overlap. If the writes overlapped,
    // it would be possible that they would be written in the reverse order corrupting the on-disk metadata log.
    static constexpr bool align_after_flush = true;

    // In memory metadata
    cluster_allocator _cluster_allocator;
    std::map<inode_t, inode_info> _inodes;
    inode_t _root_dir;
    shard_inode_allocator _inode_allocator;

    // Locks used to ensure metadata consistency while allowing concurrent usage.
    // Whenever one wants to create or delete inode or directory entry _create_or_delete_lock have to be acquired first.
    // Then appropriate unique lock for the inode / dir entry that will appear / disappear and after that the operation should take place.
    // Shared locks should be used only to ensure that an inode / dir entry won't disappear / appear, while some action is performed.
    // All this is to ensure we won't end up with a deadlock and that assumptions can be "acquired" about inode's / dir entry's
    // existence without constant checking for it. We assume that inode / dir entry creation / deletion are not the primary
    // operations that take place, so a global lock can be used for them. Other operations use shared locks to maximize concurrency.
    // E.g.
    // To create file, _create_or_delete_lock is acquired first, then corresponding dir entry is unique-locked and finally
    // file creation is performed (notice that we do not have to acquire shared lock on the directory to which we add entry because
    // we hold global lock, so it won't disappear in the meantime).
    // To make a read or write to a file, a shared lock is acquired on its inode and then the operation is performed.
    // Motivation:
    // Global unique lock ensures that only one operation using unique locks on inode / dir entry takes place at any time.
    // "Local" unique locks ensure that resource is not used by anyone else. Regarding deadlocks, the only place for
    // a deadlock to appear is while unique lock is taken on an inode / dir entry. As far as, the operation holding global lock
    // acquires only one locks it's all good. If two are acquired at the same time, an extreme caution should be used and
    // if possible two unique locks should be avoided.
    value_shared_lock<inode_t> _inode_locks;
    value_shared_lock<std::pair<inode_t, std::string>> _dir_entry_locks;
    semaphore _create_or_delete_lock {1};

    // TODO: for compaction: keep some set(?) of inode_data_vec, so that we can keep track of clusters that have lowest utilization (up-to-date data)
    // TODO: for compaction: keep estimated metadata log size (that would take when written to disk) and the real size of metadata log taken on disk to allow for detecting when compaction

    friend class metadata_log_bootstrap;
    friend class create_file_operation;

public:
    metadata_log(block_device device, unit_size_t cluster_size, unit_size_t alignment);

    metadata_log(const metadata_log&) = delete;
    metadata_log& operator=(const metadata_log&) = delete;

    future<> bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters, fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id);

private:
    void write_update(inode_info::file& file, inode_data_vec new_data_vec);

    // Deletes data vectors that are subset of @p data_range and cuts overlapping data vectors to make them not overlap
    void cut_out_data_range(inode_info::file& file, file_range range);

    void memory_only_create_inode(inode_t inode, bool is_directory, unix_metadata metadata);
    void memory_only_update_metadata(inode_t inode, unix_metadata metadata);
    void memory_only_delete_inode(inode_t inode);
    void memory_only_small_write(inode_t inode, disk_offset_t offset, temporary_buffer<uint8_t> data);
    void memory_only_update_mtime(inode_t inode, decltype(unix_metadata::mtime_ns) mtime_ns);
    void memory_only_truncate(inode_t inode, disk_offset_t size);
    void memory_only_add_dir_entry(inode_info::directory& dir, inode_t entry_inode, std::string entry_name);
    void memory_only_delete_dir_entry(inode_info::directory& dir, std::string entry_name);

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

    std::variant<inode_t, path_lookup_error> do_path_lookup(const std::string& path) const noexcept;

    future<inode_t> path_lookup(const std::string& path) const;

public:
    template<class Func>
    future<> iterate_directory(const std::string& dir_path, Func func) {
        static_assert(std::is_invocable_r_v<future<>, Func, const std::string&> or std::is_invocable_r_v<future<stop_iteration>, Func, const std::string&>);
        auto convert_func = [&]() -> decltype(auto) {
            if constexpr (std::is_invocable_r_v<future<stop_iteration>, Func, const std::string&>) {
                return std::move(func);
            } else {
                return [func = std::move(func)]() -> future<stop_iteration> {
                    return func().then([] {
                        return stop_iteration::no;
                    });
                };
            }
        };
        return path_lookup(dir_path).then([this, func = convert_func()](inode_t dir_inode) {
            return do_with(std::move(func), std::string {}, [this, dir_inode](auto& func, auto& prev_entry) {
                auto it = _inodes.find(dir_inode);
                if (it == _inodes.end()) {
                    return now(); // Directory disappeared
                }
                if (not it->second.is_directory()) {
                    return make_exception_future(path_component_not_directory_exception());
                }

                return repeat([this, dir_inode, &prev_entry, &func] {
                    auto it = _inodes.find(dir_inode);
                    if (it == _inodes.end()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // Directory disappeared
                    }
                    if (not it->second.is_directory()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // Directory became a file
                    }
                    auto& dir = it->second.get_directory();

                    auto entry_it = dir.entries.upper_bound(prev_entry);
                    if (entry_it == dir.entries.end()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // No more entries
                    }

                    prev_entry = entry_it->first;
                    return func(static_cast<const std::string&>(prev_entry));
                });
            });
        });
    }

    // TODO: add stat

    // Returns size of the file or throws exception iff @p inode is invalid
    file_offset_t file_size(inode_t inode) const;

    future<inode_t> create_file(std::string path, file_permissions perms);

    future<> create_directory(std::string path, file_permissions perms);

    // TODO: what about permissions, uid, gid etc.
    future<inode_t> open_file(std::string path) { return make_ready_future<inode_t>(0); }

    future<> close_file(inode_t inode) { return make_ready_future(); }

    // Creates name (@p path) for a file (@p inode)
    future<> link(inode_t inode, std::string path);

    // Creates name (@p destination) for a file (not directory) @p source
    future<> link(std::string source, std::string destination);

    future<> unlink_file(std::string path);

    // Removes empty directory or unlinks file
    future<> remove(std::string path);

    future<size_t> read(inode_t inode, file_offset_t pos, void* buffer, size_t len, const io_priority_class& pc = default_priority_class ()) { return make_ready_future<size_t>(0); }

    future<size_t> write(inode_t inode, file_offset_t pos, const void* buffer, size_t len, const io_priority_class& pc = default_priority_class()) { return make_ready_future<size_t>(0); }

    future<> truncate_file(inode_t inode, file_offset_t new_size) { return make_ready_future(); }

    // All disk-related errors will be exposed here // TODO: related to flush, but earlier may be exposed through other methods e.g. create_file()
    future<> flush_log() {
        return flush_curr_cluster();
    }
};

} // namespace seastar::fs
