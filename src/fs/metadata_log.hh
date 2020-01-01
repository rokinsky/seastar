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

#include "cluster_allocator.hh"
#include "cluster.hh"
#include "inode.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/block_device.hh"

#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <type_traits>
#include <variant>

namespace seastar::fs {

struct invalid_inode_exception : public std::exception {
    const char* what() const noexcept { return "Invalid inode"; }
};

struct unix_metadata {
    mode_t mode;
    uid_t uid;
    gid_t gid;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
};

struct inode_data_vec {
    file_range data_range; // data spans [beg, end) range of the file

    struct in_mem_data {
        lw_shared_ptr<std::unique_ptr<uint8_t[]>> data_store;
        uint8_t* data;
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
        std::map<sstring, inode_t> entries; // entry name => inode
    };

    struct file {
        std::map<file_offset_t, inode_data_vec> data; // file offset => data vector that begins there (data vectors do not overlap)

        file_offset_t size() const noexcept {
            return (data.empty() ? 0 : (--data.end())->second.data_range.end);
        }
    };

    std::variant<directory, file> contents;
};

class metadata_log {
    block_device _device;
    const uint32_t _cluster_size;
    const uint32_t _alignment;
    // To-disk metadata buffer
    using AlignedBuff = std::invoke_result_t<decltype(allocate_aligned_buffer<uint8_t>), size_t, size_t>;
    AlignedBuff _unwritten_metadata;
    disk_offset_t _next_write_offset;
    disk_offset_t _bytes_left_in_current_cluster;

    // In memory metadata
    cluster_allocator _cluster_allocator;
    std::map<inode_t, inode_info> _inodes;

    // TODO: for compaction: keep some set(?) of inode_data_vec, so that we can keep track of clusters that have lowest utilization (up-to-date data)
    // TODO: for compaction: keep estimated metadata log size (that would take when written to disk) and the real size of metadata log taken on disk to allow for detecting when compaction

public:
    metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment);

    metadata_log(const metadata_log&) = delete;
    metadata_log& operator=(const metadata_log&) = delete;

    future<> bootstrap(cluster_id_t first_metadata_cluster_id, cluster_range available_clusters);

private:
    void write_update(inode_info::file& file, inode_data_vec data_vec);

    // Deletes data vectors that are subset of @p data_range and cuts overlapping data vectors to make them not overlap
    void cut_out_data_range(inode_info::file& file, file_range range);

private:
    future<> append_unwritten_metadata(char* data, uint32_t len);


public:
    // TODO: add some way of iterating over a directory
    // TODO: add stat

    // Returns size of the file or throws exception iff @p inode is invalid
    file_offset_t file_size(inode_t inode) const;

    // TODO: what about permissions, uid, gid etc.
    future<inode_t> create_file(sstring path, mode_t mode);

    // TODO: what about permissions, uid, gid etc.
    future<inode_t> create_directory(sstring path, mode_t mode);

    // TODO: what about permissions, uid, gid etc.
    future<inode_t> open_file(sstring path);

    future<> close_file(inode_t inode);

    // Creates name (@p path) for a file (@p inode)
    future<> link(inode_t inode, sstring path);

    // Creates name (@p destination) for a file (not directory) @p source
    future<> link(sstring source, sstring destination);

    future<> unlink_file(inode_t inode);

    // Removes empty directory or unlinks file
    future<> remove(sstring path);

    future<size_t> read(inode_t inode, char* buffer, size_t len, const io_priority_class& pc = default_priority_class());

    future<> small_write(inode_t inode, const char* buffer, size_t len, const io_priority_class& pc = default_priority_class());

    future<> truncate_file(inode_t inode, file_offset_t new_size);

    future<> flush_log();
};

enum ondisk_type : uint8_t {
    NEXT_METADATA_CLUSTER = 1,
    CHECKPOINT,
    CREATE_INODE,
    UPDATE_METADATA,
    DELETE_INODE,
    SMALL_WRITE,
    MEDIUM_WRITE,
    LARGE_WRITE,
    LARGE_WRITE_WITHOUT_MTIME,
    TRUNCATE,
    MTIME_UPDATE,
    ADD_DIR_ENTRY,
    RENAME_DIR_ENTRY,
    DELETE_DIR_ENTRY,
};

struct ondisk_next_metadata_cluster {
    cluster_id_t cluster_id; // metadata log contiues there
} __attribute__((packed));

struct ondisk_checkpoint {
    uint32_t crc32_code; // crc of everything from this checkpoint (inclusive) to the next checkpoint
    unit_size_t checkpointed_data_length;
} __attribute__((packed));

struct ondisk_unix_metadata {
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
} __attribute__((packed));

static_assert(sizeof(decltype(ondisk_unix_metadata::mode)) >= sizeof(decltype(unix_metadata::mode)));
static_assert(sizeof(decltype(ondisk_unix_metadata::uid)) >= sizeof(decltype(unix_metadata::uid)));
static_assert(sizeof(decltype(ondisk_unix_metadata::gid)) >= sizeof(decltype(unix_metadata::gid)));
static_assert(sizeof(decltype(ondisk_unix_metadata::mtime_ns)) >= sizeof(decltype(unix_metadata::mtime_ns)));
static_assert(sizeof(decltype(ondisk_unix_metadata::ctime_ns)) >= sizeof(decltype(unix_metadata::ctime_ns)));

struct ondisk_create_inode {
    inode_t inode;
    uint8_t is_directory;
    ondisk_unix_metadata metadata;
} __attribute__((packed));

struct ondisk_update_metadata {
    inode_t inode;
    ondisk_unix_metadata metadata;
} __attribute__((packed));

struct ondisk_delete_inode {
    inode_t inode;
} __attribute__((packed));

struct ondisk_small_write_header {
    inode_t inode;
    file_offset_t offset;
    uint16_t length;
    decltype(unix_metadata::mtime_ns) mtime_ns;
    // After header comes data
} __attribute__((packed));

struct ondisk_medium_write {
    inode_t inode;
    file_offset_t offset;
    disk_offset_t disk_offset;
    uint32_t length;
    decltype(unix_metadata::mtime_ns) mtime_ns;
} __attribute__((packed));

struct ondisk_large_write {
    inode_t inode;
    file_offset_t offset;
    cluster_id_t data_cluster; // length == cluster_size
    decltype(unix_metadata::mtime_ns) mtime_ns;
} __attribute__((packed));

struct ondisk_large_write_without_mtime {
    inode_t inode;
    file_offset_t offset;
    cluster_id_t data_cluster; // length == cluster_size
} __attribute__((packed));

struct ondisk_truncate {
    inode_t inode;
    file_offset_t size;
    decltype(unix_metadata::mtime_ns) mtime_ns;
} __attribute__((packed));

struct ondisk_mtime_update {
    inode_t inode;
    decltype(unix_metadata::mtime_ns) mtime_ns;
} __attribute__((packed));

struct ondisk_add_dir_entry_header {
    inode_t dir_inode;
    inode_t entry_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));

struct ondisk_rename_dir_entry_header {
    inode_t dir_inode;
    inode_t new_dir_inode;
    uint16_t entry_old_name_length;
    uint16_t entry_new_name_length;
    // After header come: first old_name, then new_name
} __attribute__((packed));

struct ondisk_delete_dir_entry_header {
    inode_t dir_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));

} // namespace seastar::fs
