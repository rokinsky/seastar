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

namespace seastar::fs {

struct inode_data_vec {
    file_range data_range; // data spans [beg, end) range of the file

    struct in_mem_data {
        std::unique_ptr<uint8_t[]> data;
    };

    struct on_disk_data {
        file_offset_t device_offset;
    };

    std::variant<in_mem_data, on_disk_data> data_location;
};

struct unix_metadata {
    bool is_directory;
    mode_t mode;
    uid_t uid;
    gid_t gid;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
};

struct inode_info {
    uint32_t opened_files_count = 0; // Number of currently open files referencing to this inode
    uint32_t directories_containing_file = 0;
    unix_metadata metadata;
    std::map<file_offset_t, inode_data_vec> data; // file offset => data vector that begins there (data vectors do not overlap)
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
    // TODO: add directory DAG (may not be tree because of hardlinks...)
    // TODO: for compaction: keep some set(?) of inode_data_vec, so that we can keep track of clusters that have lowest utilization (up-to-date data)
    // TODO: for compaction: keep estimated metadata log size (that would take when written to disk) and the real size of metadata log taken on disk to allow for detecting when compaction

public:
    metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment);

    metadata_log(const metadata_log&) = delete;
    metadata_log& operator=(const metadata_log&) = delete;

    future<> bootstrap(cluster_id_t first_metadata_cluster_id, cluster_range available_clusters);

private:
    void write_update(inode_t inode, inode_data_vec data_vec);

    // Deletes data vectors that are subset of @p data_range and cuts overlapping data vectors to make them not overlap
    void cut_out_data_range(inode_t inode, file_range data_range);

private:
    future<> append_unwritten_metadata(char* data, uint32_t len);


public:
    // TODO: add some way of iterating over a directory

    future<inode_t> create_file(sstring path, mode_t mode);

    future<inode_t> create_directory(sstring path, mode_t mode);

    future<inode_t> open_file(sstring path);

    future<> close_file(inode_t inode);

    future<inode_t> delete_file(sstring path);

    future<size_t> read(inode_t inode, char* buffer, size_t len, const io_priority_class& pc = default_priority_class());

    future<> small_write(inode_t inode, const char* buffer, size_t len, const io_priority_class& pc = default_priority_class());

    future<> truncate_file(inode_t inode, file_offset_t new_size);

    future<> flush_log();
};

} // namespace seastar::fs
