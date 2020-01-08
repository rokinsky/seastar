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
#include "inode_info.hh"
#include "metadata_disk_entries.hh"
#include "metadata_to_disk_buffer.hh"

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


class metadata_log {
    block_device _device;
    const unit_size_t _cluster_size;
    const unit_size_t _alignment;
    metadata_to_disk_buffer _curr_cluster_buff; // Takes care of writing current cluster of serialized metadata log to device

    // In memory metadata
    cluster_allocator _cluster_allocator;
    std::map<inode_t, inode_info> _inodes;
    inode_t _root_dir;

    // TODO: for compaction: keep some set(?) of inode_data_vec, so that we can keep track of clusters that have lowest utilization (up-to-date data)
    // TODO: for compaction: keep estimated metadata log size (that would take when written to disk) and the real size of metadata log taken on disk to allow for detecting when compaction

public:
    metadata_log(block_device device, unit_size_t cluster_size, unit_size_t alignment);

    metadata_log(const metadata_log&) = delete;
    metadata_log& operator=(const metadata_log&) = delete;

    future<> bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters);

    void write_update(inode_info::file& file, inode_data_vec new_data_vec);

    // Deletes data vectors that are subset of @p data_range and cuts overlapping data vectors to make them not overlap
    void cut_out_data_range(inode_info::file& file, file_range range);

private:
    future<> append_unwritten_metadata(uint8_t* data, size_t len);

    enum class path_lookup_error {
        NOT_ABSOLUTE, // a path is not absolute
        NO_ENTRY, // no such file or directory
        NOT_DIR, // a component used as a directory in path is not, in fact, a directory
    };

    std::variant<inode_t, path_lookup_error> path_lookup(const sstring& path) const;

    future<inode_t> futurized_path_lookup(const sstring& path) const;

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

    // All disk-related errors will be exposed here // TODO: related to flush, but earlier may be exposed through other methods e.g. create_file()
    future<> flush_log();
};

} // namespace seastar::fs
