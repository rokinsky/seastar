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
#include "cluster.hh"
#include "cluster_allocator.hh"
#include "inode.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/block_device.hh"

#include <boost/crc.hpp>
#include <cstring>
#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <stdexcept>
#include <type_traits>
#include <variant>

namespace seastar::fs {

struct unix_metadata {
    mode_t mode;
    uid_t uid;
    gid_t gid;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
};

enum ondisk_type : uint8_t {
    INVALID = 0,
    NEXT_METADATA_CLUSTER,
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
    cluster_id_t cluster_id; // metadata log continues there
} __attribute__((packed));

struct ondisk_checkpoint {
    // The disk format is as follows:
    // | ondisk_checkpoint | .............................. |
    //                     |             data               |
    //                     |<-- checkpointed_data_length -->|
    //                                                      ^
    //       ______________________________________________/
    //      /
    //    there ends checkpointed data and (next checkpoint begins or metadata in the current cluster end)
    //
    // CRC is calculated from byte sequence | data | checkpointed_data_length |
    // E.g. if the data consist of bytes "abcd" and checkpointed_data_length of bytes "xyz" then the byte sequence would be "abcdxyz"
    uint32_t crc32_code;
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

class to_disk_buffer {
    temporary_buffer<uint8_t> _buff;
    const unit_size_t _alignment;
    disk_offset_t _disk_write_offset; // disk offset that corresponds to _buff.begin()
    range<size_t> _next_write; // range of unflushed bytes in _buff, beg is aligned to _alignment
    size_t _zero_padded_end; // Optimization to skip padding before write if it is already done
    boost::crc_32_type _crc;

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
        assert(aligned_max_size >= sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
        start_new_write();
    }

    // Clears buffer, leaving it in state as if it was just constructed
    void reset(disk_offset_t new_disk_aligned_write_offset) noexcept {
        assert(mod_by_power_of_2(new_disk_aligned_write_offset, _alignment) == 0);
        _disk_write_offset = new_disk_aligned_write_offset;
        _next_write = {0, 0};
        _zero_padded_end = 0;
        start_new_write();
    }

    // Clears buffer, leaving it in state as if it was just constructed
    void reset_from_bootstraped_cluster(disk_offset_t cluster_offset, const uint8_t* cluster_contents, size_t metadata_end_pos) noexcept {
        assert(mod_by_power_of_2(cluster_offset, _alignment) == 0);
        _disk_write_offset = cluster_offset;
        _next_write = {
            metadata_end_pos - mod_by_power_of_2(metadata_end_pos, _alignment),
            metadata_end_pos
        };
        memcpy(_buff.get_write() + _next_write.beg, cluster_contents + _next_write.beg, _next_write.size());
        _zero_padded_end = 0;
        start_new_write();
    }

    // Writes buffered data to disk
    // IMPORTANT: using this buffer before call completes is UB
    future<> flush_to_disk(block_device device) {
        make_checkpoint_vaild();
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

private:
    void start_new_write() noexcept {
        ondisk_type type = INVALID;
        ondisk_checkpoint checkpoint;
        memset(&checkpoint, 0, sizeof(checkpoint));

        assert(bytes_left() >= sizeof(type) + sizeof(checkpoint));
        append_bytes(&type, sizeof(type));
        append_bytes(&checkpoint, sizeof(checkpoint));

        _crc.reset();
    }

    void make_checkpoint_vaild() noexcept {
        // Make checkpoint valid
        ondisk_type checkpoint_type = CHECKPOINT;
        size_t checkpoint_pos = _next_write.beg + sizeof(checkpoint_type);
        ondisk_checkpoint checkpoint;
        checkpoint.checkpointed_data_length = _next_write.end - checkpoint_pos - sizeof(checkpoint);
        _crc.process_bytes(&checkpoint.checkpointed_data_length, sizeof(checkpoint.checkpointed_data_length));
        checkpoint.crc32_code = _crc.checksum();

        memcpy(_buff.get_write() + _next_write.beg, &checkpoint_type, sizeof(checkpoint_type));
        memcpy(_buff.get_write() + checkpoint_pos, &checkpoint, sizeof(checkpoint));
    }

    void append_bytes(const void* data, size_t len) noexcept {
        assert(len <= bytes_left());
        _crc.process_bytes(data, len);
        memcpy(_buff.get_write() + _next_write.end, data, len);
        _next_write.end += len;
    }

public:
    // Returns maximum number of bytes that may be written to buffer without calling reset()
    size_t bytes_left() const noexcept { return _buff.size() - _next_write.end; }

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
};

struct invalid_inode_exception : public std::exception {
    const char* what() const noexcept { return "Invalid inode"; }
};

struct operation_became_invalid_exception : public std::exception {
    const char* what() const noexcept { return "Operation became invalid"; }
};

struct no_more_space_exception : public std::exception {
    const char* what() const noexcept { return "No more space on device"; }
};

struct inode_data_vec {
    file_range data_range; // data spans [beg, end) range of the file

    struct in_mem_data {
        temporary_buffer<uint8_t> data;
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
        // TODO: directory entry cannot contain '/' character --> add checks for that
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
    const unit_size_t _cluster_size;
    const unit_size_t _alignment;
    to_disk_buffer _curr_cluster_buff; // Takes care of writing current cluster of serialized metadata log to device

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

    void write_update(inode_info::file& file, inode_data_vec data_vec);

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
