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

#include "cluster.hh"
#include "inode.hh"
#include "unix_metadata.hh"

namespace seastar::fs {

struct ondisk_unix_metadata {
    uint32_t perms;
    uint32_t uid;
    uint32_t gid;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
} __attribute__((packed));

static_assert(sizeof(decltype(ondisk_unix_metadata::perms)) >= sizeof(decltype(unix_metadata::perms)));
static_assert(sizeof(decltype(ondisk_unix_metadata::uid)) >= sizeof(decltype(unix_metadata::uid)));
static_assert(sizeof(decltype(ondisk_unix_metadata::gid)) >= sizeof(decltype(unix_metadata::gid)));
static_assert(sizeof(decltype(ondisk_unix_metadata::mtime_ns)) >= sizeof(decltype(unix_metadata::mtime_ns)));
static_assert(sizeof(decltype(ondisk_unix_metadata::ctime_ns)) >= sizeof(decltype(unix_metadata::ctime_ns)));

inline unix_metadata ondisk_metadata_to_metadata(const ondisk_unix_metadata& ondisk_metadata) noexcept {
    unix_metadata res;
    static_assert(sizeof(ondisk_metadata) == 28, "metadata size changed: check if below assignments need update");
    res.perms = static_cast<file_permissions>(ondisk_metadata.perms);
    res.uid = ondisk_metadata.uid;
    res.gid = ondisk_metadata.gid;
    res.mtime_ns = ondisk_metadata.mtime_ns;
    res.ctime_ns = ondisk_metadata.ctime_ns;
    return res;
}

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
