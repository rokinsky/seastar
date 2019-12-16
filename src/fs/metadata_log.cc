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

#include "fs/metadata_log.hh"
#include <endian.h>
#include <boost/crc.hpp>
#include <seastar/core/units.hh>

namespace seastar::fs {

namespace {

enum ondisk_type {
	INODE = 1,
	DELETE = 2,
	SMALL_DATA = 3,
	MEDIUM_DATA = 4,
	LARGE_DATA = 5,
	TRUNCATE = 6,
	MTIME_UPDATE = 7,
	NEXT_METADATA_CLUSTER = 8,
	CHECKPOINT = 9,
	DELETE_DIR_ENTRY = 10,
};

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

struct ondisk_inode {
	inode_t inode;
	ondisk_unix_metadata metadata;
} __attribute__((packed));

struct ondisk_delete {
	inode_t inode;
} __attribute__((packed));

struct ondisk_small_data_header {
	inode_t inode;
	offset_t offset;
	uint16_t length;
	// After header comes data
} __attribute__((packed));

struct ondisk_medium_data {
	inode_t inode;
	offset_t offset;
	offset_t disk_offset;
	uint32_t length;
} __attribute__((packed));

struct ondisk_large_data {
	inode_t inode;
	offset_t offset;
	offset_t disk_offset; // aligned to cluster size
	uint32_t length; // aligned to cluster size
} __attribute__((packed));

struct ondisk_truncate {
	inode_t inode;
	offset_t size;
} __attribute__((packed));

struct ondisk_mtime_update {
	inode_t inode;
	decltype(unix_metadata::mtime_ns) mtime_ns;
} __attribute__((packed));

struct ondisk_next_metadata_cluster {
	cluster_id_t cluster_id; // metadata log contiues there
} __attribute__((packed));

struct ondisk_checkpoint {
	uint32_t crc32_code; // crc of everything since last checkpoint (including this one)
} __attribute__((packed));

struct ondisk_delete_dir_entry {
	inode_t inode;
	offset_t offset;
	uint32_t length;
} __attribute__((packed));

} // namespace

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment) : _device(std::move(device)), _cluster_size(cluster_size), _alignment(alignment) {
	assert(is_power_of_2(alignment));
	assert(cluster_size > 0 and cluster_size % alignment == 0);
}

future<> metadata_log::bootstrap(cluster_id_t first_metadata_cluster_id, cluster_range available_clusters) {
	// TODO:
	return now();
}

} // namespace seastar::fs
