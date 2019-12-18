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

#include "fs/block_allocator.hh"
#include "fs/cluster.hh"
#include "fs/metadata_log.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"

#include <boost/crc.hpp>
#include <cstddef>
#include <seastar/core/units.hh>
#include <stdexcept>

namespace seastar::fs {

namespace {

enum ondisk_type : uint8_t {
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

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment) : _device(std::move(device)), _cluster_size(cluster_size), _alignment(alignment), _unwritten_metadata(allocate_aligned_buffer<uint8_t>(cluster_size, alignment)), _cluster_allocator({}) {
	assert(is_power_of_2(alignment));
	assert(cluster_size > 0 and cluster_size % alignment == 0);
}

future<> metadata_log::bootstrap(cluster_id_t first_metadata_cluster_id, cluster_range available_clusters) {
	// Clear state of the metadata log
	_next_write_offset = 0;
	_bytes_left_in_current_cluster = _cluster_size;
	_inodes.clear();
	// TODO: clear directory DAG
	// Bootstrap
	return do_with(first_metadata_cluster_id, std::vector<cluster_id_t>{}, [this, available_clusters, &buff = _unwritten_metadata](auto& cluster_id, auto& taken_clusters) {
		return repeat([this, &cluster_id, &taken_clusters, &buff] {
			return _device.read(cluster_id_to_offset(cluster_id, _cluster_size), buff.get(), _cluster_size).then([this](size_t bytes_read) {
				if (bytes_read != _cluster_size) {
					return make_exception_future(std::runtime_error("Failed to read whole metadata log cluster"));
				}
				return now();

			}).then([this, &cluster_id, &taken_clusters, &buff] {
				taken_clusters.emplace_back(cluster_id);
				// TODO: read the data
				size_t pos = 0;
				ondisk_type entry_type;

				auto load_entry = [this, &buff, &pos](auto& entry) {
					if (pos + sizeof(entry) > _cluster_size) {
						return false;
					}

					memcpy(&entry, &buff[pos], sizeof(entry));
					pos += sizeof(entry);
					return true;
				};

				for (;;) {
					if (not load_entry(entry_type)) {
						break;
					}

					switch (entry_type) {
					case NEXT_METADATA_CLUSTER: {
						ondisk_next_metadata_cluster entry;
						if (not load_entry(entry)) {
							break;
						}
						cluster_id = entry.cluster_id;
					}

					case CHECKPOINT: {
						ondisk_checkpoint entry;
						if (not load_entry(entry)) {
							break;
						}

						// TODO: validate metadata log since last checkpoint and add every entry to the in-memory log
						// TODO: maybe change format a little to be like this:
						// | checkpoint | data... | checkpoint | data... |
						// instead of
						// | data... | checkpoint | data... | checkpoint |
						// this would eliminate the need for parsing disk content twice / keeping "uncommited" parsed disk content in memory before reaching checkpoint
					}

					case INODE:
					case DELETE:
					case SMALL_DATA:
					case MEDIUM_DATA:
					case LARGE_DATA:
					case TRUNCATE:
					case MTIME_UPDATE:
					case DELETE_DIR_ENTRY:
						throw std::runtime_error("Not implemented");

					// default is omitted to make compiler warn if there is an unhandled type
					// unknown type => metadata log inconsistency
					}

					// TODO: rollback
				}

				return stop_iteration::no;
			});
		}).then([this, &available_clusters, &taken_clusters] {
			// Initialize _cluser_allocator
			sort(taken_clusters.begin(), taken_clusters.end(), std::greater{});
			std::queue<cluster_id_t> free_clusters;
			for (auto cid = available_clusters.beg; cid < available_clusters.end; ++cid) {
				if (taken_clusters.size() and taken_clusters.back() == cid) {
					taken_clusters.pop_back();
					continue;
				}
				free_clusters.emplace(cid);
			}
			_cluster_allocator = block_allocator(std::move(free_clusters));
		});
	});
}

} // namespace seastar::fs
