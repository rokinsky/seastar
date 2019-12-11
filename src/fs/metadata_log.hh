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

#include "block_allocator.hh"
#include "bootstrap_record.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/block_device.hh"

#include <bits/stdint-uintn.h>
#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>

namespace seastar::fs {

struct inode_data_vec {
	uint64_t file_offset; // offset of data in file
	uint64_t length; // data length in bytes

	struct in_mem_data {
		char* ptr;
	};

	struct on_disk_data {
		uint64_t device_offset;
	};

	std::variant<in_mem_data, on_disk_data> data_location;
};

class inode_data_vec_comparare {
	struct key {
		uint64_t file_offset;
	};

	static key to_key(const inode_data_vec& dv) noexcept {
		return {dv.file_offset};
	}

	static key to_key(uint64_t file_offset) noexcept { return {file_offset}; }

public:
	template<class A, class B>
	bool operator()(A&& a, B&& b) const noexcept {
		return (to_key(a) < to_key(b));
	};
};

struct unix_metadata {
	uint32_t mode;
	uint32_t uid;
	uint32_t gid;
	uint64_t atime_ns;
	uint64_t mtime_ns;
	uint64_t ctime_ns;
};

struct inode_info {
	uint32_t opened_files_count; // Number of files equivalent to this inode that are currently opened
	unix_metadata metadata;
	std::set<inode_data_vec, inode_data_vec_comparare> data;
};

class metadata_log {
	block_device _device;
	const uint32_t _cluster_size;
	const uint32_t _alignment;
	// To-disk metadata buffer
	basic_sstring<char, uint32_t, 15, false> _unwritten_metadata;
	uint64_t _current_cluster_offset;
	uint64_t _bytes_left_in_current_cluster;

	// In memory metadata
	using inode_t = uint64_t;
	std::map<inode_t, inode_info> _inodes;

public:
	metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment, uint64_t first_metadata_cluster_offset);

private:
	future<> append_unwritten_metadata(char* data, uint32_t len, block_allocator& cluster_alloc);

public:
	future<inode_t> create_file(sstring path, uint32_t mode);

	future<inode_t> create_directory(sstring path, uint32_t mode);

	future<inode_t> open_file(sstring path);

	future<> close_file(inode_t inode);

	future<inode_t> delete_file(sstring path);

	future<size_t> read(inode_t inode, char* buffer, size_t len, const io_priority_class& pc = default_priority_class());

	future<> small_write(inode_t inode, const char* aligned_buffer, size_t aligned_len, const io_priority_class& pc = default_priority_class());


	future<> truncate_file(inode_t inode, uint64_t new_size);

	future<> flush_log();
};

}