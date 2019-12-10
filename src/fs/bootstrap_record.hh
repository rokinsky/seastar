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

#include <seastar/core/reactor.hh>
#include <seastar/fs/block_device.hh>

namespace seastar::fs {

class bootstrap_record {
public:
    uint64_t _magic; /// magic number, specific for that file system
    uint64_t _version; /// bootstrap_record's version
    uint32_t _sector_size; /// sector size in bytes
    uint32_t _cluster_size; /// cluster size in bytes
    uint32_t _root_id; /// root inode number
    uint32_t _shards_nb; /// number of file system shards
    std::vector<uint64_t> _metadata_ptr; /// pointers to metadatalog for each shard

private:
    bootstrap_record(uint64_t magic, uint64_t version, uint32_t sector_size, uint32_t cluster_size,
            uint32_t root_id, uint8_t shards_nb, std::vector<uint64_t> metadata_ptr)
        : _magic(magic), _version(version), _sector_size(sector_size), _cluster_size(cluster_size),  _root_id(root_id)
        , _shards_nb(shards_nb), _metadata_ptr(std::move(metadata_ptr)) {}

public:
    static future<bootstrap_record> read_from_disk(const block_device& device);
    static future<bootstrap_record> create_from_memory();
    future<> write_to_disk(const block_device& device);
};

}
