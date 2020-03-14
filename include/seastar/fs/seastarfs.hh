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
 * Copyright (C) 2020 ScyllaDB
 */

#pragma once

#include "fs/bootstrap_record.hh"
#include "fs/cluster.hh"
#include "fs/metadata_log.hh"

#include "seastar/core/future.hh"
#include "seastar/core/sharded.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/units.hh"

namespace seastar::fs {

class filesystem {
    lw_shared_ptr<metadata_log> _metadata_log;
public:
    filesystem() = default;

    filesystem(const filesystem&) = delete;
    filesystem& operator=(const filesystem&) = delete;
    filesystem(filesystem&&) = default;

    future<> init(std::string device_path);

    future<file> open_file_dma(sstring name, open_flags flags);

    future<> stop();

private:
    future<inode_t> create_file(sstring name);
    future<inode_t> prepare_file(sstring name, open_flags flags);
};

future<sharded<filesystem>> bootfs(std::string device_path);

future<> mkfs(block_device device, uint64_t version, unit_size_t cluster_size, unit_size_t alignment,
        inode_t root_directory, uint32_t shards_nb);

future<> mkfs(std::string device_path, uint64_t version, unit_size_t cluster_size, unit_size_t alignment,
        inode_t root_directory, uint32_t shards_nb);

} // namespace seastar::fs
