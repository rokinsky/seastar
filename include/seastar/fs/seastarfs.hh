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

using shared_root_map = std::unordered_map<std::string, unsigned>;

template <typename T>
using foreign_shared_lw_ptr = foreign_ptr<lw_shared_ptr<T>>;

class shared_root {
public:
    shared_root_map root;
    shared_mutex lock;
public:
    shared_root() = default;

    shared_root(const shared_root&) = delete;
    shared_root& operator=(const shared_root&) = delete;
    shared_root(shared_root&&) = default;

    future<bool> add_entry(std::string path, unsigned shard_id) {
        const auto can_add = root.find(path) == root.end();

        if (can_add) {
            root[path] = shard_id;
        }

        return make_ready_future<bool>(can_add);
    }

    future<> remove_entry(std::string path) {
        root.erase(path);
        return make_ready_future();
    }

    future<bool> rename_entry(std::string from, std::string to) {
        const auto can_rename = root.find(to) == root.end();

        if (can_rename) {
            const auto shard_id = root[from];
            root.erase(from);
            root[to] = shard_id;
        }

        return make_ready_future<bool>(can_rename);
    }

    future<shared_root_map> pull_entries() {
        return make_ready_future<shared_root_map>(root);
    }

    future<> push_entries(shared_root_map root_shard) {
        root.insert(root_shard.begin(), root_shard.end());
        return make_ready_future();
    }
};

class filesystem {
    lw_shared_ptr<metadata_log> _metadata_log;
    std::optional<foreign_ptr<lw_shared_ptr<shared_root>>> _shared_root;
    shared_root_map _cache_root;
    shared_root_map _local_root;
public:
    filesystem() = default;

    filesystem(const filesystem&) = delete;
    filesystem& operator=(const filesystem&) = delete;
    filesystem(filesystem&&) = default;

    future<> init(std::string device_path);
    future<> init(std::string device_path, foreign_shared_lw_ptr<shared_root> shared_root);

    future<shared_root_map> get_root_entries() {
        return smp::submit_to(_shared_root->get_owner_shard(), [p = _shared_root->get()] () mutable {
            return p->pull_entries();
        });
    }

    future<shared_root_map> get_own_root_entries() {
        _local_root.clear();
        return _metadata_log->iterate_directory("/", [this] (const std::string& path) -> future<stop_iteration> {
            _local_root[path] = this_shard_id();
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).then([this] {
            return _local_root;
        });
    }

    future<> update_cache() {
        return async([this] {
            const auto root = get_root_entries().get0();
            _cache_root.clear();
            _cache_root.insert(root.begin(), root.end());
        });
    }

    future<file> open_file_dma(std::string name, open_flags flags);

    future<> create_directory(std::string name) {
        return _metadata_log->create_directory(std::move(name), file_permissions::default_file_permissions);
    }

    future<> stop();

private:
    future<inode_t> create_file(std::string name);
    future<inode_t> prepare_file(std::string name, open_flags flags);
};

future<sharded<filesystem>> bootfs(std::string device_path);

future<> mkfs(std::string device_path, uint64_t version, unit_size_t cluster_size, unit_size_t alignment,
        inode_t root_directory, uint32_t shards_nb);

} // namespace seastar::fs
