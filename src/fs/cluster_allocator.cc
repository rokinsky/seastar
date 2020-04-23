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

#include "fs/cluster.hh"
#include "fs/cluster_allocator.hh"
#include "seastar/core/future.hh"

#include <cassert>
#include <optional>

namespace seastar::fs {

cluster_allocator::cluster_allocator() : _cluster_sem(0) {}

void cluster_allocator::init(std::unordered_set<cluster_id_t> allocated_clusters, std::deque<cluster_id_t> free_clusters) {
    _allocated_clusters = std::move(allocated_clusters);
    _free_clusters = std::move(free_clusters);
    assert(_cluster_sem.waiters() == 0);
    _cluster_sem.consume(_cluster_sem.available_units());
    _cluster_sem.signal(_free_clusters.size());
}

cluster_id_t cluster_allocator::do_alloc() {
    assert(not _free_clusters.empty());

    cluster_id_t cluster_id = _free_clusters.front();
    _free_clusters.pop_front();
    _allocated_clusters.insert(cluster_id);

    return cluster_id;
}

cluster_id_t cluster_allocator::do_free(cluster_id_t cluster_id) {
    assert(_allocated_clusters.count(cluster_id) == 1);
    _free_clusters.emplace_back(cluster_id);
    _allocated_clusters.erase(cluster_id);
    return cluster_id;
}

std::optional<cluster_id_t> cluster_allocator::alloc() {
    if (_free_clusters.empty()) {
        return std::nullopt;
    }

    assert(_cluster_sem.available_units() > 0);
    _cluster_sem.consume(1);

    return do_alloc();
}

future<std::vector<cluster_id_t>> cluster_allocator::alloc_wait(size_t count) {
    return _cluster_sem.wait(count).then([this, count] {
        std::vector<cluster_id_t> cluster_ids;
        for (size_t i = 0; i < count; ++i) {
            cluster_ids.emplace_back(do_alloc());
        }
        return make_ready_future<std::vector<cluster_id_t>>(std::move(cluster_ids));
    });
}

void cluster_allocator::free(cluster_id_t cluster_id) {
    do_free(cluster_id);
    _cluster_sem.signal();
}

void cluster_allocator::free(const std::vector<cluster_id_t>& cluster_ids) {
    for (auto& cluster_id : cluster_ids) {
        do_free(cluster_id);
    }
    _cluster_sem.signal(cluster_ids.size());
}

} // namespace seastar::fs
