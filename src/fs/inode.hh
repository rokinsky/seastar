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

#include <cstdint>

namespace seastar::fs {

// Last log2(fs_shards_pool_size bits) of the inode number contain the id of shard that owns the inode
using inode_t = uint64_t;

// Obtains shard id of the shard owning @p inode.
//@p fs_shards_pool_size is the number of file system shards rounded up to a power of 2
inline unsigned inode_to_shard_no(inode_t inode, unsigned fs_shards_pool_size) noexcept {
    assert(is_power_of_2(fs_shards_pool_size));
    return mod_by_power_of_2(inode, fs_shards_pool_size);
}

// Returns inode belonging to the shard owning @p shard_previous_inode that is next after @p shard_previous_inode
// (i.e. the lowest inode greater than @p shard_previous_inode belonging to the same shard)
//@p fs_shards_pool_size is the number of file system shards rounded up to a power of 2
inline unsigned shard_next_inode(inode_t shard_previous_inode, unsigned fs_shards_pool_size) noexcept {
    return shard_previous_inode + fs_shards_pool_size;
}

// Returns first inode (lowest by value) belonging to the shard @p shard_id
inline unsigned shard_first_inode(unsigned shard_id) noexcept {
    return shard_id;
}

} // namespace seastar::fs
