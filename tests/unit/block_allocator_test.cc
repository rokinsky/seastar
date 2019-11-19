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

#define BOOST_TEST_MODULE fs

#include <boost/test/included/unit_test.hpp>
#include <deque>
#include "fs/block_allocator.hh"
#include <seastar/core/units.hh>

using namespace seastar;


BOOST_AUTO_TEST_CASE(block_allocator) {
    constexpr uint64_t block = 32 * MB;
    constexpr uint64_t blocks_per_shard = 1024;
    std::queue<uint64_t> empty_queue;
    fs::block_allocator empty_ba{empty_queue};
    BOOST_REQUIRE_EQUAL(empty_ba.alloc(), 0);
    std::deque<uint64_t> deq{1 * block, 5 * block, 3 * block, 4 * block, 2 * block};
    fs::block_allocator small_ba(std::queue<uint64_t>{deq});
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[0]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[1]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[2]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[3]);
    small_ba.free(deq[2]);
    small_ba.free(deq[1]);
    small_ba.free(deq[3]);
    small_ba.free(deq[0]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[4]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[2]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[1]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[3]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[0]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), 0);
    small_ba.free(deq[2]);
    small_ba.free(deq[4]);
    small_ba.free(deq[3]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[2]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[4]);
    small_ba.free(deq[2]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[3]);
    small_ba.free(deq[4]);
    BOOST_REQUIRE_EQUAL(small_ba.alloc(), deq[2]);

    std::queue<uint64_t> q;
    for (uint64_t i = 0; i < blocks_per_shard; i++) {
        q.push(i * block);
    }
    fs::block_allocator ordinary_ba(q);
    for (uint64_t i = 0; i < blocks_per_shard; i++) {
        BOOST_REQUIRE_EQUAL(ordinary_ba.alloc(), i * block);
    }
    for (uint64_t i = 0; i < blocks_per_shard; i++) {
        ordinary_ba.free(i * block);
    }
}
