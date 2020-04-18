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

#include "fs/inode.hh"
#include "fs/units.hh"

#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/file.hh"
#include "seastar/core/thread.hh"
#include "seastar/core/units.hh"
#include "seastar/fs/file.hh"
#include "seastar/fs/seastarfs.hh"
#include "seastar/fs/temporary_file.hh"
#include "seastar/testing/thread_test_case.hh"

#include "fs_mock_block_device.hh"

using namespace seastar;
using namespace fs;

constexpr auto device_path = "/tmp/seastarfs";
constexpr auto device_size = 16 * MB;

SEASTAR_THREAD_TEST_CASE(parallel_read_write_test) {
    const auto tf = temporary_file(device_path);
    auto f = fs::open_file_dma(tf.path(), open_flags::rw).get0();
    static auto alignment = f.memory_dma_alignment();

    parallel_for_each(boost::irange<off_t>(0, device_size / alignment), [&f](auto i) {
        auto wbuf = allocate_aligned_buffer<unsigned char>(alignment, alignment);
        std::fill(wbuf.get(), wbuf.get() + alignment, i);
        auto wb = wbuf.get();

        return f.dma_write(i * alignment, wb, alignment).then(
            [&f, i, wbuf = std::move(wbuf)](auto ret) mutable {
                BOOST_REQUIRE_EQUAL(ret, alignment);
                auto rbuf = allocate_aligned_buffer<unsigned char>(alignment, alignment);
                auto rb = rbuf.get();
                return f.dma_read(i * alignment, rb, alignment).then(
                    [f, rbuf = std::move(rbuf), wbuf = std::move(wbuf)](auto ret) {
                        BOOST_REQUIRE_EQUAL(ret, alignment);
                        BOOST_REQUIRE(std::equal(rbuf.get(), rbuf.get() + alignment, wbuf.get()));
                    });
            });
    }).wait();

    f.flush().wait();
    f.close().wait();
}

constexpr uint64_t version = 1;
constexpr unit_size_t cluster_size = 1 * MB;
constexpr unit_size_t alignment = 4 * KB;
constexpr inode_t root_directory = 0;

BOOST_TEST_DONT_PRINT_LOG_VALUE(bootstrap_record)



class B {
    bool _value;
public:
    B() : _value(false) { }
    bool value() { return _value; }
};

class A {
    std::optional<foreign_ptr<lw_shared_ptr<B>>> shared_b;
public:
    A() = default;

    future<> stop() { return make_ready_future(); }
    friend future<> init(foreign_ptr<lw_shared_ptr<B>> foreign_b);
};

future<> init(foreign_ptr<lw_shared_ptr<B>> foreign_b) {
    //seastar_logger.info("address {}", &foreign_b.release());

    return smp::submit_to(foreign_b.get_owner_shard(), [p=foreign_b.get()] {//[foreign_b=std::move(foreign_b)] {
//        seastar_logger.info("address {}", &*p);
//        seastar_logger.info("value {}", p->value());
        return make_ready_future<>();
    });
}

SEASTAR_THREAD_TEST_CASE(tteesdts) {
    sharded<A> a;
    lw_shared_ptr<B> shared_b = make_lw_shared<B>();

    a.start().wait();

//    a.invoke_on_all([foreign_b = make_foreign(shared_b)] (auto& a) mutable {
//        seastar_logger.info("hej: {}", true);
//        return make_ready_future();
//    }).wait();

//    seastar_logger.info("address {}", &*shared_b);

    parallel_for_each(smp::all_cpus(), [shared_b] (shard_id id) {
        return smp::submit_to(id, [foreign_b = make_foreign(std::move(shared_b))] () mutable {
            return init(std::move(foreign_b));
        });
    }).wait();

//    parallel_for_each(smp::all_cpus(), [&a, shared_b] (shard_id id) {
//
//        return a.invoke_on(id, [foreign_b = make_foreign(std::move(shared_b))] (auto& a) mutable {
//            return a.init(std::move(foreign_b));
//        });
//    }).wait();

    a.stop().wait();
}

SEASTAR_THREAD_TEST_CASE(valid_path_mkfs_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    const std::vector<bootstrap_record::shard_info> shards_info({{1,  {1,  device_size / MB}}});

    const bootstrap_record write_record(
        version, alignment, cluster_size, root_directory, shards_info
    );

    auto dev = open_block_device(tf.path()).get0();

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, write_record.shards_nb()).wait();

    const auto read_record = bootstrap_record::read_from_disk(dev).get0();
    dev.close().wait();

    BOOST_REQUIRE_EQUAL(write_record, read_record);
}

SEASTAR_THREAD_TEST_CASE(valid_cluster_distribution_mkfs_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    const std::vector<bootstrap_record::shard_info> shards_info({
        { 1,  { 1,  4  } }, // 3
        { 4,  { 4,  7  } }, // 3
        { 7,  { 7,  10 } }, // 3
        { 10, { 10, 12 } }, // 2
        { 12, { 12, 14 } }, // 2
        { 14, { 14, 16 } }, // 2
    });

    const bootstrap_record write_record(version, alignment, cluster_size, root_directory, shards_info);

    auto dev = open_block_device(tf.path()).get0();

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, write_record.shards_nb()).wait();

    const auto read_record = bootstrap_record::read_from_disk(dev).get0();
    dev.close().wait();

    BOOST_REQUIRE_EQUAL(write_record, read_record);
}

SEASTAR_THREAD_TEST_CASE(valid_basic_bootfs_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, smp::count).wait();

    auto fs = fs::bootfs(tf.path()).get0();

    fs.local().create_directory("/test1").wait();
    fs.local().create_directory("/test2").wait();
    fs.local().create_directory("/test3").wait();

    shared_root_map entries = fs.local().get_own_root_entries().get0();

    for (const auto& [entry, shard_id] : entries) {
        seastar_logger.info("TEST: {} {}", entry, shard_id);
    }

    fs.stop().wait();

    fs = fs::bootfs(tf.path()).get0();

    entries = fs.local().get_own_root_entries().get0();

    for (const auto& [entry, shard_id] : entries) {
        seastar_logger.info("TEST: {} {}", entry, shard_id);
    }

    fs.stop().wait();
}

SEASTAR_THREAD_TEST_CASE(valid_basic_open_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    const std::vector<bootstrap_record::shard_info> shards_info({{1,  {1,  device_size / MB}}});

    const bootstrap_record write_record(version, alignment, cluster_size, root_directory, shards_info);

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, write_record.shards_nb()).wait();

    //auto fs = filesystem();
    //fs.init(tf.path()).get0();

    // TODO: doesn't work
    // auto file = fs.open_file_dma("test", open_flags::create).get0();
    // file.close().wait();

    //fs.stop().wait();
}
