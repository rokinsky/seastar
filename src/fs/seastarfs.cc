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

#include "fs/metadata_log.hh"
#include "fs/units.hh"

#include "seastar/fs/file.hh"
#include "seastar/fs/seastarfs.hh"

namespace seastar::fs {

using shard_info_vec = std::vector<bootstrap_record::shard_info>;

future<> filesystem::init(std::string device_path) {
    return do_with_device(std::move(device_path), [=](block_device &device) {
        return bootstrap_record::read_from_disk(device).then([this, &device]
                (bootstrap_record record) {
            const auto shard_id = engine().cpu_id();

            if (shard_id > record.shards_nb() - 1) {
                return device.close(); // TODO: or throw exception ? some shards cannot be launched.
            }

            const auto shard_info = record.shards_info[shard_id];

            _metadata_log = make_lw_shared<metadata_log>(std::move(device), record.cluster_size, record.alignment);
            return _metadata_log->bootstrap(record.root_directory, shard_info.metadata_cluster,
                    std::move(shard_info.available_clusters), record.shards_nb(), shard_id);
        });
    });
}

future<> filesystem::stop() {
    if (_metadata_log) {
        return _metadata_log->shutdown();
    } else {
        return make_ready_future();
    }
}

future<file> filesystem::open_file_dma(sstring name, open_flags flags) {
    return prepare_file(std::move(name), flags).then([=](inode_t inode) {
        return file(make_shared<seastarfs_file_impl>(_metadata_log, inode, flags));
    });
}

future<inode_t> filesystem::create_file(sstring name) {
    return _metadata_log->create_file(std::move(name), file_permissions::default_file_permissions);
}

future<inode_t> filesystem::prepare_file(sstring name, open_flags flags) {
    if ((flags & open_flags::create) == open_flags::create) {
        return create_file(std::move(name));
    }

    return _metadata_log->open_file(name).then([=](inode_t inode) {
        if ((flags & open_flags::truncate) == open_flags::truncate) {
            auto file_size = _metadata_log->file_size(inode);
            return _metadata_log->truncate(inode, file_size).then([=] {
                return inode;
            });
        }
        return make_ready_future<inode_t>(inode);
    });
}

cluster_range which_cluster_bucket(cluster_range available_clusters, uint32_t shards_nb, uint32_t shard_id) {
    const cluster_id_t clusters_nb = available_clusters.end - available_clusters.beg;

    assert(available_clusters.end >= available_clusters.beg); /* TODO: invalid range, so throw some exception */
    assert(clusters_nb >= shards_nb); /* TODO: shard should have at least 1 cluster, so throw some exception */

    const uint32_t lower_bucket_size = clusters_nb / shards_nb;
    const uint32_t with_upper_bucket = clusters_nb % shards_nb;

    const cluster_id_t beg = shard_id * lower_bucket_size + std::min(shard_id, with_upper_bucket);
    const cluster_id_t end = (shard_id + 1) * lower_bucket_size + std::min(shard_id + 1, with_upper_bucket);

    return { available_clusters.beg + beg, available_clusters.beg + end };
}

future<> distribute_clusters(cluster_range available_clusters, shard_info_vec& shards_info) {
    const auto all_shards = boost::irange<uint32_t>(0, shards_info.size());
    return parallel_for_each(std::move(all_shards), [available_clusters, &shards_info](uint32_t shard_id) {
        const cluster_range bucket = which_cluster_bucket(available_clusters, shards_info.size(), shard_id);
        shards_info[shard_id] = { bucket.beg, bucket };
        return make_ready_future();
    });
}

future<bootstrap_record> make_bootstrap_record(uint64_t version, unit_size_t alignment, unit_size_t cluster_size,
        inode_t root_directory, uint32_t shards_nb, disk_offset_t block_device_size) {
    constexpr cluster_id_t first_available_cluster = 1; /* TODO: we don't support copies of bootstrap_record yet */
    const cluster_id_t last_available_cluster = offset_to_cluster_id(block_device_size, cluster_size);
    const cluster_range available_clusters = { first_available_cluster, last_available_cluster };

    return do_with(shard_info_vec(shards_nb), [=](auto& shards_info) {
        return distribute_clusters(available_clusters, shards_info).then([=, &shards_info] {
            return bootstrap_record(version, alignment, cluster_size, root_directory, shards_info);
        });
    });
}

future<sharded<filesystem>> bootfs(std::string device_path) {
    return do_with(sharded<filesystem>(), std::move(device_path), [](sharded<filesystem>& fs, std::string& dev_path) {
        return fs.start().then([=, &fs, &dev_path]() {
            return fs.invoke_on_all(&filesystem::init, std::move(dev_path));
        }).then([&fs] {
            return std::move(fs);
        });
    });
}

future<> mkfs(block_device device, uint64_t version, unit_size_t cluster_size, unit_size_t alignment,
        inode_t root_directory, uint32_t shards_nb) {
    return do_with(std::move(device), [=](block_device& device) {
        return device.size().then([=](disk_offset_t device_size) {
            return make_bootstrap_record(version, alignment, cluster_size, root_directory, shards_nb, device_size);
        }).then([&device](bootstrap_record record) {
            return record.write_to_disk(device);
        });
    });
}

future<> mkfs(std::string device_path, uint64_t version, unit_size_t cluster_size, unit_size_t alignment,
        inode_t root_directory, uint32_t shards_nb) {
    return do_with_device(std::move(device_path), [=](block_device& device) {
        return mkfs(device, version, cluster_size, alignment, root_directory, shards_nb).finally([&device]() {
            return device.close();
        });
    });
}

} // namespace seastar::fs
