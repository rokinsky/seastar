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

#include "fs/enum.hh"
#include "fs/metadata_log.hh"
#include "fs/units.hh"

#include "seastar/core/distributed.hh"
#include "seastar/fs/file.hh"
#include "seastar/fs/seastarfs.hh"

namespace seastar::fs {

future<> filesystem::init(std::string dev_path) {
    return do_with_device(std::move(dev_path), [=](block_device &device) {
        return bootstrap_record::read_from_disk(device).then([this, &device]
                (bootstrap_record record) {
            const auto shard_id = engine().cpu_id();

            if (shard_id > record.shards_nb() - 1) {
                return device.close();
            }

            const auto shard_info = record.shards_info[shard_id];

            auto cluster_buff = make_shared<metadata_to_disk_buffer>();
            cluster_buff->init(record.cluster_size, record.alignment,
                    cluster_id_to_offset(shard_info.metadata_cluster, record.cluster_size));

            _metadata_log = make_lw_shared<metadata_log>(std::move(device), record.cluster_size, record.alignment,
                    std::move(cluster_buff));
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

future<> distribute_clusters(cluster_range range, cluster_id_t step, std::vector<bootstrap_record::shard_info>& shards_info) {
    return seastar::parallel_for_each(
        boost::irange<uint64_t>(range.beg, range.end, step),
        [&](cluster_id_t metadata_cluster) {
            shards_info.emplace_back(bootstrap_record::shard_info {
                metadata_cluster,
                cluster_range {
                    metadata_cluster,
                    std::min(metadata_cluster + step, range.end)
                }
            });
            return seastar::make_ready_future();
        }
    );
}

future<bootstrap_record> build_bootstrap_record(uint64_t version, unit_size_t alignment, unit_size_t cluster_size,
        inode_t root_directory, uint32_t shards_nb, disk_offset_t block_device_size) {
    const cluster_id_t clusters_nb = div_by_power_of_2(block_device_size, cluster_size);
    const cluster_id_t allowed_clusters_nb = clusters_nb - 1;
    const cluster_id_t clusters_nb_per_shard = (allowed_clusters_nb + shards_nb - 1) / shards_nb;

    return do_with(std::vector<bootstrap_record::shard_info>(), [=](auto& shards_info) {
        return distribute_clusters(cluster_range { 1, clusters_nb }, clusters_nb_per_shard, shards_info).then(
                [=, &shards_info] {
            return bootstrap_record(version, alignment, cluster_size, root_directory, shards_info);
        });
    });
}

future<filesystem> bootfs(std::string dev_path) {
    return do_with(filesystem(), std::move(dev_path), [=](filesystem& fs, std::string& dev_path) {
        return make_ready_future().then([=, &fs, &dev_path]() {
            return fs.init(std::move(dev_path));
        }).then([&fs] {
            return std::move(fs);
        });
    });
}

future<> mkfs(std::string dev_path, uint64_t version, unit_size_t cluster_size, unit_size_t alignment,
        inode_t root_directory, uint32_t shards_nb) {
    return do_with_device(std::move(dev_path), [=](block_device &device) {
        return device.size().then([=](disk_offset_t device_size) {
            return build_bootstrap_record(version, alignment, cluster_size, root_directory, shards_nb, device_size);
        }).then([&](bootstrap_record record) {
            return record.write_to_disk(device);
        }).finally([&device]() {
            return device.close();
        });;
    });
}

} // namespace seastar::fs
