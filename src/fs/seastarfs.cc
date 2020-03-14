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

} // namespace seastar::fs
