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

#include "fs/bootstrap_record.hh"

#include <boost/crc.hpp>
#include <seastar/core/units.hh>
#include <seastar/core/print.hh>

namespace seastar::fs {

namespace {

constexpr unit_size_t alignment = 4 * KB;
constexpr disk_offset_t bootstrap_record_offset = 0;

constexpr size_t aligned_bootstrap_record_size = (1 + (sizeof(bootstrap_record_disk) - 1) / alignment) * alignment;
constexpr size_t crc_offset = offsetof(bootstrap_record_disk, crc);

inline uint32_t crc32(const void* buff, size_t len) noexcept {
    boost::crc_32_type result;
    result.process_bytes(buff, len);
    return result.checksum();
}

inline void check_sector_size(unit_size_t sector_size) {
    if (!is_power_of_2(sector_size)) {
        throw invalid_bootstrap_record("Sector size should be a power of 2",
                fmt::format("read sector size '{}'", sector_size));
    }
    if (sector_size < bootstrap_record::min_sector_size) {
        throw invalid_bootstrap_record(fmt::format("Sector size should be greater or equal to {}",
                bootstrap_record::min_sector_size), fmt::format("read sector size '{}'", sector_size));
    }
}

inline void check_cluster_size(unit_size_t cluster_size, unit_size_t sector_size) {
    if (!is_power_of_2(cluster_size)) {
        throw invalid_bootstrap_record("Cluster size should be a power of 2",
                fmt::format("read cluster size '{}'", cluster_size));
    }
    if (cluster_size % sector_size != 0) {
        throw invalid_bootstrap_record("Cluster size should be divisible by sector size",
                fmt::format("read sector size '{}', read cluster size '{}'", sector_size, cluster_size));
    }
}

inline void check_shards_number(uint32_t shards_nb) {
    if (shards_nb == 0) {
        throw invalid_bootstrap_record("Shards number should be greater than 0",
                fmt::format("read shards number '{}'", shards_nb));
    }
    if (shards_nb > bootstrap_record::max_shards_nb) {
        throw invalid_bootstrap_record(fmt::format("Shards number should be smaller or equal to {}",
                bootstrap_record::max_shards_nb), fmt::format("read shards number '{}'", shards_nb));
    }
}

void check_shards_info(std::vector<bootstrap_record::shard_info> shards_info) {
    // check 1 <= beg <= metadata_cluster < end
    for (const bootstrap_record::shard_info& info : shards_info) {
        if (info.available_clusters.beg >= info.available_clusters.end) {
            throw invalid_bootstrap_record("Invalid cluster range",
                    fmt::format("read cluster range [{}, {})", info.available_clusters.beg,
                    info.available_clusters.end));
        }
        if (info.available_clusters.beg == 0) {
            throw invalid_bootstrap_record("Range of available clusters should not contain cluster 0",
                    fmt::format("read cluster range [{}, {})", info.available_clusters.beg,
                    info.available_clusters.end));
        }
        if (info.available_clusters.beg > info.metadata_cluster ||
                info.available_clusters.end <= info.metadata_cluster) {
            throw invalid_bootstrap_record("Cluster with metadata should be inside available cluster range",
                    fmt::format("read cluster range [{}, {}), read metadata cluster '{}'", info.available_clusters.beg,
                    info.available_clusters.end, info.metadata_cluster));
        }
    }

    // check that ranges don't overlap
    sort(shards_info.begin(), shards_info.end(),
        [] (const bootstrap_record::shard_info& left,
                const bootstrap_record::shard_info& right) {
            return left.available_clusters.beg < right.available_clusters.beg;
        });
    for (size_t i = 1; i < shards_info.size(); i++) {
        if (shards_info[i - 1].available_clusters.end > shards_info[i].available_clusters.beg) {
            throw invalid_bootstrap_record("Cluster ranges should not overlap",
                    fmt::format("overlaping ranges [{}, {}), [{}, {})", shards_info[i - 1].available_clusters.beg,
                    shards_info[i - 1].available_clusters.end, shards_info[i].available_clusters.beg,
                    shards_info[i].available_clusters.end));
        }
    }
}

}

future<bootstrap_record> bootstrap_record::read_from_disk(block_device& device) {
    auto bootstrap_record_buff = temporary_buffer<char>::aligned(alignment, aligned_bootstrap_record_size);
    return device.read(bootstrap_record_offset, bootstrap_record_buff.get_write(), aligned_bootstrap_record_size)
            .then([bootstrap_record_buff = std::move(bootstrap_record_buff)] (size_t ret) {
        if (ret != aligned_bootstrap_record_size) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Error while reading bootstrap record block",
                    fmt::format("{} bytes read instead of {}", ret, aligned_bootstrap_record_size)));
        }

        bootstrap_record_disk bootstrap_record_disk;
        std::memcpy(&bootstrap_record_disk, bootstrap_record_buff.get(), sizeof(bootstrap_record_disk));

        const uint32_t crc_calc = crc32(bootstrap_record_buff.get(), crc_offset);
        if (crc_calc != bootstrap_record_disk.crc) {
            return make_exception_future<bootstrap_record>(invalid_bootstrap_record("Invalid CRC",
                    fmt::format("expected crc '{}', read crc '{}'", crc_calc, bootstrap_record_disk.crc)));
        }
        if (magic_number != bootstrap_record_disk.magic) {
            return make_exception_future<bootstrap_record>(invalid_bootstrap_record("Invalid magic number",
                    fmt::format("expected magic '{}', read magic '{}'", magic_number, bootstrap_record_disk.magic)));
        }
        check_sector_size(bootstrap_record_disk.sector_size);
        check_cluster_size(bootstrap_record_disk.cluster_size, bootstrap_record_disk.sector_size);
        check_shards_number(bootstrap_record_disk.shards_nb);

        const std::vector<shard_info> tmp_shards_info(bootstrap_record_disk.shards_info,
                bootstrap_record_disk.shards_info + bootstrap_record_disk.shards_nb);

        check_shards_info(tmp_shards_info);

        bootstrap_record bootstrap_record_mem(bootstrap_record_disk.version,
                bootstrap_record_disk.sector_size,
                bootstrap_record_disk.cluster_size,
                bootstrap_record_disk.root_directory,
                std::move(tmp_shards_info));

        return make_ready_future<bootstrap_record>(std::move(bootstrap_record_mem));
    });
}

future<> bootstrap_record::write_to_disk(block_device& device) const {
    // initial checks
    check_sector_size(sector_size);
    check_cluster_size(cluster_size, sector_size);
    check_shards_number(shards_nb());
    check_shards_info(shards_info);

    auto bootstrap_record_buff = temporary_buffer<char>::aligned(alignment, aligned_bootstrap_record_size);
    std::memset(bootstrap_record_buff.get_write(), 0, aligned_bootstrap_record_size);
    bootstrap_record_disk* bootstrap_record_disk = (struct bootstrap_record_disk*)bootstrap_record_buff.get_write();

    // prepare bootstrap_record_disk records
    bootstrap_record_disk->magic = bootstrap_record::magic_number;
    bootstrap_record_disk->version = version;
    bootstrap_record_disk->sector_size = sector_size;
    bootstrap_record_disk->cluster_size = cluster_size;
    bootstrap_record_disk->root_directory = root_directory;
    bootstrap_record_disk->shards_nb = shards_nb();
    std::copy(shards_info.begin(), shards_info.end(), bootstrap_record_disk->shards_info);
    bootstrap_record_disk->crc = crc32(bootstrap_record_disk, crc_offset);

    return device.write(bootstrap_record_offset, bootstrap_record_buff.get(), aligned_bootstrap_record_size)
            .then([bootstrap_record_buff = std::move(bootstrap_record_buff)] (size_t ret) {
        if (ret != aligned_bootstrap_record_size) {
            return make_exception_future<>(
                    invalid_bootstrap_record("Error while writing bootstrap record block to disk",
                    fmt::format("{} bytes written instead of {}", ret, aligned_bootstrap_record_size)));
        }
        return make_ready_future<>();
    });
}

bool operator==(const bootstrap_record& lhs, const bootstrap_record& rhs) noexcept {
    return lhs.version == rhs.version && lhs.sector_size == rhs.sector_size &&
            lhs.cluster_size == rhs.cluster_size && lhs.root_directory == rhs.root_directory &&
            lhs.shards_info == rhs.shards_info;
}

}
