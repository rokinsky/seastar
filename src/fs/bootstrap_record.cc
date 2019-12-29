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
#include "seastar/core/smp.hh"

#include <boost/crc.hpp>
#include <seastar/core/units.hh>

namespace seastar::fs {

namespace {

constexpr unit_size_t alignment = 4 * KB;
constexpr disk_offset_t bootstrap_record_offset = 0;
constexpr unit_size_t min_sector_size = 512;

constexpr size_t aligned_bootstrap_record_size = (1 + (sizeof(bootstrap_record_disk) - 1) / alignment) * alignment;
constexpr size_t crc_offset = offsetof(bootstrap_record_disk, crc);

inline uint32_t crc32(const void* buff, size_t len) noexcept {
    boost::crc_32_type result;
    result.process_bytes(buff, len);
    return result.checksum();
}

inline bool is_sector_size_valid(unit_size_t sector_size) noexcept {
    return is_power_of_2(sector_size) && sector_size >= min_sector_size;
}

inline bool is_cluster_size_valid(unit_size_t cluster_size, unit_size_t sector_size) noexcept {
    return is_power_of_2(cluster_size) && cluster_size % sector_size == 0;
}

bool is_shards_info_valid(std::vector<bootstrap_record::shard_info> shards_info) {
    // check 1 <= beg <= metadata_cluster < end
    for (const bootstrap_record::shard_info& info : shards_info) {
        if (info.metadata_cluster < info.available_clusters.beg ||
                info.metadata_cluster >= info.available_clusters.end ||
                info.available_clusters.beg < 1) {
            return false;
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
            return false;
        }
    }

    return true;
}

future<size_t> read_exactly(block_device& device, disk_offset_t off, char* buff, size_t size) {
    return do_with((size_t)0, [&device, off, buff, size] (size_t& count) {
        return repeat([&count, &device, off, buff, size] () {
            return device.read(off + count, buff, size - count).then([&count] (size_t ret) {
                count += ret;
                if (ret == 0 || ret % alignment != 0) {
                    return stop_iteration::yes;
                }
                return stop_iteration::no;
            });
        }).then([&count] {
            return count;
        });
    });
}

future<size_t> write_exactly(block_device& device, disk_offset_t off, const char* buff, size_t size) {
    return do_with((size_t)0, [&device, off, buff, size] (size_t& count) {
        return repeat([&count, &device, off, buff, size] () {
            return device.write(off + count, buff, size - count).then([&count] (size_t ret) {
                count += ret;
                if (ret == 0 || ret % alignment != 0) {
                    return stop_iteration::yes;
                }
                return stop_iteration::no;
            });
        }).then([&count] {
            return count;
        });
    });
}

}

future<bootstrap_record> bootstrap_record::read_from_disk(block_device& device) {
    auto bootstrap_record_buff = temporary_buffer<char>::aligned(alignment, aligned_bootstrap_record_size);
    return read_exactly(device, bootstrap_record_offset, bootstrap_record_buff.get_write(), aligned_bootstrap_record_size)
            .then([bootstrap_record_buff = std::move(bootstrap_record_buff)] (size_t ret) {
        if (ret != aligned_bootstrap_record_size) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Error while reading bootstrap record block"));
        }

        bootstrap_record_disk bootstrap_record_disk;
        std::memcpy(&bootstrap_record_disk, bootstrap_record_buff.get(), sizeof(bootstrap_record_disk));

        const uint32_t crc_calc = crc32(bootstrap_record_buff.get(), crc_offset);
        if (crc_calc != bootstrap_record_disk.crc) {
            return make_exception_future<bootstrap_record>(invalid_bootstrap_record("CRC is invalid"));
        }
        if (magic_number != bootstrap_record_disk.magic) {
            return make_exception_future<bootstrap_record>(invalid_bootstrap_record("Magic Number is invalid"));
        }
        if (!is_sector_size_valid(bootstrap_record_disk.sector_size)) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Sector size should be a power of 2 and greater than 512"));
        }
        if (!is_cluster_size_valid(bootstrap_record_disk.cluster_size, bootstrap_record_disk.sector_size)) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Cluster size should be a power of 2 and be divisible by sector size"));
        }
        if (bootstrap_record_disk.shards_nb > max_shards_nb || bootstrap_record_disk.shards_nb == 0) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Shards number is invalid"));
        }

        const std::vector<shard_info> tmp_shards_info(bootstrap_record_disk.shards_info,
                bootstrap_record_disk.shards_info + bootstrap_record_disk.shards_nb);

        if (!is_shards_info_valid(tmp_shards_info)) {
            return make_exception_future<bootstrap_record>(invalid_bootstrap_record("Shards info is invalid"));
        }

        bootstrap_record bootstrap_record_mem(bootstrap_record_disk.version,
                bootstrap_record_disk.sector_size,
                bootstrap_record_disk.cluster_size,
                bootstrap_record_disk.root_directory,
                std::move(tmp_shards_info));

        return make_ready_future<bootstrap_record>(std::move(bootstrap_record_mem));
    });
}

future<> bootstrap_record::write_to_disk(block_device& device) const {
    // initial check
    if (!is_sector_size_valid(sector_size)) {
        return make_exception_future<>(
                invalid_bootstrap_record("Sector size should be a power of 2 and greater than 512"));
    }
    if (!is_cluster_size_valid(cluster_size, sector_size)) {
        return make_exception_future<>(
                invalid_bootstrap_record("Cluster size should be a power of 2 and be divisible by sector size"));
    }
    if (shards_nb() > max_shards_nb || shards_nb() == 0) {
        return make_exception_future<>(
                invalid_bootstrap_record("Shards number is invalid"));
    }
    if (!is_shards_info_valid(shards_info)) {
        return make_exception_future<>(invalid_bootstrap_record("Shards info is invalid"));
    }

    bootstrap_record_disk bootstrap_record_disk;
    // using memset to zero out the whole memory used by bootstrap_record_disk including paddings
    std::memset(&bootstrap_record_disk, 0, sizeof(bootstrap_record_disk));

    // prepare bootstrap_record_disk records
    bootstrap_record_disk.magic = bootstrap_record::magic_number;
    bootstrap_record_disk.version = version;
    bootstrap_record_disk.sector_size = sector_size;
    bootstrap_record_disk.cluster_size = cluster_size;
    bootstrap_record_disk.root_directory = root_directory;
    bootstrap_record_disk.shards_nb = shards_nb();
    std::copy(shards_info.begin(), shards_info.end(), bootstrap_record_disk.shards_info);
    bootstrap_record_disk.crc = crc32(&bootstrap_record_disk, crc_offset);

    auto bootstrap_record_buff = temporary_buffer<char>::aligned(alignment, aligned_bootstrap_record_size);
    std::memcpy(bootstrap_record_buff.get_write(), &bootstrap_record_disk, sizeof(bootstrap_record_disk));
    std::memset(bootstrap_record_buff.get_write() + sizeof(bootstrap_record_disk), 0,
            aligned_bootstrap_record_size - sizeof(bootstrap_record_disk));

    return write_exactly(device, bootstrap_record_offset, bootstrap_record_buff.get(), aligned_bootstrap_record_size)
            .then([bootstrap_record_buff = std::move(bootstrap_record_buff)] (size_t ret) {
        if (ret != aligned_bootstrap_record_size) {
            return make_exception_future<>(
                    invalid_bootstrap_record("Error while writing bootstrap record block to disk"));
        }
        return make_ready_future<>();
    });
}


bool operator==(const bootstrap_record::shard_info& lhs, const bootstrap_record::shard_info& rhs) noexcept {
    return lhs.metadata_cluster == rhs.metadata_cluster && lhs.available_clusters == rhs.available_clusters;
}

bool operator==(const bootstrap_record& lhs, const bootstrap_record& rhs) noexcept {
    return lhs.version == rhs.version && lhs.sector_size == rhs.sector_size &&
            lhs.cluster_size == rhs.cluster_size && lhs.root_directory == rhs.root_directory &&
            lhs.shards_info == rhs.shards_info;
}

}
