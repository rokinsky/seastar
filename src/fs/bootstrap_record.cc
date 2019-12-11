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
#include <endian.h>
#include <boost/crc.hpp>
#include <seastar/core/units.hh>

namespace seastar::fs {

namespace {

constexpr size_t alignment = 4 * KB;
constexpr uint64_t boot_record_offset = 0;
constexpr uint32_t max_shards_nb = 500;

struct bootstrap_record_disk {
    uint64_t _magic;
    uint64_t _version;
    uint32_t _sector_size;
    uint32_t _cluster_size;
    uint32_t _root_id;
    uint32_t _shards_nb;
    uint64_t _metadata_ptr[max_shards_nb];
    uint32_t _crc;
    char _padding[4]; // TODO(fs): try remove that padding, maybe pack?
};

constexpr size_t aligned_boot_record_size = 4096;
constexpr size_t crc_offset = offsetof(bootstrap_record_disk, _crc);

uint32_t crc32(const char* buff, size_t len) {
    boost::crc_32_type result;
    result.process_bytes(buff, len);
    return result.checksum();
}

}

future<bootstrap_record> bootstrap_record::read_from_disk(block_device& device) {
    auto boot_record_buff = temporary_buffer<char>::aligned(alignment, aligned_boot_record_size);
    return device.read(boot_record_offset, boot_record_buff.get_write(), aligned_boot_record_size)
            .then([boot_record_buff = std::move(boot_record_buff)] (size_t ret) {
        // TODO(fs): handle case where ret != boot_cluster_size
        bootstrap_record_disk boot_record_disk;
        std::memcpy(&boot_record_disk, boot_record_buff.get(), sizeof(boot_record_disk));

        uint32_t crc_calc = crc32(boot_record_buff.get(), crc_offset);
        uint32_t crc_read = be32toh(boot_record_disk._crc);

        // TODO(fs): better exceptions, future exceptions
        if (crc_calc != crc_read)
            throw std::runtime_error("invalid crc, calculated: " +
                    to_sstring(crc_calc) + ", read: " + to_sstring(crc_read));

        bootstrap_record boot_record_mem;
        boot_record_mem._magic = be64toh(boot_record_disk._magic);
        boot_record_mem._version = be64toh(boot_record_disk._version);
        boot_record_mem._sector_size = be32toh(boot_record_disk._sector_size);
        boot_record_mem._cluster_size = be32toh(boot_record_disk._cluster_size);
        boot_record_mem._root_id = be32toh(boot_record_disk._root_id);
        boot_record_mem._shards_nb = be32toh(boot_record_disk._shards_nb);

        // TODO(fs): better exceptions, future exceptions
        if (boot_record_mem._shards_nb > max_shards_nb)
            throw std::runtime_error("too many fs shards: " + to_sstring(boot_record_mem._shards_nb));

        boot_record_mem._metadata_ptr.resize(boot_record_mem._shards_nb);
        for (size_t i = 0; i < boot_record_mem._shards_nb; i++)
            boot_record_mem._metadata_ptr[i] = be64toh(boot_record_disk._metadata_ptr[i]);

        return make_ready_future<bootstrap_record>(boot_record_mem);
    });
}

future<bootstrap_record> bootstrap_record::create_from_memory() {
    return make_ready_future<bootstrap_record>(bootstrap_record(1, 2, 3, 4, 5, 4, {7, 8, 9, 10}));
}

future<> bootstrap_record::write_to_disk(block_device& device) {
    bootstrap_record_disk boot_record;

    auto boot_record_buff = temporary_buffer<char>::aligned(alignment, aligned_boot_record_size);
    std::memset(boot_record_buff.get_write(), 0, aligned_boot_record_size);

    // prepare bootstrap_record_disk records
    boot_record._magic = htobe64(_magic);
    boot_record._version = htobe64(_version);
    boot_record._sector_size = htobe32(_sector_size);
    boot_record._cluster_size = htobe32(_cluster_size);
    boot_record._root_id = htobe32(_root_id);
    boot_record._shards_nb = htobe32(_shards_nb);
    for (size_t i = 0; i < _metadata_ptr.size(); i++)
        boot_record._metadata_ptr[i] = htobe64(_metadata_ptr[i]);
    for (size_t i = _metadata_ptr.size(); i < max_shards_nb; i++)
        boot_record._metadata_ptr[i] = 0;
    boot_record._crc = 0;

    std::memcpy(boot_record_buff.get_write(), &boot_record, sizeof(boot_record));

    // calculate crc
    uint32_t crc = crc32(boot_record_buff.get(), crc_offset);
    boot_record._crc = htobe32(crc);
    std::memcpy(boot_record_buff.get_write() + crc_offset, &boot_record._crc, sizeof(boot_record._crc));

    return device.write(boot_record_offset, boot_record_buff.get(), aligned_boot_record_size)
            .then([boot_record_buff = std::move(boot_record_buff)] (size_t ret) {
        // TODO(fs): handle case where ret != boot_cluster_size
        return make_ready_future<>();
    });
}

}
