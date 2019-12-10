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
constexpr uint32_t boot_cluster_size = 1 * MB;
constexpr uint64_t boot_cluster_offset = 0;

}

future<bootstrap_record> bootstrap_record::read_from_disk(block_device& device) {
    throw std::bad_function_call();
}

future<bootstrap_record> bootstrap_record::create_from_memory() {
    throw std::bad_function_call();
}

namespace {

class record_writer {
    char* _buff;
    uint32_t _offset = 0;
    uint32_t _max_size;

public:
    record_writer(char* buff, uint32_t max_size)
        : _buff(buff), _max_size(max_size) {}

    uint32_t get_len() const {
        return _offset;
    }

    const char* get_buff() const {
        return _buff;
    }

    void add_elem(uint32_t elem) {
        // TODO(fs): add max_size check
        elem = htobe32(elem);
        std::memcpy(_buff + _offset, &elem, sizeof(elem));
        _offset += sizeof(elem);
    }

    void add_elem(uint64_t elem) {
        // TODO(fs): add max_size check
        elem = htobe64(elem);
        std::memcpy(_buff + _offset, &elem, sizeof(elem));
        _offset += sizeof(elem);
    }

    void add_elem(const std::vector<uint64_t>& elem) {
        // TODO(fs): add max_size check
        for (auto&& i : elem)
            add_elem(i);
    }
};

uint32_t crc32(const char* buff, size_t len) {
    boost::crc_32_type result;
    result.process_bytes(buff, len);
    return result.checksum();
}

}

future<> bootstrap_record::write_to_disk(block_device& device) {
    auto boot_cluster = temporary_buffer<char>::aligned(alignment, boot_cluster_size);
    record_writer writer(boot_cluster.get_write(), boot_cluster_size);

    // TODO(fs): add error handling
    writer.add_elem(_magic);
    writer.add_elem(_version);
    writer.add_elem(_sector_size);
    writer.add_elem(_cluster_size);
    writer.add_elem(_root_id);
    writer.add_elem(_shards_nb);
    writer.add_elem(_metadata_ptr);
    writer.add_elem(crc32(writer.get_buff(), writer.get_len()));

    return device.write(boot_cluster_offset, boot_cluster.get(), boot_cluster_size)
            .then([boot_cluster = std::move(boot_cluster)] (size_t ret) {
        // TODO(fs): handle case where ret != boot_cluster_size
        return make_ready_future<>();
    });
}

}
