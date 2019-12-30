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

#include "fs/cluster.hh"
#include "fs/cluster_allocator.hh"
#include "fs/metadata_log.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/overloaded.hh"

#include <boost/crc.hpp>
#include <boost/range/irange.hpp>
#include <cstddef>
#include <limits>
#include <seastar/core/units.hh>
#include <stdexcept>
#include <unordered_set>

namespace seastar::fs {

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment) : _device(std::move(device)), _cluster_size(cluster_size), _alignment(alignment), _unwritten_metadata(allocate_aligned_buffer<uint8_t>(cluster_size, alignment)), _cluster_allocator({}, {}) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 and cluster_size % alignment == 0);
}

future<> metadata_log::bootstrap(cluster_id_t first_metadata_cluster_id, cluster_range available_clusters) {
    // Clear state of the metadata log
    _next_write_offset = 0;
    _bytes_left_in_current_cluster = _cluster_size;
    _inodes.clear();
    // TODO: clear directory DAG
    // Bootstrap
    return do_with(std::optional(first_metadata_cluster_id), std::unordered_set<cluster_id_t>{}, [this, available_clusters, &buff = _unwritten_metadata](auto& cluster_id, auto& taken_clusters) {
        return repeat([this, &cluster_id, &taken_clusters, &buff] {
            if (not cluster_id) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }

            return _device.read(cluster_id_to_offset(*cluster_id, _cluster_size), buff.get(), _cluster_size).then([this](size_t bytes_read) {
                if (bytes_read != _cluster_size) {
                    return make_exception_future(std::runtime_error("Failed to read whole metadata log cluster"));
                }
                return now();

            }).then([this, &cluster_id, &taken_clusters, &buff] {
                taken_clusters.emplace(*cluster_id);
                size_t pos = 0;
                auto load_entry = [this, &buff, &pos](auto& entry) {
                    if (pos + sizeof(entry) > _cluster_size) {
                        return false;
                    }

                    memcpy(&entry, &buff[pos], sizeof(entry));
                    pos += sizeof(entry);
                    return true;
                };

                auto invalid_entry_exception = [] {
                    return make_exception_future<stop_iteration>(std::runtime_error("Invalid metadata log entry"));
                };

                // Process cluster: the data layout format is:
                // | checkpoint1 | data1... | checkpoint2 | data2... |
                bool log_ended = false;
                cluster_id = std::nullopt;
                for (;;) {
                    if (cluster_id) {
                        break; // Committed block with next cluster id marks the end of the current metadata log cluster
                    }

                    ondisk_type entry_type;
                    if (not load_entry(entry_type) or entry_type != CHECKPOINT) {
                        log_ended = true; // Invalid entry
                        break;
                    }

                    ondisk_checkpoint checkpoint;
                    static_assert(offsetof(decltype(checkpoint), crc32_code) == 0);
                    auto checkpointed_data_beg = pos + sizeof(checkpoint.crc32_code);
                    if (not load_entry(checkpoint) or checkpointed_data_beg + checkpoint.checkpointed_data_length > _cluster_size) {
                        log_ended = true; // Invalid checkpoint
                        break;
                    }

                    boost::crc_32_type crc;
                    crc.process_bytes(&buff[checkpointed_data_beg], checkpoint.checkpointed_data_length);
                    if (crc.checksum() != checkpoint.crc32_code) {
                        log_ended = true; // Invalid CRC code
                        break;
                    }

                    auto checkpointed_data_end = checkpointed_data_beg + checkpoint.checkpointed_data_length;
                    while (pos < checkpointed_data_end) {
                        switch (entry_type) {
                        case NEXT_METADATA_CLUSTER: {
                            ondisk_next_metadata_cluster entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            cluster_id = (cluster_id_t)entry.cluster_id;
                            break;
                        }

                        case CHECKPOINT: return invalid_entry_exception();

                        case INODE: {
                            ondisk_inode entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            inode_info& inode = _inodes[entry.inode];
                            static_assert(sizeof(entry.metadata) == 29, "metadata size changed: check if below assignments needs update");
                            inode.metadata.is_directory = entry.metadata.is_directory;
                            inode.metadata.mode = entry.metadata.mode;
                            inode.metadata.uid = entry.metadata.uid;
                            inode.metadata.gid = entry.metadata.gid;
                            inode.metadata.mtime_ns = entry.metadata.mtime_ns;
                            inode.metadata.ctime_ns = entry.metadata.ctime_ns;
                            break;
                        }

                        case DELETE: {
                            // TODO: for compaction: update used inode_data_vec
                            ondisk_inode entry;
                            if (not load_entry(entry) or _inodes.erase(entry.inode) != 1) {
                                return invalid_entry_exception();
                            }

                            break;
                        }

                        case SMALL_DATA: {
                            // TODO: for compaction: update used inode_data_vec
                            ondisk_small_data_header entry;
                            if (not load_entry(entry) or pos + entry.length > _cluster_size or _inodes.count(entry.inode) != 1) {
                                return invalid_entry_exception();
                            }

                            auto data_store = make_shared<std::unique_ptr<uint8_t[]>>(std::make_unique<uint8_t[]>(entry.length));
                            memcpy(data_store->get(), &buff[pos], entry.length);

                            inode_data_vec data_vec = {
                                {entry.offset, entry.offset + entry.length},
                                inode_data_vec::in_mem_data {data_store, data_store->get()}
                            };
                            write_update(entry.inode, std::move(data_vec));
                        }

                        case MTIME_UPDATE: {
                            ondisk_mtime_update entry;
                            if (not load_entry(entry) or _inodes.count(entry.inode) != 1) {
                                return invalid_entry_exception();
                            }

                            _inodes[entry.inode].metadata.mtime_ns = entry.mtime_ns;
                        }

                        case TRUNCATE: {
                            ondisk_truncate entry;
                            // TODO: add checks for being directory
                            if (not load_entry(entry) or _inodes.count(entry.inode) != 1) {
                                return invalid_entry_exception();
                            }

                            auto fsize = file_size(entry.inode);
                            if (entry.size > fsize) {
                                _inodes[entry.inode].data.emplace(fsize, inode_data_vec {
                                    {fsize, entry.size},
                                    inode_data_vec::hole_data {}
                                });
                            } else {
                                cut_out_data_range(entry.inode, {
                                    entry.size,
                                    std::numeric_limits<decltype(file_range::end)>::max()
                                });
                            }
                        }

                        case ADD_DIR_ENTRY: // TODO: priority
                        case DELETE_DIR_ENTRY: // TODO: priority
                        case RENAME_DIR_ENTRY:
                        case MEDIUM_DATA: // TODO: will be very similar to SMALL_DATA
                        case LARGE_DATA: // TODO: will be very similar to SMALL_DATA
                            // TODO:
                            throw std::runtime_error("Not implemented");

                        // default is omitted to make compiler warn if there is an unhandled type
                        // unknown type => metadata log inconsistency
                        }
                    }

                    if (pos != checkpointed_data_end) {
                        return invalid_entry_exception();
                    }

                    if (log_ended) {
                        // Update _unwritten_metadata, as the place where metadata log continues may be unaligned
                        // TODO: update _unwritten_metadata to be correct

                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                }

                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        }).then([this, &available_clusters, &taken_clusters] {
            // Initialize _cluser_allocator
            std::deque<cluster_id_t> free_clusters;
            for (auto cid : boost::irange(available_clusters.beg, available_clusters.end)) {
                if (taken_clusters.count(cid) == 0) {
                    free_clusters.emplace_back(cid);
                }
            }
            _cluster_allocator = cluster_allocator(std::move(taken_clusters), std::move(free_clusters));
        });
    });
}

void metadata_log::write_update(inode_t inode, inode_data_vec data_vec) {
    cut_out_data_range(inode, data_vec.data_range);
    _inodes[inode].data.emplace(data_vec.data_range.beg, std::move(data_vec));
}

void metadata_log::cut_out_data_range(inode_t inode, file_range range) {
    // Cut all vectors intersecting with range
    auto& inode_data = _inodes[inode].data;
    auto it = inode_data.lower_bound(range.beg);
    if (it != inode_data.begin() and are_intersecting(range, prev(it)->second.data_range)) {
        --it;
    }

    while (it != inode_data.end() and are_intersecting(range, it->second.data_range)) {
        const auto data_vec = std::move(it->second);
        inode_data.erase(it++);
        const auto cap = intersection(range, data_vec.data_range);
        if (cap == data_vec.data_range) {
            continue; // Fully intersects => remove it
        }

        // Overlaps => cut it, possibly into two parts:
        // |       data_vec      |
        //         | cap |
        // | left  |     | right |
        inode_data_vec left, right;
        left.data_range = {data_vec.data_range.beg, cap.beg};
        right.data_range = {cap.end, data_vec.data_range.end};
        auto right_beg_shift = right.data_range.beg - data_vec.data_range.beg;
        std::visit(overloaded {
            [&](const inode_data_vec::in_mem_data& data) {
                left.data_location = data;
                right.data_location = inode_data_vec::in_mem_data {data.data_store, data.data + right_beg_shift};
            },
            [&](const inode_data_vec::on_disk_data& data) {
                left.data_location = data;
                right.data_location = inode_data_vec::on_disk_data {data.device_offset + right_beg_shift};
            },
            [&](const inode_data_vec::hole_data&) {
                left.data_location = right.data_location = inode_data_vec::hole_data {};
            },
        }, data_vec.data_location);

        // Save new data vectors
        if (not left.data_range.is_empty()) {
            inode_data.emplace(left.data_range.beg, std::move(left));
        }
        if (not right.data_range.is_empty()) {
            inode_data.emplace(right.data_range.beg, std::move(right));
        }
    }
}

file_offset_t metadata_log::file_size(inode_t inode) const {
    auto it = _inodes.find(inode);
    if (it == _inodes.end()) {
        throw invalid_inode_exception();
    }

    const auto& data = it->second.data;
    if (data.empty()) {
        return 0;
    }

    return (--data.end())->second.data_range.end;
}

} // namespace seastar::fs
