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
#include "seastar/fs/path.hh"

#include <boost/crc.hpp>
#include <boost/range/irange.hpp>
#include <cstddef>
#include <cstring>
#include <limits>
#include <seastar/core/units.hh>
#include <stdexcept>
#include <unordered_set>
#include <variant>

namespace seastar::fs {

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _curr_cluster_buff(cluster_size, alignment, 0)
, _cluster_allocator({}, {}) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 and cluster_size % alignment == 0);
}

future<> metadata_log::bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters) {
    // Clear the metadata log
    _root_dir = root_dir;
    _inodes.clear();
    // Bootstrap
    return do_with(std::optional(first_metadata_cluster_id), temporary_buffer<uint8_t>::aligned(_alignment, _cluster_size), std::unordered_set<cluster_id_t>{}, [this, available_clusters](auto& cluster_id, auto& buff, auto& taken_clusters) {
        return repeat([this, &cluster_id, &taken_clusters, &buff] {
            if (not cluster_id) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }

            disk_offset_t curr_cluster_offset = cluster_id_to_offset(*cluster_id, _cluster_size);
            return _device.read(curr_cluster_offset, buff.get_write(), _cluster_size).then([this](size_t bytes_read) {
                if (bytes_read != _cluster_size) {
                    return make_exception_future(std::runtime_error("Failed to read whole metadata log cluster"));
                }
                return now();

            }).then([this, &cluster_id, &taken_clusters, &buff, curr_cluster_offset] {
                taken_clusters.emplace(*cluster_id);
                size_t pos = 0;
                auto load_entry = [this, &buff, &pos](auto& entry) {
                    if (pos + sizeof(entry) > _cluster_size) {
                        return false;
                    }

                    memcpy(&entry, buff.get() + pos, sizeof(entry));
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
                        break; // Committed block with next cluster_id marks the end of the current metadata log cluster
                    }

                    ondisk_type entry_type;
                    if (not load_entry(entry_type) or entry_type != CHECKPOINT) {
                        log_ended = true; // Invalid entry
                        break;
                    }

                    ondisk_checkpoint checkpoint;
                    if (not load_entry(checkpoint) or pos + checkpoint.checkpointed_data_length > _cluster_size) {
                        log_ended = true; // Invalid checkpoint
                        break;
                    }

                    boost::crc_32_type crc;
                    crc.process_bytes(buff.get() + pos, checkpoint.checkpointed_data_length);
                    crc.process_bytes(&checkpoint.checkpointed_data_length, sizeof(checkpoint.checkpointed_data_length));
                    if (crc.checksum() != checkpoint.crc32_code) {
                        log_ended = true; // Invalid CRC code
                        break;
                    }

                    auto checkpointed_data_end = pos + checkpoint.checkpointed_data_length;
                    while (pos < checkpointed_data_end) {
                        switch (entry_type) {
                        case INVALID: {
                            return invalid_entry_exception();
                        }

                        case NEXT_METADATA_CLUSTER: {
                            ondisk_next_metadata_cluster entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            cluster_id = (cluster_id_t)entry.cluster_id;
                            continue;
                        }

                        case CHECKPOINT: {
                            // CHECKPOINT is handled before entering the switch
                            return invalid_entry_exception();
                        }

                        case CREATE_INODE: {
                            ondisk_create_inode entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto [it, was_inserted] = _inodes.emplace((inode_t)entry.inode, inode_info {
                                0,
                                0, // TODO: maybe creating dangling inode (not attached to any directory is invalid), if not then we have to drop them after finishing bootstraping, because at that point dangling inodes are invaild (that's how I see it now)
                                ondisk_metadata_to_metadata(entry.metadata),
                                [&]() -> decltype(inode_info::contents) {
                                    if (entry.is_directory) {
                                        return inode_info::directory {};
                                    } else {
                                        return inode_info::file {};
                                    }
                                }()
                            });

                            if (not was_inserted) {
                                return invalid_entry_exception(); // inode already exists
                            }
                            continue;
                        }

                        case UPDATE_METADATA: {
                            ondisk_update_metadata entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            it->second.metadata = ondisk_metadata_to_metadata(entry.metadata);
                            continue;
                        }

                        case DELETE_INODE: {
                            // TODO: for compaction: update used inode_data_vec
                            ondisk_delete_inode entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            inode_info& inode_info = it->second;
                            if (inode_info.directories_containing_file > 0) {
                                return invalid_entry_exception();
                            }

                            bool failed = std::visit(overloaded {
                                [](const inode_info::directory& dir) {
                                    return (not dir.entries.empty());
                                },
                                [](const inode_info::file&) {
                                    return false;
                                }
                            }, inode_info.contents);
                            if (failed) {
                                return invalid_entry_exception();
                            }

                            _inodes.erase(it);
                            continue;
                        }

                        case SMALL_WRITE: {
                            // TODO: for compaction: update used inode_data_vec
                            ondisk_small_write_header entry;
                            if (not load_entry(entry) or pos + entry.length > checkpointed_data_end) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            temporary_buffer<uint8_t> data(buff.get() + pos, entry.length);
                            pos += entry.length;

                            inode_data_vec data_vec = {
                                {entry.offset, entry.offset + entry.length},
                                inode_data_vec::in_mem_data {std::move(data)}
                            };

                            inode_info& inode_info = it->second;
                            if (not std::holds_alternative<inode_info::file>(inode_info.contents)) {
                                return invalid_entry_exception();
                            }

                            auto& file = std::get<inode_info::file>(inode_info.contents);
                            write_update(file, std::move(data_vec));
                            inode_info.metadata.mtime_ns = entry.mtime_ns;
                            continue;
                        }

                        case MTIME_UPDATE: {
                            ondisk_mtime_update entry;
                            if (not load_entry(entry) or _inodes.count(entry.inode) != 1) {
                                return invalid_entry_exception();
                            }

                            _inodes[entry.inode].metadata.mtime_ns = entry.mtime_ns;
                            continue;
                        }

                        case TRUNCATE: {
                            ondisk_truncate entry;
                            // TODO: add checks for being directory
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            inode_info& inode_info = it->second;
                            if (not std::holds_alternative<inode_info::file>(inode_info.contents)) {
                                return invalid_entry_exception();
                            }

                            auto& file = std::get<inode_info::file>(inode_info.contents);
                            auto fsize = file.size();
                            if (entry.size > fsize) {
                                file.data.emplace(fsize, inode_data_vec {
                                    {fsize, entry.size},
                                    inode_data_vec::hole_data {}
                                });
                            } else {
                                cut_out_data_range(file, {
                                    entry.size,
                                    std::numeric_limits<decltype(file_range::end)>::max()
                                });
                            }

                            inode_info.metadata.mtime_ns = entry.mtime_ns;
                            continue;
                        }

                        case ADD_DIR_ENTRY: {
                            ondisk_add_dir_entry_header entry;
                            if (not load_entry(entry) or pos + entry.entry_name_length > checkpointed_data_end or _inodes.count(entry.dir_inode) != 1 or _inodes.count(entry.entry_inode) != 1) {
                                return invalid_entry_exception();
                            }

                            sstring dir_entry_name((const char*)buff.get() + pos, entry.entry_name_length);
                            pos += entry.entry_name_length;

                            inode_info& dir_inode_info = _inodes[entry.dir_inode];
                            inode_info& dir_entry_inode_info = _inodes[entry.entry_inode];

                            if (not std::holds_alternative<inode_info::directory>(dir_inode_info.contents)) {
                                return invalid_entry_exception();
                            }
                            auto& dir = std::get<inode_info::directory>(dir_inode_info.contents);

                            if (std::holds_alternative<inode_info::directory>(dir_entry_inode_info.contents) and dir_entry_inode_info.directories_containing_file > 0) {
                                return invalid_entry_exception(); // Directory may only be linked once
                            }

                            dir.entries.emplace(std::move(dir_entry_name), (inode_t)entry.entry_inode);
                            ++dir_entry_inode_info.directories_containing_file;
                            continue;
                        }

                        case DELETE_DIR_ENTRY: {
                            ondisk_delete_dir_entry_header entry;
                            if (not load_entry(entry) or pos + entry.entry_name_length > checkpointed_data_end or _inodes.count(entry.dir_inode) != 1) {
                                return invalid_entry_exception();
                            }

                            sstring dir_entry_name((const char*)buff.get() + pos, entry.entry_name_length);
                            pos += entry.entry_name_length;

                            inode_info& dir_inode_info = _inodes[entry.dir_inode];
                            if (not std::holds_alternative<inode_info::directory>(dir_inode_info.contents)) {
                                return invalid_entry_exception();
                            }
                            auto& dir = std::get<inode_info::directory>(dir_inode_info.contents);

                            auto it = dir.entries.find(dir_entry_name);
                            if (it == dir.entries.end()) {
                                return invalid_entry_exception();
                            }

                            inode_t inode = it->second;
                            auto inode_it = _inodes.find(inode);
                            if (inode_it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            inode_info& inode_info = inode_it->second;
                            assert(inode_info.directories_containing_file > 0);
                            --inode_info.directories_containing_file;

                            dir.entries.erase(it);
                            continue;
                        }

                        case RENAME_DIR_ENTRY: {
                            // TODO: implement it
                            continue;
                        }

                        case MEDIUM_WRITE: // TODO: will be very similar to SMALL_WRITE
                        case LARGE_WRITE: // TODO: will be very similar to SMALL_WRITE
                        case LARGE_WRITE_WITHOUT_MTIME: // TODO: will be very similar to SMALL_WRITE
                            // TODO: implement it
                            throw std::runtime_error("Not implemented");

                        // default: is omitted to make compiler warn if there is an unhandled type
                        }

                        // unknown type => metadata log inconsistency
                        return invalid_entry_exception();
                    }

                    if (pos != checkpointed_data_end) {
                        return invalid_entry_exception();
                    }

                    if (log_ended) {
                        // Bootstrap _curr_cluster_buff
                        _curr_cluster_buff.reset_from_bootstraped_cluster(curr_cluster_offset, buff.get(), pos);
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

void metadata_log::write_update(inode_info::file& file, inode_data_vec new_data_vec) {
    // TODO: for compaction: update used inode_data_vec
    cut_out_data_range(file, new_data_vec.data_range);
    file.data.emplace(new_data_vec.data_range.beg, std::move(new_data_vec));
}

void metadata_log::cut_out_data_range(inode_info::file& file, file_range range) {
    file.cut_out_data_range(range, [](const inode_data_vec& data_vec) {
        (void)data_vec; // TODO: for compaction: update used inode_data_vec
    });
}

std::variant<inode_t, metadata_log::path_lookup_error> metadata_log::path_lookup(const sstring& path) const {
    if (path.empty() or path[0] != '/') {
        return path_lookup_error::NOT_ABSOLUTE;
    }

    std::vector<inode_t> components_stack = {_root_dir};
    size_t beg = 0;
    while (beg < path.size()) {
        range component_range = {beg, path.find('/', beg)};
        bool check_if_dir = false;
        if (component_range.end == path.npos) {
            component_range.end = path.size();
            beg = path.size();
        } else {
            check_if_dir = true;
            beg = component_range.end + 1; // Jump over '/'
        }

        // TODO: I don't like that we make a copy here -- it is totally redundant and inhibits adding noexcept
        sstring component = path.substr(component_range.beg, component_range.size());
        // Process the component
        if (component == "") {
            continue;
        } else if (component == ".") {
            assert(component_range.beg > 0 and path[component_range.beg - 1] == '/' and "Since path is absolute we do not have to check if the current component is a directory");
            continue;
        } else if (component == "..") {
            if (components_stack.size() > 1) { // Root dir cannot be popped
                components_stack.pop_back();
            }
        } else {
            auto dir_it = _inodes.find(components_stack.back());
            assert(dir_it != _inodes.end() and "inode comes from some previous lookup (or is a root directory) hence dir_it has to be valid");
            assert(std::holds_alternative<inode_info::directory>(dir_it->second.contents) and "every previous component is a directory and it was checked when they were processed");
            auto& curr_dir = std::get<inode_info::directory>(dir_it->second.contents);

            auto it = curr_dir.entries.find(component);
            if (it == curr_dir.entries.end()) {
                return path_lookup_error::NO_ENTRY;
            }

            inode_t entry_inode = it->second;
            if (check_if_dir) {
                auto entry_it = _inodes.find(entry_inode);
                assert(entry_it != _inodes.end() and "dir entries have to exist");
                if (not std::holds_alternative<inode_info::directory>(entry_it->second.contents)) {
                    return path_lookup_error::NOT_DIR;
                }
            }

            components_stack.emplace_back(entry_inode);
        }
    }

    return components_stack.back();
}

future<inode_t> metadata_log::futurized_path_lookup(const sstring& path) const {
    auto lookup_res = path_lookup(path);
    return std::visit(overloaded {
        [](path_lookup_error error) {
            switch (error) {
            case path_lookup_error::NOT_ABSOLUTE:
                return make_exception_future<inode_t>(std::runtime_error("Path is not absolute"));
            case path_lookup_error::NO_ENTRY:
                return make_exception_future<inode_t>(std::runtime_error("No such file or directory"));
            case path_lookup_error::NOT_DIR:
                return make_exception_future<inode_t>(std::runtime_error("A component used as directory is not a directory"));
            }
            __builtin_unreachable();
        },
        [](inode_t inode) {
            return make_ready_future<inode_t>(inode);
        }
    }, lookup_res);
}

file_offset_t metadata_log::file_size(inode_t inode) const {
    auto it = _inodes.find(inode);
    if (it == _inodes.end()) {
        throw invalid_inode_exception();
    }

    return std::visit(overloaded {
        [](const inode_info::file& file) {
            return file.size();
        },
        [](const inode_info::directory&) -> file_offset_t {
            throw invalid_inode_exception();
        }
    }, it->second.contents);
}

future<inode_t> metadata_log::create_file(sstring path, mode_t mode) {
    return now().then([this, path = std::move(path), mode]() mutable {
        // TODO: checking permissions...
        sstring entry = last_component(path);
        if (entry.empty()) {
            return make_exception_future<inode_t>(std::runtime_error("Path has to end with character different than '/'"));
        }
        path.erase(path.end() - entry.size(), path.end());

        return futurized_path_lookup(path).then([this, entry = std::move(entry)](auto dir_inode) {
            auto dir_it = _inodes.find(dir_inode);
            if (dir_it == _inodes.end()) {
                return make_exception_future<inode_t>(operation_became_invalid_exception());
            }

            // TODO: continue here
            return make_ready_future<inode_t>();
        });
    });
}

// TODO: think about how to make filesystem recoverable from ENOSPACE situation: flush() (or something else) throws ENOSPACE, then it should be possible to compact some data (e.g. by truncating a file) via top-level interface and retrying the flush() without a ENOSPACE error. In particular if we delete all files after ENOSPACE it should be successful. It becomes especially hard if we write metadata to the last cluster and there is no enough room to write these delete operations. We have to guarantee that the filesystem is in a recoverable state then.

} // namespace seastar::fs
