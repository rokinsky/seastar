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
#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs/metadata_log_bootstrap.hh"
#include "fs/metadata_log_operations/create_file.hh"
#include "fs/metadata_to_disk_buffer.hh"
#include "fs/unix_metadata.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_mutex.hh"
#include "seastar/fs/overloaded.hh"
#include "seastar/fs/path.hh"

#include <bits/stdint-uintn.h>
#include <boost/crc.hpp>
#include <boost/range/irange.hpp>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <limits>
#include <pthread.h>
#include <seastar/core/units.hh>
#include <stdexcept>
#include <unistd.h>
#include <unordered_set>
#include <variant>

namespace seastar::fs {

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _cluster_allocator({}, {})
, _inode_allocator(1, 0) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 and cluster_size % alignment == 0);
}

future<> metadata_log::bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters, fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id) {
    return metadata_log_bootstrap::bootstrap(*this, root_dir, first_metadata_cluster_id, available_clusters, fs_shards_pool_size, fs_shard_id);
}

void metadata_log::memory_only_create_inode(inode_t inode, bool is_directory, unix_metadata metadata) {
    assert(_inodes.count(inode) == 0);
    _inodes.emplace(inode, inode_info {
        0,
        0,
        metadata,
        [&]() -> decltype(inode_info::contents) {
            if (is_directory)
                return inode_info::directory {};

            return inode_info::file {};
        }()
    });
}

void metadata_log::memory_only_delete_inode(inode_t inode) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.opened_files_count == 0);
    assert(it->second.directories_containing_file == 0);

    std::visit(overloaded {
        [](const inode_info::directory& dir) {
            assert(dir.entries.empty());
        },
        [](const inode_info::file&) {
            // TODO: for compaction: update used inode_data_vec
        }
    }, it->second.contents);

    _inodes.erase(it);
}

void metadata_log::memory_only_small_write(inode_t inode, disk_offset_t offset, temporary_buffer<uint8_t> data) {
    inode_data_vec data_vec = {
        {offset, offset + data.size()},
        inode_data_vec::in_mem_data {std::move(data)}
    };

    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(std::holds_alternative<inode_info::file>(it->second.contents));
    write_update(std::get<inode_info::file>(it->second.contents), std::move(data_vec));
}

void metadata_log::memory_only_update_mtime(inode_t inode, decltype(unix_metadata::mtime_ns) mtime_ns) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    it->second.metadata.mtime_ns = mtime_ns;
}

void metadata_log::memory_only_truncate(inode_t inode, disk_offset_t size) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(std::holds_alternative<inode_info::file>(it->second.contents));
    auto& file = std::get<inode_info::file>(it->second.contents);

    auto file_size = file.size();
    if (size > file_size) {
        file.data.emplace(file_size, inode_data_vec {
            {file_size, size},
            inode_data_vec::hole_data {}
        });
    } else {
        // TODO: for compaction: update used inode_data_vec
        cut_out_data_range(file, {
            size,
            std::numeric_limits<decltype(file_range::end)>::max()
        });
    }
}

void metadata_log::memory_only_add_dir_entry(inode_info::directory& dir, inode_t entry_inode, sstring entry_name) {
    auto it = _inodes.find(entry_inode);
    assert(it != _inodes.end());
    // Directory may only be linked once (to avoid creating cycles)
    assert(not std::holds_alternative<inode_info::directory>(it->second.contents) or it->second.directories_containing_file == 0);

    bool inserted = dir.entries.emplace(std::move(entry_name), entry_inode).second;
    assert(inserted);
    ++it->second.directories_containing_file;
}

void metadata_log::memory_only_delete_dir_entry(inode_info::directory& dir, sstring entry_name) {
    auto it = dir.entries.find(entry_name);
    assert(it != dir.entries.end());

    auto entry_it = _inodes.find(it->second);
    assert(entry_it != _inodes.end());
    assert(entry_it->second.directories_containing_file > 0);

    --entry_it->second.directories_containing_file;
    dir.entries.erase(it);
}

void metadata_log::schedule_curr_cluster_flush() {
    // Make writes concurrent (TODO: maybe serialized within *one* cluster would be faster?)
    _previous_flushes = when_all_succeed(_previous_flushes.get_future(), do_with(_curr_cluster_buff, &_device, [](auto& crr_clstr_bf, auto& device) {
        return crr_clstr_bf->flush_to_disk(*device, true);
    }));
}

future<> metadata_log::flush_curr_cluster() {
    if (_curr_cluster_buff->bytes_left_after_flush_if_done_now(true) == 0) {
        return flush_curr_cluster_and_change_it_to_new_one();
    }

    schedule_curr_cluster_flush();
    return _previous_flushes.get_future();
}

future<> metadata_log::flush_curr_cluster_and_change_it_to_new_one() {
    auto next_cluster = _cluster_allocator.alloc();
    if (not next_cluster) {
        // Here metadata log dies, we cannot even flush current cluster because from there we won't be able to recover
        return make_exception_future(no_more_space_exception());
    }

    auto append_res = _curr_cluster_buff->append(ondisk_next_metadata_cluster {*next_cluster});
    assert(append_res == metadata_to_disk_buffer::APPENDED);
    schedule_curr_cluster_flush();

    // Make next cluster the current cluster to allow writing of next metadata entries before flushing finishes
    _curr_cluster_buff = make_lw_shared<metadata_to_disk_buffer>(_cluster_size, _alignment, cluster_id_to_offset(*next_cluster, _cluster_size));

    return _previous_flushes.get_future();
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

future<inode_t> metadata_log::create_file(sstring path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), false);
}

future<inode_t> metadata_log::create_directory(sstring path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), true);
}

// TODO: think about how to make filesystem recoverable from ENOSPACE situation: flush() (or something else) throws ENOSPACE, then it should be possible to compact some data (e.g. by truncating a file) via top-level interface and retrying the flush() without a ENOSPACE error. In particular if we delete all files after ENOSPACE it should be successful. It becomes especially hard if we write metadata to the last cluster and there is no enough room to write these delete operations. We have to guarantee that the filesystem is in a recoverable state then.

} // namespace seastar::fs
