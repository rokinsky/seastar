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
#include "fs/metadata_log_operations/unlink_or_remove_file.hh"
#include "fs/metadata_to_disk_buffer.hh"
#include "fs/path.hh"
#include "fs/units.hh"
#include "fs/unix_metadata.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_mutex.hh"
#include "seastar/fs/overloaded.hh"

#include <boost/crc.hpp>
#include <boost/range/irange.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <variant>

namespace seastar::fs {

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment,
    shared_ptr<metadata_to_disk_buffer> cluster_buff)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _curr_cluster_buff(std::move(cluster_buff))
, _cluster_allocator({}, {})
, _inode_allocator(1, 0) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 and cluster_size % alignment == 0);
}

future<> metadata_log::bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters,
        fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id) {
    return metadata_log_bootstrap::bootstrap(*this, root_dir, first_metadata_cluster_id, available_clusters,
            fs_shards_pool_size, fs_shard_id);
}

void metadata_log::write_update(inode_info::file& file, inode_data_vec new_data_vec) {
    // TODO: for compaction: update used inode_data_vec
    auto file_size = file.size();
    if (file_size < new_data_vec.data_range.beg) {
        file.data.emplace(file_size, inode_data_vec {
            {file_size, new_data_vec.data_range.beg},
            inode_data_vec::hole_data {}
        });
    } else {
        cut_out_data_range(file, new_data_vec.data_range);
    }

    file.data.emplace(new_data_vec.data_range.beg, std::move(new_data_vec));
}

void metadata_log::cut_out_data_range(inode_info::file& file, file_range range) {
    file.cut_out_data_range(range, [](inode_data_vec data_vec) {
        (void)data_vec; // TODO: for compaction: update used inode_data_vec
    });
}

void metadata_log::memory_only_create_inode(inode_t inode, bool is_directory, unix_metadata metadata) {
    assert(_inodes.count(inode) == 0);
    _inodes.emplace(inode, inode_info {
        0,
        0,
        metadata,
        [&]() -> decltype(inode_info::contents) {
            if (is_directory) {
                return inode_info::directory {};
            }

            return inode_info::file {};
        }()
    });
}

void metadata_log::memory_only_update_metadata(inode_t inode, unix_metadata metadata) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    it->second.metadata = std::move(metadata);
}

void metadata_log::memory_only_delete_inode(inode_t inode) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(not it->second.is_open());
    assert(not it->second.is_linked());

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
    assert(it->second.is_file());
    write_update(it->second.get_file(), std::move(data_vec));
}

void metadata_log::memory_only_update_mtime(inode_t inode, decltype(unix_metadata::mtime_ns) mtime_ns) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    it->second.metadata.mtime_ns = mtime_ns;
}

void metadata_log::memory_only_truncate(inode_t inode, disk_offset_t size) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.is_file());
    auto& file = it->second.get_file();

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

void metadata_log::memory_only_add_dir_entry(inode_info::directory& dir, inode_t entry_inode, std::string entry_name) {
    auto it = _inodes.find(entry_inode);
    assert(it != _inodes.end());
    // Directory may only be linked once (to avoid creating cycles)
    assert(not it->second.is_directory() or not it->second.is_linked());

    bool inserted = dir.entries.emplace(std::move(entry_name), entry_inode).second;
    assert(inserted);
    ++it->second.directories_containing_file;
}

void metadata_log::memory_only_delete_dir_entry(inode_info::directory& dir, std::string entry_name) {
    auto it = dir.entries.find(entry_name);
    assert(it != dir.entries.end());

    auto entry_it = _inodes.find(it->second);
    assert(entry_it != _inodes.end());
    assert(entry_it->second.is_linked());

    --entry_it->second.directories_containing_file;
    dir.entries.erase(it);
}

void metadata_log::schedule_flush_of_curr_cluster() {
    // Make writes concurrent (TODO: maybe serialized within *one* cluster would be faster?)
    schedule_background_task(do_with(_curr_cluster_buff, &_device, [](auto& crr_clstr_bf, auto& device) {
        return crr_clstr_bf->flush_to_disk(*device);
    }));
}

future<> metadata_log::flush_curr_cluster() {
    if (_curr_cluster_buff->bytes_left_after_flush_if_done_now() == 0) {
        switch (schedule_flush_of_curr_cluster_and_change_it_to_new_one()) {
        case flush_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case flush_result::DONE:
            break;
        }
    } else {
        schedule_flush_of_curr_cluster();
    }

    return _background_futures.get_future();
}

metadata_log::flush_result metadata_log::schedule_flush_of_curr_cluster_and_change_it_to_new_one() {
    auto next_cluster = _cluster_allocator.alloc();
    if (not next_cluster) {
        // Here metadata log dies, we cannot even flush current cluster because from there we won't be able to recover
        // TODO: ^ add protection from it and take it into account during compaction
        return flush_result::NO_SPACE;
    }

    auto append_res = _curr_cluster_buff->append(ondisk_next_metadata_cluster {*next_cluster});
    assert(append_res == metadata_to_disk_buffer::APPENDED);
    schedule_flush_of_curr_cluster();

    // Make next cluster the current cluster to allow writing next metadata entries before flushing finishes
    _curr_cluster_buff->virtual_constructor(_cluster_size, _alignment);
    _curr_cluster_buff->init(cluster_id_to_offset(*next_cluster, _cluster_size));
    return flush_result::DONE;
}


void metadata_log::schedule_attempt_to_delete_inode(inode_t inode) {
    return schedule_background_task([this, inode] {
        auto it = _inodes.find(inode);
        if (it == _inodes.end() or it->second.is_linked() or it->second.is_open()) {
            return now(); // Scheduled delete became invalid
        }

        switch (append_ondisk_entry(ondisk_delete_inode {inode})) {
        case append_result::TOO_BIG:
            assert(false and "ondisk entry cannot be too big");
        case append_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case append_result::APPENDED:
            memory_only_delete_inode(inode);
            return now();
        }
        __builtin_unreachable();
    });
}

std::variant<inode_t, metadata_log::path_lookup_error> metadata_log::do_path_lookup(const std::string& path) const noexcept {
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

        std::string_view component(path.data() + component_range.beg, component_range.size());
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
            assert(dir_it->second.is_directory() and "every previous component is a directory and it was checked when they were processed");
            auto& curr_dir = dir_it->second.get_directory();

            auto it = curr_dir.entries.find(component);
            if (it == curr_dir.entries.end()) {
                return path_lookup_error::NO_ENTRY;
            }

            inode_t entry_inode = it->second;
            if (check_if_dir) {
                auto entry_it = _inodes.find(entry_inode);
                assert(entry_it != _inodes.end() and "dir entries have to exist");
                if (not entry_it->second.is_directory()) {
                    return path_lookup_error::NOT_DIR;
                }
            }

            components_stack.emplace_back(entry_inode);
        }
    }

    return components_stack.back();
}

future<inode_t> metadata_log::path_lookup(const std::string& path) const {
    auto lookup_res = do_path_lookup(path);
    return std::visit(overloaded {
        [](path_lookup_error error) {
            switch (error) {
            case path_lookup_error::NOT_ABSOLUTE:
                return make_exception_future<inode_t>(path_is_not_absolute_exception());
            case path_lookup_error::NO_ENTRY:
                return make_exception_future<inode_t>(no_such_file_or_directory_exception());
            case path_lookup_error::NOT_DIR:
                return make_exception_future<inode_t>(path_component_not_directory_exception());
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

future<inode_t> metadata_log::create_file(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), false);
}

future<> metadata_log::create_directory(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), true).discard_result();
}

future<inode_t> metadata_log::open_file(std::string path) {
    return path_lookup(path).then([this](inode_t inode) {
        auto inode_it = _inodes.find(inode);
        if (inode_it == _inodes.end()) {
            return make_exception_future<inode_t>(operation_became_invalid_exception());
        }
        inode_info* inode_info = &inode_it->second;
        if (inode_info->is_directory()) {
            return make_exception_future<inode_t>(is_directory_exception());
        }

        // TODO: can be replaced by sth like _inode_info.during_delete
        return _locks.with_lock(metadata_log::locks::shared {inode}, [this, inode_info = std::move(inode_info), inode] {
            if (not inode_exists(inode)) {
                return make_exception_future<inode_t>(operation_became_invalid_exception());
            }
            ++inode_info->opened_files_count;
            return make_ready_future<inode_t>(inode);
        });
    });
}

future<> metadata_log::close_file(inode_t inode) {
    auto inode_it = _inodes.find(inode);
    if (inode_it == _inodes.end()) {
        return make_exception_future(invalid_inode_exception());
    }
    inode_info* inode_info = &inode_it->second;
    if (inode_info->is_directory()) {
        return make_exception_future(is_directory_exception());
    }


    return _locks.with_lock(metadata_log::locks::shared {inode}, [this, inode, inode_info] {
        if (not inode_exists(inode)) {
            return make_exception_future(operation_became_invalid_exception());
        }

        assert(inode_info->is_open());

        --inode_info->opened_files_count;
        if (not inode_info->is_linked() and not inode_info->is_open()) {
            // Unlinked and not open file should be removed
            schedule_attempt_to_delete_inode(inode);
        }
        return now();
    });
}

future<size_t> metadata_log::write(inode_t inode, file_offset_t pos, const void* buffer, size_t len,
        [[maybe_unused]] const io_priority_class& pc) {
    auto inode_it = _inodes.find(inode);
    if (inode_it == _inodes.end()) {
        return make_exception_future<size_t>(invalid_inode_exception());
    }
    if (inode_it->second.is_directory()) {
        return make_exception_future<size_t>(is_directory_exception());
    }

    return _locks.with_lock(metadata_log::locks::shared {inode}, [this, inode, pos, buffer, len] {
        if (not inode_exists(inode)) {
            return make_exception_future<size_t>(operation_became_invalid_exception());
        }

        if (len <= std::numeric_limits<decltype(ondisk_small_write_header::length)>::max()) {
            using namespace std::chrono;
            uint64_t mtime_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
            ondisk_small_write_header ondisk_entry {
                inode,
                pos,
                static_cast<decltype(ondisk_small_write_header::length)>(len),
                mtime_ns
            };

            switch (append_ondisk_entry(ondisk_entry, buffer)) {
            case append_result::TOO_BIG:
                assert(false and "ondisk entry cannot be too big");
            case append_result::NO_SPACE:
                return make_exception_future<size_t>(no_more_space_exception());
            case append_result::APPENDED:
                temporary_buffer<uint8_t> tmp_buffer(static_cast<const uint8_t*>(buffer), len);
                memory_only_small_write(inode, pos, std::move(tmp_buffer));
                memory_only_update_mtime(inode, mtime_ns);
                return make_ready_future<size_t>(len);
            }
            __builtin_unreachable();
        }
        return make_exception_future<size_t>(invalid_argument_exception());
    });
}

future<size_t> metadata_log::read(inode_t inode, file_offset_t pos, void* buffer, size_t len,
        const io_priority_class& pc) {
    auto inode_it = _inodes.find(inode);
    if (inode_it == _inodes.end()) {
        return make_exception_future<size_t>(invalid_inode_exception());
    }
    if (inode_it->second.is_directory()) {
        return make_exception_future<size_t>(is_directory_exception());
    }
    inode_info::file* file_info = &inode_it->second.get_file();

    return _locks.with_lock(metadata_log::locks::shared {inode}, [this, inode, pos, buffer, len, file_info, pc] {
        // TODO: do we want to keep that lock during reading? Everything should work even after file removal
        if (not inode_exists(inode)) {
            return make_exception_future<size_t>(operation_became_invalid_exception());
        }

        std::vector<inode_data_vec> data_vecs;
        // Extract data vectors from file_info
        file_info->execute_on_data_range({pos, pos + len}, [&data_vecs](inode_data_vec data_vec) {
            // TODO: for compaction: mark that clusters shouldn't be moved to _cluster_allocator before that read ends
            data_vecs.emplace_back(std::move(data_vec));
        });

        return do_with(std::move(data_vecs), (size_t)0, (size_t)0,
                [this, pos, buffer, pc](std::vector<inode_data_vec>& data_vecs, size_t& vec_idx, size_t& valid_read_size) {
            uint8_t* buffer_u = static_cast<uint8_t*>(buffer);
            return repeat([this, &valid_read_size, &data_vecs, &vec_idx, pos, buffer_u, pc] {
                if (vec_idx == data_vecs.size()) {
                    return make_ready_future<bool_class<stop_iteration_tag>>(stop_iteration::yes);
                }

                inode_data_vec& data_vec = data_vecs[vec_idx++];
                size_t expected_read_size = data_vec.data_range.size();
                future<size_t> disk_read = make_ready_future<size_t>(expected_read_size);

                std::visit(overloaded {
                    [&](inode_data_vec::in_mem_data& mem) {
                        std::memcpy(buffer_u + data_vec.data_range.beg - pos, mem.data.get(), expected_read_size);
                    },
                    [&](inode_data_vec::on_disk_data& disk_data) {
                        // TODO: we need to align those reads, we shouldn't read the same block more than one time,
                        // without assuming aligned reads here we will need to save previous disk read and copy data
                        // from it
                        disk_read = _device.read(disk_data.device_offset, buffer_u + data_vec.data_range.beg - pos,
                                expected_read_size, pc);
                    },
                    [&](inode_data_vec::hole_data&) {
                        std::memset(buffer_u + data_vec.data_range.beg - pos, 0, expected_read_size);
                    },
                }, data_vec.data_location);

                return disk_read.then([&valid_read_size, expected_read_size](size_t read_size) {
                    valid_read_size += read_size;
                    if (read_size != expected_read_size) {
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                });
            }).then([&valid_read_size] {
                return make_ready_future<size_t>(valid_read_size);
            });
        });
    });
}

future<> metadata_log::unlink_file(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), false);
}

future<> metadata_log::remove(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), true);
}

// TODO: think about how to make filesystem recoverable from ENOSPACE situation: flush() (or something else) throws ENOSPACE,
// then it should be possible to compact some data (e.g. by truncating a file) via top-level interface and retrying the flush()
// without a ENOSPACE error. In particular if we delete all files after ENOSPACE it should be successful. It becomes especially
// hard if we write metadata to the last cluster and there is no enough room to write these delete operations. We have to
// guarantee that the filesystem is in a recoverable state then.

} // namespace seastar::fs
