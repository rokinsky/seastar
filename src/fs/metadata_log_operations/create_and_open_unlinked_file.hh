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

#pragma once

#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "seastar/core/future.hh"

namespace seastar::fs {

class create_and_open_unlinked_file_operation {
    metadata_log& _metadata_log;

    create_and_open_unlinked_file_operation(metadata_log& metadata_log) : _metadata_log(metadata_log) {}

    future<inode_t> create_and_open_unlinked_file(file_permissions perms) {
        using namespace std::chrono;
        uint64_t curr_time_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        unix_metadata unx_mtdt = {
            perms,
            0, // TODO: Eventually, we'll want a user to be able to pass his credentials when bootstrapping the
            0, //       file system -- that will allow us to authorize users on startup (e.g. via LDAP or whatnot).
            curr_time_ns,
            curr_time_ns,
            curr_time_ns
        };

        inode_t new_inode = _metadata_log._inode_allocator.alloc();
        ondisk_create_inode ondisk_entry {
            new_inode,
            false,
            metadata_to_ondisk_metadata(unx_mtdt)
        };

        switch (_metadata_log.append_ondisk_entry(ondisk_entry)) {
        case metadata_log::append_result::TOO_BIG:
            return make_exception_future<inode_t>(cluster_size_too_small_to_perform_operation_exception());
        case metadata_log::append_result::NO_SPACE:
            return make_exception_future<inode_t>(no_more_space_exception());
        case metadata_log::append_result::APPENDED:
            inode_info& new_inode_info = _metadata_log.memory_only_create_inode(new_inode, false, unx_mtdt);
            // We don't have to lock, as there was no context switch since the allocation of the inode number
            ++new_inode_info.opened_files_count;
            return make_ready_future<inode_t>(new_inode);
        }
        __builtin_unreachable();
    }

public:
    static future<inode_t> perform(metadata_log& metadata_log, file_permissions perms) {
        return do_with(create_and_open_unlinked_file_operation(metadata_log),
                [perms = std::move(perms)](auto& obj) {
            return obj.create_and_open_unlinked_file(std::move(perms));
        });
    }
};

} // namespace seastar::fs
