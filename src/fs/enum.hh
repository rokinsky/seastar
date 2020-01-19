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

#include <seastar/core/file.hh>

namespace seastar::fs {

template<typename EnumType>
struct enum_traits {};

template<>
struct enum_traits<open_flags> {
    static constexpr bool has_any = true;
};

template<typename EnumType>
typename std::enable_if<enum_traits<EnumType>::has_any, bool>::type
any(EnumType e) {
    return e != EnumType{};
}

} // namespace seastar::fs
