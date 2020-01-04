// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#define nssv_CONFIG_SELECT_STRING_VIEW nssv_STRING_VIEW_NONSTD

#include <cstdint>
#include <seastar/util/std-compat.hh>

namespace arrow {
namespace util {

using ::seastar::compat::string_view;
using ::seastar::compat::basic_string_view;
using bytes_view = basic_string_view<uint8_t>;

}  // namespace util
}  // namespace arrow
