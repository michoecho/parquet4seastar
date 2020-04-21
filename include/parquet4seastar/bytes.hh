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

#include <string>
#include <string_view>
#include <type_traits>

namespace parquet4seastar {

using bytes = std::basic_string<uint8_t>;
using bytes_view = std::basic_string_view<uint8_t>;
using byte = bytes::value_type;

struct bytes_hasher {
    size_t operator()(const bytes &s) const {
        return std::hash<std::string_view>{}(std::string_view{reinterpret_cast<const char *>(s.data()), s.size()});
    }
};

template<typename T, typename = std::enable_if_t<std::is_trivially_copyable_v<T>>>
void append_raw_bytes(bytes &b, T v) {
    const byte *data = reinterpret_cast<const byte *>(&v);
    b.insert(b.end(), data, data + sizeof(v));
}

} // namespace parquet4seastar
