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
#include <cstdint>
#include <cstddef>
#include <parquet4seastar/bytes.hh>
#include <parquet4seastar/parquet_types.h>

namespace parquet4seastar {

class compressor {
public:
    // out has to be big enough to hold the uncompressed data.
    // Otherwise, an exception is thrown.
    // We are always supposed to know the exact uncompressed size in Parquet.
    virtual bytes decompress(bytes_view in, bytes&& out) const = 0;

    // out will be resized appropriately to hold the compressed data.
    virtual bytes compress(bytes_view in, bytes&& out = bytes()) const = 0;

    virtual format::CompressionCodec::type type() const = 0;

    static std::unique_ptr<compressor> make(format::CompressionCodec::type compression);

    virtual ~compressor() = default;
};

} // namespace
