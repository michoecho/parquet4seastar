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

#define BOOST_TEST_MODULE parquet

#include <parquet4seastar/compression.hh>
#include <parquet4seastar/exception.hh>
#include <boost/test/included/unit_test.hpp>

namespace parquet4seastar::compression {

void test_compression_happy(format::CompressionCodec::type compression) {
    bytes raw;
    for (size_t i = 0; i < 70000; ++i) {
        raw.push_back(static_cast<byte>(i));
    }
    auto c = compressor::make(compression);
    bytes compressed = c->compress(raw);
    bytes decompressed = c->decompress(compressed, bytes(raw.size() + 1, 0));
    BOOST_CHECK(raw == decompressed);
}

void test_compression_overflow(format::CompressionCodec::type compression) {
    bytes raw(42, 0);
    auto c = compressor::make(compression);
    bytes compressed = c->compress(raw);
    BOOST_CHECK_THROW(c->decompress(compressed, bytes(raw.size() - 1, 0)), parquet_exception);
}

BOOST_AUTO_TEST_CASE(compression_uncompressed) {
    test_compression_happy(format::CompressionCodec::UNCOMPRESSED);
    test_compression_overflow(format::CompressionCodec::UNCOMPRESSED);
}

BOOST_AUTO_TEST_CASE(compression_gzip) {
    test_compression_happy(format::CompressionCodec::GZIP);
    test_compression_overflow(format::CompressionCodec::GZIP);
}

BOOST_AUTO_TEST_CASE(compression_snappy) {
    test_compression_happy(format::CompressionCodec::SNAPPY);
    test_compression_overflow(format::CompressionCodec::SNAPPY);
}

} // namespace parquet4seastar
