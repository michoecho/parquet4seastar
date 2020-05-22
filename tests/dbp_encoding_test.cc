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

#include <parquet4seastar/dbp_encoding.hh>
#include <parquet4seastar/exception.hh>
#include <boost/test/included/unit_test.hpp>
#include <iostream>

namespace parquet4seastar {

void test_encoding_happy() {
    int64_t values[] = {1, 2, 3, 4, 5};
    size_t values_len = 5;
    uint8_t* header_buffer = new uint8_t[MAX_PAGE_HEADER_WRITER_SIZE];
    uint8_t* output_buffer = new uint8_t[64];
    DeltaBitPackEncoder<format::Type::INT64> encoder(header_buffer, MAX_PAGE_HEADER_WRITER_SIZE, output_buffer, 64);
    encoder.put(values, values_len);
    encoder.flush_buffer();

    std::cout << "Header: " << std::endl;
    for (int i = 0; i<MAX_PAGE_HEADER_WRITER_SIZE; i++) {
        std::cout << "" << std::endl;
    }
}

//void test_compression_overflow(format::CompressionCodec::type compression) {
//    bytes raw(42, 0);
//    auto c = compressor::make(compression);
//    bytes compressed = c->compress(raw);
//    BOOST_CHECK_THROW(c->decompress(compressed, bytes(raw.size() - 1, 0)), parquet_exception);
//}

BOOST_AUTO_TEST_CASE(encoding_ok) {
    test_encoding_happy();
//    test_compression_overflow(format::CompressionCodec::UNCOMPRESSED);
}


} // namespace parquet4seastar
