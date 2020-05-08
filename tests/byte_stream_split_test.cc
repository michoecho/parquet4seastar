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

#include <parquet4seastar/encoding.hh>
#include <boost/test/included/unit_test.hpp>
#include <vector>
#include <array>

void test_byte_stream_split_float() {
    using namespace parquet4seastar;
    auto decoder = value_decoder<format::Type::FLOAT>({});

    bytes test_data = {
        0xa1, 0xb1, 0xc1,
        0xa2, 0xb2, 0xc2,
        0xa3, 0xb3, 0xc3,
        0xa4, 0xb4, 0xc4,
    };

    decoder.reset(test_data, format::Encoding::BYTE_STREAM_SPLIT);

    using output_type = decltype(decoder)::output_type;
    std::vector<output_type> out(10000);
    size_t n_read = decoder.read_batch(std::size(out), std::data(out));
    out.resize(n_read);

    bytes expected_bytes = {
        0xa1, 0xa2, 0xa3, 0xa4,
        0xb1, 0xb2, 0xb3, 0xb4,
        0xc1, 0xc2, 0xc3, 0xc4,
    };

    bytes_view out_bytes(
            reinterpret_cast<byte*>(out.data()),
            out.size() * sizeof(output_type));

    BOOST_CHECK_EQUAL_COLLECTIONS(
            std::begin(out_bytes), std::end(out_bytes),
            std::begin(expected_bytes), std::end(expected_bytes));
}

void test_byte_stream_split_double() {
    using namespace parquet4seastar;
    auto decoder = value_decoder<format::Type::DOUBLE>({});

    bytes test_data = {
        0xa1, 0xb1, 0xc1,
        0xa2, 0xb2, 0xc2,
        0xa3, 0xb3, 0xc3,
        0xa4, 0xb4, 0xc4,
        0xa5, 0xb5, 0xc5,
        0xa6, 0xb6, 0xc6,
        0xa7, 0xb7, 0xc7,
        0xa8, 0xb8, 0xc8,
    };

    decoder.reset(test_data, format::Encoding::BYTE_STREAM_SPLIT);

    using output_type = decltype(decoder)::output_type;
    std::vector<output_type> out(10000);
    size_t n_read = decoder.read_batch(std::size(out), std::data(out));
    out.resize(n_read);

    bytes expected_bytes = {
        0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8,
        0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8,
        0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8,
    };

    bytes_view out_bytes(
            reinterpret_cast<byte*>(out.data()),
            out.size() * sizeof(output_type));

    BOOST_CHECK_EQUAL_COLLECTIONS(
            std::begin(out_bytes), std::end(out_bytes),
            std::begin(expected_bytes), std::end(expected_bytes));
}

BOOST_AUTO_TEST_CASE(happy) {
    test_byte_stream_split_float();
    test_byte_stream_split_double();
}
