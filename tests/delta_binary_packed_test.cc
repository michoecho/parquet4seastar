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

BOOST_AUTO_TEST_CASE(happy) {
    using namespace parquet4seastar;
    auto decoder = value_decoder<format::Type::INT32>({});

    bytes block_size = {0x80, 0x01}; // 128
    bytes miniblocks_in_block = {0x4}; // 4
    bytes values_in_total = {0x42}; // 66
    bytes first_value = {0x10}; // 8
    bytes header
            = block_size
            + miniblocks_in_block
            + values_in_total
            + first_value;

    bytes min_delta = {0x1}; // -1
    bytes miniblock_bitwidths = {0x4, 0x3, 0x2, 0x1}; // 4, 3, 2, 1
    bytes miniblocks = {
        0b00010001, 0b00010001, 0b00010001, 0b00010001,
        0b00000000, 0b00000000, 0b00000000, 0b00000000,
        0b00000000, 0b00000000, 0b00000000, 0b00000000,
        0b00011001, 0b00010001, 0b00010001, 0b00010001,

        0b01001001, 0b10010010, 0b00100100, 0b01001001,
        0b10010010, 0b00100100, 0b01001001, 0b10010010,
        0b00100100, 0b01001001, 0b10010010, 0b00100100,
        0b01001001, 0b10010010, 0b00100100, 0b01001001,

        0b11111101, 0b11111111, 0b11111111, 0b11111111,
        0b11111111, 0b11111111, 0b11111111, 0b11111111,

        0b11111111, 0b11111111, 0b11111111, 0b11111111,
    };
    bytes block = min_delta + miniblock_bitwidths + miniblocks;

    bytes test_data = header + block;
    decoder.reset(test_data, format::Encoding::DELTA_BINARY_PACKED);

    std::vector<int32_t> out(10000);
    size_t n_read = decoder.read_batch(std::size(out), std::data(out));
    out.resize(n_read);

    int32_t expected[] = {
        8,

        8, 8, 8, 8, 8, 8, 8, 8,
        7, 6, 5, 4, 3, 2, 1, 0,
        -1, -2, -3, -4, -5, -6, -7, -8,
        0, 0, 0, 0, 0, 0, 0, 0,

        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,

        0,
    };

    BOOST_CHECK_EQUAL_COLLECTIONS(
            std::begin(out), std::end(out),
            std::begin(expected), std::end(expected));
}
