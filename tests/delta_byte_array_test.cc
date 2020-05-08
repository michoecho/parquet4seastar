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

constexpr parquet4seastar::bytes_view operator ""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

BOOST_AUTO_TEST_CASE(happy) {
    using namespace parquet4seastar;
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    bytes suffixes;
    {
        bytes block_size = {0x80, 0x01}; // 128
        bytes miniblocks_in_block = {0x1}; // 1
        bytes values_in_total = {0x4}; // 4
        bytes first_value = {0x0a}; // 5
        bytes header
                = block_size
                + miniblocks_in_block
                + values_in_total
                + first_value;

        bytes min_delta = {0x0}; // 0
        bytes miniblock_bitwidths = {0x1}; // 1
        bytes miniblocks = {
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
        };
        bytes block = min_delta + miniblock_bitwidths + miniblocks;

        bytes_view strings[] = {
            "aaaaa"_bv,
            "bbbbbb"_bv,
            "ccccccc"_bv,
            "dddddddd"_bv,
        };

        bytes concatenated_strings;
        concatenated_strings += strings[0];
        concatenated_strings += strings[1];
        concatenated_strings += strings[2];
        concatenated_strings += strings[3];

        suffixes = header + block + concatenated_strings;
    }

    bytes lengths;
    {
        bytes block_size = {0x80, 0x01}; // 128
        bytes miniblocks_in_block = {0x1}; // 1
        bytes values_in_total = {0x4}; // 4
        bytes first_value = {0x0}; // 0
        bytes header
                = block_size
                + miniblocks_in_block
                + values_in_total
                + first_value;

        bytes min_delta = {0x2}; // 0
        bytes miniblock_bitwidths = {0x1}; // 1
        bytes miniblocks = {
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
            0b11111111, 0b11111111, 0b11111111, 0b11111111,
        };
        bytes block = min_delta + miniblock_bitwidths + miniblocks;

        lengths = header + block;
    }

    bytes test_data = lengths + suffixes;
    decoder.reset(test_data, format::Encoding::DELTA_BYTE_ARRAY);

    using output_type = decltype(decoder)::output_type;

    std::vector<output_type> out(10000);
    size_t n_read = decoder.read_batch(std::size(out), std::data(out));
    out.resize(n_read);

    bytes_view expected_bv[] = {
        "aaaaa"_bv,
        "aabbbbbb"_bv,
        "aabbccccccc"_bv,
        "aabbccdddddddd"_bv,
    };
    output_type expected[] = {
        output_type(expected_bv[0].data(), expected_bv[0].size()),
        output_type(expected_bv[1].data(), expected_bv[1].size()),
        output_type(expected_bv[2].data(), expected_bv[2].size()),
        output_type(expected_bv[3].data(), expected_bv[3].size()),
    };

    BOOST_CHECK_EQUAL(std::size(out), std::size(expected));
    BOOST_CHECK(std::equal(
            std::begin(out), std::end(out),
            std::begin(expected), std::end(expected)));
}
