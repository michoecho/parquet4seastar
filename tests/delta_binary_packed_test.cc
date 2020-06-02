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
#include <limits>

BOOST_AUTO_TEST_CASE(decoding) {
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

BOOST_AUTO_TEST_CASE(encoding32) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::DELTA_BINARY_PACKED);
    auto decoder = value_decoder<format::Type::INT32>({});

    for (int repeat = 0; repeat < 3; ++repeat) {
        std::vector<int32_t> input;
        for (size_t i = 0; i < 1337; ++i) {
            input.push_back(i);
        }
        input.push_back(std::numeric_limits<int32_t>::min());
        input.push_back(std::numeric_limits<int32_t>::max());
        input.push_back(std::numeric_limits<int32_t>::min());
        input.push_back(std::numeric_limits<int32_t>::max());
        for (int32_t i = 0; i < 420; ++i) {
            input.push_back(i * i);
        }
        size_t size1 = std::size(input) / 3;
        encoder->put_batch(std::data(input), size1);
        encoder->put_batch(std::data(input) + size1, std::size(input) - size1);

        bytes encoded(encoder->max_encoded_size(), 0);
        auto [n_written, encoding] = encoder->flush(encoded.data());
        encoded.resize(n_written);

        decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
        std::vector<int32_t> decoded(input.size());
        size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
        decoded.resize(n_read);

        BOOST_CHECK_EQUAL_COLLECTIONS(
                std::begin(decoded), std::end(decoded),
                std::begin(input), std::end(input));
    }
}

BOOST_AUTO_TEST_CASE(encoding64) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::DELTA_BINARY_PACKED);
    auto decoder = value_decoder<format::Type::INT64>({});

    for (int repeat = 0; repeat < 3; ++repeat) {
        std::vector<int64_t> input;
        for (size_t i = 0; i < 1337; ++i) {
            input.push_back(i);
        }
        input.push_back(std::numeric_limits<int64_t>::min());
        input.push_back(std::numeric_limits<int64_t>::max());
        input.push_back(std::numeric_limits<int64_t>::min());
        input.push_back(std::numeric_limits<int64_t>::max());
        for (int64_t i = -420; i < 420; ++i) {
            input.push_back(i * i);
        }
        size_t size1 = std::size(input) / 3;
        encoder->put_batch(std::data(input), size1);
        encoder->put_batch(std::data(input) + size1, std::size(input) - size1);

        bytes encoded(encoder->max_encoded_size(), 0);
        auto [n_written, encoding] = encoder->flush(encoded.data());
        encoded.resize(n_written);

        decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
        std::vector<int64_t> decoded(input.size());
        size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
        decoded.resize(n_read);

        BOOST_CHECK_EQUAL_COLLECTIONS(
                std::begin(decoded), std::end(decoded),
                std::begin(input), std::end(input));
    }
}

BOOST_AUTO_TEST_CASE(encoding64_empty) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::DELTA_BINARY_PACKED);
    auto decoder = value_decoder<format::Type::INT64>({});

    for (int repeat = 0; repeat < 3; ++repeat) {
        std::vector<int64_t> input;
        size_t size1 = std::size(input) / 3;
        encoder->put_batch(std::data(input), size1);
        encoder->put_batch(std::data(input) + size1, std::size(input) - size1);

        bytes encoded(encoder->max_encoded_size(), 0);
        auto [n_written, encoding] = encoder->flush(encoded.data());
        encoded.resize(n_written);

        decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
        std::vector<int64_t> decoded(input.size());
        size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
        decoded.resize(n_read);

        BOOST_CHECK_EQUAL_COLLECTIONS(
                std::begin(decoded), std::end(decoded),
                std::begin(input), std::end(input));
    }
}
