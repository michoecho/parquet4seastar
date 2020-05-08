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

#include <parquet4seastar/rle_encoding.hh>

#include <boost/test/included/unit_test.hpp>

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>
#include <array>

using parquet4seastar::BitReader;
using parquet4seastar::RleDecoder;

BOOST_AUTO_TEST_CASE(BitReader_happy) {
    constexpr int bit_width = 3;
    std::array<uint8_t, 6> packed = {
        0b10001000, 0b01000110, // {0, 1, 2, 3, 4} packed with bit width 3
        0b10000000, 0b00000001, // 128 encoded as LEB128
        0b11111111, 0b00000001, // -128 encoded as zigzag
    };

    std::array<int, 2> unpacked_1;
    const std::array<int, 2> expected_1 = {0, 1};
    std::array<int, 3> unpacked_2;
    const std::array<int, 3> expected_2 = {2, 3, 4};
    uint32_t unpacked_3;
    const uint32_t expected_3 = 128;
    int32_t unpacked_4;
    const int32_t expected_4 = -128;

    bool ok;
    int values_read;
    BitReader reader(packed.data(), packed.size());

    values_read = reader.GetBatch(bit_width, unpacked_1.data(), expected_1.size());
    BOOST_CHECK_EQUAL(values_read, expected_1.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(unpacked_1.begin(), unpacked_1.end(), expected_1.begin(), expected_1.end());

    values_read = reader.GetBatch(bit_width, unpacked_2.data(), expected_2.size());
    BOOST_CHECK_EQUAL(values_read, expected_2.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(unpacked_2.begin(), unpacked_2.end(), expected_2.begin(), expected_2.end());

    ok = reader.GetVlqInt(&unpacked_3);
    BOOST_CHECK(ok);
    BOOST_CHECK_EQUAL(unpacked_3, expected_3);

    ok = reader.GetZigZagVlqInt(&unpacked_4);
    BOOST_CHECK(ok);
    BOOST_CHECK_EQUAL(unpacked_4, expected_4);

    values_read = reader.GetBatch(bit_width, unpacked_2.data(), 999999);
    BOOST_CHECK_EQUAL(values_read, 0);
}

BOOST_AUTO_TEST_CASE(BitReader_ULEB128_corrupted) {
    std::array<uint8_t, 1> packed = {
        0b10000000,// Incomplete ULEB128
    };

    uint32_t unpacked;
    BitReader reader(packed.data(), packed.size());
    bool ok = reader.GetVlqInt(&unpacked);
    BOOST_CHECK(!ok);
}

BOOST_AUTO_TEST_CASE(BitReader_ULEB128_overflow) {
    std::array<uint8_t, 7> packed = {
        0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b00000000
    };

    uint32_t unpacked;
    BitReader reader(packed.data(), packed.size());
    bool ok = reader.GetVlqInt(&unpacked);
    BOOST_CHECK(!ok);
}

BOOST_AUTO_TEST_CASE(BitReader_zigzag_corrupted) {
    std::array<uint8_t, 1> packed = {
        0b10000000, // Incomplete zigzag
    };

    int32_t unpacked;
    BitReader reader(packed.data(), packed.size());
    bool ok = reader.GetZigZagVlqInt(&unpacked);
    BOOST_CHECK(!ok);
}

BOOST_AUTO_TEST_CASE(BitReader_zigzag_overflow) {
    std::array<uint8_t, 7> packed = {
        0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b00000000
    };

    int32_t unpacked;
    BitReader reader(packed.data(), packed.size());
    bool ok = reader.GetZigZagVlqInt(&unpacked);
    BOOST_CHECK(!ok);
}

// Refer to doc/parquet/Encodings.md (section about RLE)
// for a description of the encoding being tested here.

BOOST_AUTO_TEST_CASE(RleDecoder_happy) {
    constexpr int bit_width = 3;
    std::array<uint8_t, 6> packed = {
        0b00000011, 0b10001000, 0b11000110, 0b11111010, // bit-packed-run {0, 1, 2, 3, 4, 5, 6, 7}
        0b00001000, 0b00000101 // rle-run {5, 5, 5, 5}
    };
    std::array<int, 6> unpacked_1;
    const std::array<int, 6> expected_1 = {0, 1, 2, 3, 4, 5};
    std::array<int, 4> unpacked_2;
    const std::array<int, 4> expected_2 = {6, 7, 5, 5};
    std::array<int, 2> unpacked_3;
    const std::array<int, 2> expected_3 = {5, 5};

    int values_read;
    RleDecoder reader(packed.data(), packed.size(), bit_width);

    values_read = reader.GetBatch(unpacked_1.data(), expected_1.size());
    BOOST_CHECK_EQUAL(values_read, expected_1.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(unpacked_1.begin(), unpacked_1.end(), expected_1.begin(), expected_1.end());

    values_read = reader.GetBatch(unpacked_2.data(), expected_2.size());
    BOOST_CHECK_EQUAL(values_read, expected_2.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(unpacked_2.begin(), unpacked_2.end(), expected_2.begin(), expected_2.end());

    values_read = reader.GetBatch(unpacked_3.data(), 9999999);
    BOOST_CHECK_EQUAL(values_read, expected_3.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(unpacked_3.begin(), unpacked_3.end(), expected_3.begin(), expected_3.end());

    values_read = reader.GetBatch(unpacked_2.data(), 9999999);
    BOOST_CHECK_EQUAL(values_read, 0);
}

BOOST_AUTO_TEST_CASE(RleDecoder_bit_packed_ULEB128) {
    constexpr int bit_width = 16;
    std::array<uint8_t, 1026> packed = {
        0b10000001, 0b00000001// bit-packed-run with 8 * 64 values
    };
    std::array<int, 512> expected;
    std::array<int, 512> unpacked;
    for (size_t i = 0; i < expected.size(); ++i) {
        uint16_t value = i;
        memcpy(&packed[2 + i*2], &value, 2);
        expected[i] = i;
    }

    int values_read;
    RleDecoder reader(packed.data(), packed.size(), bit_width);

    values_read = reader.GetBatch(unpacked.data(), 9999999);
    BOOST_CHECK_EQUAL(values_read, expected.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(unpacked.begin(), unpacked.end(), expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(RleDecoder_rle_ULEB128) {
    constexpr int bit_width = 8;
    std::array<uint8_t, 3> packed = {
        0b10000000, 0b00000001, 0b00000101 // rle-run with 64 copies of 5
    };
    std::array<int, 64> expected;
    std::array<int, 64> unpacked;
    for (size_t i = 0; i < expected.size(); ++i) {
        expected[i] = 5;
    }

    int values_read;
    RleDecoder reader(packed.data(), packed.size(), bit_width);

    values_read = reader.GetBatch(unpacked.data(), 9999999);
    BOOST_CHECK_EQUAL(values_read, expected.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(unpacked.begin(), unpacked.end(), expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(RleDecoder_bit_packed_too_short) {
    constexpr int bit_width = 3;
    std::array<uint8_t, 3> packed = {
        0b00000011, 0b10001000, 0b11000110 // bit-packed-run {0, 1, 2, 3, 4, EOF
    };
    std::array<int, 8> unpacked;

    RleDecoder reader(packed.data(), packed.size(), bit_width);
    int values_read = reader.GetBatch(unpacked.data(), unpacked.size());
    BOOST_CHECK_EQUAL(values_read, 0);
}

BOOST_AUTO_TEST_CASE(RleDecoder_rle_too_short) {
    constexpr int bit_width = 3;
    std::array<uint8_t, 1> packed = {
        0b00001000 // rle-run without value
    };
    std::array<int, 4> unpacked;

    RleDecoder reader(packed.data(), packed.size(), bit_width);
    int values_read = reader.GetBatch(unpacked.data(), unpacked.size());
    BOOST_CHECK_EQUAL(values_read, 0);
}

BOOST_AUTO_TEST_CASE(RleDecoder_bit_packed_ULEB128_too_short) {
    constexpr int bit_width = 3;
    std::array<uint8_t, 1> packed = {
        0b10000001 // bit-packed-run of incomplete ULEB128 length
    };
    std::array<int, 512> unpacked;

    RleDecoder reader(packed.data(), packed.size(), bit_width);
    int values_read = reader.GetBatch(unpacked.data(), unpacked.size());
    BOOST_CHECK_EQUAL(values_read, 0);
}

BOOST_AUTO_TEST_CASE(RleDecoder_rle_ULEB128_too_short) {
    constexpr int bit_width = 3;
    std::array<uint8_t, 1> packed = {
        0b10000000 // rle-run of incomplete ULEB128 length
    };
    std::array<int, 512> unpacked;

    RleDecoder reader(packed.data(), packed.size(), bit_width);
    int values_read = reader.GetBatch(unpacked.data(), unpacked.size());
    BOOST_CHECK_EQUAL(values_read, 0);
}
