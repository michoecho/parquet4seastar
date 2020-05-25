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
#include <parquet4seastar/parquet_types.h>
#include <boost/test/included/unit_test.hpp>

namespace parquet4seastar {

void test_encoding_happy_32() {
    const int NUM_VALUES = 10000;
    std::vector<int32_t> values;

    values.push_back(2*NUM_VALUES);
    for (int i = 1; i < NUM_VALUES; i++) {
        values.push_back(values[i-1] + i);
    }
    std::array<uint8_t, NUM_VALUES * 4 * 2> encoding_buffer;
    std::basic_string_view<uint8_t> encoding_buffer_bb{encoding_buffer.data(), encoding_buffer.size()};
    std::array<int32_t, NUM_VALUES> decoding_buffer;

    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::DELTA_BINARY_PACKED);
    value_decoder<format::Type::INT32> decoder{false};

    encoder->put_batch(values.data(), NUM_VALUES);
    encoder->flush((uint8_t*)encoding_buffer.data());

    decoder.reset(encoding_buffer_bb, format::Encoding::DELTA_BINARY_PACKED);
    decoder.read_batch(NUM_VALUES, decoding_buffer.data());

    for(int i = 0; i < NUM_VALUES; i++) {
        assert(values[i] == decoding_buffer[i]);
    }
}

void test_encoding_happy_64() {
    const int NUM_VALUES = 10000;
    std::vector<int64_t> values;

    values.push_back(2*NUM_VALUES);
    for (int i = 1; i < NUM_VALUES; i++) {
        values.push_back(values[i-1] + i);
    }
    std::array<uint8_t, NUM_VALUES * 4 * 2> encoding_buffer;
    std::basic_string_view<uint8_t> encoding_buffer_bb{encoding_buffer.data(), encoding_buffer.size()};
    std::array<int64_t, NUM_VALUES> decoding_buffer;

    auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::DELTA_BINARY_PACKED);
    value_decoder<format::Type::INT64> decoder{false};

    encoder->put_batch(values.data(), NUM_VALUES);
    encoder->flush((uint8_t*)encoding_buffer.data());

    decoder.reset(encoding_buffer_bb, format::Encoding::DELTA_BINARY_PACKED);
    decoder.read_batch(NUM_VALUES, decoding_buffer.data());

    for(int i = 0; i < NUM_VALUES; i++) {
        assert(values[i] == decoding_buffer[i]);
    }
}


BOOST_AUTO_TEST_CASE(encoding_ok) {
    test_encoding_happy_64();
    test_encoding_happy_32();
}

} // namespace parquet4seastar