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
#include <parquet4seastar/dbp_decoding.hh>
#include <parquet4seastar/exception.hh>
#include <boost/test/included/unit_test.hpp>
#include <iostream>
#include <bitset>

namespace parquet4seastar {

//void test_encoding_happy_small() {
//    const size_t MAX_PAGE_HEADER_WRITER_SIZE = 4; // assumint 32bit values, should be 16, idk
//    const size_t MAX_BIT_WRITER_SIZE = 32;
//
//    int32_t values[] = {1, 2, 5, 8, 13};
//    size_t values_len = 5;
//    std::vector<int32_t> decoding_buffer(values_len);
//    std::vector<uint32_t> encoding_buffer(MAX_PAGE_HEADER_WRITER_SIZE + MAX_BIT_WRITER_SIZE);
//    uint8_t* header_buffer = (uint8_t*) encoding_buffer.data();
//    uint8_t* bit_buffer = ((uint8_t*) encoding_buffer.data()) + MAX_PAGE_HEADER_WRITER_SIZE;
//    DeltaBitPackEncoder<format::Type::INT32> encoder(header_buffer, MAX_PAGE_HEADER_WRITER_SIZE, bit_buffer, MAX_BIT_WRITER_SIZE);
//    DeltaBitPackDecoder<format::Type::INT32> decoder;
//
//    encoder.put(values, values_len);
//    encoder.flush_buffer();
//
//    decoder.set_data((uint8_t*) encoding_buffer.data(), MAX_PAGE_HEADER_WRITER_SIZE + MAX_BIT_WRITER_SIZE);
//    decoder.get((int32_t*) decoding_buffer.data(), values_len);
//
//    for(int i = 0; i < values_len; i++) {
//        std::cout << decoding_buffer[i] << std::endl;
//    }
//}

void print_header(uint8_t* header) {
    std::cout << "HEADER\n";
    for (int j = 0; j < 4; j++) {
        for (int i = 0; i < 4; i++) {
            std::bitset<8> x(header[j * 4 + i]);
            std::cout << x << ' ';
        }
        std::cout << "\n";
    }
}

void test_encoding_happy_large() {
    const int NUM_VALUES = 10000;

    std::vector<int32_t> values;

    values.push_back(2*NUM_VALUES);
    for (int i = 1; i < NUM_VALUES; i++) {
        values.push_back(values[i-1] + i);
    }
    std::array<uint8_t, NUM_VALUES * 4 * 2> encoded_buffer;
    DeltaBitPackEncoder<format::Type::INT32> encoder;
    encoder.put(values.data(), NUM_VALUES);
    encoder.flush_buffer((uint8_t*)encoded_buffer.data());

    print_header(encoded_buffer.data());

    std::array<int32_t, NUM_VALUES> decoding_buffer;
    DeltaBitPackDecoder<format::Type::INT32> decoder;
    decoder.set_data((uint8_t*) encoded_buffer.data(), encoded_buffer.size());
    decoder.get(decoding_buffer.data(), NUM_VALUES);

    for(int i = 0; i < NUM_VALUES; i++) {
        assert(values[i] == decoding_buffer[i]);
    }
}

BOOST_AUTO_TEST_CASE(encoding_ok) {
//    test_encoding_happy_small();
    test_encoding_happy_large();
}


} // namespace parquet4seastar
