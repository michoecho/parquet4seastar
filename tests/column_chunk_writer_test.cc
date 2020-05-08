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

#include <seastar/testing/test_case.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/seastar.hh>
#include <parquet4seastar/column_chunk_reader.hh>
#include <parquet4seastar/column_chunk_writer.hh>
#include <unistd.h>

namespace parquet4seastar {

constexpr bytes_view operator ""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

seastar::temporary_buffer<uint8_t> operator ""_tb(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

constexpr std::string_view test_file_name = "/tmp/parquet4seastar_column_chunk_writer_test.bin";

SEASTAR_TEST_CASE(column_roundtrip) {
    return seastar::async([] {
        seastar::file output_file = seastar::open_file_dma(
                test_file_name.data(), seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create).get0();

        // Write
        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file);
        constexpr format::Type::type FLBA = format::Type::FIXED_LEN_BYTE_ARRAY;
        column_chunk_writer<FLBA> w{
            1,
            1,
            make_value_encoder<FLBA>(format::Encoding::RLE_DICTIONARY),
            compressor::make(format::CompressionCodec::SNAPPY)};
        w.put(1, 1, "a"_bv);
        w.put(0, 1, "b"_bv);
        w.put(1, 1, "c"_bv);
        w.flush_page();
        w.put(1, 1, "a"_bv);
        w.put(0, 1, "d"_bv);
        w.put(1, 1, "e"_bv);
        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get0();
        output.flush().get();
        output.close().get();

        BOOST_CHECK_EQUAL(cmd->num_values, 6);

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file_name.data(), seastar::open_flags::ro).get0();

        column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY> r{
            page_reader{seastar::make_file_input_stream(std::move(input_file))},
            format::CompressionCodec::SNAPPY,
            1,
            1,
            std::optional<uint32_t>(1)};

        constexpr size_t n_levels = 6;
        constexpr size_t n_values = 4;

        int32_t def[n_levels];
        int32_t rep[n_levels];
        seastar::temporary_buffer<uint8_t> val[n_values];
        int32_t expected_def[] = {1, 0, 1, 1, 0, 1};
        int32_t expected_rep[] = {1, 1, 1, 1, 1, 1};
        seastar::temporary_buffer<uint8_t> expected_val[] = {"a"_tb, "c"_tb, "a"_tb, "e"_tb};

        int32_t* defp = def;
        int32_t* repp = rep;
        seastar::temporary_buffer<uint8_t>* valp = val;
        size_t n_to_read = n_levels;
        while (size_t n_read = r.read_batch(n_to_read, defp, repp, valp).get0()) {
            for (size_t i = 0; i < n_read; ++i) {
                if (defp[i] == 1) {
                    ++valp;
                }
            }
            defp += n_read;
            repp += n_read;
            n_to_read -= n_read;
        }

        BOOST_CHECK_EQUAL(defp - def, n_levels);
        BOOST_CHECK_EQUAL(repp - rep, n_levels);
        BOOST_CHECK_EQUAL(valp - val, n_values);
        BOOST_CHECK(std::equal(std::begin(def), std::end(def), std::begin(expected_def), std::end(expected_def)));
        BOOST_CHECK(std::equal(std::begin(rep), std::end(rep), std::begin(expected_rep), std::end(expected_rep)));
        BOOST_CHECK(std::equal(std::begin(val), std::end(val), std::begin(expected_val), std::end(expected_val)));
    });
}

} // namespace parquet4seastar
