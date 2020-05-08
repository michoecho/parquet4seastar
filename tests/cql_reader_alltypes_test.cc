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
#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/reader_schema.hh>
#include <parquet4seastar/cql_reader.hh>
#include <map>
#include <sstream>

namespace parquet4seastar {

const char * generated_cql = R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "bool_ct" boolean, "bool_lt" boolean, "int8_ct" tinyint, "int8_lt" tinyint, "int16_ct" smallint, "int16_lt" smallint, "int32_ct" int, "int32_lt" int, "int64_ct" bigint, "int64_lt" bigint, "int96_ct" varint, "uint8_ct" smallint, "uint8_lt" smallint, "uint16_ct" int, "uint16_lt" int, "uint32_ct" bigint, "uint32_lt" bigint, "uint64_ct" varint, "uint64_lt" varint, "float_ct" float, "float_lt" float, "double_ct" double, "double_lt" double, "utf8" text, "string" text, "10_byte_array_ct" blob, "10_byte_array_lt" blob, "date_ct" int, "date_lt" int, "decimal_int32_ct" decimal, "decimal_int32_lt" decimal, "decimal_int64_ct" decimal, "decimal_int64_lt" decimal, "decimal_byte_array_ct" decimal, "decimal_byte_array_lt" decimal, "decimal_flba_ct" decimal, "decimal_flba_lt" decimal, "enum_ct" text, "enum_lt" text, "time_millis_ct" time, "time_utc_millis_lt" time, "time_nonutc_millis_lt" time, "time_micros_ct" time, "time_utc_micros_lt" time, "time_nonutc_micros_lt" time, "time_utc_nanos" time, "time_nonutc_nanos" time, "timestamp_millis_ct" timestamp, "timestamp_utc_millis_lt" timestamp, "timestamp_nonutc_millis_lt" timestamp, "timestamp_micros_ct" bigint, "timestamp_utc_micros_lt" bigint, "timestamp_nonutc_micros_lt" bigint, "timestamp_utc_nanos" bigint, "timestamp_nonutc_nanos" bigint, "interval_ct" duration, "interval_lt" duration, "json_ct" text, "json_lt" text, "bson_ct" blob, "bson_lt" blob, "uuid" uuid, "uint64_dictionary" varint, "optional_uint32" bigint, "twice_repeated_uint16" frozen<list<int>>, "optional_undefined_null" int, "map_int32_int32" frozen<map<int, int>>, "map_key_value_bool_bool" frozen<map<boolean, boolean>>, "map_logical" frozen<map<int, int>>, "list_float" frozen<list<float>>, "list_double" frozen<list<double>>);
INSERT INTO "parquet"("row_number", "bool_ct", "bool_lt", "int8_ct", "int8_lt", "int16_ct", "int16_lt", "int32_ct", "int32_lt", "int64_ct", "int64_lt", "int96_ct", "uint8_ct", "uint8_lt", "uint16_ct", "uint16_lt", "uint32_ct", "uint32_lt", "uint64_ct", "uint64_lt", "float_ct", "float_lt", "double_ct", "double_lt", "utf8", "string", "10_byte_array_ct", "10_byte_array_lt", "date_ct", "date_lt", "decimal_int32_ct", "decimal_int32_lt", "decimal_int64_ct", "decimal_int64_lt", "decimal_byte_array_ct", "decimal_byte_array_lt", "decimal_flba_ct", "decimal_flba_lt", "enum_ct", "enum_lt", "time_millis_ct", "time_utc_millis_lt", "time_nonutc_millis_lt", "time_micros_ct", "time_utc_micros_lt", "time_nonutc_micros_lt", "time_utc_nanos", "time_nonutc_nanos", "timestamp_millis_ct", "timestamp_utc_millis_lt", "timestamp_nonutc_millis_lt", "timestamp_micros_ct", "timestamp_utc_micros_lt", "timestamp_nonutc_micros_lt", "timestamp_utc_nanos", "timestamp_nonutc_nanos", "interval_ct", "interval_lt", "json_ct", "json_lt", "bson_ct", "bson_lt", "uuid", "uint64_dictionary", "optional_uint32", "twice_repeated_uint16", "optional_undefined_null", "map_int32_int32", "map_key_value_bool_bool", "map_logical", "list_float", "list_double") VALUES(0, false, false, -1, -1, -1, -1, -1, -1, -1, -1, 4294967295, 255, 255, 65535, 65535, 4294967295, 4294967295, 18446744073709551615, 18446744073709551615, -1.100000e+00, -1.100000e+00, -1.111111e+00, -1.111111e+00, 'parquet00/', 'parquet00/', 0xFFFFFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFFFFFF, -1, -1, -1e-5, -1e-5, -1e-10, -1e-10, -1e-2, -1e-2, -1e-5, -1e-5, 'ENUM   000', 'ENUM   000', '00:00:00.000', '00:00:00.000', '00:00:00.000', '00:00:00.000000', '00:00:00.000000', '00:00:00.000000', '00:00:00.000000000', '00:00:00.000000000', -1, -1, -1, -1, -1, -1, -1, -1, 0mo0d0ms, 0mo0d0ms, '{"key":"value"}', '{"key":"value"}', 0x42534F4E, 0x42534F4E, FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF, 18446744073709551615, 4294967295, [0], null, {-1: -1, 0: 0}, {false: false, false: false}, {-1: -1, 0: 0}, [-1.100000e+00, 0.000000e+00], [-1.111110e+00, 0.000000e+00]);
INSERT INTO "parquet"("row_number", "bool_ct", "bool_lt", "int8_ct", "int8_lt", "int16_ct", "int16_lt", "int32_ct", "int32_lt", "int64_ct", "int64_lt", "int96_ct", "uint8_ct", "uint8_lt", "uint16_ct", "uint16_lt", "uint32_ct", "uint32_lt", "uint64_ct", "uint64_lt", "float_ct", "float_lt", "double_ct", "double_lt", "utf8", "string", "10_byte_array_ct", "10_byte_array_lt", "date_ct", "date_lt", "decimal_int32_ct", "decimal_int32_lt", "decimal_int64_ct", "decimal_int64_lt", "decimal_byte_array_ct", "decimal_byte_array_lt", "decimal_flba_ct", "decimal_flba_lt", "enum_ct", "enum_lt", "time_millis_ct", "time_utc_millis_lt", "time_nonutc_millis_lt", "time_micros_ct", "time_utc_micros_lt", "time_nonutc_micros_lt", "time_utc_nanos", "time_nonutc_nanos", "timestamp_millis_ct", "timestamp_utc_millis_lt", "timestamp_nonutc_millis_lt", "timestamp_micros_ct", "timestamp_utc_micros_lt", "timestamp_nonutc_micros_lt", "timestamp_utc_nanos", "timestamp_nonutc_nanos", "interval_ct", "interval_lt", "json_ct", "json_lt", "bson_ct", "bson_lt", "uuid", "uint64_dictionary", "optional_uint32", "twice_repeated_uint16", "optional_undefined_null", "map_int32_int32", "map_key_value_bool_bool", "map_logical", "list_float", "list_double") VALUES(1, true, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00, 'parquet000', 'parquet000', 0x00000000000000000000, 0x00000000000000000000, 0, 0, 0e-5, 0e-5, 0e-10, 0e-10, 0e-2, 0e-2, 0e-5, 0e-5, 'ENUM   001', 'ENUM   001', '01:01:01.000', '01:01:01.000', '01:01:01.000', '01:01:01.000000', '01:01:01.000000', '01:01:01.000000', '01:01:01.000000000', '01:01:01.000000000', 0, 0, 0, 0, 0, 0, 0, 0, 1mo1d1ms, 1mo1d1ms, '{"key":"value"}', '{"key":"value"}', 0x42534F4E, 0x42534F4E, 00000000-0000-0000-0000-000000000000, 0, null, [1, 2], null, {0: 0, 1: 1}, {true: true, false: false}, {0: 0, 1: 1}, [0.000000e+00, 1.100000e+00], [0.000000e+00, 1.111110e+00]);
INSERT INTO "parquet"("row_number", "bool_ct", "bool_lt", "int8_ct", "int8_lt", "int16_ct", "int16_lt", "int32_ct", "int32_lt", "int64_ct", "int64_lt", "int96_ct", "uint8_ct", "uint8_lt", "uint16_ct", "uint16_lt", "uint32_ct", "uint32_lt", "uint64_ct", "uint64_lt", "float_ct", "float_lt", "double_ct", "double_lt", "utf8", "string", "10_byte_array_ct", "10_byte_array_lt", "date_ct", "date_lt", "decimal_int32_ct", "decimal_int32_lt", "decimal_int64_ct", "decimal_int64_lt", "decimal_byte_array_ct", "decimal_byte_array_lt", "decimal_flba_ct", "decimal_flba_lt", "enum_ct", "enum_lt", "time_millis_ct", "time_utc_millis_lt", "time_nonutc_millis_lt", "time_micros_ct", "time_utc_micros_lt", "time_nonutc_micros_lt", "time_utc_nanos", "time_nonutc_nanos", "timestamp_millis_ct", "timestamp_utc_millis_lt", "timestamp_nonutc_millis_lt", "timestamp_micros_ct", "timestamp_utc_micros_lt", "timestamp_nonutc_micros_lt", "timestamp_utc_nanos", "timestamp_nonutc_nanos", "interval_ct", "interval_lt", "json_ct", "json_lt", "bson_ct", "bson_lt", "uuid", "uint64_dictionary", "optional_uint32", "twice_repeated_uint16", "optional_undefined_null", "map_int32_int32", "map_key_value_bool_bool", "map_logical", "list_float", "list_double") VALUES(2, false, false, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.100000e+00, 1.100000e+00, 1.111111e+00, 1.111111e+00, 'parquet001', 'parquet001', 0x01010101010101010101, 0x01010101010101010101, 1, 1, 1e-5, 1e-5, 1e-10, 1e-10, 1e-2, 1e-2, 1e-5, 1e-5, 'ENUM   002', 'ENUM   002', '02:02:02.000', '02:02:02.000', '02:02:02.000', '02:02:02.000000', '02:02:02.000000', '02:02:02.000000', '02:02:02.000000000', '02:02:02.000000000', 1, 1, 1, 1, 1, 1, 1, 1, 2mo2d2ms, 2mo2d2ms, '{"key":"value"}', '{"key":"value"}', 0x42534F4E, 0x42534F4E, 01000000-0100-0000-0100-000001000000, 1, 1, [3, 4], null, {1: 1, 2: 2}, {false: false, true: true}, {1: 1, 2: 2}, [1.100000e+00, 2.200000e+00], [1.111110e+00, 2.222220e+00]);
)###";

SEASTAR_TEST_CASE(parquet_to_cql) {
    return seastar::async([] {
        std::vector<std::pair<std::string, std::string>> test_cases = {
        {"generated_alltypes.uncompressed.parquet", generated_cql},
        {"generated_alltypes.snappy.parquet", generated_cql},
        {"generated_alltypes.gzip.parquet", generated_cql}
        }; // test_cases
        for (const auto& [filename, output] : test_cases) {
            std::stringstream ss;
            ss << '\n';
            auto reader = file_reader::open(filename).get0();
            cql::parquet_to_cql(reader, "parquet", "row_number", ss).get();
            BOOST_CHECK_EQUAL(ss.str(), output);
        }
    });
}

} // namespace parquet4seastar
