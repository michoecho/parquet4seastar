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
#include <seastar/core/print.hh>
#include <map>
#include <sstream>

namespace parquet4seastar {

using namespace seastar;

SEASTAR_TEST_CASE(parquet_to_cql) {
    return async([] {
        std::vector<std::pair<std::string, std::string>> test_cases = {
        {"data/alltypes_plain.snappy.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "id" int, "bool_col" boolean, "tinyint_col" int, "smallint_col" int, "int_col" int, "bigint_col" bigint, "float_col" float, "double_col" double, "date_string_col" blob, "string_col" blob, "timestamp_col" varint);
INSERT INTO "parquet"("row_number", "id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col") VALUES(0, 6, true, 0, 0, 0, 0, 0.000000e+00, 0.000000e+00, 0x30342F30312F3039, 0x30, 2454923);
INSERT INTO "parquet"("row_number", "id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col") VALUES(1, 7, false, 1, 1, 1, 10, 1.100000e+00, 1.010000e+01, 0x30342F30312F3039, 0x31, -2389630777127629293778274933);
)###"},
        {"data/alltypes_dictionary.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "id" int, "bool_col" boolean, "tinyint_col" int, "smallint_col" int, "int_col" int, "bigint_col" bigint, "float_col" float, "double_col" double, "date_string_col" blob, "string_col" blob, "timestamp_col" varint);
INSERT INTO "parquet"("row_number", "id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col") VALUES(0, 0, true, 0, 0, 0, 0, 0.000000e+00, 0.000000e+00, 0x30312F30312F3039, 0x30, 2454833);
INSERT INTO "parquet"("row_number", "id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col") VALUES(1, 1, false, 1, 1, 1, 10, 1.100000e+00, 1.010000e+01, 0x30312F30312F3039, 0x31, -2389630777127629293778275023);
)###"},
        {"data/binary.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "foo" blob);
INSERT INTO "parquet"("row_number", "foo") VALUES(0, 0x00);
INSERT INTO "parquet"("row_number", "foo") VALUES(1, 0x01);
INSERT INTO "parquet"("row_number", "foo") VALUES(2, 0x02);
INSERT INTO "parquet"("row_number", "foo") VALUES(3, 0x03);
INSERT INTO "parquet"("row_number", "foo") VALUES(4, 0x04);
INSERT INTO "parquet"("row_number", "foo") VALUES(5, 0x05);
INSERT INTO "parquet"("row_number", "foo") VALUES(6, 0x06);
INSERT INTO "parquet"("row_number", "foo") VALUES(7, 0x07);
INSERT INTO "parquet"("row_number", "foo") VALUES(8, 0x08);
INSERT INTO "parquet"("row_number", "foo") VALUES(9, 0x09);
INSERT INTO "parquet"("row_number", "foo") VALUES(10, 0x0A);
INSERT INTO "parquet"("row_number", "foo") VALUES(11, 0x0B);
)###"},
        {"data/byte_array_decimal.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "value" decimal);
INSERT INTO "parquet"("row_number", "value") VALUES(0, 100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(1, 200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(2, 300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(3, 400e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(4, 500e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(5, 600e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(6, 700e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(7, 800e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(8, 900e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(9, 1000e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(10, 1100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(11, 1200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(12, 1300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(13, 1400e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(14, 1500e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(15, 1600e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(16, 1700e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(17, 1800e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(18, 1900e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(19, 2000e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(20, 2100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(21, 2200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(22, 2300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(23, 2400e-2);
)###"},
        {"data/datapage_v2.snappy.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "a" text, "b" int, "c" double, "d" boolean, "e" frozen<list<int>>);
INSERT INTO "parquet"("row_number", "a", "b", "c", "d", "e") VALUES(0, 'abc', 1, 2.000000e+00, false, [1, 2, 3]);
INSERT INTO "parquet"("row_number", "a", "b", "c", "d", "e") VALUES(1, 'abc', 2, 3.000000e+00, true, null);
INSERT INTO "parquet"("row_number", "a", "b", "c", "d", "e") VALUES(2, 'abc', 3, 4.000000e+00, true, null);
INSERT INTO "parquet"("row_number", "a", "b", "c", "d", "e") VALUES(3, null, 4, 5.000000e+00, true, [1, 2, 3]);
INSERT INTO "parquet"("row_number", "a", "b", "c", "d", "e") VALUES(4, 'abc', 5, 2.000000e+00, false, [1, 2]);
)###"},
        {"data/int32_decimal.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "value" decimal);
INSERT INTO "parquet"("row_number", "value") VALUES(0, 100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(1, 200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(2, 300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(3, 400e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(4, 500e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(5, 600e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(6, 700e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(7, 800e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(8, 900e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(9, 1000e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(10, 1100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(11, 1200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(12, 1300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(13, 1400e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(14, 1500e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(15, 1600e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(16, 1700e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(17, 1800e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(18, 1900e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(19, 2000e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(20, 2100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(21, 2200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(22, 2300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(23, 2400e-2);
)###"},
        {"data/int64_decimal.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "value" decimal);
INSERT INTO "parquet"("row_number", "value") VALUES(0, 100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(1, 200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(2, 300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(3, 400e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(4, 500e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(5, 600e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(6, 700e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(7, 800e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(8, 900e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(9, 1000e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(10, 1100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(11, 1200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(12, 1300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(13, 1400e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(14, 1500e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(15, 1600e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(16, 1700e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(17, 1800e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(18, 1900e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(19, 2000e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(20, 2100e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(21, 2200e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(22, 2300e-2);
INSERT INTO "parquet"("row_number", "value") VALUES(23, 2400e-2);
)###"},
        {"data/nested_lists.snappy.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "a" frozen<list<frozen<list<frozen<list<text>>>>>>, "b" int);
INSERT INTO "parquet"("row_number", "a", "b") VALUES(0, [[['a', 'b'], ['c']], [null, ['d']]], 1);
INSERT INTO "parquet"("row_number", "a", "b") VALUES(1, [[['a', 'b'], ['c', 'd']], [null, ['e']]], 1);
INSERT INTO "parquet"("row_number", "a", "b") VALUES(2, [[['a', 'b'], ['c', 'd'], ['e']], [null, ['f']]], 1);
)###"},
        {"data/nested_maps.snappy.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "a" frozen<map<text, frozen<map<int, boolean>>>>, "b" int, "c" double);
INSERT INTO "parquet"("row_number", "a", "b", "c") VALUES(0, {'a': {1: true, 2: false}}, 1, 1.000000e+00);
INSERT INTO "parquet"("row_number", "a", "b", "c") VALUES(1, {'b': {1: true}}, 1, 1.000000e+00);
INSERT INTO "parquet"("row_number", "a", "b", "c") VALUES(2, {'c': null}, 1, 1.000000e+00);
INSERT INTO "parquet"("row_number", "a", "b", "c") VALUES(3, {'d': {}}, 1, 1.000000e+00);
INSERT INTO "parquet"("row_number", "a", "b", "c") VALUES(4, {'e': {1: true}}, 1, 1.000000e+00);
INSERT INTO "parquet"("row_number", "a", "b", "c") VALUES(5, {'f': {3: true, 4: false, 5: true}}, 1, 1.000000e+00);
)###"},
        {"data/repeated_no_annotation.parquet", R"###(
CREATE TYPE "parquet_udt_0" ("number" bigint, "kind" text);
CREATE TYPE "parquet_udt_1" ("phone" frozen<list<"parquet_udt_0">>);
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "id" int, "phoneNumbers" "parquet_udt_1");
INSERT INTO "parquet"("row_number", "id", "phoneNumbers") VALUES(0, 1, null);
INSERT INTO "parquet"("row_number", "id", "phoneNumbers") VALUES(1, 2, null);
INSERT INTO "parquet"("row_number", "id", "phoneNumbers") VALUES(2, 3, {"phone": []});
INSERT INTO "parquet"("row_number", "id", "phoneNumbers") VALUES(3, 4, {"phone": [{"number": 5555555555, "kind": null}]});
INSERT INTO "parquet"("row_number", "id", "phoneNumbers") VALUES(4, 5, {"phone": [{"number": 1111111111, "kind": 'home'}]});
INSERT INTO "parquet"("row_number", "id", "phoneNumbers") VALUES(5, 6, {"phone": [{"number": 1111111111, "kind": 'home'}, {"number": 2222222222, "kind": null}, {"number": 3333333333, "kind": 'mobile'}]});
)###"},
        {"data/nonnullable.impala.parquet", R"###(
CREATE TYPE "parquet_udt_0" ("e" int, "f" text);
CREATE TYPE "parquet_udt_1" ("D" frozen<list<frozen<list<"parquet_udt_0">>>>);
CREATE TYPE "parquet_udt_2" ("i" frozen<list<double>>);
CREATE TYPE "parquet_udt_3" ("h" frozen<"parquet_udt_2">);
CREATE TYPE "parquet_udt_4" ("a" int, "B" frozen<list<int>>, "c" frozen<"parquet_udt_1">, "G" frozen<map<text, "parquet_udt_3">>);
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "ID" bigint, "Int_Array" frozen<list<int>>, "int_array_array" frozen<list<frozen<list<int>>>>, "Int_Map" frozen<map<text, int>>, "int_map_array" frozen<list<frozen<map<text, int>>>>, "nested_Struct" "parquet_udt_4");
INSERT INTO "parquet"("row_number", "ID", "Int_Array", "int_array_array", "Int_Map", "int_map_array", "nested_Struct") VALUES(0, 8, [-1], [[-1, -2], []], {'k1': -1}, [{}, {'k1': 1}, {}, {}], {"a": -1, "B": [-1], "c": {"D": [[{"e": -1, "f": 'nonnullable'}]]}, "G": {}});
)###"},
        {"data/nullable.impala.parquet", R"###(
CREATE TYPE "parquet_udt_0" ("E" int, "F" text);
CREATE TYPE "parquet_udt_1" ("d" frozen<list<frozen<list<"parquet_udt_0">>>>);
CREATE TYPE "parquet_udt_2" ("i" frozen<list<double>>);
CREATE TYPE "parquet_udt_3" ("H" frozen<"parquet_udt_2">);
CREATE TYPE "parquet_udt_4" ("A" int, "b" frozen<list<int>>, "C" frozen<"parquet_udt_1">, "g" frozen<map<text, "parquet_udt_3">>);
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "id" bigint, "int_array" frozen<list<int>>, "int_array_Array" frozen<list<frozen<list<int>>>>, "int_map" frozen<map<text, int>>, "int_Map_Array" frozen<list<frozen<map<text, int>>>>, "nested_struct" "parquet_udt_4");
INSERT INTO "parquet"("row_number", "id", "int_array", "int_array_Array", "int_map", "int_Map_Array", "nested_struct") VALUES(0, 1, [1, 2, 3], [[1, 2], [3, 4]], {'k1': 1, 'k2': 100}, [{'k1': 1}], {"A": 1, "b": [1], "C": {"d": [[{"E": 10, "F": 'aaa'}, {"E": -10, "F": 'bbb'}], [{"E": 11, "F": 'c'}]]}, "g": {'foo': {"H": {"i": [1.100000e+00]}}}});
INSERT INTO "parquet"("row_number", "id", "int_array", "int_array_Array", "int_map", "int_Map_Array", "nested_struct") VALUES(1, 2, [null, 1, 2, null, 3, null], [[null, 1, 2, null], [3, null, 4], [], null], {'k1': 2, 'k2': null}, [{'k3': null, 'k1': 1}, null, {}], {"A": null, "b": [null], "C": {"d": [[{"E": null, "F": null}, {"E": 10, "F": 'aaa'}, {"E": null, "F": null}, {"E": -10, "F": 'bbb'}, {"E": null, "F": null}], [{"E": 11, "F": 'c'}, null], [], null]}, "g": {'g1': {"H": {"i": [2.200000e+00, null]}}, 'g2': {"H": {"i": []}}, 'g3': null, 'g4': {"H": {"i": null}}, 'g5': {"H": null}}});
INSERT INTO "parquet"("row_number", "id", "int_array", "int_array_Array", "int_map", "int_Map_Array", "nested_struct") VALUES(2, 3, [], [null], {}, [null, null], {"A": null, "b": null, "C": {"d": []}, "g": {}});
INSERT INTO "parquet"("row_number", "id", "int_array", "int_array_Array", "int_map", "int_Map_Array", "nested_struct") VALUES(3, 4, null, [], {}, [], {"A": null, "b": null, "C": {"d": null}, "g": null});
INSERT INTO "parquet"("row_number", "id", "int_array", "int_array_Array", "int_map", "int_Map_Array", "nested_struct") VALUES(4, 5, null, null, {}, null, {"A": null, "b": null, "C": null, "g": {'foo': {"H": {"i": [2.200000e+00, 3.300000e+00]}}}});
INSERT INTO "parquet"("row_number", "id", "int_array", "int_array_Array", "int_map", "int_Map_Array", "nested_struct") VALUES(5, 6, null, null, null, null, null);
INSERT INTO "parquet"("row_number", "id", "int_array", "int_array_Array", "int_map", "int_Map_Array", "nested_struct") VALUES(6, 7, null, [null, [5, 6]], {'k1': null, 'k3': null}, null, {"A": 7, "b": [2, 3, null], "C": {"d": [[], [null], null]}, "g": null});
)###"},
        {"data/nulls.snappy.parquet", R"###(
CREATE TYPE "parquet_udt_0" ("b_c_int" int);
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "b_struct" "parquet_udt_0");
INSERT INTO "parquet"("row_number", "b_struct") VALUES(0, {"b_c_int": null});
INSERT INTO "parquet"("row_number", "b_struct") VALUES(1, {"b_c_int": null});
INSERT INTO "parquet"("row_number", "b_struct") VALUES(2, {"b_c_int": null});
INSERT INTO "parquet"("row_number", "b_struct") VALUES(3, {"b_c_int": null});
INSERT INTO "parquet"("row_number", "b_struct") VALUES(4, {"b_c_int": null});
INSERT INTO "parquet"("row_number", "b_struct") VALUES(5, {"b_c_int": null});
INSERT INTO "parquet"("row_number", "b_struct") VALUES(6, {"b_c_int": null});
INSERT INTO "parquet"("row_number", "b_struct") VALUES(7, {"b_c_int": null});
)###"},
        {"data/single_nan.parquet", R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "mycol" double);
INSERT INTO "parquet"("row_number", "mycol") VALUES(0, null);
)###"},
        }; // test_cases
        for (const auto& [filename, output] : test_cases) {
            std::stringstream ss;
            ss << '\n';
            future<file_reader> future_reader = file_reader::open(filename);
            file_reader reader = future_reader.handle_exception([] (auto eptr) {
                std::string hint =
                        "Make sure that the parquet-testing submodule is initialized"
                        " and that the working directory of this test is set to it";
                std::string error = seastar::format("{}.\n{}.\n",
                        // eptr, // this should compile, but doesn't
                        "Error",
                        hint);
                return make_exception_future<file_reader>(parquet_exception(error));
            }).get0();
            cql::parquet_to_cql(reader, "parquet", "row_number", ss).get();
            BOOST_CHECK_EQUAL(ss.str(), output);
        }
    });
}

} // namespace parquet4seastar
