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

/* This file implements the rules found in:
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 * doc/parquet/LogicalTypes.md
 */

#pragma once

#include <parquet4seastar/exception.hh>
#include <parquet4seastar/parquet_types.h>
#include <variant>

namespace parquet4seastar::logical_type {

struct BOOLEAN { static constexpr format::Type::type physical_type = format::Type::BOOLEAN; };
struct INT32 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct INT64 { static constexpr format::Type::type physical_type = format::Type::INT64; };
struct INT96 { static constexpr format::Type::type physical_type = format::Type::INT96; };
struct FLOAT { static constexpr format::Type::type physical_type = format::Type::FLOAT; };
struct DOUBLE { static constexpr format::Type::type physical_type = format::Type::DOUBLE; };
struct BYTE_ARRAY { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct FIXED_LEN_BYTE_ARRAY { static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY; };
struct STRING { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct ENUM { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct UUID { static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY; };
struct INT8 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct INT16 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT8 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT16 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT32 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT64 { static constexpr format::Type::type physical_type = format::Type::INT64; };
struct DECIMAL_INT32 {
    static constexpr format::Type::type physical_type = format::Type::INT32;
    uint32_t scale;
    uint32_t precision;
};
struct DECIMAL_INT64 {
    static constexpr format::Type::type physical_type = format::Type::INT64;
    uint32_t scale;
    uint32_t precision;
};
struct DECIMAL_BYTE_ARRAY {
    static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY;
    uint32_t scale;
    uint32_t precision;
};
struct DECIMAL_FIXED_LEN_BYTE_ARRAY {
    static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY;
    uint32_t scale;
    uint32_t precision;
};
struct DATE { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct TIME_INT32 {
    static constexpr format::Type::type physical_type = format::Type::INT32;
    bool utc_adjustment;
};
struct TIME_INT64 {
    static constexpr format::Type::type physical_type = format::Type::INT64;
    bool utc_adjustment;
    enum {MICROS, NANOS} unit;
};
struct TIMESTAMP {
    static constexpr format::Type::type physical_type = format::Type::INT64;
    bool utc_adjustment;
    enum {MILLIS, MICROS, NANOS} unit;
};
struct INTERVAL { static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY; };
struct JSON { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct BSON { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct UNKNOWN { static constexpr format::Type::type physical_type = format::Type::INT32; };

using logical_type = std::variant<
        BOOLEAN,
        INT32,
        INT64,
        INT96,
        FLOAT,
        DOUBLE,
        BYTE_ARRAY,
        FIXED_LEN_BYTE_ARRAY,
        STRING,
        ENUM,
        UUID,
        INT8,
        INT16,
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        DECIMAL_INT32,
        DECIMAL_INT64,
        DECIMAL_BYTE_ARRAY,
        DECIMAL_FIXED_LEN_BYTE_ARRAY,
        DATE,
        TIME_INT32,
        TIME_INT64,
        TIMESTAMP,
        INTERVAL,
        JSON,
        BSON,
        UNKNOWN
>;

logical_type read_logical_type(const format::SchemaElement& x);
void write_logical_type(logical_type logical_type, format::SchemaElement& leaf);

} // namespace parquet4seastar::logical_type
