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

#include <parquet4seastar/logical_type.hh>
#include <parquet4seastar/overloaded.hh>

namespace parquet4seastar::logical_type {

logical_type read_logical_type(const format::SchemaElement& x) {
    static auto verify = [] (bool condition, const std::string& error) {
        if (!condition) { throw parquet_exception::corrupted_file(error); }
    };
    if (!x.__isset.type) {
        return UNKNOWN{};
    }
    if (x.__isset.logicalType) {
        if (x.logicalType.__isset.TIME) {
            if (x.logicalType.TIME.unit.__isset.MILLIS) {
                verify(x.type == format::Type::INT32, "TIME MILLIS must annotate the INT32 physical type");
                return TIME_INT32{x.logicalType.TIME.isAdjustedToUTC};
            } else if (x.logicalType.TIME.unit.__isset.MICROS) {
                verify(x.type == format::Type::INT64, "TIME MICROS must annotate the INT64 physical type");
                return TIME_INT64{x.logicalType.TIME.isAdjustedToUTC, TIME_INT64::MICROS};
            } else if (x.logicalType.TIME.unit.__isset.NANOS) {
                verify(x.type == format::Type::INT64, "TIME NANOS must annotate the INT64 physical type");
                return TIME_INT64{x.logicalType.TIME.isAdjustedToUTC, TIME_INT64::NANOS};
            }
        } else if (x.logicalType.__isset.TIMESTAMP) {
            verify(x.type == format::Type::INT64, "TIMESTAMP must annotate the INT64 physical type");
            if (x.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
                return TIMESTAMP{x.logicalType.TIMESTAMP.isAdjustedToUTC, TIMESTAMP::MILLIS};
            } else if (x.logicalType.TIMESTAMP.unit.__isset.MICROS) {
                return TIMESTAMP{x.logicalType.TIMESTAMP.isAdjustedToUTC, TIMESTAMP::MICROS};
            } else if (x.logicalType.TIMESTAMP.unit.__isset.NANOS) {
                return TIMESTAMP{x.logicalType.TIMESTAMP.isAdjustedToUTC, TIMESTAMP::NANOS};
            }
        } else if (x.logicalType.__isset.UUID) {
            verify(x.type == format::Type::FIXED_LEN_BYTE_ARRAY && x.type_length == 16,
                   "UUID must annotate the 16-byte fixed-length binary type");
            return UUID{};
        }
    }
    if (x.__isset.converted_type) {
        if (x.converted_type == format::ConvertedType::UTF8) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "UTF8 must annotate the binary physical type");
            return STRING{};
        } else if (x.converted_type == format::ConvertedType::ENUM) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "ENUM must annotate the binary physical type");
            return ENUM{};
        } else if (x.converted_type == format::ConvertedType::INT_8) {
            verify(x.type == format::Type::INT32, "INT_8 must annotate the INT32 physical type");
            return INT8{};
        } else if (x.converted_type == format::ConvertedType::INT_16) {
            verify(x.type == format::Type::INT32, "INT_16 must annotate the INT32 physical type");
            return INT16{};
        } else if (x.converted_type == format::ConvertedType::INT_32) {
            verify(x.type == format::Type::INT32, "INT_32 must annotate the INT32 physical type");
            return INT32{};
        } else if (x.converted_type == format::ConvertedType::INT_64) {
            verify(x.type == format::Type::INT64, "INT_64 must annotate the INT64 physical type");
            return INT64{};
        } else if (x.converted_type == format::ConvertedType::UINT_8) {
            verify(x.type == format::Type::INT32, "UINT_8 must annotate the INT32 physical type");
            return UINT8{};
        } else if (x.converted_type == format::ConvertedType::UINT_16) {
            verify(x.type == format::Type::INT32, "UINT_16 must annotate the INT32 physical type");
            return UINT16{};
        } else if (x.converted_type == format::ConvertedType::UINT_32) {
            verify(x.type == format::Type::INT32, "UINT_32 must annotate the INT32 physical type");
            return UINT32{};
        } else if (x.converted_type == format::ConvertedType::UINT_64) {
            verify(x.type == format::Type::INT64, "UINT_64 must annotate the INT64 physical type");
            return UINT64{};
        } else if (x.converted_type == format::ConvertedType::DECIMAL) {
            verify(x.__isset.precision && x.__isset.scale, "precision and scale must be set for DECIMAL");
            uint32_t precision = x.precision;
            uint32_t scale = x.scale;
            if (x.type == format::Type::INT32) {
                verify(1 <= precision && precision <= 9,
                       "precision " + std::to_string(precision) + " out of bounds for INT32 decimal");
                return DECIMAL_INT32{scale, precision};
            } else if (x.type == format::Type::INT64) {
                verify(1 <= precision && precision <= 18,
                       "precision " + std::to_string(precision) + " out of bounds for INT64 decimal");
                return DECIMAL_INT64{scale, precision};
            } else if (x.type == format::Type::BYTE_ARRAY) {
                return DECIMAL_BYTE_ARRAY{scale, precision};
            } else if (x.type == format::Type::FIXED_LEN_BYTE_ARRAY) {
                verify(precision > 0,
                       "precision " + std::to_string(precision) + " out of bounds for FIXED_LEN_BYTE_ARRAY decimal");
                return DECIMAL_FIXED_LEN_BYTE_ARRAY{scale, precision};
            } else {
                verify(false,"DECIMAL must annotate INT32, INT64, BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY");
                return UNKNOWN{}; // Unreachable
            }
        } else if (x.converted_type == format::ConvertedType::DATE) {
            verify(x.type == format::Type::INT32, "DATE must annotate the INT32 physical type");
        } else if (x.converted_type == format::ConvertedType::TIME_MILLIS) {
            verify(x.type == format::Type::INT32, "TIME_MILLIS must annotate the INT32 physical type");
            return TIME_INT32{true};
        } else if (x.converted_type == format::ConvertedType::TIME_MICROS) {
            verify(x.type == format::Type::INT64, "TIME_MICROS must annotate the INT64 physical type");
            return TIME_INT64{true, TIME_INT64::MICROS};
        } else if (x.converted_type == format::ConvertedType::TIMESTAMP_MILLIS) {
            verify(x.type == format::Type::INT64, "TIMESTAMP_MILLIS must annotate the INT64 physical type");
            return TIMESTAMP{true, TIMESTAMP::MILLIS};
        } else if (x.converted_type == format::ConvertedType::TIMESTAMP_MICROS) {
            verify(x.type == format::Type::INT64, "TIMESTAMP_MICROS must annotate the INT64 physical type");
            return TIMESTAMP{true, TIMESTAMP::MICROS};
        } else if (x.converted_type == format::ConvertedType::INTERVAL) {
            verify(x.type == format::Type::FIXED_LEN_BYTE_ARRAY && x.type_length == 12,
                   "INTERVAL must annotate the INT32 physical type");
            return INTERVAL{};
        } else if (x.converted_type == format::ConvertedType::JSON) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "JSON must annotate the binary physical type");
            return JSON{};
        } else if (x.converted_type == format::ConvertedType::BSON) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "BSON must annotate the binary physical type");
            return BSON{};
        }
    }
    switch (x.type) {
        case format::Type::BOOLEAN: return BOOLEAN{};
        case format::Type::INT32: return INT32{};
        case format::Type::INT64: return INT64{};
        case format::Type::INT96: return INT96{};
        case format::Type::FLOAT: return FLOAT{};
        case format::Type::DOUBLE: return DOUBLE{};
        case format::Type::BYTE_ARRAY: return BYTE_ARRAY{};
        case format::Type::FIXED_LEN_BYTE_ARRAY: return FIXED_LEN_BYTE_ARRAY{};
        default: return UNKNOWN{};
    }
}

void write_logical_type(logical_type logical_type, format::SchemaElement& leaf) {
    using namespace logical_type;
    auto int_type = [] (int bit_width, bool is_signed) -> format::LogicalType {
        format::LogicalType logical_type;
        format::IntType int_type;
        int_type.__set_bitWidth(bit_width);
        int_type.__set_isSigned(is_signed);
        logical_type.__set_INTEGER(int_type);
        return logical_type;
    };
    auto decimal_type = [] (int precision, int scale) -> format::LogicalType {
        format::DecimalType decimal_type;
        decimal_type.__set_precision(precision);
        decimal_type.__set_scale(scale);
        format::LogicalType logical_type;
        logical_type.__set_DECIMAL(decimal_type);
        return logical_type;
    };
    return std::visit(overloaded {
            [&] (const STRING&) {
                leaf.__set_converted_type(format::ConvertedType::UTF8);
                format::LogicalType logical_type;
                logical_type.__set_STRING(format::StringType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const ENUM&) {
                leaf.__set_converted_type(format::ConvertedType::ENUM);
                format::LogicalType logical_type;
                logical_type.__set_ENUM(format::EnumType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const UUID&) {
                format::LogicalType logical_type;
                logical_type.__set_UUID(format::UUIDType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const INT8&) {
                leaf.__set_converted_type(format::ConvertedType::INT_8);
                leaf.__set_logicalType(int_type(8, true));
            },
            [&] (const INT16&) {
                leaf.__set_converted_type(format::ConvertedType::INT_16);
                leaf.__set_logicalType(int_type(16, true));
            },
            [&] (const INT32&) {
                leaf.__set_converted_type(format::ConvertedType::INT_32);
                leaf.__set_logicalType(int_type(32, true));
            },
            [&] (const INT64&) {
                leaf.__set_converted_type(format::ConvertedType::INT_64);
                leaf.__set_logicalType(int_type(64, true));
            },
            [&] (const UINT8&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_8);
                leaf.__set_logicalType(int_type(8, false));
            },
            [&] (const UINT16&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_16);
                leaf.__set_logicalType(int_type(16, false));
            },
            [&] (const UINT32&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_32);
                leaf.__set_logicalType(int_type(32, false));
            },
            [&] (const UINT64&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_64);
                leaf.__set_logicalType(int_type(64, false));
            },
            [&] (const DECIMAL_INT32& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DECIMAL_INT64& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DECIMAL_BYTE_ARRAY& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DECIMAL_FIXED_LEN_BYTE_ARRAY& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DATE&) {
                leaf.__set_converted_type(format::ConvertedType::DATE);
                format::LogicalType logical_type;
                logical_type.__set_DATE(format::DateType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const TIME_INT32& x) {
                leaf.__set_converted_type(format::ConvertedType::TIME_MILLIS);
                format::LogicalType logical_type;
                format::TimeType time_type;
                format::TimeUnit time_unit;
                time_unit.__set_MILLIS(format::MilliSeconds{});
                time_type.__set_isAdjustedToUTC(x.utc_adjustment);
                time_type.__set_unit(time_unit);
                logical_type.__set_TIME(time_type);
                leaf.__set_logicalType(logical_type);
            },
            [&] (const TIME_INT64& x) {
                format::LogicalType logical_type;
                format::TimeType time_type;
                format::TimeUnit time_unit;
                if (x.unit == TIME_INT64::MICROS) {
                    leaf.__set_converted_type(format::ConvertedType::TIME_MICROS);
                    time_unit.__set_MICROS(format::MicroSeconds{});
                } else {
                    time_unit.__set_NANOS(format::NanoSeconds{});
                }
                time_type.__set_isAdjustedToUTC(x.utc_adjustment);
                time_type.__set_unit(time_unit);
                logical_type.__set_TIME(time_type);
                leaf.__set_logicalType(logical_type);
            },
            [&] (const TIMESTAMP& x) {
                format::LogicalType logical_type;
                format::TimestampType timestamp_type;
                format::TimeUnit time_unit;
                if (x.unit == TIMESTAMP::MILLIS) {
                    leaf.__set_converted_type(format::ConvertedType::TIMESTAMP_MILLIS);
                    time_unit.__set_MILLIS(format::MilliSeconds{});
                } else if (x.unit == TIMESTAMP::MICROS) {
                    leaf.__set_converted_type(format::ConvertedType::TIMESTAMP_MICROS);
                    time_unit.__set_MICROS(format::MicroSeconds{});
                } else {
                    time_unit.__set_NANOS(format::NanoSeconds{});
                }
                timestamp_type.__set_isAdjustedToUTC(x.utc_adjustment);
                timestamp_type.__set_unit(time_unit);
                logical_type.__set_TIMESTAMP(timestamp_type);
                leaf.__set_logicalType(logical_type);
            },
            [&] (const INTERVAL&) {
                leaf.__set_converted_type(format::ConvertedType::INTERVAL);
            },
            [&] (const JSON&) {
                leaf.__set_converted_type(format::ConvertedType::JSON);
                format::LogicalType logical_type;
                logical_type.__set_JSON(format::JsonType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const BSON&) {
                leaf.__set_converted_type(format::ConvertedType::BSON);
                format::LogicalType logical_type;
                logical_type.__set_BSON(format::BsonType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const FLOAT&) {},
            [&] (const DOUBLE&) {},
            [&] (const BYTE_ARRAY&) {},
            [&] (const FIXED_LEN_BYTE_ARRAY&) {},
            [&] (const INT96&) {},
            [&] (const BOOLEAN&) {},
            [&] (const UNKNOWN&) {
                format::LogicalType logical_type;
                logical_type.__set_UNKNOWN(format::NullType{});
                leaf.__set_logicalType(logical_type);
            }
    }, logical_type);
}

} // namespace parquet4seastar::logical_type
