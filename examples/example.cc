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

#include <parquet4seastar/file_writer.hh>
#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/overloaded.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/core/thread.hh>

namespace p4s = parquet4seastar;

using p4s::overloaded;
using p4s::y_combinator;

// A helper variadic vector constructor, which works with move-only types (like schema nodes).
// The initializer list constructor doesn't work with move-only types.
template <typename T, typename Targ>
void vec_fill(std::vector<T>& v, Targ&& arg) {
    v.push_back(std::forward<Targ>(arg));
}

template <typename T, typename Targ, typename... Targs>
void vec_fill(std::vector<T>& v, Targ&& arg, Targs&&... args) {
    v.push_back(std::forward<Targ>(arg));
    vec_fill(v, std::forward<Targs>(args)...);
}

template <typename T, typename... Targs>
std::vector<T> vec(Targs&&... args) {
    std::vector<T> v;
    vec_fill(v, std::forward<Targs>(args)...);
    return v;
}

template <typename T>
std::vector<T> vec() {
    return std::vector<T>();
}

// The input type (passed by user to the library) for Parquet writers of blob types
// (BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY in Parquet nomenclature)
// is parquet4seastar::bytes_view (currently aliased to std::basic_string_view<uint8_t>).
//
// The output type (passed by the library to the user) for Parquet readers of blob types
// is seastar::temporary_buffer<parquet4seastar::byte>
// (parquet4seastar::byte is currently aliased to uint8_t).
// This is so that the effectiveness of dictionary encoding isn't wasted:
// copies of the same blob can be cheaply returned as shared views to the dictionary page.
//
// The output type for Parquet's INT96 is std::array<int32_t, 3>.
// INT96 is deprecated and writes are not supported.
//
// The I/O type for Parquet's BOOLEAN is uint8_t. This was done to work around the special
// behaviour of std::vector<bool>.
//
// The other I/O types are obvious:
// float for FLOAT,
// double for DOUBLE
// int32_t for INT32,
// int64_t for INT64.
//
constexpr p4s::bytes_view operator ""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const p4s::byte*>(static_cast<const void*>(str)), len};
}

const std::string filename = "/tmp/parquet4seastar_example.parquet";

p4s::writer_schema::schema
make_test_schema() {
    using namespace p4s::writer_schema;
    // To open a Parquet file writer, you need to build a writer schema.
    // The writer schema is a tree with `schema` node at the root, and
    // variants of `map_node`, `struct_node`, `list_node` and `primitive_node` nested below.
    // See writer_schema.hh for the type definition of this tree.
    return schema {vec<node>(
        map_node {
            "my_map", // Map name.
            true, // Is the map (as a whole) optional?
            std::make_unique<node>(primitive_node { // Map key.
                "my_map_key", // Column name.
                false, // Is this field optional?
                p4s::logical_type::STRING{}, // Logical type. See logical_type.hh for details.
                {}, // Value size. Required only for FIXED_LEN_BYTE_ARRAY, ignored for other types.
                p4s::format::Encoding::RLE_DICTIONARY, // Encoding. See Parquet's documentation for details.
                p4s::format::CompressionCodec::GZIP}), // Compression algorithm. See Parquet's documentation for details.
            std::make_unique<node>(primitive_node { // Map value.
                "my_map_value",
                false,
                p4s::logical_type::INT32{},
                {},
                p4s::format::Encoding::DELTA_BINARY_PACKED,
                p4s::format::CompressionCodec::SNAPPY})},
        list_node {
            "my_list", // List name.
            true, // Is the list (as a whole) optional?
            std::make_unique<node>(struct_node { // List element node.
                "my_struct", // Struct name.
                true, // Is the struct (as a whole) optional?
                vec<node>( // Child (field) nodes.
                    primitive_node {
                        "my_struct_1",
                        false,
                        p4s::logical_type::FLOAT{},
                        {},
                        p4s::format::Encoding::PLAIN,
                        p4s::format::CompressionCodec::GZIP},
                    primitive_node {
                        "my_struct_2",
                        false,
                        p4s::logical_type::DOUBLE{},
                        {},
                        p4s::format::Encoding::PLAIN,
                        p4s::format::CompressionCodec::GZIP})})})};
}

// Example writer usage.
void do_write() {
    p4s::writer_schema::schema schema = make_test_schema();
    std::unique_ptr<p4s::file_writer> fw = p4s::file_writer::open(filename, schema).get0();

    // The Parquet columns for the given schema are numbered from 0,
    // in the order they appear in the schema.
    //
    // The physical Parquet type of the column (format::Type::X) is a function of its logical type.
    // See logical_type.hh for details.
    //
    // Passing a wrong enum parameter to file_writer::column will result in an exception.
    p4s::column_chunk_writer<p4s::format::Type::BYTE_ARRAY>& map_key = fw->column<p4s::format::Type::BYTE_ARRAY>(0);
    p4s::column_chunk_writer<p4s::format::Type::INT32>& map_value = fw->column<p4s::format::Type::INT32>(1);
    p4s::column_chunk_writer<p4s::format::Type::FLOAT>& struct_field_1 = fw->column<p4s::format::Type::FLOAT>(2);
    p4s::column_chunk_writer<p4s::format::Type::DOUBLE>& struct_field_2 = fw->column<p4s::format::Type::DOUBLE>(3);

    // Values are appended to the columns with column_chunk_writer<T>::put.
    // The call is: put(definition level, repetition level, value).
    // The value is ignored when the definition level is below the column's full definition level
    // (which signalized that the value is null).
    //
    // There is no validation for the inserted levels. Passing incorrect levels will result in
    // an incorrect Parquet file.
    map_key.put(0, 0, "1337"_bv); // Null value.
    map_value.put(0, 0, 1337); // Null value.
    struct_field_1.put(0, 0, 1337.0f); // Null value.
    struct_field_2.put(0, 0, 1337.0); // Null value.

    // The values are buffered in RAM until it's flushed with
    // flush_row_group(). (There is no other way other than writing each column
    // to a separate file. This is permitted by Parquet, but not used by us.)
    //
    // The flushing is not done automatically because it would require
    // synchronization between column writers (because a row group can only be
    // flushed on row boundaries, i.e. when all columns for all rows in the
    // group has been written).
    //
    // Hence, the user has to flush the row group periodically, using
    // file_writer::flush_row_group(). According to the Parquet specification,
    // a row group should be several hundred MB in size. Anything bigger than
    // that would also be impractical because the entire row group has to fit
    // in memory before the flush. The current estimated row group size can be
    // obtained with file_writer::estimated_row_group_size().
    //
    // There is no control that an equal number of rows was written to every
    // column before the flush. Failure to do so will result in an incorrect
    // output file.
    fw->flush_row_group().get0();

    // Note that put() is synchronous. This is because the writes are buffered
    // until flush_row_group().
    map_key.put(2, 0, "key1"_bv);
    // It is also the user's responsibility to flush pages periodically using
    // flush_page().
    //
    // Pages should be several KB in size. Anything bigger than that could cause
    // problems with big allocations and/or reactor stalls for readers.
    //
    // The current max possible size of the page after encoding can be viewed with
    // column_chunk_writer::current_page_max_size(). It is an upper bound - the
    // actual size might be smaller. (Currently, it can be up to 3KB smaller.
    // This may happen with DELTA_BINARY_PACKED encoding of INT64. In this
    // encoding values are encoded in batches of 256. Hence, up to 256 values
    // may be estimated at their full size (256 * 8B = 2KB), even if the batch
    // will shrink to almost nothing).
    //
    // Unlike with row groups, automating the page flushing could (and
    // should) be easily implemented, but we hesitated because the Parquet
    // specification is somewhat unclear about whether page breaks can occur in
    // the middle of a row. (See parquet.thrift:904, the comment to
    // first_row_index). If rows cannot be broken, this could potentially
    // result in pages of unbounded size (e.g. with very long lists), which
    // would be unacceptable.
    //
    // If rows can be broken, or that bit of specification is determined to be
    // of no concern, page flushing should be automated.
    map_key.flush_page();
    map_value.put(2, 0, 1);
    map_key.put(2, 1, "key2"_bv);
    map_value.put(2, 1, 1);
    struct_field_1.put(2, 0, 1337.0f);
    struct_field_2.put(2, 0, 1337.0);
    struct_field_1.put(3, 1, 1.1f);
    struct_field_2.put(3, 1, 1.1);

    // The writer must be closed (with file_writer::close).
    fw->close().get0();

    // End result:
    // ----Row group 1---
    // {
    //     my_map: null,
    //     my_list: null
    // }
    // ----Row group 2---
    // {
    //     my_map: {
    //         "key1": 1,
    //         "key2": 1,
    //     },
    //     my_list: [
    //         null,
    //         {
    //             "my_struct_1": 1.0,
    //             "my_struct_2": 1.0,
    //         }
    //     ]
    // }

}

// How to switch over logical types.
const char* get_logical_type_name(const p4s::logical_type::logical_type& lt) {
    using namespace p4s::logical_type;
    return std::visit(overloaded {
        [] (const STRING&) { return "STRING"; },
        [] (const ENUM&) { return "ENUM"; },
        [] (const UUID&) { return "UUID"; },
        [] (const INT8&) { return "INT8"; },
        [] (const INT16&) { return "INT16"; },
        [] (const INT32&) { return "INT32"; },
        [] (const INT64&) { return "INT64"; },
        [] (const UINT8&) { return "UINT8"; },
        [] (const UINT16&) { return "UINT16"; },
        [] (const UINT32&) { return "UINT32"; },
        [] (const UINT64&) { return "UINT64"; },
        [] (const DECIMAL_INT32&) { return "DECIMAL_INT32"; },
        [] (const DECIMAL_INT64&) { return "DECIMAL_INT64"; },
        [] (const DECIMAL_BYTE_ARRAY&) { return "DECIMAL_BYTE_ARRAY"; },
        [] (const DECIMAL_FIXED_LEN_BYTE_ARRAY&) { return "DECIMAL_FIXED_LEN_BYTE_ARRAY"; },
        [] (const DATE&) { return "DATE"; },
        [] (const TIME_INT32&) { return "TIME_INT32"; },
        [] (const TIME_INT64&) { return "TIME_INT64"; },
        [] (const TIMESTAMP& t) { return t.unit == TIMESTAMP::MILLIS ? "timestamp (millis)" : "timestamp(nanos)"; },
        [] (const INTERVAL&) { return "INTERVAL"; },
        [] (const JSON&) { return "JSON"; },
        [] (const BSON&) { return "BSON"; },
        [] (const FLOAT&) { return "FLOAT"; },
        [] (const DOUBLE&) { return "DOUBLE"; },
        [] (const BYTE_ARRAY&) { return "BYTE_ARRAY"; },
        [] (const FIXED_LEN_BYTE_ARRAY&) { return "FIXED_LEN_BYTE_ARRAY"; },
        [] (const INT96&) { return "INT96"; },
        [] (const BOOLEAN&) { return "BOOLEAN"; },
        [] (const UNKNOWN&) { return "UNKNOWN"; },
        [] (const auto&) -> const char* { throw std::runtime_error("Unreachable"); },
    }, lt);
}

void print_node(const p4s::reader_schema::node& x, int depth, bool optional) {
    std::string indent(depth * 4, ' ');
    std::cout << indent;
    std::visit(overloaded {
        [&] (const p4s::reader_schema::primitive_node& x) {
            std::cout << x.info.name << " " << get_logical_type_name(x.logical_type);
        },
        [&] (const p4s::reader_schema::list_node& x) {
            std::cout << x.info.name << " LIST";
        },
        [&] (const p4s::reader_schema::map_node& x) {
            std::cout << x.info.name << " MAP";
        },
        [&] (const p4s::reader_schema::optional_node& x) {
        },
        [&] (const p4s::reader_schema::struct_node& x) {
            std::cout << x.info.name << " STRUCT";
        },
    }, x);
    if (!optional) {
        std::cout << " OPTIONAL";
    }
    std::cout << "\n";
}

// Example schema walk.
void print_schema(const p4s::reader_schema::schema& schema) {
    std::cout << "Schema dump:\n";
    auto walk = y_combinator{[&] (auto&& walk, const p4s::reader_schema::node& node, int depth=0, bool optional=false) -> void {
        std::visit(overloaded {
            [&] (const p4s::reader_schema::primitive_node& x) {
                print_node(node, depth, optional);
            },
            [&] (const p4s::reader_schema::list_node& x) {
                print_node(node, depth, optional);
                walk(*x.element, depth + 1);
            },
            [&] (const p4s::reader_schema::map_node& x) {
                print_node(node, depth, optional);
                walk(*x.key, depth + 1);
                walk(*x.value, depth + 1);
            },
            [&] (const p4s::reader_schema::optional_node& x) {
                walk(*x.child, depth, true);
            },
            [&] (const p4s::reader_schema::struct_node& x) {
                print_node(node, depth, optional);
                for (const auto& field : x.fields) {
                    walk(field, depth + 1);
                }
            },
        }, node);
    }};
    for (const p4s::reader_schema::node& field : schema.fields) {
        walk(field, 0);
    }
}

void do_read() {
    p4s::file_reader fr = p4s::file_reader::open(filename).get0();

    // This metadata is directly passed from the file, without intervention.
    // It may be invalid, if the writer was incorrect.
    // Here it will be used only to get the number of row groups.
    // See parquet.thrift and parquet_types.h for the details of the structure.
    const p4s::format::FileMetaData& metadata = fr.metadata();

    // This is the logical schema of the file, extracted from the metadata
    // and validated. This is the information wanted by the user.
    // It is similar to the writer_schema.  See reader_schema.hh
    // for the details.
    const p4s::reader_schema::schema& schema = fr.schema();

    BOOST_CHECK_EQUAL(metadata.row_groups.size(), 2);
    BOOST_CHECK_EQUAL(schema.leaves.size(), 4);

    // Example schema processing routine
    print_schema(schema);

    std::cout << "\nFile dump:" << "\n";
    for (size_t i = 0; i < metadata.row_groups.size(); ++i) {
        std::cout << "Row group " << i << "\n";

        // Similar to the writer counterpart, but the row group has to selected.
        // This is asynchronous, because each open_column_chunk_reader() can possibly
        // open a different file.
        p4s::column_chunk_reader<p4s::format::Type::BYTE_ARRAY> map_key =
                fr.open_column_chunk_reader<p4s::format::Type::BYTE_ARRAY>(i, 0).get0();
        p4s::column_chunk_reader<p4s::format::Type::INT32> map_value =
                fr.open_column_chunk_reader<p4s::format::Type::INT32>(i, 1).get0();
        p4s::column_chunk_reader<p4s::format::Type::FLOAT> struct_field_1 =
                fr.open_column_chunk_reader<p4s::format::Type::FLOAT>(i, 2).get0();
        p4s::column_chunk_reader<p4s::format::Type::DOUBLE> struct_field_2 =
                fr.open_column_chunk_reader<p4s::format::Type::DOUBLE>(i, 3).get0();

        std::cout << "Column 0\n";
        // The user reads batches of (def level, rep level, value) triplets
        // using column_chunk_reader::read_batch().
        // The last batch of values will be smaller than requested.
        for (;;) {
            int16_t def[2];
            int16_t rep[2];
            seastar::temporary_buffer<uint8_t> val[2];

            size_t n_read = map_key.read_batch(2, def, rep, val).get0();
            if (!n_read) {
                break;
            }

            for (size_t i = 0, v = 0; i < n_read; ++i) {
                std::cout << def[i] << " " << rep[i];
                const seastar::temporary_buffer<uint8_t>& vv = val[v++];
                if (def[i] == 2) {
                    std::cout << " " << std::string(reinterpret_cast<const char*>(vv.get()), vv.size());
                }
                std::cout << '\n';
            }
        }

        std::cout << "Column 1\n";
        for (;;) {
            int16_t def[2];
            int16_t rep[2];
            int32_t val[2];

            size_t n_read = map_value.read_batch(2, def, rep, val).get0();
            if (!n_read) {
                break;
            }

            for (size_t i = 0, v = 0; i < n_read; ++i) {
                std::cout << def[i] << " " << rep[i];
                if (def[i] == 2) {
                    std::cout << " " << val[v++];
                }
                std::cout << '\n';
            }
        }

        std::cout << "Column 2\n";
        for (;;) {
            int16_t def[2];
            int16_t rep[2];
            float val[2];

            size_t n_read = struct_field_1.read_batch(2, def, rep, val).get0();
            if (!n_read) {
                break;
            }

            for (size_t i = 0, v = 0; i < n_read; ++i) {
                std::cout << def[i] << " " << rep[i];
                if (def[i] == 3) {
                    std::cout << " " << val[v++];
                }
                std::cout << '\n';
            }
        }

        std::cout << "Column 3\n";
        for (;;) {
            int16_t def[2];
            int16_t rep[2];
            double val[2];

            size_t n_read = struct_field_2.read_batch(2, def, rep, val).get0();
            if (!n_read) {
                break;
            }

            for (size_t i = 0, v = 0; i < n_read; ++i) {
                std::cout << def[i] << " " << rep[i];
                if (def[i] == 3) {
                    std::cout << " " << val[v++];
                }
                std::cout << '\n';
            }
        }
    }

    // Remember to close your files.
    fr.close().get0();
}

SEASTAR_TEST_CASE(parquet_example) {
    return seastar::async([] {
        do_write();
        do_read();
    });
}
