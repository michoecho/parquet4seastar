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
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>
#include <boost/algorithm/string.hpp>

#include <stdlib.h>

namespace bpo = boost::program_options;

using namespace parquet4seastar;

enum class FileType {
    numerical,
    mixed,
    nested,
    strings
};

enum class DataType {
    boolean,
    double_precision,
    floating_point,
    int8,
    int16,
    int32,
    int64,
    string,
    flba_blob
};

struct file_config {
    std::string filename;
    FileType file_type;
    format::CompressionCodec::type compression;
    int64_t rows;
    int64_t rowgroups;
    int64_t page_size;
    format::Encoding::type encoding;
    int64_t string_length;
    int64_t flba_length;
};

constexpr bytes_view operator ""_bv(const char *str, size_t len) noexcept {
    return {static_cast<const uint8_t *>(static_cast<const void *>(str)), len};
}

template<typename T>
std::unique_ptr<T> box(T &&x) {
    return std::make_unique<T>(std::forward<T>(x));
}

template<typename T, typename Targ>
void vec_fill(std::vector<T> &v, Targ &&arg) {
    v.push_back(std::forward<Targ>(arg));
}

template<typename T, typename Targ, typename... Targs>
void vec_fill(std::vector<T> &v, Targ &&arg, Targs &&... args) {
    v.push_back(std::forward<Targ>(arg));
    vec_fill(v, std::forward<Targs>(args)...);
}

template<typename T, typename... Targs>
std::vector<T> vec(Targs &&... args) {
    std::vector<T> v;
    vec_fill(v, std::forward<Targs>(args)...);
    return v;
}

template<typename T>
std::vector<T> vec() {
    return std::vector<T>();
}

template<enum DataType, format::Type::type ParquetType>
void write_column(column_chunk_writer<ParquetType> &column_writer, struct file_config config, int64_t rg,
                  int16_t *record_defs, int16_t *record_reps, int16_t record_length);

template<>
void write_column<DataType::boolean>(column_chunk_writer<format::Type::BOOLEAN> &column_writer, struct file_config config, int64_t rg,
                                     int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        bool value = k % 2 == 1;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        if (++page_counter >= 8 * config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
}

template<>
void write_column<DataType::double_precision>(column_chunk_writer<format::Type::DOUBLE> &column_writer, struct file_config config, int64_t rg,
                                              int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? sizeof(double) : 1;
    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        int64_t i = k % 256;
        double value = i * 1.1111111;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
}

template<>
void write_column<DataType::floating_point>(column_chunk_writer<format::Type::FLOAT> &column_writer, struct file_config config, int64_t rg,
                                            int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? sizeof(float) : 1;
    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        int64_t i = k % 256;
        float value = i * 1.1111111;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
}

template<>
void write_column<DataType::int8>(column_chunk_writer<format::Type::INT32> &column_writer, struct file_config config, int64_t rg,
                                  int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? sizeof(int32_t) : 1;
    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        int64_t i = k % 256;
        int32_t value = (int8_t) i;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
}

template<>
void write_column<DataType::int16>(column_chunk_writer<format::Type::INT32> &column_writer, struct file_config config, int64_t rg,
                                   int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? sizeof(int32_t) : 1;
    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        int64_t i = k % 256;
        int32_t value = (int16_t) i;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
}

template<>
void write_column<DataType::int32>(column_chunk_writer<format::Type::INT32> &column_writer, struct file_config config, int64_t rg,
                                   int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? sizeof(int32_t) : 1;
    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        int64_t i = k % 256;
        int32_t value = (int32_t) i;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
}

template<>
void write_column<DataType::int64>(column_chunk_writer<format::Type::INT64> &column_writer, struct file_config config, int64_t rg,
                                   int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? sizeof(int64_t) : 1;
    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        int64_t i = k % 256;
        int64_t value = (int64_t) i;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
}

template<>
void write_column<DataType::string>(column_chunk_writer<format::Type::BYTE_ARRAY> &column_writer, struct file_config config, int64_t rg,
                                    int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? config.string_length + 4 : 1;

    std::vector<int8_t> bytes(config.string_length, 0);
    bytes_view value{static_cast<const uint8_t *>(static_cast<const void *>(bytes.data())), (uint64_t) config.string_length};

    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        bytes[0] = (int8_t) k;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
    return;
}

template<>
void write_column<DataType::flba_blob>(column_chunk_writer<format::Type::FIXED_LEN_BYTE_ARRAY> &column_writer, struct file_config config, int64_t rg,
                                       int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    int64_t page_counter = 0;
    int64_t value_size = config.encoding == format::Encoding::PLAIN ? config.flba_length : 1;

    std::vector<int8_t> bytes(config.flba_length, 0);
    bytes_view value{static_cast<const uint8_t *>(static_cast<const void *>(bytes.data())), (uint64_t) config.flba_length};

    for (int64_t k = record_length * rg * config.rows; k < record_length * (rg + 1) * config.rows; k++) {
        bytes[0] = (int8_t) k;
        column_writer.put(record_defs[k % record_length], record_reps[k % record_length], value);
        page_counter += value_size;
        if (page_counter >= config.page_size) {
            page_counter = 0;
            column_writer.flush_page();
        }
    }
    return;
}

template<enum FileType>
void write_file(std::unique_ptr<file_writer> &fw, struct file_config config);

template<>
void write_file<FileType::numerical>(std::unique_ptr<file_writer> &fw, struct file_config config) {
    int16_t zero[1] = {0};

    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        write_column<DataType::int16>(fw->column<format::Type::INT32>(0), config, rg, &(*zero), &(*zero), 1);
        write_column<DataType::int32>(fw->column<format::Type::INT32>(1), config, rg, &(*zero), &(*zero), 1);
        write_column<DataType::int64>(fw->column<format::Type::INT64>(2), config, rg, &(*zero), &(*zero), 1);
        fw->flush_row_group().get0();
    }
    return;
}

template<>
void write_file<FileType::strings>(std::unique_ptr<file_writer> &fw, struct file_config config) {
    int16_t zero[1] = {0};
    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        write_column<DataType::string>(fw->column<format::Type::BYTE_ARRAY>(0), config, rg, &(*zero), &(*zero), 1);
        fw->flush_row_group().get0();
    }
    return;
}

template<>
void write_file<FileType::mixed>(std::unique_ptr<file_writer> &fw, struct file_config config) {
    int16_t zero[1] = {0};
    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        write_column<DataType::boolean>(fw->column<format::Type::BOOLEAN>(0), config, rg, &(*zero), &(*zero), 1);
        write_column<DataType::int32>(fw->column<format::Type::INT32>(1), config, rg, &(*zero), &(*zero), 1);
        write_column<DataType::int64>(fw->column<format::Type::INT64>(2), config, rg, &(*zero), &(*zero), 1);
        write_column<DataType::flba_blob>(fw->column<format::Type::FIXED_LEN_BYTE_ARRAY>(3), config, rg, &(*zero), &(*zero), 1);
        fw->flush_row_group().get0();
    }
    return;
}

template<>
void write_file<FileType::nested>(std::unique_ptr<file_writer> &fw, struct file_config config) {
    int16_t zero[1] = {0};

    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        write_column<DataType::int64>(fw->column<format::Type::INT64>(0), config, rg,
                                      &(*zero), &(*zero), 1);

        int16_t backward_defs[4] = {3, 3, 3, 3};
        int16_t backward_reps[4] = {0, 1, 1, 1};
        write_column<DataType::int64>(fw->column<format::Type::INT64>(1), config, rg,
                                      &(*backward_defs), &(*backward_reps), 4);

        int16_t forward_defs[1] = {1};
        int16_t forward_reps[1] = {0};
        write_column<DataType::int64>(fw->column<format::Type::INT64>(2), config, rg,
                                      &(*forward_defs), &(*forward_reps), 1);

        int16_t code_defs[4] = {5, 5, 2, 5};
        int16_t code_reps[4] = {0, 2, 1, 1};
        write_column<DataType::string>(fw->column<format::Type::BYTE_ARRAY>(3), config, rg,
                                       &(*code_defs), &(*code_reps), 4);

        int16_t country_defs[4] = {6, 5, 4, 6};
        int16_t country_reps[4] = {0, 2, 1, 1};
        write_column<DataType::string>(fw->column<format::Type::BYTE_ARRAY>(4), config, rg,
                                       &(*country_defs), &(*country_reps), 4);

        int16_t url_defs[3] = {3, 3, 2};
        int16_t url_reps[3] = {0, 1, 1};
        write_column<DataType::string>(fw->column<format::Type::BYTE_ARRAY>(5), config, rg,
                                       &(*url_defs), &(*url_reps), 3);
        fw->flush_row_group().get0();
    }
    return;
}

template<enum FileType>
writer_schema::schema create_schema(struct file_config config);

template<>
writer_schema::schema create_schema<FileType::numerical>(struct file_config config) {
    using namespace writer_schema;
    return schema{vec<node>(
            primitive_node{
                    "int16",
                    false,
                    logical_type::INT16{},
                    {},
                    config.encoding,
                    config.compression
            },
            primitive_node{
                    "int32",
                    false,
                    logical_type::INT32{},
                    {},
                    config.encoding,
                    config.compression
            },
            primitive_node{
                    "int64",
                    false,
                    logical_type::INT64{},
                    {},
                    config.encoding,
                    config.compression
            }
    )};
};

template<>
writer_schema::schema create_schema<FileType::strings>(struct file_config config) {
    using namespace writer_schema;
    return schema{vec<node>(
            primitive_node{
                    "string",
                    false,
                    logical_type::STRING{},
                    {},
                    config.encoding,
                    config.compression
            }
    )};
};

template<>
writer_schema::schema create_schema<FileType::mixed>(struct file_config config) {
    using namespace writer_schema;
    return schema{vec<node>(
            primitive_node{
                    "bool",
                    false,
                    logical_type::BOOLEAN{},
                    {},
                    format::Encoding::PLAIN,
                    config.compression
            },
            primitive_node{
                    "int32",
                    false,
                    logical_type::INT32{},
                    {},
                    config.encoding,
                    config.compression
            },
            primitive_node{
                    "int64",
                    false,
                    logical_type::INT64{},
                    {},
                    config.encoding,
                    config.compression
            },
            primitive_node{
                    "blob",
                    false,
                    logical_type::FIXED_LEN_BYTE_ARRAY{},
                    config.flba_length,
                    config.encoding,
                    config.compression
            }
    )};
};

template<>
writer_schema::schema create_schema<FileType::nested>(struct file_config config) {
    using namespace writer_schema;
    return schema{vec<node>(
            primitive_node{
                    "DocId",
                    false,
                    logical_type::INT64{},
                    {},
                    config.encoding,
                    config.compression
            },
            struct_node{
                    "Links",
                    true,
                    vec<node>(
                            list_node{
                                    "BackwardList",
                                    true,
                                    box<node>(primitive_node{"Backward", false, logical_type::INT64{}, {}, config.encoding, config.compression})
                            },
                            list_node{
                                    "ForwardList",
                                    true,
                                    box<node>(primitive_node{"Forward", false, logical_type::INT64{}, {}, config.encoding, config.compression})
                            }
                    )
            },
            list_node{
                    "NameList",
                    true,
                    box<node>(struct_node{"Name", false, vec<node>(
                            list_node{
                                    "LanguageList",
                                    true,
                                    box<node>(struct_node{"Language", true, vec<node>(
                                            primitive_node{"Code", false, logical_type::STRING{}, {}, config.encoding, config.compression},
                                            primitive_node{"Country", true, logical_type::STRING{}, {}, config.encoding, config.compression}
                                    )})
                            },
                            primitive_node{"Url", true, logical_type::STRING{}, {}, config.encoding, config.compression}
                    )})
            }
    )};
};

int main(int argc, char *argv[]) {
    namespace po = boost::program_options;

    seastar::app_template app;
    app.add_options()
            ("filename", bpo::value<std::string>(), "Parquet file path")
            ("filetype", bpo::value<std::string>()->default_value("numerical"), "File type")
            ("rowgroups", bpo::value<int64_t>()->default_value(3), "Number of row groups")
            ("rows", bpo::value<int64_t>()->default_value(100000), "Number of rows in a rowgroup")
            ("compression", bpo::value<std::string>()->default_value("uncompressed"), "Compression of all columns")
            ("page", bpo::value<int64_t>()->default_value(8192), "Maximal page size")
            ("plain", bpo::value<bool>()->default_value(false), "Use plain encoding")
            ("string", bpo::value<int64_t>()->default_value(12), "String length")
            ("flba", bpo::value<int64_t>()->default_value(16), "Fixed length byte array length");

    app.run(argc, argv, [&app] {
        auto &&config = app.configuration();
        struct file_config fc;

        if (boost::iequals(config["filetype"].as<std::string>(), "strings")) {
            fc.file_type = FileType::strings;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "mixed")) {
            fc.file_type = FileType::mixed;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "nested")) {
            fc.file_type = FileType::nested;
        } else {
            fc.file_type = FileType::numerical;
        }

        if (boost::iequals(config["compression"].as<std::string>(), "snappy")) {
            fc.compression = format::CompressionCodec::SNAPPY;
        } else if (boost::iequals(config["compression"].as<std::string>(), "gzip")) {
            fc.compression = format::CompressionCodec::GZIP;
        } else {
            fc.compression = format::CompressionCodec::UNCOMPRESSED;
        }

        if (config["plain"].as<bool>()) {
            fc.encoding = format::Encoding::PLAIN;
        } else {
            fc.encoding = format::Encoding::RLE_DICTIONARY;
        }

        fc.rowgroups = config["rowgroups"].as<int64_t>();
        fc.rows = config["rows"].as<int64_t>();
        fc.filename = config["filename"].as<std::string>();
        fc.page_size = config["page"].as<int64_t>();
        fc.flba_length = config["flba"].as<int64_t>();
        fc.string_length = config["string"].as<int64_t>();

        return seastar::async([fc] {
            writer_schema::schema writer_schema;
            switch (fc.file_type) {
                case FileType::numerical:
                    writer_schema = create_schema<FileType::numerical>(fc);
                    break;
                case FileType::strings:
                    writer_schema = create_schema<FileType::strings>(fc);
                    break;
                case FileType::mixed:
                    writer_schema = create_schema<FileType::mixed>(fc);
                    break;
                case FileType::nested:
                    writer_schema = create_schema<FileType::nested>(fc);
                    break;
            }

            std::unique_ptr<file_writer> fw = file_writer::open(fc.filename, writer_schema).get0();
            switch (fc.file_type) {
                case FileType::numerical:
                    write_file<FileType::numerical>(fw, fc);
                    break;
                case FileType::strings:
                    write_file<FileType::strings>(fw, fc);
                    break;
                case FileType::mixed:
                    write_file<FileType::mixed>(fw, fc);
                    break;
                case FileType::nested:
                    write_file<FileType::nested>(fw, fc);
                    break;
            }
            fw->close().get0();
        });
    });
    return 0;
}