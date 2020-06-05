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
    int32,
    int64,
    string
};

enum class DataType {
    int32,
    int64,
    string
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

int32_t* null_lvl = static_cast<int32_t*>(nullptr);

template<enum FileType>
void write_file(std::unique_ptr<file_writer> &fw, struct file_config config);

template<>
void write_file<FileType::int32>(std::unique_ptr<file_writer> &fw, struct file_config config) {
    int32_t batch[1024];
    for (int i = 0; i < std::size(batch); ++i) {
        batch[i] = i % 256;
    }
    auto &column_writer = fw->column<format::Type::INT32>(0);
    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        for (int64_t k = 0; k < config.rows; k += std::size(batch)) {
            column_writer.put_batch(std::size(batch), null_lvl, null_lvl, batch);
            if (column_writer.current_page_max_size() > config.page_size) {
                column_writer.flush_page();
                seastar::thread::yield();
            }
        }
        fw->flush_row_group().get0();
    }
}

template<>
void write_file<FileType::int64>(std::unique_ptr<file_writer> &fw, struct file_config config) {
    int64_t batch[1024];
    for (int i = 0; i < std::size(batch); ++i) {
        batch[i] = i % 256;
    }
    auto &column_writer = fw->column<format::Type::INT64>(0);
    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        for (int64_t k = 0; k < config.rows; k += std::size(batch)) {
            column_writer.put_batch(std::size(batch), null_lvl, null_lvl, batch);
            if (column_writer.current_page_max_size() > config.page_size) {
                column_writer.flush_page();
                seastar::thread::yield();
            }
        }
        fw->flush_row_group().get0();
    }
}

template<>
void write_file<FileType::string>(std::unique_ptr<file_writer> &fw, struct file_config config) {
    auto &column_writer = fw->column<format::Type::BYTE_ARRAY>(0);
    bytes strings[256];
    for (int i = 0; i < std::size(strings); ++i) {
        strings[i] = bytes(config.string_length, 0);
        strings[i][0] = (uint8_t)(i % 256);
    }
    bytes_view batch[1024];
    for (int i = 0; i < std::size(batch); ++i) {
        batch[i] = bytes_view(strings[i % 256]);
    }
    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        for (int64_t k = 0; k < config.rows; k += std::size(batch)) {
            column_writer.put_batch(std::size(batch), null_lvl, null_lvl, batch);
            if (column_writer.current_page_max_size() > config.page_size) {
                column_writer.flush_page();
                seastar::thread::yield();
            }
        }
        fw->flush_row_group().get0();
    }
}

template<enum FileType>
writer_schema::schema create_schema(struct file_config config);

template<>
writer_schema::schema create_schema<FileType::int32>(struct file_config config) {
    using namespace writer_schema;
    return schema{vec<node>(
            primitive_node{
                    "int32",
                    false,
                    logical_type::INT32{},
                    {},
                    config.encoding,
                    config.compression
            }
    )};
};

template<>
writer_schema::schema create_schema<FileType::int64>(struct file_config config) {
    using namespace writer_schema;
    return schema{vec<node>(
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
writer_schema::schema create_schema<FileType::string>(struct file_config config) {
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

int main(int argc, char *argv[]) {
    namespace po = boost::program_options;

    seastar::app_template app;
    app.add_options()
            ("filename", bpo::value<std::string>(), "Parquet file path")
            ("filetype", bpo::value<std::string>()->default_value("int32"), "File type")
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

        if (boost::iequals(config["filetype"].as<std::string>(), "string")) {
            fc.file_type = FileType::string;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "int32")) {
            fc.file_type = FileType::int32;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "int64")) {
            fc.file_type = FileType::int64;
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
                case FileType::int32:
                    writer_schema = create_schema<FileType::int32>(fc);
                    break;
                case FileType::int64:
                    writer_schema = create_schema<FileType::int64>(fc);
                    break;
                case FileType::string:
                    writer_schema = create_schema<FileType::string>(fc);
                    break;
            }

            std::unique_ptr<file_writer> fw = file_writer::open(fc.filename, writer_schema).get0();
            switch (fc.file_type) {
                case FileType::int32:
                    write_file<FileType::int32>(fw, fc);
                    break;
                case FileType::int64:
                    write_file<FileType::int64>(fw, fc);
                    break;
                case FileType::string:
                    write_file<FileType::string>(fw, fc);
                    break;
            }
            fw->close().get0();
        });
    });
    return 0;
}
