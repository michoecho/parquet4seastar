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

#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/exception.hh>
#include <seastar/core/seastar.hh>

namespace parquet4seastar {

seastar::future<std::unique_ptr<format::FileMetaData>> file_reader::read_file_metadata(seastar::file file) {
    return file.size().then([file] (uint64_t size) mutable {
        if (size < 8) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "File too small ({}B) to be a parquet file", size));
        }

        // Parquet file structure:
        // ...
        // File Metadata (serialized with thrift compact protocol)
        // 4-byte length in bytes of file metadata (little endian)
        // 4-byte magic number "PAR1"
        // EOF
        return file.dma_read_exactly<uint8_t>(size - 8, 8).then(
        [file, size] (seastar::temporary_buffer<uint8_t> footer) mutable {
            if (std::memcmp(footer.get() + 4, "PARE", 4) == 0) {
                throw parquet_exception("Parquet encryption is currently unsupported");
            } else if (std::memcmp(footer.get() + 4, "PAR1", 4) != 0) {
                throw parquet_exception::corrupted_file("Magic bytes not found in footer");
            }

            uint32_t metadata_len;
            std::memcpy(&metadata_len, footer.get(), 4);
            if (metadata_len + 8 > size) {
                throw parquet_exception::corrupted_file(seastar::format(
                        "Metadata size reported by footer ({}B) greater than file size ({}B)",
                        metadata_len + 8, size));
            }

            return file.dma_read_exactly<uint8_t>(size - 8 - metadata_len, metadata_len);
        }).then([file] (seastar::temporary_buffer<uint8_t> serialized_metadata) {
            auto deserialized_metadata = std::make_unique<format::FileMetaData>();
            deserialize_thrift_msg(serialized_metadata.get(), serialized_metadata.size(), *deserialized_metadata);
            return deserialized_metadata;
        });
    });
}

seastar::future<file_reader> file_reader::open(std::string path) {
    return seastar::open_file_dma(path, seastar::open_flags::ro).then(
    [path] (seastar::file file) {
        return read_file_metadata(file).then(
        [path = std::move(path), file] (std::unique_ptr<format::FileMetaData> metadata) {
            file_reader fr;
            fr._path = std::move(path);
            fr._file = std::move(file);
            fr._metadata = std::move(metadata);
            return fr;
        });
    }).handle_exception([path = std::move(path)] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            return seastar::make_exception_future<file_reader>(parquet_exception(seastar::format(
                    "Could not open parquet file {} for reading: {}", path, e.what())));
        }
    });
}

namespace {

seastar::future<std::unique_ptr<format::ColumnMetaData>> read_chunk_metadata(seastar::input_stream<char> &&s) {
    using return_type = seastar::future<std::unique_ptr<format::ColumnMetaData>>;
    return seastar::do_with(peekable_stream{std::move(s)}, [](peekable_stream &stream) -> return_type {
        auto column_metadata = std::make_unique<format::ColumnMetaData>();
        return read_thrift_from_stream(stream, *column_metadata).then(
        [column_metadata = std::move(column_metadata)](bool read) mutable {
            if (read) {
                return std::move(column_metadata);
            } else {
                throw parquet_exception::corrupted_file("Could not deserialize ColumnMetaData: empty stream");
            }
        });
    });
}

} // namespace

/* ColumnMetaData is a structure that has to be read in order to find the beginning of a column chunk.
 * It is written directly after the chunk it describes, and its offset is saved to the FileMetaData.
 * Optionally, the entire ColumnMetaData might be embedded in the FileMetaData.
 * That's what the documentation says. However, Arrow always assumes that ColumnMetaData is always
 * present in the FileMetaData, and doesn't bother reading it from it's required location.
 * One of the tests in parquet-testing also gets this wrong (the offset saved in FileMetaData points to something
 * different than ColumnMetaData), so I'm not sure whether this entire function is needed.
 */
template <format::Type::type T>
seastar::future<column_chunk_reader<T>>
file_reader::open_column_chunk_reader_internal(uint32_t row_group, uint32_t column) {
    assert(column < raw_schema().leaves.size());
    assert(row_group < metadata().row_groups.size());
    if (column >= metadata().row_groups[row_group].columns.size()) {
        return seastar::make_exception_future<column_chunk_reader<T>>(
                parquet_exception::corrupted_file(seastar::format(
                        "Selected column metadata is missing from row group metadata: {}",
                        metadata().row_groups[row_group])));
    }
    const format::ColumnChunk& column_chunk = metadata().row_groups[row_group].columns[column];
    const reader_schema::raw_node& leaf = *raw_schema().leaves[column];
    return [this, &column_chunk] {
        if (!column_chunk.__isset.file_path) {
            return seastar::make_ready_future<seastar::file>(file());
        } else {
            return seastar::open_file_dma(path() + column_chunk.file_path, seastar::open_flags::ro);
        }
    }().then([&column_chunk, &leaf] (seastar::file f) {
        return [&column_chunk, f] {
            if (column_chunk.__isset.meta_data) {
                return seastar::make_ready_future<std::unique_ptr<format::ColumnMetaData>>(
                        std::make_unique<format::ColumnMetaData>(column_chunk.meta_data));
            } else {
                return read_chunk_metadata(seastar::make_file_input_stream(f, column_chunk.file_offset, {8192, 16}));
            }
        }().then([f, &leaf] (std::unique_ptr<format::ColumnMetaData> column_metadata) {
            size_t file_offset = column_metadata->__isset.dictionary_page_offset
                                 ? column_metadata->dictionary_page_offset
                                 : column_metadata->data_page_offset;

            return column_chunk_reader<T>{
                    page_reader{seastar::make_file_input_stream(f, file_offset, column_metadata->total_compressed_size, {8192, 16})},
                    column_metadata->codec,
                    leaf.def_level,
                    leaf.rep_level,
                    (leaf.info.__isset.type_length ? std::optional<uint32_t>(leaf.info.type_length) : std::optional<uint32_t>{})};
        });
    });
}

template <format::Type::type T>
seastar::future<column_chunk_reader<T>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) {
    return open_column_chunk_reader_internal<T>(row_group, column).handle_exception(
    [column, row_group] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            return seastar::make_exception_future<column_chunk_reader<T>>(parquet_exception(seastar::format(
                    "Could not open column chunk {} in row group {}: {}", column, row_group, e.what())));
        }
    });
}

template seastar::future<column_chunk_reader<format::Type::INT32>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::INT64>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::INT96>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::FLOAT>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::DOUBLE>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::BOOLEAN>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::BYTE_ARRAY>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);

} // namespace parquet4seastar
