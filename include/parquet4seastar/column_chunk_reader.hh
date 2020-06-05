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

#pragma once

#include <parquet4seastar/thrift_serdes.hh>
#include <parquet4seastar/overloaded.hh>
#include <parquet4seastar/compression.hh>
#include <parquet4seastar/encoding.hh>

namespace parquet4seastar {

struct page {
    const format::PageHeader* header;
    bytes_view contents;
};

class page_reader {
    peekable_stream _source;
    std::unique_ptr<format::PageHeader> _latest_header;
    static constexpr uint32_t _default_expected_header_size = 1024;
    static constexpr uint32_t _max_allowed_header_size = 16 * 1024 * 1024;
public:
    explicit page_reader(seastar::input_stream<char>&& source)
        : _source{std::move(source)}
        , _latest_header{std::make_unique<format::PageHeader>()} {};
    // View the next page. Returns an empty result on eof.
    seastar::future<std::optional<page>> next_page();
};

// The core low-level interface. Takes the relevant metadata and an input_stream set to the beginning of a column chunk
// and extracts batches of (repetition level, definition level, value (optional)) from it.
template<format::Type::type T>
class column_chunk_reader {
public:
    using output_type = typename value_decoder_traits<T>::output_type;
private:
    page_reader _source;
    std::unique_ptr<compressor> _decompressor;
    bytes _decompression_buffer;
    level_decoder _rep_decoder;
    level_decoder _def_decoder;
    value_decoder<T> _val_decoder;
    std::optional<std::vector<output_type>> _dict;
    bool _initialized = false;
    bool _eof = false;
    int64_t _page_ordinal = -1; // Only used for error reporting.
private:
    uint32_t _def_level;
    uint32_t _rep_level;
    std::optional<uint32_t> _type_length;
private:
    bytes_view decompress(bytes_view compressed, size_t uncompressed_size);
    seastar::future<> load_next_page();
    void load_dictionary_page(page p);
    void load_data_page(page p);
    void load_data_page_v2(page p);

    template<typename LevelT>
    seastar::future<size_t> read_batch_internal(size_t n, LevelT def[], LevelT rep[], output_type val[]);
public:
    explicit column_chunk_reader(
            page_reader&& source,
            format::CompressionCodec::type codec,
            uint32_t def_level,
            uint32_t rep_level,
            std::optional<uint32_t> type_length)
        : _source{std::move(source)}
        , _decompressor{compressor::make(codec)}
        , _rep_decoder{rep_level}
        , _def_decoder{def_level}
        , _val_decoder{type_length}
        , _def_level{def_level}
        , _rep_level{rep_level}
        , _type_length{type_length}
        {};
    // Read a batch of n (rep, def, value) triplets. The last batch may be smaller than n.
    // Return the number of triplets read. Note that null values are not read into the output array.
    // Example output: def == [1, 1, 0, 1, 0], rep = [0, 0, 0, 0, 0], val = ["a", "b", "d"].
    template<typename LevelT>
    seastar::future<size_t> read_batch(size_t n, LevelT def[], LevelT rep[], output_type val[]);
};

template<format::Type::type T>
template<typename LevelT>
seastar::future<size_t>
column_chunk_reader<T>::read_batch_internal(size_t n, LevelT def[], LevelT rep[], output_type val[]) {
    if (_eof) {
        return seastar::make_ready_future<size_t>(0);
    }
    if (!_initialized) {
        return load_next_page().then([this, n, def, rep, val] {
            return read_batch_internal(n, def, rep, val);
        });
    }
    size_t def_levels_read = _def_decoder.read_batch(n, def);
    size_t rep_levels_read = _rep_decoder.read_batch(n, rep);
    if (def_levels_read != rep_levels_read) {
        return seastar::make_exception_future<size_t>(parquet_exception::corrupted_file(seastar::format(
                "Number of definition levels {} does not equal the number of repetition levels {} in batch",
                def_levels_read, rep_levels_read)));
    }
    if (def_levels_read == 0) {
        _initialized = false;
        return read_batch_internal(n, def, rep, val);
    }
#if 0
    for (size_t i = 0; i < def_levels_read; ++i) {
        if (def[i] < 0 || def[i] > static_cast<LevelT>(_def_level)) {
            return seastar::make_exception_future<size_t>(parquet_exception::corrupted_file(seastar::format(
                    "Definition level ({}) out of range (0 to {})", def[i], _def_level)));
        }
        if (rep[i] < 0 || rep[i] > static_cast<LevelT>(_rep_level)) {
            return seastar::make_exception_future<size_t>(parquet_exception::corrupted_file(seastar::format(
                    "Repetition level ({}) out of range (0 to {})", rep[i], _rep_level)));
        }
    }
#endif
    size_t values_to_read = _def_level == 0 ? def_levels_read : std::count(def, def + def_levels_read, static_cast<LevelT>(_def_level));
    size_t values_read = _val_decoder.read_batch(values_to_read, val);
    if (values_read != values_to_read) {
        return seastar::make_exception_future<size_t>(parquet_exception::corrupted_file(seastar::format(
                "Number of values in batch {} is less than indicated by def levels {}", values_read, values_to_read)));
    }
    return seastar::make_ready_future<size_t>(def_levels_read);
}

template<format::Type::type T>
template<typename LevelT>
seastar::future<size_t>
inline column_chunk_reader<T>::read_batch(size_t n, LevelT def[], LevelT rep[], output_type val[]) {
    return read_batch_internal(n, def, rep, val)
    .handle_exception_type([this] (const std::exception& e) {
        return seastar::make_exception_future<size_t>(parquet_exception(seastar::format(
                "Error while reading page number {}: {}", _page_ordinal, e.what())));
    });
}

extern template class column_chunk_reader<format::Type::INT32>;
extern template class column_chunk_reader<format::Type::INT64>;
extern template class column_chunk_reader<format::Type::INT96>;
extern template class column_chunk_reader<format::Type::FLOAT>;
extern template class column_chunk_reader<format::Type::DOUBLE>;
extern template class column_chunk_reader<format::Type::BOOLEAN>;
extern template class column_chunk_reader<format::Type::BYTE_ARRAY>;
extern template class column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>;

} // namespace parquet4seastar
