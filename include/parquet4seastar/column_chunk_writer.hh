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

#include <parquet4seastar/column_chunk_reader.hh>
#include <parquet4seastar/bytes.hh>
#include <parquet4seastar/encoding.hh>
#include <boost/iterator/counting_iterator.hpp>
#include <unordered_map>
#include <vector>

namespace parquet4seastar {

struct writer_options {
    uint32_t def_level;
    uint32_t rep_level;
    format::Encoding::type encoding;
    format::CompressionCodec::type compression;
};

template <format::Type::type ParquetType>
class column_chunk_writer {
    thrift_serializer _thrift_serializer;
    rle_builder _rep_encoder;
    rle_builder _def_encoder;
    std::unique_ptr<value_encoder<ParquetType>> _val_encoder;
    std::unique_ptr<compressor> _compressor;
    std::vector<bytes> _pages;
    std::vector<format::PageHeader> _page_headers;
    bytes _dict_page;
    format::PageHeader _dict_page_header;
    std::unordered_set<format::Encoding::type> _used_encodings;
    uint64_t _levels_in_current_page = 0;
    uint64_t _values_in_current_page = 0;
    uint32_t _rep_level;
    uint32_t _def_level;
    uint64_t _rows_written = 0;
    size_t _estimated_chunk_size = 0;
public:
    using input_type = typename value_encoder<ParquetType>::input_type;

    column_chunk_writer(
            uint32_t def_level,
            uint32_t rep_level,
            std::unique_ptr<value_encoder<ParquetType>> val_encoder,
            std::unique_ptr<compressor> compressor)
        : _rep_encoder{bit_width(rep_level)}
        , _def_encoder{bit_width(def_level)}
        , _val_encoder{std::move(val_encoder)}
        , _compressor{std::move(compressor)}
        , _used_encodings(10)
        , _rep_level{rep_level}
        , _def_level{def_level}
        {}

    template <typename LevelT>
    void put_batch(size_t count, LevelT def[], LevelT rep[], input_type val[]) {
        if (_rep_level > 0) {
            _rep_encoder.put_batch(rep, count);
        }
        if (_def_level > 0) {
            _def_encoder.put_batch(def, count);
        }

        size_t value_count = _def_level == 0 ? count : std::count(def, def + count, 0);
        _val_encoder->put_batch(val, value_count);

        size_t row_count = _rep_level == 0 ? count : std::count(rep, rep + count, 0);
        _rows_written += row_count;
        _levels_in_current_page += count;
    }

    void put(uint32_t def_level, uint32_t rep_level, input_type val) {
        if (_rep_level > 0) {
            _rep_encoder.put(rep_level);
        }
        if (_rep_level == 0 || rep_level == 0) {
            ++_rows_written;
        }
        if (_def_level > 0) {
            _def_encoder.put(def_level);
        }
        if (_def_level == 0 || def_level == _def_level) {
            _val_encoder->put_batch(&val, 1);
        }
        ++_levels_in_current_page;
    }

    size_t current_page_max_size() const {
        size_t def_size = _def_level ? _def_encoder.max_encoded_size() : 0;
        size_t rep_size = _rep_level ? _rep_encoder.max_encoded_size() : 0;
        size_t value_size = _val_encoder->max_encoded_size();
        return def_size + rep_size + value_size;
    }

    void flush_page() {
        bytes page;
        size_t page_max_size = current_page_max_size();
        page.reserve(page_max_size);
        if (_rep_level > 0) {
            bytes_view levels = _rep_encoder.view();
            append_raw_bytes<uint32_t>(page, levels.size());
            page.insert(page.end(), levels.begin(), levels.end());
        }
        if (_def_level > 0) {
            bytes_view levels = _def_encoder.view();
            append_raw_bytes<uint32_t>(page, levels.size());
            page.insert(page.end(), levels.begin(), levels.end());
        }
        size_t data_offset = page.size();
        page.resize(page_max_size);
        auto flush_info = _val_encoder->flush(page.data() + data_offset);
        page.resize(data_offset + flush_info.size);

        bytes compressed_page = _compressor->compress(page);

        format::DataPageHeader data_page_header;
        data_page_header.__set_num_values(_levels_in_current_page);
        data_page_header.__set_encoding(flush_info.encoding);
        data_page_header.__set_definition_level_encoding(format::Encoding::RLE);
        data_page_header.__set_repetition_level_encoding(format::Encoding::RLE);
        format::PageHeader page_header;
        page_header.__set_type(format::PageType::DATA_PAGE);
        page_header.__set_uncompressed_page_size(page.size());
        page_header.__set_compressed_page_size(compressed_page.size());
        page_header.__set_data_page_header(data_page_header);

        _estimated_chunk_size += compressed_page.size();
        _def_encoder.clear();
        _rep_encoder.clear();
        _levels_in_current_page = 0;
        _values_in_current_page = 0;

        _used_encodings.insert(flush_info.encoding);
        _page_headers.push_back(std::move(page_header));
        _pages.push_back(std::move(compressed_page));
    }

    seastar::future<seastar::lw_shared_ptr<format::ColumnMetaData>> flush_chunk(seastar::output_stream<char>& sink) {
        if (_levels_in_current_page > 0) {
            flush_page();
        }
        auto metadata = seastar::make_lw_shared<format::ColumnMetaData>();
        metadata->__set_type(ParquetType);
        metadata->__set_encodings(
                std::vector<format::Encoding::type>(
                        _used_encodings.begin(), _used_encodings.end()));
        metadata->__set_codec(_compressor->type());
        metadata->__set_num_values(0);
        metadata->__set_total_compressed_size(0);
        metadata->__set_total_uncompressed_size(0);

        auto write_page = [this, metadata, &sink] (const format::PageHeader& header, bytes_view contents) {
            bytes_view serialized_header = _thrift_serializer.serialize(header);
            metadata->total_uncompressed_size += serialized_header.size();
            metadata->total_uncompressed_size += header.uncompressed_page_size;
            metadata->total_compressed_size += serialized_header.size();
            metadata->total_compressed_size += header.compressed_page_size;

            const char* data = reinterpret_cast<const char*>(serialized_header.data());
            return sink.write(data, serialized_header.size()).then([this, contents, &sink] {
                const char* data = reinterpret_cast<const char*>(contents.data());
                return sink.write(data, contents.size());
            });
        };

        return [this, metadata, write_page, &sink] {
            if (_val_encoder->view_dict()) {
                fill_dictionary_page();
                metadata->__set_dictionary_page_offset(metadata->total_compressed_size);
                return write_page(_dict_page_header, _dict_page);
            } else {
                return seastar::make_ready_future<>();
            }
        }().then([this, write_page, metadata, &sink] {
            metadata->__set_data_page_offset(metadata->total_compressed_size);
            using it = boost::counting_iterator<size_t>;
            return seastar::do_for_each(it(0), it(_page_headers.size()),
                [this, metadata, write_page, &sink] (size_t i) {
                metadata->num_values += _page_headers[i].data_page_header.num_values;
                return write_page(_page_headers[i], _pages[i]);
            });
        }).then([this, metadata] {
            _pages.clear();
            _page_headers.clear();
            _estimated_chunk_size = 0;
            return metadata;
        });
    }

    size_t rows_written() const { return _rows_written; }
    size_t estimated_chunk_size() const { return _estimated_chunk_size; }

private:
    void fill_dictionary_page() {
        bytes_view dict = *_val_encoder->view_dict();
        _dict_page = _compressor->compress(dict);

        format::DictionaryPageHeader dictionary_page_header;
        dictionary_page_header.__set_num_values(_val_encoder->cardinality());
        dictionary_page_header.__set_encoding(format::Encoding::PLAIN);
        dictionary_page_header.__set_is_sorted(false);
        _dict_page_header.__set_type(format::PageType::DICTIONARY_PAGE);
        _dict_page_header.__set_uncompressed_page_size(dict.size());
        _dict_page_header.__set_compressed_page_size(_dict_page.size());
        _dict_page_header.__set_dictionary_page_header(dictionary_page_header);
    }
};

template <format::Type::type ParquetType>
column_chunk_writer<ParquetType>
make_column_chunk_writer(const writer_options& options) {
    return column_chunk_writer<ParquetType>(
            options.def_level,
            options.rep_level,
            make_value_encoder<ParquetType>(options.encoding),
            compressor::make(options.compression));
}

} // namespace parquet4seastar
