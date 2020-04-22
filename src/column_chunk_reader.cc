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

#include <parquet4seastar/column_chunk_reader.hh>
#include <parquet4seastar/compression.hh>

namespace parquet4seastar {

seastar::future<std::optional<page>> page_reader::next_page() {
    *_latest_header = format::PageHeader{}; // Thrift does not clear the structure by itself before writing to it.
    return read_thrift_from_stream(_source, *_latest_header).then([this] (bool read) {
        if (!read) {
            return seastar::make_ready_future<std::optional<page>>();
        }
        if (_latest_header->compressed_page_size < 0) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "Negative compressed_page_size in header: {}", *_latest_header));
        }
        size_t compressed_size = static_cast<uint32_t>(_latest_header->compressed_page_size);
        return _source.peek(compressed_size).then(
        [this, compressed_size] (bytes_view page_contents) {
            if (page_contents.size() < compressed_size) {
                throw parquet_exception::corrupted_file(seastar::format(
                        "Unexpected end of column chunk while reading compressed page contents (expected {}B, got {}B)",
                        compressed_size, page_contents.size()));
            }
            return _source.advance(compressed_size).then([this, page_contents] {
                return seastar::make_ready_future<std::optional<page>>(page{_latest_header.get(), page_contents});
            });
        });
    });
}

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page(page p) {
    if (!p.header->__isset.data_page_header) {
        throw parquet_exception::corrupted_file(seastar::format(
                "DataPageHeader not set for DATA_PAGE header: {}", *p.header));
    }
    const format::DataPageHeader& header = p.header->data_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative num_values in header: {}", header));
    }
    if (p.header->uncompressed_page_size < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative uncompressed_page_size in header: {}", *p.header));
    }

    _decompression_buffer.resize(p.header->uncompressed_page_size);
    _decompression_buffer = _decompressor->decompress(p.contents, std::move(_decompression_buffer));
    bytes_view contents = _decompression_buffer;

    size_t n_read = 0;
    n_read = _rep_decoder.reset_v1(contents, header.repetition_level_encoding, header.num_values);
    contents.remove_prefix(n_read);
    n_read = _def_decoder.reset_v1(contents, header.definition_level_encoding, header.num_values);
    contents.remove_prefix(n_read);
    _val_decoder.reset(contents, header.encoding);
}

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page_v2(page p) {
    if (!p.header->__isset.data_page_header_v2) {
        throw parquet_exception::corrupted_file(seastar::format(
                "DataPageHeaderV2 not set for DATA_PAGE_V2 header: {}", *p.header));
    }
    const format::DataPageHeaderV2& header = p.header->data_page_header_v2;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative num_values in header: {}", header));
    }
    if (header.repetition_levels_byte_length < 0 || header.definition_levels_byte_length < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative levels byte length in header: {}", header));
    }
    if (p.header->uncompressed_page_size < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative uncompressed_page_size in header: {}", *p.header));
    }
    bytes_view contents = p.contents;
    _rep_decoder.reset_v2(contents.substr(0, header.repetition_levels_byte_length), header.num_values);
    contents.remove_prefix(header.repetition_levels_byte_length);
    _def_decoder.reset_v2(contents.substr(0, header.definition_levels_byte_length), header.num_values);
    contents.remove_prefix(header.definition_levels_byte_length);
    if (header.__isset.is_compressed && header.is_compressed) {
        size_t n_read = header.repetition_levels_byte_length + header.definition_levels_byte_length;
        size_t uncompressed_values_size = static_cast<size_t>(p.header->uncompressed_page_size) - n_read;
        _decompression_buffer.resize(uncompressed_values_size);
        _decompression_buffer = _decompressor->decompress(contents, std::move(_decompression_buffer));
    }
    _val_decoder.reset(_decompression_buffer, header.encoding);
}

template<format::Type::type T>
void column_chunk_reader<T>::load_dictionary_page(page p) {
    if (!p.header->__isset.dictionary_page_header) {
        throw parquet_exception::corrupted_file(seastar::format(
                "DictionaryPageHeader not set for DICTIONARY_PAGE header: {}", *p.header));
    }
    const format::DictionaryPageHeader& header = p.header->dictionary_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    if (p.header->uncompressed_page_size < 0) {
        throw parquet_exception::corrupted_file(
                seastar::format("Negative uncompressed_page_size in header: {}", *p.header));
    }
    _dict = std::vector<output_type>(header.num_values);
    _decompression_buffer.resize(p.header->uncompressed_page_size);
    _decompression_buffer = _decompressor->decompress(p.contents, std::move(_decompression_buffer));
    value_decoder<T> vd{_type_length};
    vd.reset(_decompression_buffer, format::Encoding::PLAIN);
    size_t n_read = vd.read_batch(_dict->size(), _dict->data());
    if (n_read < _dict->size()) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Unexpected end of dictionary page (expected {} values, got {})", _dict->size(), n_read));
    }
    _val_decoder.reset_dict(_dict->data(), _dict->size());
}

template<format::Type::type T>
seastar::future<> column_chunk_reader<T>::load_next_page() {
    ++_page_ordinal;
    return _source.next_page().then([this] (std::optional<page> p) {
        if (!p) {
            _eof = true;
        } else {
            switch (p->header->type) {
            case format::PageType::DATA_PAGE:
                load_data_page(*p);
                _initialized = true;
                return;
            case format::PageType::DATA_PAGE_V2:
                load_data_page_v2(*p);
                _initialized = true;
                return;
            case format::PageType::DICTIONARY_PAGE:
                load_dictionary_page(*p);
                return;
            default:; // Unknown page types are to be skipped
            }
        }
    });
}

template class column_chunk_reader<format::Type::INT32>;
template class column_chunk_reader<format::Type::INT64>;
template class column_chunk_reader<format::Type::INT96>;
template class column_chunk_reader<format::Type::FLOAT>;
template class column_chunk_reader<format::Type::DOUBLE>;
template class column_chunk_reader<format::Type::BOOLEAN>;
template class column_chunk_reader<format::Type::BYTE_ARRAY>;
template class column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>;

} // namespace parquet4seastar
