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

#include <parquet4seastar/encoding.hh>

namespace parquet4seastar {

size_t level_decoder::reset_v1(
        bytes_view buffer,
        format::Encoding::type encoding,
        uint32_t num_values) {
    _num_values = num_values;
    _values_read = 0;
    if (_bit_width == 0) {
        return 0;
    }
    if (encoding == format::Encoding::RLE) {
        if (buffer.size() < 4) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading levels (needed {}B, got {}B)", 4, buffer.size()));
        }
        int32_t len;
        std::memcpy(&len, buffer.data(), 4);
        if (len < 0) {
            throw parquet_exception::corrupted_file(seastar::format("Negative RLE levels length ({})", len));
        }
        if (static_cast<size_t>(len) > buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading levels (needed {}B, got {}B)", len, buffer.size()));
        }
        _decoder = RleDecoder{buffer.data() + 4, len, static_cast<int>(_bit_width)};
        return 4 + len;
    } else if (encoding == format::Encoding::BIT_PACKED) {
        uint64_t bit_len = static_cast<uint64_t>(num_values) * _bit_width;
        uint64_t byte_len = (bit_len + 7) >> 3;
        if (byte_len > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            throw parquet_exception::corrupted_file(seastar::format("BIT_PACKED length exceeds int ({}B)", byte_len));
        }
        if (byte_len > buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading levels (needed {}B, got {}B)", byte_len, buffer.size()));
        }
        _decoder = BitReader{buffer.data(), static_cast<int>(byte_len)};
        return byte_len;
    } else {
        throw parquet_exception(seastar::format("Unknown level encoding ({})", encoding));
    }
}

void level_decoder::reset_v2(bytes_view encoded_levels, uint32_t num_values) {
    _num_values = num_values;
    _values_read = 0;
    if (encoded_levels.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Levels length exceeds int ({}B)", encoded_levels.size()));
    }
    _decoder = RleDecoder{
            encoded_levels.data(),
            static_cast<int>(encoded_levels.size()),
            static_cast<int>(_bit_width)};
}

template <format::Type::type ParquetType>
void plain_decoder_trivial<ParquetType>::reset(bytes_view data) {
    _buffer = data;
}

void plain_decoder_boolean::reset(bytes_view data) {
    _decoder.Reset(static_cast<const uint8_t*>(data.data()), data.size());
}

void plain_decoder_byte_array::reset(bytes_view data) {
    _buffer = seastar::temporary_buffer<uint8_t>(data.size());
    std::memcpy(_buffer.get_write(), data.data(), data.size());
}

void plain_decoder_fixed_len_byte_array::reset(bytes_view data) {
    _buffer = seastar::temporary_buffer<uint8_t>(data.size());
    std::memcpy(_buffer.get_write(), data.data(), data.size());
}

template <format::Type::type ParquetType>
size_t plain_decoder_trivial<ParquetType>::read_batch(size_t n, output_type out[]) {
    size_t n_to_read = std::min(_buffer.size() / sizeof(output_type), n);
    size_t bytes_to_read = sizeof(output_type) * n_to_read;
    if (bytes_to_read > 0) {
        std::memcpy(out, _buffer.data(), bytes_to_read);
    }
    _buffer.remove_prefix(bytes_to_read);
    return n_to_read;
}

size_t plain_decoder_boolean::read_batch(size_t n, uint8_t out[]) {
    return _decoder.GetBatch(1, out, n);
}

size_t plain_decoder_byte_array::read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]) {
    for (size_t i = 0; i < n; ++i) {
        if (_buffer.size() == 0) {
            return i;
        }
        if (_buffer.size() < 4) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading BYTE_ARRAY length (needed {}B, got {}B)", 4, _buffer.size()));
        }
        uint32_t len;
        std::memcpy(&len, _buffer.get(), 4);
        _buffer.trim_front(4);
        if (len > _buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading BYTE_ARRAY (needed {}B, got {}B)", len, _buffer.size()));
        }
        out[i] = _buffer.share(0, len);
        _buffer.trim_front(len);
    }
    return n;
}

size_t plain_decoder_fixed_len_byte_array::read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]) {
    for (size_t i = 0; i < n; ++i) {
        if (_buffer.size() == 0) {
            return i;
        }
        if (_fixed_len > _buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading FIXED_LEN_BYTE_ARRAY (needed {}B, got {}B)",
                    _fixed_len, _buffer.size()));
        }
        out[i] = _buffer.share(0, _fixed_len);
        _buffer.trim_front(_fixed_len);
    }
    return n;
}

template <format::Type::type ParquetType>
void dict_decoder<ParquetType>::reset(bytes_view data) {
    if (data.size() == 0) {
        _rle_decoder.Reset(data.data(), data.size(), 0);
    }
    int bit_width = data.data()[0];
    if (bit_width < 0 || bit_width > 32) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Illegal dictionary index bit width (should be 0 <= bit width <= 32, got {})", bit_width));
    }
    _rle_decoder.Reset(data.data() + 1, data.size() - 1, bit_width);
}

template <format::Type::type ParquetType>
size_t dict_decoder<ParquetType>::read_batch(size_t n, output_type out[]) {
    std::array<uint32_t, 1000> buf;
    size_t completed = 0;
    while (completed < n) {
        size_t n_to_read = std::min(n - completed, buf.size());
        size_t n_read = _rle_decoder.GetBatch(buf.data(), n_to_read);
        for (size_t i = 0; i < n_read; ++i) {
            if (buf[i] > _dict_size) {
                throw parquet_exception::corrupted_file(seastar::format(
                        "Dict index exceeds dict size (dict size = {}, index = {})", _dict_size, buf[i]));
            }
        }
        for (size_t i = 0; i < n_read; ++i) {
            if constexpr (std::is_trivially_copyable_v<output_type>) {
                out[completed + i] = _dict[buf[i]];
            } else {
                // Why isn't seastar::temporary_buffer copyable though?
                out[completed + i] = _dict[buf[i]].share();
            }
        }
        completed += n_read;
        if (n_read < n_to_read) {
            return completed;
        }
    }
    return n;
}

void rle_decoder_boolean::reset(bytes_view data) {
    _rle_decoder.Reset(data.data(), data.size(), 1);
}

size_t rle_decoder_boolean::read_batch(size_t n, uint8_t out[]) {
    return _rle_decoder.GetBatch(out, n);
}

template <format::Type::type ParquetType>
void delta_binary_packed_decoder<ParquetType>::reset(bytes_view data) {
    _decoder.Reset(data.data(), data.size());
    _values_current_block = 0;
    _values_current_mini_block = 0;
}

template <format::Type::type ParquetType>
void delta_binary_packed_decoder<ParquetType>::init_block() {
    uint32_t block_size;
    if (!_decoder.GetVlqInt(&block_size)) { throw; }
    if (!_decoder.GetVlqInt(&_num_mini_blocks)) { throw; }
    if (!_decoder.GetVlqInt(&_values_current_block)) { throw; }
    if (!_decoder.GetZigZagVlqInt(&_last_value)) { throw; }

    if (_delta_bit_widths.size() < static_cast<uint32_t>(_num_mini_blocks)) {
        _delta_bit_widths = buffer(_num_mini_blocks);
    }

    if (!_decoder.GetZigZagVlqInt(&_min_delta)) { throw; };
    for (uint32_t i = 0; i < _num_mini_blocks; ++i) {
        if (!_decoder.GetAligned<uint8_t>(1, _delta_bit_widths.data() + i)) {
            throw;
        }
    }
    _values_per_mini_block = block_size / _num_mini_blocks;
    _mini_block_idx = 0;
    _delta_bit_width = _delta_bit_widths.data()[0];
    _values_current_mini_block = _values_per_mini_block;
}

template <format::Type::type ParquetType>
size_t delta_binary_packed_decoder<ParquetType>::read_batch(size_t n, output_type out[]) {
    try {
        for (size_t i = 0; i < n; ++i) {
            if (__builtin_expect(_values_current_mini_block == 0, 0)) {
                ++_mini_block_idx;
                if (_mini_block_idx < static_cast<size_t>(_num_mini_blocks)) {
                    _delta_bit_width = _delta_bit_widths.data()[_mini_block_idx];
                    _values_current_mini_block = _values_per_mini_block;
                } else {
                    init_block();
                    out[i] = _last_value;
                    continue;
                }
            }

            // TODO: the entire miniblock should be decoded at once.
            int64_t delta;
            if (!_decoder.GetValue(_delta_bit_width, &delta)) { throw; }
            delta += _min_delta;
            _last_value += static_cast<int32_t>(delta);
            out[i] = _last_value;
            --_values_current_mini_block;
        }
    } catch (...) {
        throw parquet_exception::corrupted_file("Could not decode DELTA_BINARY_PACKED batch");
    }
    return n;
}

template <format::Type::type ParquetType>
void value_decoder<ParquetType>::reset_dict(output_type dictionary[], size_t dictionary_size) {
    _dict = dictionary;
    _dict_size = dictionary_size;
    _dict_set = true;
};

template<format::Type::type ParquetType>
void value_decoder<ParquetType>::reset(bytes_view buf, format::Encoding::type encoding) {
    switch (encoding) {
        case format::Encoding::PLAIN:
            if constexpr (ParquetType == format::Type::BOOLEAN) {
                _decoder = std::make_unique<plain_decoder_boolean>();
            } else if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
                _decoder = std::make_unique<plain_decoder_byte_array>();
            } else if constexpr (ParquetType == format::Type::FIXED_LEN_BYTE_ARRAY) {
                _decoder = std::make_unique<plain_decoder_fixed_len_byte_array>(static_cast<size_t>(*_type_length));
            } else {
                _decoder = std::make_unique<plain_decoder_trivial<ParquetType>>();
            }
            break;
        case format::Encoding::RLE_DICTIONARY:
        case format::Encoding::PLAIN_DICTIONARY:
            if (!_dict_set) {
                throw parquet_exception::corrupted_file("No dictionary page found before a dictionary-encoded page");
            }
            _decoder = std::make_unique<dict_decoder<ParquetType>>(_dict, _dict_size);
            break;
        case format::Encoding::RLE:
            if constexpr (ParquetType == format::Type::BOOLEAN) {
                _decoder = std::make_unique<rle_decoder_boolean>();
            } else {
                throw parquet_exception::corrupted_file("RLE encoding is valid only for BOOLEAN values");
            }
            break;
        case format::Encoding::DELTA_BINARY_PACKED:
            if constexpr (ParquetType == format::Type::INT32 || ParquetType == format::Type::INT64) {
                _decoder = std::make_unique<delta_binary_packed_decoder<ParquetType>>();
            } else {
                throw parquet_exception::corrupted_file("DELTA_BINARY_PACKED is valid only for INT32 and INT64");
            }
            break;
        default:
            throw parquet_exception(seastar::format("Encoding {} not implemented", encoding));
    }
    _decoder->reset(buf);
};

template<format::Type::type ParquetType>
size_t value_decoder<ParquetType>::read_batch(size_t n, output_type out[]) {
    return _decoder->read_batch(n, out);
};

/*
 * Explicit instantiation of value_decoder shouldn't be needed,
 * because column_chunk_reader<T> has a value_decoder<T> member.
 * Yet, without explicit instantiation of value_decoder<T>,
 * value_decoder<T>::read_batch is not generated. Why?
 *
 * Quote: When an explicit instantiation names a class template specialization,
 * it serves as an explicit instantiation of the same kind (declaration or definition)
 * of each of its non-inherited non-template members that has not been previously
 * explicitly specialized in the translation unit. If this explicit instantiation
 * is a definition, it is also an explicit instantiation definition only for
 * the members that have been defined at this point.
 * https://en.cppreference.com/w/cpp/language/class_template
 *
 * Has value_decoder<T> not been "defined at this point" or what?
 */
template class value_decoder<format::Type::INT32>;
template class value_decoder<format::Type::INT64>;
template class value_decoder<format::Type::INT96>;
template class value_decoder<format::Type::FLOAT>;
template class value_decoder<format::Type::DOUBLE>;
template class value_decoder<format::Type::BOOLEAN>;
template class value_decoder<format::Type::BYTE_ARRAY>;
template class value_decoder<format::Type::FIXED_LEN_BYTE_ARRAY>;

} // namespace parquet4seastar
