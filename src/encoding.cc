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
class delta_binary_packed_decoder final : public decoder<ParquetType> {
    BitReader _decoder;
    uint32_t _values_per_block;
    uint32_t _num_mini_blocks;
    uint32_t _values_remaining;
    int32_t _last_value;

    int32_t _min_delta;
    buffer _delta_bit_widths;

    uint8_t _delta_bit_width;
    uint32_t _mini_block_idx;
    uint64_t _values_current_mini_block;
    uint32_t _values_per_mini_block;
private:
    void init_block() {
        if (!_decoder.GetZigZagVlqInt(&_min_delta)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED block header");
        }

        for (uint32_t i = 0; i < _num_mini_blocks; ++i) {
            if (!_decoder.GetAligned<uint8_t>(1, _delta_bit_widths.data() + i)) {
                throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED block header");
            }
        }
        _mini_block_idx = 0;
    }
public:
    using typename decoder<ParquetType>::output_type;

    size_t bytes_left() {
        return _decoder.bytes_left();
    }

    void reset(bytes_view data) override {
        _decoder.Reset(data.data(), data.size());

        if (!_decoder.GetVlqInt(&_values_per_block)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }
        if (!_decoder.GetVlqInt(&_num_mini_blocks)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }
        if (_num_mini_blocks == 0) {
            throw parquet_exception("In DELTA_BINARY_PACKED number miniblocks per block is 0");
        }
        if (!_decoder.GetVlqInt(&_values_remaining)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }
        if (!_decoder.GetZigZagVlqInt(&_last_value)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }

        if (_delta_bit_widths.size() < static_cast<uint32_t>(_num_mini_blocks)) {
            _delta_bit_widths = buffer(_num_mini_blocks);
        }

        _values_per_mini_block = _values_per_block / _num_mini_blocks;
        _values_current_mini_block = 0;
        _mini_block_idx = _num_mini_blocks;
    }

    size_t read_batch(size_t n, output_type out[]) override {
        if (_values_remaining == 0) {
            return 0;
        }
        size_t i = 0;
        while (i < n) {
            out[i] = _last_value;
            ++i;
            --_values_remaining;
            if (_values_remaining == 0) {
                eat_final_padding();
                break;
            }
            if (__builtin_expect(_values_current_mini_block == 0, 0)) {
                if (_mini_block_idx == _num_mini_blocks) {
                    init_block();
                }
                _delta_bit_width = _delta_bit_widths.data()[_mini_block_idx];
                _values_current_mini_block = _values_per_mini_block;
                ++_mini_block_idx;
            }
            // TODO: an optimized implementation would decode the entire
            // miniblock at once.
            int64_t delta;
            if (!_decoder.GetValue(_delta_bit_width, &delta)) {
                throw parquet_exception("Unexpected end of data in DELTA_BINARY_PACKED");
            }
            delta += _min_delta;
            _last_value += static_cast<int32_t>(delta);
            --_values_current_mini_block;
        }
        return i;
    }

    void eat_final_padding() {
        while (_values_current_mini_block > 0) {
            int64_t unused_delta;
            if (!_decoder.GetValue(_delta_bit_width, &unused_delta)) {
                throw parquet_exception("Unexpected end of data in DELTA_BINARY_PACKED");
            }
            --_values_current_mini_block;
        }
    }
};

class delta_length_byte_array_decoder final : public decoder<format::Type::BYTE_ARRAY> {
    seastar::temporary_buffer<byte> _values;
    std::vector<int32_t> _lengths;
    size_t _current_idx;
    static constexpr size_t BATCH_SIZE = 1000;
public:
    using typename decoder<format::Type::BYTE_ARRAY>::output_type;
    size_t read_batch(size_t n, output_type out[]) override {
        n = std::min(n, _lengths.size() - _current_idx);
        for (size_t i = 0; i < n; ++i) {
            uint32_t len = _lengths[_current_idx];
            if (len > _values.size()) {
                throw parquet_exception(
                        "Unexpected end of values in DELTA_LENGTH_BYTE_ARRAY");
            }
            out[i] = _values.share(0, len);
            _values.trim_front(len);
            ++_current_idx;
        }
        return n;
    }
    void reset(bytes_view data) override {
        delta_binary_packed_decoder<format::Type::INT32> _len_decoder;
        _len_decoder.reset(data);

        size_t lengths_read = 0;
        while (true) {
            _lengths.resize(lengths_read + BATCH_SIZE);
            int32_t* output = _lengths.data() + _lengths.size() - BATCH_SIZE;
            size_t n_read = _len_decoder.read_batch(BATCH_SIZE, output);
            if (n_read == 0) {
                break;
            }
            lengths_read += n_read;
        }
        _lengths.resize(lengths_read);

        size_t len_bytes = data.size() - _len_decoder.bytes_left();
        data.remove_prefix(len_bytes);
        _values = seastar::temporary_buffer<byte>(data.data(), data.size());
        _current_idx = 0;
    }
};

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
        case format::Encoding::DELTA_LENGTH_BYTE_ARRAY:
            if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
                _decoder = std::make_unique<delta_length_byte_array_decoder>();
            } else {
                throw parquet_exception::corrupted_file("DELTA_LENGTH_BYTE_ARRAY is valid only for BYTE_ARRAY");
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

template <format::Type::type ParquetType>
class plain_encoder : public value_encoder<ParquetType> {
public:
    using typename value_encoder<ParquetType>::input_type;
    using typename value_encoder<ParquetType>::flush_result;
private:
    std::vector<input_type> _buf;
public:
    bytes_view view() const {
        const byte* data = reinterpret_cast<const byte*>(_buf.data());
        size_t size = _buf.size() * sizeof(input_type);
        return {data, size};
    }
    void put_batch(const input_type data[], size_t size) override {
        _buf.insert(_buf.end(), data, data + size);
    }
    size_t max_encoded_size() const override { return view().size(); }
    flush_result flush(byte sink[]) override {
        bytes_view v = view();
        std::copy(v.begin(), v.end(), sink);
        _buf.clear();
        return {v.size(), format::Encoding::PLAIN};
    }
    std::optional<bytes_view> view_dict() override { return {}; }
    uint64_t cardinality() override { return 0; }
};

template <>
class plain_encoder<format::Type::BYTE_ARRAY>
        : public value_encoder<format::Type::BYTE_ARRAY> {
public:
    using typename value_encoder<format::Type::BYTE_ARRAY>::input_type;
    using typename value_encoder<format::Type::BYTE_ARRAY>::flush_result;
private:
    bytes _buf;
private:
    void put(const input_type& str) {
        append_raw_bytes<uint32_t>(_buf, str.size());
        _buf.insert(_buf.end(), str.begin(), str.end());
    }
public:
    bytes_view view() const {
        return {_buf.data(), _buf.size()};
    }
    void put_batch(const input_type data[], size_t size) override {
        for (size_t i = 0; i < size; ++i) {
            put(data[i]);
        }
    }
    size_t max_encoded_size() const override { return _buf.size(); }
    flush_result flush(byte sink[]) override {
        std::copy(_buf.begin(), _buf.end(), sink);
        size_t size = _buf.size();
        _buf.clear();
        return {size, format::Encoding::PLAIN};
    }
    std::optional<bytes_view> view_dict() override { return {}; }
    uint64_t cardinality() override { return 0; }
};

template <>
class plain_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>
        : public value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY> {
public:
    using typename value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>::input_type;
    using typename value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>::flush_result;
private:
    bytes _buf;
private:
    void put(const input_type& str) {
        _buf.insert(_buf.end(), str.begin(), str.end());
    }
public:
    bytes_view view() const {
        return {_buf.data(), _buf.size()};
    }
    void put_batch(const input_type data[], size_t size) override {
        for (size_t i = 0; i < size; ++i) {
            put(data[i]);
        }
    }
    size_t max_encoded_size() const override { return _buf.size(); }
    flush_result flush(byte sink[]) override {
        std::copy(_buf.begin(), _buf.end(), sink);
        size_t size = _buf.size();
        _buf.clear();
        return {size, format::Encoding::PLAIN};
    }
    std::optional<bytes_view> view_dict() override { return {}; }
    uint64_t cardinality() override { return 0; }
};

template <format::Type::type ParquetType>
class dict_builder {
public:
    using input_type = typename value_decoder_traits<ParquetType>::input_type;
private:
    std::unordered_map<input_type, uint32_t> _accumulator;
    plain_encoder<ParquetType> _dict;
public:
    uint32_t put(input_type key) {
        auto [iter, was_new_key] = _accumulator.try_emplace(key, _accumulator.size());
        if (was_new_key) {
            _dict.put_batch(&key, 1);
        }
        return iter->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return _dict.view(); }
};

template <>
class dict_builder<format::Type::BYTE_ARRAY> {
private:
    std::unordered_map<bytes, uint32_t, bytes_hasher> _accumulator;
    plain_encoder<format::Type::BYTE_ARRAY> _dict;
public:
    uint32_t put(bytes_view key) {
        auto [it, was_new_key] = _accumulator.try_emplace(bytes{key}, _accumulator.size());
        if (was_new_key) {
            _dict.put_batch(&key, 1);
        }
        return it->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return _dict.view(); }
};

template <>
class dict_builder<format::Type::FIXED_LEN_BYTE_ARRAY> {
private:
    std::unordered_map<bytes, uint32_t, bytes_hasher> _accumulator;
    plain_encoder<format::Type::FIXED_LEN_BYTE_ARRAY> _dict;
public:
    uint32_t put(bytes_view key) {
        auto [it, was_new_key] = _accumulator.try_emplace(bytes{key}, _accumulator.size());
        if (was_new_key) {
            _dict.put_batch(&key, 1);
        }
        return it->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return _dict.view(); }
};

template <format::Type::type ParquetType>
class dict_encoder : public value_encoder<ParquetType> {
private:
    std::vector<uint32_t> _indices;
    dict_builder<ParquetType> _values;
private:
    int index_bit_width() const {
        return bit_width(_values.cardinality());
    }
public:
    using typename value_encoder<ParquetType>::input_type;
    using typename value_encoder<ParquetType>::flush_result;
    void put_batch(const input_type data[], size_t size) override {
        _indices.reserve(_indices.size() + size);
        for (size_t i = 0; i < size; ++i) {
            _indices.push_back(_values.put(data[i]));
        }
    }
    size_t max_encoded_size() const override {
        return 1
                + RleEncoder::MinBufferSize(index_bit_width())
                + RleEncoder::MaxBufferSize(index_bit_width(), _indices.size());
    }
    flush_result flush(byte sink[]) override {
        *sink = static_cast<byte>(index_bit_width());
        RleEncoder encoder{sink + 1, static_cast<int>(max_encoded_size() - 1), index_bit_width()};
        for (uint32_t index : _indices) {
            encoder.Put(index);
        }
        encoder.Flush();
        _indices.clear();
        size_t size = 1 + encoder.len();
        return {size, format::Encoding::RLE_DICTIONARY};
    }
    std::optional<bytes_view> view_dict() override { return _values.view(); }
    uint64_t cardinality() override { return _values.cardinality(); }
};

// Dict encoder, but it falls back to plain encoding
// when the dict page grows too big.
template <format::Type::type ParquetType>
class dict_or_plain_encoder : public value_encoder<ParquetType> {
private:
    dict_encoder<ParquetType> _dict_encoder;
    plain_encoder<ParquetType> _plain_encoder;
    bool fallen_back = false; // Have we fallen back to plain yet?
public:
    using typename value_encoder<ParquetType>::input_type;
    using typename value_encoder<ParquetType>::flush_result;
    // Will fall back to plain encoding when dict page grows
    // beyond this threshold.
    static constexpr size_t fallback_threshold = 16 * 1024;
    void put_batch(const input_type data[], size_t size) override {
        if (fallen_back) {
            _plain_encoder.put_batch(data, size);
        } else {
            _dict_encoder.put_batch(data, size);
        }
    }
    size_t max_encoded_size() const override {
        if (fallen_back) {
            return _plain_encoder.max_encoded_size();
        } else {
            return _dict_encoder.max_encoded_size();
        }
    }
    flush_result flush(byte sink[]) override {
        if (fallen_back) {
            return _plain_encoder.flush(sink);
        } else {
            if (_dict_encoder.view_dict()->size() > fallback_threshold) {
                fallen_back = true;
            }
            return _dict_encoder.flush(sink);
        }
    }
    std::optional<bytes_view> view_dict() override {
        return _dict_encoder.view_dict();
    }
    uint64_t cardinality() override {
        return _dict_encoder.cardinality();
    }
};

template <format::Type::type ParquetType>
std::unique_ptr<value_encoder<ParquetType>>
make_value_encoder(format::Encoding::type encoding) {
    if constexpr (ParquetType == format::Type::INT96) {
        throw parquet_exception(
                "INT96 is deprecated and writes of this type are unsupported");
    }
    const auto not_implemented = [&] () {
        return parquet_exception(seastar::format(
                "Encoding type {} as {} is not implemented yet", ParquetType, encoding));
    };
    const auto invalid = [&] () {
        return parquet_exception(seastar::format(
                "Encoding {} is invalid for type {}", encoding, ParquetType));
    };
    if (encoding == format::Encoding::PLAIN) {
        return std::make_unique<plain_encoder<ParquetType>>();
    } else if (encoding == format::Encoding::PLAIN_DICTIONARY) {
        throw parquet_exception("PLAIN_DICTIONARY is deprecated. Use RLE_DICTIONARY instead");
    } else if (encoding == format::Encoding::RLE) {
        if constexpr (ParquetType == format::Type::BOOLEAN) {
            throw not_implemented();
        }
        throw invalid();
    } else if (encoding == format::Encoding::BIT_PACKED) {
        throw invalid();
    } else if (encoding == format::Encoding::DELTA_BINARY_PACKED) {
        if constexpr (ParquetType == format::Type::INT32) {
            throw not_implemented();
        }
        if constexpr (ParquetType == format::Type::INT64) {
            throw not_implemented();
        }
        throw invalid();
    } else if (encoding == format::Encoding::DELTA_LENGTH_BYTE_ARRAY) {
        if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
            throw not_implemented();
        }
        throw invalid();
    } else if (encoding == format::Encoding::DELTA_BYTE_ARRAY) {
        if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
            throw not_implemented();
        }
        throw invalid();
    } else if (encoding == format::Encoding::RLE_DICTIONARY) {
        return std::make_unique<dict_or_plain_encoder<ParquetType>>();
    } else if (encoding == format::Encoding::BYTE_STREAM_SPLIT) {
        throw not_implemented();
    }
    throw parquet_exception(seastar::format("Unknown encoding ({})", encoding));
}

template std::unique_ptr<value_encoder<format::Type::INT32>>
make_value_encoder<format::Type::INT32>(format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::INT64>>
make_value_encoder<format::Type::INT64>(format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::FLOAT>>
make_value_encoder<format::Type::FLOAT>(format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::DOUBLE>>
make_value_encoder<format::Type::DOUBLE>(format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::BOOLEAN>>
make_value_encoder<format::Type::BOOLEAN>(format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::BYTE_ARRAY>>
make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>>
make_value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>(format::Encoding::type);

} // namespace parquet4seastar
