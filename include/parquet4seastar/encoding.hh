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

#include <parquet4seastar/bytes.hh>
#include <parquet4seastar/thrift_serdes.hh>
#include <parquet4seastar/overloaded.hh>
#include <parquet4seastar/parquet_types.h>
#include <parquet4seastar/rle_encoding.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/bitops.hh>
#include <variant>

namespace parquet4seastar {

constexpr inline uint32_t bit_width(uint64_t max_n) {
    return (max_n == 0) ? 0 : seastar::log2floor(max_n) + 1;
}

/* There are two encodings used for definition and repetition levels: RLE and BIT_PACKED.
 * level_decoder provides a common interface to them.
 */
class level_decoder {
    std::variant<RleDecoder, BitReader> _decoder;
    uint32_t _bit_width;
    uint32_t _num_values;
    uint32_t _values_read;
    uint32_t bit_width(uint32_t max_n) {
        return (max_n == 0) ? 0 : seastar::log2floor(max_n) + 1;
    }
public:
    explicit level_decoder(uint32_t max_level) : _bit_width(bit_width(max_level)) {}

    // Set a new source of levels. V1 and V2 are for data pages V1 and V2 respectively.
    // In data pages V1 the size of levels is not specified in the metadata,
    // so reset_v1 receives a view of the full page and returns the number of bytes consumed.
    size_t reset_v1(bytes_view buffer, format::Encoding::type encoding, uint32_t num_values);
    // reset_v2 is passed a view of the levels only, because the size of levels is known in advance.
    void reset_v2(bytes_view encoded_levels, uint32_t num_values);

    // Read a batch of n levels (the last batch may be smaller than n).
    template <typename T>
    uint32_t read_batch(uint32_t n, T out[]) {
        n = std::min(n, _num_values - _values_read);
#if 0
        if (_bit_width == 0) {
            std::fill(out, out + n, 0);
#endif
            _values_read += n;
            return n;
#if 0
        }
#endif
        return std::visit(overloaded {
                [this, n, out] (BitReader& r) {
                    size_t n_read = r.GetBatch(_bit_width, out, n);
                    _values_read += n_read;
                    return n_read;
                },
                [this, n, out] (RleDecoder& r) {
                    size_t n_read = r.GetBatch(out, n);
                    _values_read += n_read;
                    return n_read;
                },
        }, _decoder);
    }
};

template<format::Type::type T>
struct value_decoder_traits;

// output_type = the c++ type which we will use to return the values in
// decoder_type = the variant of all supported decoders for a given encoding

template<> struct value_decoder_traits<format::Type::INT32> {
    using output_type = int32_t;
    using input_type = int32_t;
};

template<> struct value_decoder_traits<format::Type::INT64> {
    using output_type = int64_t;
    using input_type = int64_t;
};

template<> struct value_decoder_traits<format::Type::INT96> {
    using output_type = std::array<int32_t, 3>;
    static_assert(sizeof(output_type) == 12);
};

template<> struct value_decoder_traits<format::Type::FLOAT> {
    using output_type = float;
    using input_type = float;
};

template<> struct value_decoder_traits<format::Type::DOUBLE> {
    using output_type = double;
    using input_type = double;
};

template<> struct value_decoder_traits<format::Type::BOOLEAN> {
    using output_type = uint8_t;
    using input_type = uint8_t;
};

template<> struct value_decoder_traits<format::Type::BYTE_ARRAY> {
    using output_type = seastar::temporary_buffer<uint8_t>;
    using input_type = std::basic_string_view<uint8_t>;
};

template<> struct value_decoder_traits<format::Type::FIXED_LEN_BYTE_ARRAY> {
    using output_type = seastar::temporary_buffer<uint8_t>;
    using input_type = std::basic_string_view<uint8_t>;
};

/* Refer to the parquet documentation for the description of supported encodings:
 * https://github.com/apache/parquet-format/blob/master/Encodings.md
 * doc/parquet/Encodings.md
 */

template<format::Type::type ParquetType>
class decoder {
public:
    using output_type = typename value_decoder_traits<ParquetType>::output_type;
    // Set a new dictionary.
    virtual void reset_dict(output_type* dictionary, size_t dictionary_size) {
    }
    // Set a new source of encoded data.
    virtual void reset(bytes_view buf) = 0;
    // Read a batch of n values (the last batch may be smaller than n).
    virtual size_t read_batch(size_t n, output_type out[]) = 0;
    virtual ~decoder() = default;
};

// A uniform interface to all the various value decoders.
template<format::Type::type ParquetType>
class value_decoder {
public:
    using output_type = typename value_decoder_traits<ParquetType>::output_type;
private:
    std::unique_ptr<decoder<ParquetType>> _decoder;
    std::optional<uint32_t> _type_length;
    bool _dict_set = false;
    output_type* _dict = nullptr;
    size_t _dict_size = 0;
public:
    value_decoder(std::optional<uint32_t>(type_length))
            : _type_length(type_length) {
        if constexpr (ParquetType == format::Type::FIXED_LEN_BYTE_ARRAY) {
            if (!_type_length) {
                throw parquet_exception::corrupted_file("type_length not set for FIXED_LEN_BYTE_ARRAY");
            }
        }
    }
    // Set a new dictionary (to be used for decoding RLE_DICTIONARY) for this reader.
    void reset_dict(output_type* dictionary, size_t dictionary_size);
    // Set a new source of encoded data.
    void reset(bytes_view buf, format::Encoding::type encoding);
    // Read a batch of n values (the last batch may be smaller than n).
    size_t read_batch(size_t n, output_type out[]);
};

extern template class value_decoder<format::Type::INT32>;
extern template class value_decoder<format::Type::INT64>;
extern template class value_decoder<format::Type::INT96>;
extern template class value_decoder<format::Type::FLOAT>;
extern template class value_decoder<format::Type::DOUBLE>;
extern template class value_decoder<format::Type::BOOLEAN>;
extern template class value_decoder<format::Type::BYTE_ARRAY>;
extern template class value_decoder<format::Type::FIXED_LEN_BYTE_ARRAY>;

template <format::Type::type ParquetType>
class value_encoder {
public:
    struct flush_result {
        size_t size;
        format::Encoding::type encoding;
    };
    using input_type = typename value_decoder_traits<ParquetType>::input_type;
    virtual void put_batch(const input_type data[], size_t size) = 0;
    virtual size_t max_encoded_size() const = 0;
    virtual flush_result flush(byte sink[]) = 0;
    virtual std::optional<bytes_view> view_dict() { return {}; };
    virtual uint64_t cardinality() { return 0; }
    virtual ~value_encoder() = default;
};

template <format::Type::type ParquetType>
std::unique_ptr<value_encoder<ParquetType>>
make_value_encoder(format::Encoding::type encoding);

class rle_builder {
    size_t _buffer_offset = 0;
    bytes _buffer;
    uint32_t _bit_width;
    RleEncoder _encoder;
public:
    rle_builder(uint32_t bit_width)
            : _buffer(RleEncoder::MinBufferSize(bit_width), 0)
            , _bit_width{bit_width}
            , _encoder{_buffer.data(), static_cast<int>(_buffer.size()), static_cast<int>(_bit_width)}
    {};
    void put(uint64_t value) {
        while (!_encoder.Put(value)) {
            _encoder.Flush();
            _buffer_offset += _encoder.len();
            _buffer.resize(_buffer.size() * 2);
            _encoder = RleEncoder{
                    _buffer.data() + _buffer_offset,
                    static_cast<int>(_buffer.size() - _buffer_offset),
                    static_cast<int>(_bit_width)};
        }
    }
    template <typename T>
    void put_batch(const T data[], size_t size) {
        for (size_t i = 0; i < size; ++i) {
            put(static_cast<T>(data[i]));
        }
    }
    void clear() {
        _buffer.clear();
        _buffer.resize(RleEncoder::MinBufferSize(_bit_width));
        _buffer_offset = 0;
        _encoder = RleEncoder{_buffer.data(), static_cast<int>(_buffer.size()), static_cast<int>(_bit_width)};
    }
    bytes_view view() {
        _encoder.Flush();
        return {_buffer.data(), _buffer_offset + _encoder.len()};
    }
    size_t max_encoded_size() const { return _buffer.size(); }
};

} // namespace parquet4seastar
