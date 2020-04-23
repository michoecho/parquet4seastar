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
        if (_bit_width == 0) {
            std::fill(out, out + n, 0);
            _values_read += n;
            return n;
        }
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

template <format::Type::type ParquetType>
class plain_decoder_trivial final : public decoder<ParquetType> {
    bytes_view _buffer;
public:
    using typename decoder<ParquetType>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class plain_decoder_boolean final : public decoder<format::Type::BOOLEAN> {
    BitReader _decoder;
public:
    using typename decoder<format::Type::BOOLEAN>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class plain_decoder_byte_array final : public decoder<format::Type::BYTE_ARRAY> {
    seastar::temporary_buffer<uint8_t> _buffer;
public:
    using typename decoder<format::Type::BYTE_ARRAY>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class plain_decoder_fixed_len_byte_array final : public decoder<format::Type::FIXED_LEN_BYTE_ARRAY> {
    size_t _fixed_len;
    seastar::temporary_buffer<uint8_t> _buffer;
public:
    using typename decoder<format::Type::FIXED_LEN_BYTE_ARRAY>::output_type;
    explicit plain_decoder_fixed_len_byte_array(size_t fixed_len=0)
            : _fixed_len(fixed_len) {}
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

template <format::Type::type ParquetType>
class dict_decoder final : public decoder<ParquetType> {
public:
    using typename decoder<ParquetType>::output_type;
private:
    output_type* _dict;
    size_t _dict_size;
    RleDecoder _rle_decoder;
public:
    explicit dict_decoder(output_type dict[], size_t dict_size)
            : _dict(dict)
            , _dict_size(dict_size) {};
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class rle_decoder_boolean final : public decoder<format::Type::BOOLEAN> {
    RleDecoder _rle_decoder;
public:
    using typename decoder<format::Type::BOOLEAN>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

template <format::Type::type ParquetType>
class delta_binary_packed_decoder final : public decoder<ParquetType> {
    BitReader _decoder;
    uint32_t _values_current_block;
    uint32_t _num_mini_blocks;
    uint64_t _values_per_mini_block;
    uint64_t _values_current_mini_block;
    int32_t _min_delta;
    size_t _mini_block_idx;
    buffer _delta_bit_widths;
    int _delta_bit_width;
    int32_t _last_value;
private:
    void init_block();
public:
    using typename decoder<ParquetType>::output_type;
    size_t read_batch(size_t n, output_type out[]) override;
    void reset(bytes_view data) override;
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

} // namespace parquet4seastar
