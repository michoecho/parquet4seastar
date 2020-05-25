// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// From Apache Impala (incubating) as of 2016-01-29

#include <bitset>
#include <parquet4seastar/parquet_types.h>
#include <parquet4seastar/bit_stream_utils.hh>
#include <parquet4seastar/exception.hh>

namespace parquet4seastar {

using BitReader = BitUtil::BitReader;

template<format::Type::type>
class DeltaBitPackDecoderConverter {};

template<>
struct DeltaBitPackDecoderConverter<format::Type::INT32> {
    using input_type = int32_t;

    inline void set_decoded_value(int32_t* buffer, size_t index, int64_t value) {
        buffer[index] = (int32_t) value;
    }
};

template<>
struct DeltaBitPackDecoderConverter<format::Type::INT64> {
    using input_type = int64_t;

    inline void set_decoded_value(int64_t* buffer, size_t index, int64_t value) {
        buffer[index] = value;
    }
};


template<format::Type::type ParquetType>
class DeltaBitPackDecoder {
    using input_type = typename DeltaBitPackDecoderConverter<ParquetType>::input_type;
private:
    BitReader bit_reader;
    bool initialized;

    // Header info
    size_t num_values;
    uint64_t num_mini_blocks;
    size_t values_per_mini_block;
    size_t values_current_mini_block;
    int64_t first_value; // should be int64_t
    bool first_value_read;

    // Per block info
    int64_t min_delta; // should be int64_t
    size_t mini_block_idx;
    uint8_t delta_bit_width;
    std::vector<uint8_t> delta_bit_widths;
    DeltaBitPackDecoderConverter<ParquetType> converter;
    std::vector<input_type> deltas_in_mini_block; // eagerly loaded deltas for a mini block
    bool use_batch;

    int64_t current_value;

    inline int64_t get_delta(size_t index) {
        return (int64_t) deltas_in_mini_block[index];
    }

    /// Initializes new mini block.
    inline void init_block() {
        if (!bit_reader.GetZigZagVlqInt(&min_delta)) {
            throw parquet_exception("Not enough data to decode 'min_delta'");
        }

        // TODO why use loop here instead of GetAligned(num_mini_blocks, widths)?
        std::vector<uint8_t> widths;
        widths.reserve(num_mini_blocks);
        for (size_t _ = 0; _ < num_mini_blocks; _++) {
            uint8_t w;
            if (!bit_reader.GetAligned(1, &w)) {
                throw parquet_exception("Not enough data to decode 'width'");
            }
            widths.push_back(w);
        }

        delta_bit_widths = widths;
        mini_block_idx = 0;
        delta_bit_width = delta_bit_widths.data()[0];
        values_current_mini_block = values_per_mini_block;
    }

    /// Loads delta into mini block
    inline void load_deltas_in_mini_block() {
        deltas_in_mini_block.clear();
        if (use_batch) {
            deltas_in_mini_block.resize(values_current_mini_block);
            size_t num_loaded = bit_reader.GetBatch(
                    (size_t) delta_bit_width,
                    deltas_in_mini_block.data(),
                    values_current_mini_block
            );
            assert(num_loaded == values_current_mini_block);
        } else {
            for (size_t _ = 0; _ < values_current_mini_block; _++) {
                // TODO: load one batch at a time similar to int32
                input_type delta;
                if (!bit_reader.GetValue((size_t) delta_bit_width, &delta)) {
                    throw parquet_exception("Not enough data to decode 'delta'");
                }
                deltas_in_mini_block.push_back(delta);
            }
        }
    }

public:
    /// Creates new delta bit packed decoder.
    DeltaBitPackDecoder():
            bit_reader(),
            initialized(false),
            num_values(0),
            num_mini_blocks(0),
            values_per_mini_block(0),
            values_current_mini_block(0),
            first_value(0),
            first_value_read(false),
            min_delta(0),
            mini_block_idx(0),
            delta_bit_width(0),
            delta_bit_widths(),
            deltas_in_mini_block(),
            use_batch(sizeof(input_type) == 4),
            current_value(0) {
    }

    void set_data(const uint8_t* data, size_t data_len) {
        bit_reader.Reset(data, data_len);
        initialized = true;

        uint64_t block_size;
        if(!bit_reader.GetVlqInt(&block_size)) {
            throw parquet_exception("Not enough data to decode 'block_size'");
        }
        if(!bit_reader.GetVlqInt(&num_mini_blocks)) {
            throw parquet_exception("Not enough data to decode 'num_mini_blocks'");
        }
        if(!bit_reader.GetVlqInt(&num_values)) {
            throw parquet_exception("Not enough data to decode 'num_values'");
        }
        if(!bit_reader.GetZigZagVlqInt(&first_value)) {
            throw parquet_exception("Not enough data to decode 'first_value'");
        }

        // Reset decoding state
        first_value_read = false;
        mini_block_idx = 0;
        delta_bit_widths.clear();
        values_current_mini_block = 0;

        values_per_mini_block = (size_t) (block_size / num_mini_blocks);
        assert(values_per_mini_block % 8 == 0);
    }

    size_t get(input_type* buffer, size_t buffer_len) {
        if(!initialized) {
            throw parquet_exception("Bit reader is not initialized");
        }
        size_t cur_num_values = std::min(buffer_len, (size_t)num_values);
        for (size_t i = 0; i < cur_num_values; i++) {
            if (!first_value_read) {
                converter.set_decoded_value(buffer, i, first_value);
                current_value = first_value;
                first_value_read = true;
                continue;
            }

            if (values_current_mini_block == 0) {
                mini_block_idx += 1;
                if (mini_block_idx < delta_bit_widths.size()) {
                    delta_bit_width = delta_bit_widths.data()[mini_block_idx];
                    values_current_mini_block = values_per_mini_block;
                } else {
                    init_block();
                }
                load_deltas_in_mini_block();
            }

            // we decrement values in current mini block, so we need to invert index for
            // delta
            input_type delta = get_delta(deltas_in_mini_block.size() - values_current_mini_block);
            // It is OK for deltas to contain "overflowed" values after encoding,
            // e.g. int64_t::MAX - int64_t::MIN, so we use `wrapping_add` to "overflow" again and
            // restore original value.
            current_value = current_value + min_delta;
            current_value = current_value + (int64_t)delta;
            converter.set_decoded_value(buffer, i, current_value);
            values_current_mini_block -= 1;
        }

        num_values -= cur_num_values;
        return num_values;
    }

    inline size_t values_left() const {
        return num_values;
    }

    inline format::Encoding::type encoding() const {
        return format::Encoding::DELTA_BINARY_PACKED;
    }
};

} // namespace parquet4seastar