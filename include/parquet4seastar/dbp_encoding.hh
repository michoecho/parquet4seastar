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

#include <parquet4seastar/bit_stream_utils.hh>
#include <sstream>
#include <parquet4seastar/exception.hh>
#include <seastar/core/bitops.hh>
#include <parquet4seastar/parquet_types.h>

namespace parquet4seastar {

class BitWriterVec {
private:
    BitUtil::BitWriter bit_writer;
    std::vector<uint8_t> buffer_{0};

public:
    BitWriterVec(size_t initial_size): bit_writer(buffer_.data(), buffer_.size()) {
        buffer_.resize(initial_size);
        bit_writer.update_buffer(buffer_.data(), buffer_.size());
    }

    inline void Clear() {
        bit_writer.Clear();
    }

    inline int bytes_written() const {
        return bit_writer.bytes_written();
    }

    inline uint8_t *buffer() const {
        return bit_writer.buffer();
    }

    inline int buffer_len() const {
        return bit_writer.buffer_len();
    }

    inline void PutValue(uint64_t v, int num_bits) {
        while(!bit_writer.PutValue(v, num_bits)) {
            double_buffer();
        }
    }

    template<typename T>
    inline void PutAligned(T v, int num_bytes) {
        while(!bit_writer.PutAligned(v, num_bytes)) {
            double_buffer();
        }
    }

    inline void PutVlqInt(uint32_t v) {
        while(!bit_writer.PutVlqInt(v)) {
            double_buffer();
        }
    }

    inline void PutZigZagVlqInt(int32_t v) {
        while(!bit_writer.PutZigZagVlqInt(v)) {
            double_buffer();
        }
    }

    // TODO take care of this, can be unreliable
    inline uint8_t *GetNextBytePtr(int num_bytes) {
        uint8_t* next_byte_ptr = bit_writer.GetNextBytePtr(num_bytes);
        while(!next_byte_ptr) {
            double_buffer();
            next_byte_ptr = bit_writer.GetNextBytePtr(num_bytes);
        }
        return next_byte_ptr;
    }

    inline void Flush(bool align = false) {
        bit_writer.Flush(align);
    }
private:
    inline void double_buffer() {
        buffer_.resize(buffer_.size() * 2);
        bit_writer.update_buffer(buffer_.data(), buffer_.size());
    }
};

constexpr inline uint32_t required_bits(uint32_t max_n) {
    return (max_n == 0) ? 0 : seastar::log2floor(max_n) + 1;
}

template<format::Type::type>
class DeltaBitPackEncoderConverter {};

template<>
struct DeltaBitPackEncoderConverter<format::Type::INT32> {
    using intput_type = int32_t;

    inline int32_t as_int32_t(const int32_t* values, size_t index) {
        return (int32_t) values[index];
    }

    inline int32_t subtract(int32_t left, int32_t right) {
        // It is okay for values to overflow, wrapping_sub wrapping around at the boundary
        return (int32_t) ((int32_t)left - (int32_t)right);
    }

    inline uint32_t subtract_u64(int32_t left, int32_t right) {
        // Conversion of i32 -> u32 -> u64 is to avoid non-zero left most bytes in int
        // representation
        return (uint32_t)(uint32_t)((int32_t)left - (int32_t)right);
    }
};

template<>
struct DeltaBitPackEncoderConverter<format::Type::INT64> {
    using intput_type = int32_t;

    inline int32_t as_int32_t(const int32_t* values, size_t index) {
        return values[index];
    }

    inline int32_t subtract(int32_t left, int32_t right) {
        // It is okay for values to overflow, they are wrapping around at the boundary
        return left - right;
    }

    inline uint32_t subtract_u64(int32_t left, int32_t right) {
        return (uint32_t) (left - right);
    }
};

template <format::Type::type ParquetType>
class DeltaBitPackEncoder {
    static_assert(ParquetType == format::Type::INT32 || ParquetType == format::Type::INT64);

    static const size_t MAX_PAGE_HEADER_WRITER_SIZE = 32;
    static const size_t MAX_BIT_WRITER_SIZE = 10 * 1024 * 1024;
    static const size_t INITIAL_BIT_WRITER_SIZE = 1024;
    static const size_t DEFAULT_BLOCK_SIZE = 128;
    static const size_t DEFAULT_NUM_MINI_BLOCKS = 4;

    /// Writes page header for blocks, this method is invoked when we are done encoding
    /// values. It is also okay to encode when no values have been provided
    void write_page_header() {
        // We ignore the result of each 'put' operation, because
        // MAX_PAGE_HEADER_WRITER_SIZE is chosen to fit all header values and
        // guarantees that writes will not fail.

        // Write the size of each block
        page_header_writer.PutVlqInt((uint32_t)block_size);
        // Write the number of mini blocks
        page_header_writer.PutVlqInt((uint32_t)num_mini_blocks);
        // Write the number of all values (including non-encoded first value)
        page_header_writer.PutVlqInt((uint32_t)total_values);
        // Write first value
        page_header_writer.PutZigZagVlqInt(first_value);
    }

    // Write current delta buffer (<= 'block size' values) into bit writer
    void flush_block_values() {
        if (values_in_block == 0) {
            return;
        }

        int32_t min_delta = std::numeric_limits<int32_t>::max();
        for (size_t i = 0; i < values_in_block; i++) {
            min_delta = std::min(min_delta, deltas[i]);
        }

        // Write min delta
        bit_writer.PutZigZagVlqInt(min_delta);

        // Create the pointer for miniblock widths array
        uint8_t mini_block_widths[num_mini_blocks];

        for(size_t i = 0; i < num_mini_blocks; i++) {
            // Find how many values we need to encode - either block size or whatever
            // values left
            size_t n = std::min(mini_block_size, values_in_block);
            if (n == 0) {
                break;
            }

            // Compute the max delta in current mini block
            int32_t max_delta = std::numeric_limits<int32_t>::min();
            for (size_t j = 0; j < n; j++) {
                max_delta = std::max(max_delta, deltas[i * mini_block_size + j]);
            }

            // Compute bit width to store (max_delta - min_delta)
            mini_block_widths[i] = (uint8_t) required_bits(converter.subtract_u64(max_delta, min_delta));
            bit_writer.PutAligned(mini_block_widths[i], 1);
        }

        for(size_t i = 0; i < num_mini_blocks; i++) {
            size_t n = std::min(mini_block_size, values_in_block);
            if (n == 0) {
                break; // TODO shouldn't we put zeros as mini_block_widths of remaining mini_blocks?
            }

            // Encode values in current mini block using min_delta and bit_width
            for (size_t j = 0; j<n; j++) {
                uint32_t packed_value = converter.subtract_u64(deltas[i * mini_block_size + j], min_delta);
                bit_writer.PutValue(packed_value, mini_block_widths[i]);
            }

            // Pad the last block (n < mini_block_size)
            for (size_t _ = n; _ < mini_block_size; _++) {
                bit_writer.PutValue(0, mini_block_widths[i]);
            }

            values_in_block -= n;
        }


        if (values_in_block != 0) {
            std::ostringstream msg;
            msg << "Expected 0 values in block, found " << values_in_block;
            throw parquet_exception(msg.str());
        }
    }

private:
    BitWriterVec bit_writer;
    BitWriterVec page_header_writer;
    size_t total_values;
    int32_t first_value;
    int32_t current_value;
    size_t block_size;
    size_t mini_block_size;
    size_t num_mini_blocks;
    size_t values_in_block;
    std::vector<int32_t> deltas;
    DeltaBitPackEncoderConverter<ParquetType> converter;

public:
    DeltaBitPackEncoder():
            page_header_writer(MAX_PAGE_HEADER_WRITER_SIZE),
            bit_writer(INITIAL_BIT_WRITER_SIZE) {
        block_size = DEFAULT_BLOCK_SIZE; // can write fewer values than block size for last block
        num_mini_blocks = DEFAULT_NUM_MINI_BLOCKS;
        mini_block_size = block_size / num_mini_blocks;
        assert(mini_block_size % 8 == 0);
        total_values = 0;
        first_value = 0;
        current_value = 0; // current value to keep adding deltas
        values_in_block = 0; // will be at most block_size
        deltas.resize(block_size);
    }

    //    typename DeltaBitPackEncoderConverter<ParquetType>::input_type
    void put(const int32_t* values, size_t values_len) {
        if (!values_len) {
            return;
        }

        size_t idx;
        // Define values to encode, initialize state
        if (total_values == 0) {
            first_value = converter.as_int32_t(values, 0);
            current_value = first_value;
            idx = 1;
        } else {
            idx = 0;
        }

        // Add all values (including first value)
        total_values += values_len;

        // Write block
        while (idx < values_len) {
            int32_t value = converter.as_int32_t(values, idx);
            deltas[values_in_block] = converter.subtract(value, current_value);
            current_value = value;
            idx++;
            values_in_block++;
            if (values_in_block == block_size) {
                flush_block_values();
            }
        }
    }
 
    inline format::Encoding::type encoding() const {
        return format::Encoding::DELTA_BINARY_PACKED;
    }
 
    size_t encoded_header_size() const {
        return page_header_writer.bytes_written();
    }
 
    size_t encoded_data_size() const {
        return bit_writer.bytes_written();
    }

    // TODO Are we assuming that output_buffer will fit the entire data?
    size_t flush_buffer(uint8_t* sink) {
        // Write remaining values
        flush_block_values();
        // Write page header with total values
        write_page_header();

        // Flush writers
        page_header_writer.Flush();
        bit_writer.Flush();

        std::copy(page_header_writer.buffer(), page_header_writer.buffer() + encoded_header_size(), sink);
        std::copy(bit_writer.buffer(), bit_writer.buffer() + encoded_data_size(), sink + encoded_header_size());

        // Reset state
        page_header_writer.Clear();
        bit_writer.Clear();
        total_values = 0;
        first_value = 0;
        current_value = 0;
        values_in_block = 0;

        return encoded_header_size() + encoded_data_size();
    }
};

} // namespace parquet4seastar