#include <parquet4seastar/bit_stream_utils.hh>
#include <sstream>
#include <parquet4seastar/exception.hh>
#include <seastar/core/bitops.hh>
#include <parquet4seastar/parquet_types.h>

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED encoding

const size_t MAX_PAGE_HEADER_WRITER_SIZE = 32;
const size_t MAX_BIT_WRITER_SIZE = 10 * 1024 * 1024;
const size_t DEFAULT_BLOCK_SIZE = 128;
const size_t DEFAULT_NUM_MINI_BLOCKS = 4;

namespace parquet4seastar {

constexpr inline uint64_t required_bits(uint64_t max_n) {
    return (max_n == 0) ? 0 : seastar::log2floor(max_n) + 1;
}

template<format::Type::type>
class DeltaBitPackEncoderConverter {};

template<>
struct DeltaBitPackEncoderConverter<format::Type::INT32> {
    using intput_type = int32_t;

    inline int64_t as_int64_t(int32_t* values, size_t index) {
        return (int64_t) values[index];
    }

    inline int64_t subtract(int64_t left, int64_t right) {
        // It is okay for values to overflow, wrapping_sub wrapping around at the boundary
        return (int64_t) ((int32_t)left - (int32_t)right);
    }

    inline uint64_t subtract_u64(int64_t left, int64_t right) {
        // Conversion of i32 -> u32 -> u64 is to avoid non-zero left most bytes in int
        // representation
        return (uint64_t)(uint32_t)((int32_t)left - (int32_t)right);
    }
};

template<>
struct DeltaBitPackEncoderConverter<format::Type::INT64> {
    using intput_type = int64_t;

    inline int64_t as_int64_t(int64_t* values, size_t index) {
        return values[index];
    }

    inline int64_t subtract(int64_t left, int64_t right) {
        // It is okay for values to overflow, wrapping_sub wrapping around at the boundary
        return left - right;
    }

    inline uint64_t subtract_u64(int64_t left, int64_t right) {
        return (uint64_t) left - right;
    }
};

template <format::Type::type ParquetType>
class DeltaBitPackEncoder {
    /// Writes page header for blocks, this method is invoked when we are done encoding
    /// values. It is also okay to encode when no values have been provided
    void write_page_header() {
        // We ignore the result of each 'put' operation, because
        // MAX_PAGE_HEADER_WRITER_SIZE is chosen to fit all header values and
        // guarantees that writes will not fail.

        // Write the size of each block
        page_header_writer.PutVlqInt((uint64_t)block_size);
        // Write the number of mini blocks
        page_header_writer.PutVlqInt((uint64_t)num_mini_blocks);
        // Write the number of all values (including non-encoded first value)
        page_header_writer.PutVlqInt((uint64_t)total_values);
        // Write first value
        page_header_writer.PutZigZagVlqInt(first_value);
    }

    // Write current delta buffer (<= 'block size' values) into bit writer
    void flush_block_values() {
        if (values_in_block == 0) {
            return;
        }

        int64_t min_delta = std::numeric_limits<int64_t>::min();
        for (size_t i = 0; i < values_in_block; i++) {
            min_delta = std::min(min_delta, deltas[i]);
        }

        // Write min delta
        bit_writer.PutZigZagVlqInt(min_delta);

        // Create the pointer for miniblock widths array
        uint8_t* mini_block_widths = bit_writer.GetNextBytePtr(num_mini_blocks);

        for(size_t i = 0; i < num_mini_blocks; i++) {
            // Find how many values we need to encode - either block size or whatever
            // values left
            size_t n = std::min(mini_block_size, values_in_block);
            if (n == 0) {
                break; // TODO shouldn't we put zeros as mini_block_widths of remaining mini_blocks?
            }

            // Compute the max delta in current mini block
            int64_t max_delta = std::numeric_limits<int64_t>::min();
            for (size_t j = 0; j < n; j++) {
                max_delta = std::max(max_delta, deltas[i * mini_block_size + j]);
            }

            // Compute bit width to store (max_delta - min_delta)
            size_t bit_width = required_bits(converter.subtract_u64(max_delta, min_delta));
            mini_block_widths[i] = (uint8_t)bit_width;

            // Encode values in current mini block using min_delta and bit_width
            for (size_t j = 0; j<n; j++) {
                uint64_t packed_value = converter.subtract_u64(deltas[i * mini_block_size + j], min_delta);
                bit_writer.PutValue(packed_value, bit_width);
            }

            // Pad the last block (n < mini_block_size)
            for (size_t _ = n; _ < mini_block_size; _++) {
                bit_writer.PutValue(0, bit_width);
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
    BitUtil::BitWriter bit_writer;
    BitUtil::BitWriter page_header_writer;
    size_t total_values;
    int64_t first_value;
    int64_t current_value;
    size_t block_size;
    size_t mini_block_size;
    size_t num_mini_blocks;
    size_t values_in_block;
    std::vector<int64_t> deltas;
    DeltaBitPackEncoderConverter<ParquetType> converter;

public:
    DeltaBitPackEncoder(uint8_t* header_output, int header_output_len, uint8_t* output, int output_len):
            page_header_writer(header_output, header_output_len),
            bit_writer(output, output_len) {
        block_size = DEFAULT_BLOCK_SIZE; // can write fewer values than block size for last block
        num_mini_blocks = DEFAULT_NUM_MINI_BLOCKS;
        mini_block_size = block_size / num_mini_blocks;
        assert(mini_block_size % 8 == 0);
        total_values = 0;
        first_value = 0;
        current_value = 0; // current value to keep adding deltas
        values_in_block = 0; // will be at most block_size
        deltas.reserve(block_size);
    }

    //    typename DeltaBitPackEncoderConverter<ParquetType>::input_type
    void put(int64_t* values, size_t values_len) {
        if (!values_len) {
            return;
        }

        size_t idx;
        // Define values to encode, initialize state
        if (total_values == 0) {
            first_value = converter.as_int64_t(values, 0);
            current_value = first_value;
            idx = 1;
        } else {
            idx = 0;
        }

        // Add all values (including first value)
        total_values += values_len;

        // Write block
        while (idx < values_len) {
            int64_t value = converter.as_int64_t(values, idx);
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

//    size_t estimated_data_encoded_size() const {
//        return bit_writer.bytes_written()
//    }

    void flush_buffer() {
        // Write remaining values
        flush_block_values();
        // Write page header with total values
        write_page_header();

        // Flush writers
        page_header_writer.Flush();
        bit_writer.Flush();

        // Reset state
        page_header_writer.Clear();
        bit_writer.Clear();
        total_values = 0;
        first_value = 0;
        current_value = 0;
        values_in_block = 0;
    }
};

} // namespace parquet4seastar