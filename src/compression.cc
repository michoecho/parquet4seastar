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

#include <parquet4seastar/compression.hh>
#include <parquet4seastar/exception.hh>
#include <snappy.h>
#include <zlib.h>
#include <brotli/encode.h>
#include <brotli/decode.h>
#include <brotli/types.h>
#include <lz4.h>
#include <zstd.h>

namespace parquet4seastar {

class uncompressed_compressor final : public compressor {
    bytes decompress(bytes_view in, bytes&& out) const override {
        if (out.size() < in.size()) {
            throw parquet_exception::corrupted_file("Uncompression buffer size too small");
        }
        out.clear();
        out.insert(out.end(), in.begin(), in.end());
        return std::move(out);
    }
    bytes compress(bytes_view in, bytes&& out) const override {
        out.clear();
        out.reserve(in.size());
        out.insert(out.end(), in.begin(), in.end());
        return std::move(out);
    }
    format::CompressionCodec::type type() const override {
        return format::CompressionCodec::UNCOMPRESSED;
    }
};

class snappy_compressor final : public compressor {
    bytes decompress(bytes_view in, bytes&& out) const override {
        size_t uncompressed_size;
        const char* in_data = reinterpret_cast<const char*>(in.data());
        if (!snappy::GetUncompressedLength(in_data, in.size(), &uncompressed_size)) {
            throw parquet_exception::corrupted_file("Corrupt snappy data");
        }
        if (out.size() < uncompressed_size) {
            throw parquet_exception::corrupted_file("Uncompression buffer size too small");
        }
        out.resize(uncompressed_size);
        char* out_data = reinterpret_cast<char*>(out.data());
        if (!snappy::RawUncompress(in_data, in.size(), out_data)) {
            throw parquet_exception("Could not decompress snappy.");
        }
        return std::move(out);
    }
    bytes compress(bytes_view in, bytes&& out) const override {
        out.resize(snappy::MaxCompressedLength(in.size()));
        const char* in_data = reinterpret_cast<const char*>(in.data());
        char* out_data = reinterpret_cast<char*>(out.data());
        size_t compressed_size;
        snappy::RawCompress(in_data, in.size(), out_data, &compressed_size);
        out.resize(compressed_size);
        return std::move(out);
    }
    format::CompressionCodec::type type() const override {
        return format::CompressionCodec::SNAPPY;
    }
};

class gzip_compressor final : public compressor {
    bytes decompress(bytes_view in, bytes&& out) const override {
        z_stream zs;
        zs.zalloc = Z_NULL;
        zs.zfree = Z_NULL;
        zs.opaque = Z_NULL;
        zs.avail_in = 0;
        zs.next_in = Z_NULL;

        // Determine if this is libz or gzip from header.
        constexpr int DETECT_CODEC = 32;
        // Maximum window size
        constexpr int WINDOW_BITS = 15;
        if (inflateInit2(&zs, DETECT_CODEC | WINDOW_BITS) != Z_OK) {
            throw parquet_exception("deflate decompression init failure");
        }

        zs.next_in = reinterpret_cast<unsigned char*>(const_cast<byte*>(in.data()));
        zs.avail_in = in.size();
        zs.next_out = reinterpret_cast<unsigned char*>(out.data());
        zs.avail_out = out.size();

        auto res = inflate(&zs, Z_FINISH);
        inflateEnd(&zs);

        if (res == Z_STREAM_END) {
            out.resize(out.size() - zs.avail_out);
        } else if (res == Z_BUF_ERROR) {
            throw parquet_exception::corrupted_file("Decompression buffer size too small");
        } else {
            throw parquet_exception("deflate decompression failure");
        }
        return std::move(out);
    }
    bytes compress(bytes_view in, bytes&& out) const override {
        z_stream zs;
        zs.zalloc = Z_NULL;
        zs.zfree = Z_NULL;
        zs.opaque = Z_NULL;
        zs.avail_in = 0;
        zs.next_in = Z_NULL;

        if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
            throw parquet_exception("deflate compression init failure");
        }

        out.resize(deflateBound(&zs, in.size()));

        zs.next_in = reinterpret_cast<unsigned char*>(const_cast<byte*>(in.data()));
        zs.avail_in = in.size();
        zs.next_out = reinterpret_cast<unsigned char*>(out.data());
        zs.avail_out = out.size();

        auto res = deflate(&zs, Z_FINISH);
        deflateEnd(&zs);

        if (res == Z_STREAM_END) {
            out.resize(out.size() - zs.avail_out);
        } else {
            throw parquet_exception("deflate compression failure");
        }
        return std::move(out);
    }
    format::CompressionCodec::type type() const override {
        return format::CompressionCodec::GZIP;
    }
};

class brotli_compressor final : public compressor {
    bytes decompress(bytes_view in, bytes&& out) const override {
        const uint8_t* in_data = reinterpret_cast<const uint8_t*>(in.data());
        uint8_t* out_data = reinterpret_cast<uint8_t*>(out.data());
        size_t out_size = out.size();
        if (BrotliDecoderDecompress(in.size(), in_data,
                &out_size, out_data) != BROTLI_DECODER_RESULT_SUCCESS) {
            throw parquet_exception("Brotli decompression error (corrupted input or output buffer too small)");
        }
        out.resize(out_size);
        return std::move(out);
    }
    bytes compress(bytes_view in, bytes&& out) const override {
        static_assert(quality >= BROTLI_MIN_QUALITY &&
            quality <= BROTLI_MAX_QUALITY,
            "Invalid parameter for quality");
        static_assert(window_size >= BROTLI_MIN_WINDOW_BITS &&
            window_size <= BROTLI_MAX_WINDOW_BITS,
            "Invalid parameter for window_size");

        size_t max_input_size = BrotliEncoderMaxCompressedSize(in.size());
        if (!max_input_size) {
            throw parquet_exception::corrupted_file("Maximal compression size for Brotli exeeded");
        }
        out.resize(max_input_size);
        const uint8_t* in_data = reinterpret_cast<const uint8_t*>(in.data());
        uint8_t* out_data = reinterpret_cast<uint8_t*>(out.data());
        size_t compressed_size;
        if (BrotliEncoderCompress(quality, window_size, compression_mode,
                in.size(), in_data, &compressed_size, out_data) != BROTLI_TRUE) {
            throw parquet_exception("Could not compress Brotli.");
        }
        out.resize(compressed_size);
        return std::move(out);
    }
    format::CompressionCodec::type type() const override {
        return format::CompressionCodec::BROTLI;
    }
private:
    // The higher the quality, the slower the compression
    static const int quality = BROTLI_DEFAULT_QUALITY;
    // The wider the window, the slower the compression
    static const int window_size = BROTLI_DEFAULT_WINDOW;
    // Mode can be used to incorporate additional information about input
    // possible values are
    // BROTLI_MODE_GENERIC - no assuptions about the input
    // BROTLI_MODE_TEXT - compression for UTF-8 text
    // BROTLI_MODE_FONT - compression for font data, used in WOFF 2.0
    static const BrotliEncoderMode compression_mode = BROTLI_DEFAULT_MODE;
};

class lz4_compressor final : public compressor {
    bytes decompress(bytes_view in, bytes&& out) const override {
        const char* in_data = reinterpret_cast<const char*>(in.data());
        char* out_data = reinterpret_cast<char*>(out.data());
        int decompressed_size = LZ4_decompress_safe(in_data, out_data, in.size(), out.size());
        if (decompressed_size <= 0) {
            throw parquet_exception("LZ4 decompression error");
        }
        out.resize(decompressed_size);
        return std::move(out);
    }
    bytes compress(bytes_view in, bytes&& out) const override {
        size_t max_input_size = LZ4_compressBound(in.size());
        if (!max_input_size) {
            throw parquet_exception::corrupted_file("Input size for LZ4 incorrect");
        }
        out.resize(max_input_size);
        const char* in_data = reinterpret_cast<const char*>(in.data());
        char* out_data = reinterpret_cast<char*>(out.data());
        size_t compressed_size = LZ4_compress_default(in_data, out_data, in.size(), out.size());
        if (compressed_size == 0) {
            throw parquet_exception("Could not compress LZ4.");
        }
        out.resize(compressed_size);
        return std::move(out);
    }
    format::CompressionCodec::type type() const override {
        return format::CompressionCodec::LZ4;
    }
};

class zstd_compressor final : public compressor {
    bytes decompress(bytes_view in, bytes&& out) const override {
        const void* in_data = reinterpret_cast<const void*>(in.data());
        void* out_data = reinterpret_cast<void*>(out.data());
        size_t decompressed_size = ZSTD_decompress(out_data, out.size(), in_data, in.size());
        if (ZSTD_isError(decompressed_size)) {
            std::ostringstream error_msg;
            error_msg << "Zstd decompression error - code " << decompressed_size;
            throw parquet_exception(error_msg.str());
        }
        out.resize(decompressed_size);
        return std::move(out);
    }
    bytes compress(bytes_view in, bytes&& out) const override {
        static_assert(compression_level >= MIN_COMPRESSION_LEVEL &&
            compression_level <= MAX_COMPRESSION_LEVEL,
            "Invalid parameter for compression_level");

        size_t max_input_size = ZSTD_compressBound(in.size());
        if (!max_input_size) {
            throw parquet_exception::corrupted_file("Input size for ZSTD incorrect");
        }
        out.resize(max_input_size);
        const char* in_data = reinterpret_cast<const char*>(in.data());
        char* out_data = reinterpret_cast<char*>(out.data());
        size_t compressed_size = ZSTD_compress(out_data, out.size(),
            in_data, in.size(), compression_level);
        if (compressed_size == 0) {
            throw parquet_exception("Could not compress ZSTD.");
        }
        out.resize(compressed_size);
        return std::move(out);
    }
    format::CompressionCodec::type type() const override {
        return format::CompressionCodec::ZSTD;
    }

private:
    // The higher the level, the slower the compression
    static const int MAX_COMPRESSION_LEVEL = 22;
    static const int MIN_COMPRESSION_LEVEL = -5;
    static const int DEFAULT_COMPRESSION_LEVEL = 3;
    static const int compression_level = DEFAULT_COMPRESSION_LEVEL;
};

std::unique_ptr<compressor> compressor::make(format::CompressionCodec::type compression) {
    if (compression == format::CompressionCodec::UNCOMPRESSED) {
        return std::make_unique<uncompressed_compressor>();
    } else if (compression == format::CompressionCodec::GZIP) {
        return std::make_unique<gzip_compressor>();
    } else if (compression == format::CompressionCodec::SNAPPY) {
        return std::make_unique<snappy_compressor>();
    } else if (compression == format::CompressionCodec::BROTLI) {
        return std::make_unique<brotli_compressor>();
    } else if (compression == format::CompressionCodec::LZ4) {
        return std::make_unique<lz4_compressor>();
    } else if (compression == format::CompressionCodec::ZSTD) {
        return std::make_unique<zstd_compressor>();
    } else {
        throw parquet_exception(seastar::format("Unsupported compression ({})", compression));
    }
}

} // namespace parquet4seastar
