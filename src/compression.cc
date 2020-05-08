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
        char *out_data = reinterpret_cast<char*>(out.data());
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

std::unique_ptr<compressor> compressor::make(format::CompressionCodec::type compression) {
    if (compression == format::CompressionCodec::UNCOMPRESSED) {
        return std::make_unique<uncompressed_compressor>();
    } else if (compression == format::CompressionCodec::GZIP) {
        return std::make_unique<gzip_compressor>();
    } else if (compression == format::CompressionCodec::SNAPPY) {
        return std::make_unique<snappy_compressor>();
    } else {
        throw parquet_exception(seastar::format("Unsupported compression ({})", compression));
    }
}

} // namespace parquet4seastar
