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
#include <parquet4seastar/exception.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/print.hh>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TProtocolException.h>
#include <thrift/transport/TBufferTransports.h>

namespace parquet4seastar {

/* A dynamically sized buffer. Rounds up the size given in constructor to a power of 2.
 */
class buffer {
    size_t _size;
    std::unique_ptr<byte[]> _data;
    static constexpr inline uint64_t next_power_of_2(uint64_t n) {
        if (n < 2) {
            return n;
        }
        return 1ull << seastar::log2ceil(n);
    }
public:
    explicit buffer(size_t size = 0)
        : _size(next_power_of_2(size))
        , _data(new byte[_size]) {}
    byte* data() { return _data.get(); }
    size_t size() { return _size; }
};

/* The problem: we need to read a stream of objects of unknown, variable size (page headers)
 * placing each of them into contiguous memory for deserialization. Because we can only learn the size
 * of a page header after we deserialize it, we will inevitably read too much from the source stream,
 * and then we have to move the leftovers around to keep them contiguous with future reads.
 * peekable_stream takes care of that.
 */
class peekable_stream {
    seastar::input_stream<char> _source;
    buffer _buffer;
    size_t _buffer_start = 0;
    size_t _buffer_end = 0;
private:
    void ensure_space(size_t n);
    seastar::future<> read_exactly(size_t n);
public:
    explicit peekable_stream(seastar::input_stream<char>&& source)
        : _source{std::move(source)} {};

    // Assuming there is k bytes remaining in stream, view the next unconsumed min(k, n) bytes.
    seastar::future<bytes_view> peek(size_t n);
    // Consume n bytes. If there is less than n bytes in stream, throw.
    seastar::future<> advance(size_t n);
};

// Deserialize a single thrift structure. Return the number of bytes used.
template <typename DeserializedType>
uint32_t deserialize_thrift_msg(
        const byte serialized_msg[],
        uint32_t serialized_len,
        DeserializedType& deserialized_msg) {
    using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;
    uint8_t* casted_msg = reinterpret_cast<uint8_t*>(const_cast<byte*>(serialized_msg));
    auto tmem_transport = std::make_shared<ThriftBuffer>(casted_msg, serialized_len);
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
    deserialized_msg.read(tproto.get());
    uint32_t bytes_left = tmem_transport->available_read();
    return serialized_len - bytes_left;
}

class thrift_serializer {
    using thrift_buffer = apache::thrift::transport::TMemoryBuffer;
    using thrift_protocol = apache::thrift::protocol::TProtocol;
    std::shared_ptr<thrift_buffer> _transport;
    std::shared_ptr<thrift_protocol> _protocol;
public:
    thrift_serializer(size_t starting_size = 1024)
        : _transport{std::make_shared<thrift_buffer>(starting_size)}
        , _protocol{apache::thrift::protocol::TCompactProtocolFactoryT<thrift_buffer>{}.getProtocol(_transport)} {}

    template <typename DeserializedType>
    bytes_view serialize(const DeserializedType& msg) {
        _transport->resetBuffer();
        msg.write(_protocol.get());
        byte* data;
        uint32_t size;
        _transport->getBuffer(&data, &size);
        return {data, static_cast<size_t>(size)};
    }
};

// Deserialize (and consume from the stream) a single thrift structure.
// Return false if the stream is empty.
template <typename DeserializedType>
seastar::future<bool> read_thrift_from_stream(
        peekable_stream& stream,
        DeserializedType& deserialized_msg,
        size_t expected_size = 1024,
        size_t max_allowed_size = 1024 * 1024 * 16
) {
    if (expected_size > max_allowed_size) {
        return seastar::make_exception_future<bool>(parquet_exception(seastar::format(
                "Could not deserialize thrift: max allowed size of {} exceeded", max_allowed_size)));
    }
    return stream.peek(expected_size).then(
    [&stream, &deserialized_msg, expected_size, max_allowed_size] (bytes_view peek) {
        uint32_t len = peek.size();
        if (len == 0) {
            return seastar::make_ready_future<bool>(false);
        }
        try {
            len = deserialize_thrift_msg(peek.data(), len, deserialized_msg);
        } catch (const apache::thrift::transport::TTransportException& e) {
            if (e.getType() == apache::thrift::transport::TTransportException::END_OF_FILE) {
                // The serialized structure was bigger than expected. Retry with a bigger expectation.
                if (peek.size() < expected_size) {
                    throw parquet_exception(seastar::format(
                            "Could not deserialize thrift: unexpected end of stream at {}B", peek.size()));
                }
                return read_thrift_from_stream(stream, deserialized_msg, expected_size * 2, max_allowed_size);
            } else {
                throw parquet_exception(seastar::format("Could not deserialize thrift: {}", e.what()));
            }
        } catch (const std::exception& e) {
            throw parquet_exception(seastar::format("Could not deserialize thrift: {}", e.what()));
        }
        return stream.advance(len).then([] {
            return true;
        });
    });
}

} // namespace parquet4seastar
