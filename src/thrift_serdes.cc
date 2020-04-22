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

#include <parquet4seastar/thrift_serdes.hh>

namespace parquet4seastar {

/* Assuming there is k bytes remaining in stream, append exactly min(k, n) bytes to the internal buffer.
 * seastar::input_stream has a read_exactly method of it's own, which does exactly what we want internally,
 * except instead of returning the k buffered bytes on eof, it discards all of it and returns an empty buffer.
 * Bummer. */
seastar::future<> peekable_stream::read_exactly(size_t n) {
    assert(_buffer.size() - _buffer_end >= n);
    if (n == 0) {
        return seastar::make_ready_future<>();
    }
    return _source.read_up_to(n).then([this, n] (seastar::temporary_buffer<char> newbuf) {
        if (newbuf.size() == 0) {
            return seastar::make_ready_future<>();
        } else {
            std::memcpy(_buffer.data() + _buffer_end, newbuf.get(), newbuf.size());
            _buffer_end += newbuf.size();
            return read_exactly(n - newbuf.size());
        }
    });
}

/* Ensure that there is at least n bytes of space after _buffer_end.
 * We want to strike a balance between rewinding the buffer and reallocating it.
 * If we are too stingy with reallocation, we might do a lot of pointless rewinding.
 * If we are too stingy with rewinding, we will allocate lots of unused memory too big a buffer.
 * Case in point: imagine that buffer.size() == 1024.
 * Then, imagine a peek(1024), advance(1), peek(1024), advance(1)... sequence.
 * If we never reallocate the buffer, we will have to move 1023 bytes every time we consume a byte.
 * If we never rewind the buffer, it will keep growing indefinitely, even though we only need 1024 contiguous
 * bytes.
 * Our strategy (rewind only when _buffer_start moves past half of buffer.size()) guarantees that
 * we will actively use at least 1/2 of allocated memory, and that any given byte is rewound at most once.
 */
void peekable_stream::ensure_space(size_t n) {
    if (_buffer.size() - _buffer_end >= n) {
        return;
    } else if (_buffer.size() > n + (_buffer_end - _buffer_start) && _buffer_start > _buffer.size() / 2) {
        // Rewind the buffer.
        std::memmove(_buffer.data(), _buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        _buffer_end -= _buffer_start;
        _buffer_start = 0;
    } else {
        // Allocate a bigger buffer and move unconsumed data into it.
        buffer b{_buffer_end + n};
        if (_buffer_end - _buffer_start > 0) {
            std::memcpy(b.data(), _buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        }
        _buffer = std::move(b);
        _buffer_end -= _buffer_start;
        _buffer_start = 0;
    }
}

// Assuming there is k bytes remaining in stream, view the next unconsumed min(k, n) bytes.
seastar::future<bytes_view> peekable_stream::peek(size_t n) {
    if (n == 0) {
        return seastar::make_ready_future<bytes_view>();
    } else if (_buffer_end - _buffer_start >= n) {
        return seastar::make_ready_future<bytes_view>(
                bytes_view{_buffer.data() +_buffer_start, n});
    } else {
        size_t bytes_needed = n - (_buffer_end - _buffer_start);
        ensure_space(bytes_needed);
        return read_exactly(bytes_needed).then([this] {
            return bytes_view(_buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        });
    }
}

// Consume n bytes. If there is less than n bytes in stream, throw.
seastar::future<> peekable_stream::advance(size_t n) {
    if (_buffer_end - _buffer_start > n) {
        _buffer_start += n;
        return seastar::make_ready_future<>();
    } else {
        size_t remaining = n - (_buffer_end - _buffer_start);
        return _source.skip(remaining).then([this] {
            _buffer_end = 0;
            _buffer_start = 0;
        });
    }
}

} // namespace parquet4seastar
