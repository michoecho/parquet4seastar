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

#include <seastar/core/print.hh>
#include <exception>
#include <sstream>

namespace parquet4seastar {

/*
 * These is the exception type thrown by code concerning parquet logic.
 * Other exceptions (of type std::exception) might also arise from the
 * library functions in case of other errors.
 *
 * The library provides only the basic exception safety guarantee.
 *
 * If an exception arises while reading/writing a parquet file,
 * the reader/writer shall be considered broken and it may not be used
 * in any way other than destruction, unless noted otherwise.
 */

class parquet_exception : public std::exception {
    std::string _msg;
public:
    ~parquet_exception() throw() override {}

    static parquet_exception corrupted_file(const std::string& msg) {
        return parquet_exception(seastar::format("Invalid or corrupted parquet file: {}", msg));
    }

    explicit parquet_exception(const char* msg) : _msg(msg) {}

    explicit parquet_exception(std::string msg) : _msg(std::move(msg)) {}

    const char* what() const throw() override { return _msg.c_str(); }
};

} // namespace parquet4seastar
