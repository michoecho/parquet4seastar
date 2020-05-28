# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Copyright (C) 2020 ScyllaDB
#

cmake_minimum_required (VERSION 3.5)

project(seastar-parquet-tools LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include(FindPkgConfig)

find_package(parquet4seastar REQUIRED)

add_executable(parquet-reader reader/main.cc)
target_link_libraries(parquet-reader parquet4seastar::parquet4seastar)

add_executable(parquet-writer writer/main.cc)
target_link_libraries(parquet-writer parquet4seastar::parquet4seastar)
