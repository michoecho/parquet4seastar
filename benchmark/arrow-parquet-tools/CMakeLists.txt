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

project(arrow-parquet-tools LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include(FindPkgConfig)

find_package(Boost 1.40 COMPONENTS program_options REQUIRED)

include_directories(${ARROW_DIR}/cpp/src)
include_directories(${ARROW_DIR}/cpp/build/src)

add_executable(parquet-reader reader/main.cc reader/program_options.cc)
target_link_libraries(parquet-reader ${ARROW_DIR}/cpp/build/release/libparquet.so  ${ARROW_DIR}/cpp/build/release/libarrow.so ${Boost_LIBRARIES})

add_executable(parquet-writer writer/main.cc writer/program_options.cc)
target_link_libraries(parquet-writer ${ARROW_DIR}/cpp/build/release/libparquet.so  ${ARROW_DIR}/cpp/build/release/libarrow.so ${Boost_LIBRARIES})
set_source_files_properties(parquet-writer/parquet-writer.cc PROPERTIES COMPILE_FLAGS -Wno-error)
