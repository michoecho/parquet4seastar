#!/usr/bin/bash

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

set -e

MAIN="$PWD"

git submodule update --depth 1

cd "$MAIN/arrow/cpp"
mkdir -p build
cd build
cmake .. -D CMAKE_BUILD_TYPE=Release -D ARROW_PARQUET=ON -D ARROW_WITH_SNAPPY=ON
make -j4

cd "$MAIN/arrow-parquet-tools"
mkdir -p build
cd build
cmake .. -D CMAKE_BUILD_TYPE=Release -D ARROW_DIR="$MAIN/arrow"
make -j4

cd "$MAIN/seastar-parquet-tools"
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j4
