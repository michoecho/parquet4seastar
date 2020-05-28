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

#!/usr/bin/bash

export MAIN_DIR=$(pwd)

if [ -z ${FLAMEGRAPH_DIR} ]; then
	git clone -b master --single-branch https://github.com/brendangregg/FlameGraph.git ${MAIN_DIR}/FlameGraph
	FLAMEGRAPH_DIR=${MAIN_DIR}/FlameGraph
fi
export FLAMEGRAPH_DIR

if [ -z ${ARROW_READER} ] || [ -z ${ARROW_WRITER} ]; then
	if [ -z ${ARROW_DIR} ]; then
		git clone -b master --single-branch https://github.com/apache/arrow.git ${MAIN_DIR}/arrow
		export ARROW_DIR=${MAIN_DIR}/arrow
	fi

	cd ${MAIN_DIR}/arrow/cpp
	cmake . -D CMAKE_BUILD_TYPE=Release -D ARROW_PARQUET=ON -D ARROW_WITH_SNAPPY=ON -D ARROW_WITH_ZLIB=ON
	make -j4

	mkdir -p ${MAIN_DIR}/arrow-parquet-tools/build
	cd ${MAIN_DIR}/arrow-parquet-tools/build
	cmake .. -DCMAKE_BUILD_TYPE=Release -DARROW_DIR=${ARROW_DIR}
	make -j4

	ARROW_READER=${MAIN_DIR}/arrow-parquet-tools/build/parquet-reader
	ARROW_WRITER=${MAIN_DIR}/arrow-parquet-tools/build/parquet-writer
fi
export ARROW_READER
export ARROW_WRITER

if [ -z ${SEASTAR_READER} ] || [ -z ${SEASTAR_WRITER} ]; then
	mkdir -p ${MAIN_DIR}/seastar-parquet-tools/build
	cd ${MAIN_DIR}/seastar-parquet-tools/build
	cmake .. -DCMAKE_BUILD_TYPE=Release
	make -j4

	SEASTAR_READER=${MAIN_DIR}/seastar-parquet-tools/build/parquet-reader
	SEASTAR_WRITER=${MAIN_DIR}/seastar-parquet-tools/build/parquet-writer
fi
export SEASTAR_READER
export SEASTAR_WRITER

if [ -z ${DATA_DIR} ]; then
	DATA_DIR=${MAIN_DIR}/pq
	rm -rf ${DATA_DIR}
	mkdir -p ${DATA_DIR}
fi
export DATA_DIR

cd ${MAIN_DIR}
mkdir -p time stalls svg

${MAIN_DIR}/benchmark_writers.sh > writers.report
${MAIN_DIR}/benchmark_readers.sh > readers.report