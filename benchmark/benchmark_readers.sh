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

#!/usr/bin/bash

set -e

SEASTAR_READER=seastar-parquet-tools/build/parquet-reader
ARROW_READER=arrow-parquet-tools/build/parquet-reader

function drop_cache {
	echo 3 | sudo tee -a /proc/sys/vm/drop_caches 1>/dev/null
}

export TIMEFORMAT=%R

declare -A CASES

CASES[int32_plain_uncompressed]=int32
CASES[int32_plain_snappy]=int32
CASES[int32_dict_uncompressed]=int32
CASES[int32_dict_snappy]=int32
CASES[int64_plain_uncompressed]=int64
CASES[int64_plain_snappy]=int64
CASES[int64_dict_uncompressed]=int64
CASES[int64_dict_snappy]=int64
CASES[string8_plain_uncompressed]=string
CASES[string8_plain_snappy]=string
CASES[string8_dict_uncompressed]=string
CASES[string8_dict_snappy]=string
CASES[string80_plain_uncompressed]=string
CASES[string80_plain_snappy]=string
CASES[string80_dict_uncompressed]=string
CASES[string80_dict_snappy]=string

for CASE in ${!CASES[@]}; do
	echo $CASE
	FILENAME=pq/$CASE.parquet
	FILETYPE="${CASES[$CASE]}"
	for READER in $ARROW_READER $SEASTAR_READER; do
		drop_cache
		time ${READER} --filetype $FILETYPE --filename $FILENAME
	done
done
