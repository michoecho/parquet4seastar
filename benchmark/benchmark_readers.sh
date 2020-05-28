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

#environment variables expected to be set:
export DATA_DIR
export FLAMEGRAPH_DIR
export ARROW_READER
export SEASTAR_READER

declare -A name_type_map

name_type_map[numerical]=numerical
name_type_map[mixed]=mixed
name_type_map[nested]=nested
name_type_map[strings]=strings
name_type_map[numerical_snappy]=strings
name_type_map[numerical_gzip]=strings

for name in "${!name_type_map[@]}"
do
	for kind in arrow seastar
	do
		for compression in uncompressed snappy gzip
		do
			for plainness in plain dict
			do
				export filename="${name}.${kind}.${plainness}.${compression}"
				if [ ! -f ${DATA_DIR}/${filename}.parquet ]; then
				    continue
				fi

				(echo 3 | sudo tee -a /proc/sys/vm/dropfile_caches) 1>/dev/null 2>/dev/null
				time ( perf record --call-graph dwarf -o reader-arrow-${filename}.perf \
					${ARROW_READER} --filename ${DATA_DIR}/${filename}.parquet --filetype ${name_type_map[${name}]} \
					1>/dev/null 2>/dev/null \
				) 2>time/reader-arrow-${filename}.time
				(perf script -i reader-arrow-${filename}.perf | ${FLAMEGRAPH_DIR}/stackcollapse-perf.pl | ${FLAMEGRAPH_DIR}/flamegraph.pl > svg/reader-arrow-${filename}.svg) 1>/dev/null 2>/dev/null
				rm -f reader-arrow-${filename}.perf

				(echo 3 | sudo tee -a /proc/sys/vm/drop_caches) 1>/dev/null 2>/dev/null
				time ( perf record --call-graph dwarf -o reader-seastar-${filename}.perf \
					${SEASTAR_READER} -c1 --filename ${DATA_DIR}/${filename}.parquet --filetype ${name_type_map[${name}]} \
					--blocked-reactor-notify-ms 1 --blocked-reactor-reports-per-minute 1000 2>stalls/reader-seastar-${filename}.stalls 1>/dev/null \
				) 2>time/reader-seastar-${filename}.time
				(perf script -i reader-seastar-${filename}.perf | ${FLAMEGRAPH_DIR}/stackcollapse-perf.pl | ${FLAMEGRAPH_DIR}/flamegraph.pl > svg/reader-seastar-${filename}.svg)  1>/dev/null 2>/dev/null
				rm reader-seastar-${filename}.perf

				echo ${filename}:
				echo "--------------------------------------------------------------------------------"
				echo arrow reader time: $(cat time/reader-arrow-${filename}.time)
				echo seastar reader time: $(cat time/reader-seastar-${filename}.time)
				sed '0,/\[/!d' stalls/reader-seastar-${filename}.stalls  | head --lines=-1
				echo "================================================================================"
			done
		done
	done
done
