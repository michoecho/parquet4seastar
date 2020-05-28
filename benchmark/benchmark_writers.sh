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
#DATA_DIR
#ARROW_WRITER
#PARQUET_WRITER

declare -A name_filetype_map=([numerical]=numerical [strings]=strings [nested]=nested [mixed]=mixed [numerical_gzip]=numerical [numerical_snappy]=numerical)
declare -A name_rows_map=([numerical]=1250000 [strings]=250000 [nested]=175000 [mixed]=1000000 [numerical_snappy]=1250000 [numerical_gzip]=1250000)
declare -A name_rowgroups_map=([numerical]=50 [strings]=375 [nested]=45 [mixed]=50 [numerical_snappy]=300 [numerical_gzip]=800)
declare -A name_pagesize_map=([numerical]=8192 [strings]=8192 [nested]=8192 [mixed]=8192 [numerical_snappy]=8192 [numerical_gzip]=8192)
declare -A name_compression_map=([numerical]=uncompressed [strings]=uncompressed [nested]=uncompressed [mixed]=uncompressed [numerical_snappy]=snappy [numerical_gzip]=gzip)

declare -A plainness_name_map=([true]=plain [false]=dict)

for name in numerical mixed nested strings numerical_snappy numerical_gzip
do
	for plainness in true false
	do
		export arrow_filename="${name}.arrow.${plainness_name_map[${plainness}]}.${name_compression_map[${name}]}"

		time ( perf record --call-graph dwarf -o ${arrow_filename}.perf \
			${ARROW_WRITER} --filename ${DATA_DIR}/${arrow_filename}.parquet \
			--filetype ${name_filetype_map[${name}]} --compression ${name_compression_map[${name}]} --plain ${plainness} \
			--rows ${name_rows_map[${name}]} --rowgroups ${name_rowgroups_map[${name}]} --page ${name_pagesize_map[${name}]} \
			--string 8 --flba 8 \
			1>/dev/null 2>/dev/null \
		) 2>time/writer-${arrow_filename}.time
		(perf script -i ${arrow_filename}.perf | ${FLAMEGRAPH_DIR}/stackcollapse-perf.pl | ${FLAMEGRAPH_DIR}/flamegraph.pl > svg/${arrow_filename}.svg) 1>/dev/null 2>/dev/null
		rm -f ${arrow_filename}.perf

		export seastar_filename="${name}.seastar.${plainness_name_map[${plainness}]}.${name_compression_map[${name}]}"
		time ( perf record --call-graph dwarf -o ${seastar_filename}.perf \
			${SEASTAR_WRITER} -c1 --blocked-reactor-notify-ms 1 --blocked-reactor-reports-per-minute 1000 \
			--filename ${DATA_DIR}/${seastar_filename}.parquet \
			--filetype ${name_filetype_map[${name}]} --compression ${name_compression_map[${name}]} --plain ${plainness} \
			--rows ${name_rows_map[${name}]} --rowgroups ${name_rowgroups_map[${name}]} --page ${name_pagesize_map[${name}]} \
			--string 8 --flba 8 \
			2>stalls/writer-${seastar_filename}.stalls 1>/dev/null \
		) 2>time/writer-${seastar_filename}.time
		(perf script -i ${seastar_filename}.perf | ${FLAMEGRAPH_DIR}/stackcollapse-perf.pl | ${FLAMEGRAPH_DIR}/flamegraph.pl > svg/${seastar_filename}.svg)  1>/dev/null 2>/dev/null
		rm -f ${seastar_filename}.perf

		echo ${name}-${plainness_name_map[${plainness}]}:
		echo "--------------------------------------------------------------------------------"
		echo arrow writer time: $(cat time/writer-${arrow_filename}.time)
		echo seastar writer time: $(cat time/writer-${seastar_filename}.time)
		sed '0,/\[/!d' stalls/writer-${seastar_filename}.stalls  | head --lines=-1
		echo "================================================================================"
	done
done
