#!/bin/bash
set -e

time perf record -q --call-graph dwarf -o perf_data "$@"
perf script -i perf_data |
	FlameGraph/stackcollapse-perf.pl |
	FlameGraph/flamegraph.pl > flamechart.svg
rm perf_data
