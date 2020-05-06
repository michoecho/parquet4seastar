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

add_custom_target (tests)

function (seastar_add_test name)
  set (test_kinds SEASTAR BOOST)

  cmake_parse_arguments (parsed_args
    ""
    "WORKING_DIRECTORY;KIND"
    "RUN_ARGS;SOURCES;LIBRARIES"
    ${ARGN})

  if (NOT parsed_args_KIND)
    set (parsed_args_KIND SEASTAR)
  elseif (NOT (parsed_args_KIND IN_LIST test_kinds))
    message (FATAL_ERROR "Invalid test kind. KIND must be one of ${test_kinds}")
  endif ()

  if (parsed_args_SOURCES)
    set (libraries "${parsed_args_LIBRARIES}")
    set (args ${parsed_args_RUN_ARGS})

    if (parsed_args_KIND STREQUAL "SEASTAR")
      list (APPEND args -- -c2)
      list (APPEND libraries
        Seastar::seastar_testing
        parquet4seastar)
    elseif (parsed_args_KIND STREQUAL "BOOST")
      list (APPEND libraries
        Boost::unit_test_framework
        parquet4seastar)
    endif ()

    set (executable_target test_unit_${name})
    add_executable (${executable_target} ${parsed_args_SOURCES})
    target_link_libraries (${executable_target}
      PRIVATE ${libraries})
    target_compile_definitions (${executable_target}
      PRIVATE SEASTAR_TESTING_MAIN)
    target_include_directories (${executable_target}
      PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties (${executable_target}
      PROPERTIES
        OUTPUT_NAME ${name}_test)
    add_dependencies (tests ${executable_target})
    set (forwarded_args COMMAND ${executable_target} ${args})
  else ()
      message (FATAL_ERROR "SOURCES are required for ${parsed_args_KIND} tests")
  endif ()

  #
  # We expect `forwarded_args` to be populated correctly at this point.
  #

  set (target test_unit_${name}_run)

  if (parsed_args_WORKING_DIRECTORY)
    list (APPEND forwarded_args WORKING_DIRECTORY ${parsed_args_WORKING_DIRECTORY})
  endif ()

  add_custom_target (${target}
    ${forwarded_args}
    USES_TERMINAL)

  add_test (
    NAME ${name}
    COMMAND ${CMAKE_COMMAND} --build ${PROJECT_BINARY_DIR} --target ${target})
endfunction ()

seastar_add_test (rle_encoding
  KIND BOOST
  SOURCES rle_encoding_test.cc)

seastar_add_test (compression
  KIND BOOST
  SOURCES compression_test.cc)

seastar_add_test (thrift_serdes_test
  KIND BOOST
  SOURCES thrift_serdes_test.cc)

seastar_add_test (cql_reader_alltypes
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test_data/alltypes/
  SOURCES cql_reader_alltypes_test.cc)

seastar_add_test (cql_reader
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test_data/parquet-testing/
  SOURCES cql_reader_test.cc)

seastar_add_test (dictionary_encoder
  KIND BOOST
  SOURCES dictionary_encoder_test.cc)

seastar_add_test (column_chunk_writer
  SOURCES column_chunk_writer_test.cc)

seastar_add_test (file_writer
  SOURCES file_writer_test.cc)

seastar_add_test (delta_binary_packed
  KIND BOOST
  SOURCES delta_binary_packed_test.cc)

seastar_add_test (delta_length_byte_array
  KIND BOOST
  SOURCES delta_length_byte_array_test.cc)

seastar_add_test (delta_byte_array
  KIND BOOST
  SOURCES delta_byte_array_test.cc)

seastar_add_test (byte_stream_split
  KIND BOOST
  SOURCES byte_stream_split_test.cc)
