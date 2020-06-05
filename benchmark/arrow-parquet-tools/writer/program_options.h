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

#ifndef PARQUET_GENERATOR__PROGRAM_OPTIONS_H_
#define PARQUET_GENERATOR__PROGRAM_OPTIONS_H_

#include <string>
#include <boost/program_options.hpp>
#include <parquet/platform.h>

enum class FileType {
    int32,
    int64,
    string
};

struct generator_config {
  uint64_t rows;
  uint64_t page_size;
  uint64_t row_groups;
  std::string filename;
  parquet::Compression::type compression;
  FileType filetype;
  bool plain;
  uint64_t string_length;
  uint64_t flba_length;
};

extern struct generator_config g_config;

void parse_args(int argc, char **argv);

#endif //PARQUET_GENERATOR__PROGRAM_OPTIONS_H_
