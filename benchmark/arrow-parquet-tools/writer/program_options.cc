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

#include "program_options.h"
#include <iostream>
#include <boost/algorithm/string.hpp>

std::string compression_name;
std::string file_type;

struct generator_config g_config;

void parse_args(int argc, char **argv) {
  namespace po = boost::program_options;

  po::options_description desc("options");
  desc.add_options()
      ("help", "help message")
      ("filename", po::value<std::string>(&g_config.filename)->required(),
          "output filename")
      ("rows", po::value<uint64_t>(&g_config.rows)->default_value(100000),
          "number of rows of data per row group")
      ("page", po::value<uint64_t>(&g_config.page_size)->default_value(8192),
          "data page size")
      ("rowgroups", po::value<uint64_t>(&g_config.row_groups)->default_value(3),
          "number of row groups")
      ("compression", po::value<std::string>(&compression_name)->default_value("UNCOMPRESSED"),
          "compression type")
      ("filetype", po::value<std::string>(&file_type)->default_value("NUMERICAL"),
          "type of generated file's contents")
      ("plain", po::value<bool>(&g_config.plain)->default_value(false), "don't use dictionary encoding")
      ("string", po::value<uint64_t>(&g_config.string_length)->default_value(12), "String length")
      ("flba", po::value<uint64_t>(&g_config.flba_length)->default_value(16), "Fixed length byte array length");

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc)
                .style(po::command_line_style::unix_style | po::command_line_style::allow_long_disguise)
                .run(), vm);

  if (vm.count("help")) {
    std::cout << argv[0] << " [FLAGS] <OUTPUT FILE>" << std::endl << desc << std::endl;
    exit(0);
  }

  po::notify(vm);

  if (boost::iequals(file_type, "mixed")) {
      g_config.filetype = FileType::mixed;
  } else if (boost::iequals(file_type, "nested")) {
      g_config.filetype = FileType::nested;
  } else if (boost::iequals(file_type, "strings")) {
      g_config.filetype = FileType::strings;
  } else {
      g_config.filetype = FileType::numerical;
  }

  if (boost::iequals(compression_name, "snappy")) {
    g_config.compression = parquet::Compression::SNAPPY;
  } else if (boost::iequals(compression_name, "gzip")) {
    g_config.compression = parquet::Compression::GZIP;
  } else if (boost::iequals(compression_name, "brotli")) {
    g_config.compression = parquet::Compression::BROTLI;
  } else if (boost::iequals(compression_name, "zstd")) {
    g_config.compression = parquet::Compression::ZSTD;
  } else if (boost::iequals(compression_name, "lz4")) {
    g_config.compression = parquet::Compression::LZ4;
  } else if (boost::iequals(compression_name, "lzo")) {
    g_config.compression = parquet::Compression::LZO;
  } else if (boost::iequals(compression_name, "bz2")) {
    g_config.compression = parquet::Compression::BZ2;
  } else {
    g_config.compression = parquet::Compression::UNCOMPRESSED;
  }
}
