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

struct reader_config r_config;

std::string file_type;

void parse_args(int argc, char **argv) {
  namespace po = boost::program_options;

  po::options_description desc("options");
  desc.add_options()
      ("filename,f", po::value<std::string>(&r_config.filename)->required(),
          "output filename")
      ("filetype,t", po::value<std::string>(&file_type)->required(),
          "input file's type")
      ("mmap,m", "set usage of mmap");

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc)
                .style(po::command_line_style::unix_style | po::command_line_style::allow_long_disguise)
                .run(), vm);

  if (vm.count("help")) {
    std::cout << argv[0] << " [FLAGS] <OUTPUT FILE>" << std::endl << desc << std::endl;
    exit(0);
  }

  po::notify(vm);

  r_config.use_mmap = vm.count("mmap");

  if (boost::iequals(file_type, "string")) {
    r_config.filetype = FileType::string;
  } else if (boost::iequals(file_type, "int32")) {
    r_config.filetype = FileType::int32;
  } else {
    r_config.filetype = FileType::int64;
  }
}
