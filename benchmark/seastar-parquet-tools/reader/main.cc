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

#include <parquet4seastar/file_reader.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/future-util.hh>

#include <boost/algorithm/string.hpp>
#include <boost/iterator/counting_iterator.hpp>

namespace bpo = boost::program_options;
using namespace parquet4seastar;
constexpr size_t BATCH_SIZE = 1000;

enum class FileType {
    int32,
    int64,
    string
};

template<format::Type::type T>
void readColumn(file_reader &fr, int row, int column) {
    using OutputType = typename value_decoder_traits<T>::output_type;

    auto column_reader = fr.open_column_chunk_reader<T>(row, column).get0();
    OutputType values[BATCH_SIZE];
    int16_t def_levels[BATCH_SIZE];
    int16_t rep_levels[BATCH_SIZE];
    while (column_reader.read_batch(BATCH_SIZE, def_levels, rep_levels, values).get0()) {
        // nothing
    }
}

int main(int argc, char *argv[]) {
    seastar::app_template app;
    app.add_options()
            ("filename", bpo::value<std::string>(), "Parquet file path")
            ("filetype", bpo::value<std::string>(), "File type");

    app.run(argc, argv, [&app] {
        auto &&config = app.configuration();

        FileType file_type;
        if (boost::iequals(config["filetype"].as<std::string>(), "int32")) {
            file_type = FileType::int32;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "int64")) {
            file_type = FileType::int64;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "string")) {
            file_type = FileType::string;
        }

        std::string file = config["filename"].as<std::string>();
        return seastar::async([file, file_type] {
            auto fr = file_reader::open(file).get0();
            int num_row_groups = fr.metadata().row_groups.size();

            for (int r = 0; r < num_row_groups; r++) {
                switch (file_type) {
                    case FileType::string:
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 0);
                        break;
                    case FileType::int32:
                        readColumn<format::Type::INT32>(fr, r, 0);
                        break;
                    case FileType::int64:
                        readColumn<format::Type::INT64>(fr, r, 0);
                        break;
                }
            }
            fr.close().get0();
        });
    });
    return 0;
}
