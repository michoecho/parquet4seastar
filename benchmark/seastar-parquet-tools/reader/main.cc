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
    numerical,
    mixed,
    nested,
    strings
};

template<format::Type::type T>
void readColumn(file_reader &fr, int row, int column) {
    using OutputType = typename value_decoder_traits<T>::output_type;

    auto column_reader = fr.open_column_chunk_reader<T>(row, column).get0();
    OutputType values[BATCH_SIZE];
    int16_t def_levels[BATCH_SIZE];
    int16_t rep_levels[BATCH_SIZE];
    while (int64_t rows_read = column_reader.read_batch(BATCH_SIZE, def_levels, rep_levels, (OutputType *) values).get0()) { ;
    }
}

template void readColumn<format::Type::BOOLEAN>(file_reader &fr, int row, int column);
template void readColumn<format::Type::INT32>(file_reader &fr, int row, int column);
template void readColumn<format::Type::INT64>(file_reader &fr, int row, int column);
template void readColumn<format::Type::INT96>(file_reader &fr, int row, int column);
template void readColumn<format::Type::FLOAT>(file_reader &fr, int row, int column);
template void readColumn<format::Type::DOUBLE>(file_reader &fr, int row, int column);
template void readColumn<format::Type::BYTE_ARRAY>(file_reader &fr, int row, int column);
template void readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(file_reader &fr, int row, int column);

int main(int argc, char *argv[]) {
    seastar::app_template app;
    app.add_options()
            ("filename", bpo::value<std::string>(), "Parquet file path")
            ("filetype", bpo::value<std::string>(), "File type");

    app.run(argc, argv, [&app] {
        auto &&config = app.configuration();

        FileType file_type;
        if (boost::iequals(config["filetype"].as<std::string>(), "strings")) {
            file_type = FileType::strings;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "mixed")) {
            file_type = FileType::mixed;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "nested")) {
            file_type = FileType::nested;
        } else {
            file_type = FileType::numerical;
        }

        std::string file = config["filename"].as<std::string>();
        return seastar::async([file, file_type] {
            auto fr = file_reader::open(file).get0();
            int num_row_groups = fr.metadata().row_groups.size();

            for (int r = 0; r < num_row_groups; r++) {
                switch (file_type) {
                    case FileType::strings:
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 0);
                        break;
                    case FileType::mixed:
                        readColumn<format::Type::BOOLEAN>(fr, r, 0);
                        readColumn<format::Type::INT32>(fr, r, 1);
                        readColumn<format::Type::INT64>(fr, r, 2);
                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, 3);
                        break;
                    case FileType::nested:
                        readColumn<format::Type::INT64>(fr, r, 0);
                        readColumn<format::Type::INT64>(fr, r, 1);
                        readColumn<format::Type::INT64>(fr, r, 2);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 3);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 4);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 5);
                        break;
                    case FileType::numerical:
                        readColumn<format::Type::INT32>(fr, r, 0);
                        readColumn<format::Type::INT32>(fr, r, 1);
                        readColumn<format::Type::INT64>(fr, r, 2);
                        break;
                }
            }
            fr.close().get0();
        });
    });
    return 0;
}
