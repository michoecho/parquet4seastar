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
 * Copyright (C) 2020 Scylladb, Ltd.
 */

#include <parquet4seastar/cql_reader.hh>
#include <seastar/core/app-template.hh>

namespace bpo = boost::program_options;

int main(int argc, char* argv[]) {
    using namespace parquet4seastar;
    seastar::app_template app;
    app.add_options()
        ("file", bpo::value<std::string>(), "Parquet file path")
        ("table", bpo::value<std::string>(), "CQL table name")
        ("pk", bpo::value<std::string>(), "Primary key (row number) column name");
    app.run(argc, argv, [&app] {
        auto&& config = app.configuration();
        std::string file = config["file"].as<std::string>();
        std::string table = config["table"].as<std::string>();
        std::string pk = config["pk"].as<std::string>();

        return file_reader::open(file).then(
        [table, pk] (file_reader&& fr) {
            return seastar::do_with(std::move(fr),
            [table, pk] (file_reader& fr) {
                return cql::parquet_to_cql(fr, table, pk, std::cout);
            });
        });
    });
    return 0;
}
