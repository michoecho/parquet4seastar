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

#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
#include <cstdlib>
#include <numeric>
#include <array>
#include <cmath>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include "program_options.h"

using namespace parquet;

constexpr size_t BATCH_SIZE = 1000;

template <typename Reader, typename Output>
void iterateColumn(std::shared_ptr<parquet::ColumnReader> column_reader){
    Reader *reader = static_cast<Reader *>(column_reader.get());

    while (reader->HasNext()) {
        int64_t values_read;
        Output values[BATCH_SIZE];
        int16_t def_levels[BATCH_SIZE];
        int16_t rep_levels[BATCH_SIZE];

        reader->ReadBatch(BATCH_SIZE, def_levels, rep_levels, values, &values_read);
    }
}

void iterateStrings(std::shared_ptr<parquet::RowGroupReader> row_group_reader) {
    iterateColumn<ByteArrayReader, ByteArray>(row_group_reader->Column(0));
}

void iterateInt32(std::shared_ptr<parquet::RowGroupReader> row_group_reader) {
    iterateColumn<Int32Reader, int32_t>(row_group_reader->Column(0));
}

void iterateInt64(std::shared_ptr<parquet::RowGroupReader> row_group_reader) {
    iterateColumn<Int64Reader, int64_t>(row_group_reader->Column(0));
}

int main(int argc, char *argv[]) {
    parse_args(argc, argv);
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(r_config.filename, r_config.use_mmap);

    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

    int num_row_groups = file_metadata->num_row_groups();

    for (int r = 0; r < num_row_groups; ++r) {
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);

        switch (r_config.filetype) {
            case FileType::int32:
                iterateInt32(row_group_reader);
                break;
            case FileType::int64:
                iterateInt64(row_group_reader);
                break;
            case FileType::string:
                iterateStrings(row_group_reader);
                break;
        }
    }

    return 0;
}

