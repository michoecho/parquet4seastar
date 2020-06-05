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

#include <iostream>
#include <memory>
#include <cstdlib>
#include <numeric>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/writer.h>
#include "program_options.h"

using parquet::ConvertedType;
using parquet::LogicalType;
using parquet::Repetition;
using parquet::Type;
using parquet::Encoding;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

enum class DataType {
    int32,
    int64,
    string
};

template<enum DataType>
void write_column(parquet::RowGroupWriter *rg_writer);

std::shared_ptr<GroupNode> StringSchema() {
    parquet::schema::NodeVector fields;
    fields.push_back(PrimitiveNode::Make("string", Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY));
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::shared_ptr<GroupNode> Int32Schema() {
    parquet::schema::NodeVector fields;
    fields.push_back(PrimitiveNode::Make("int32", Repetition::REQUIRED, LogicalType::Int(32, true), Type::INT32));
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::shared_ptr<GroupNode> Int64Schema() {
    parquet::schema::NodeVector fields;
    fields.push_back(PrimitiveNode::Make("int64", Repetition::REQUIRED, LogicalType::Int(64, true), Type::INT64));
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::shared_ptr<GroupNode> SetupSchema() {
    switch (g_config.filetype) {
        case FileType::int32:
            return Int32Schema();
        case FileType::int64:
            return Int64Schema();
        case FileType::string:
            return StringSchema();
    }
}

void WriteInt32(std::shared_ptr<parquet::ParquetFileWriter> file_writer) {
    int32_t batch[1024];
    for (int i = 0; i < std::size(batch); ++i) {
        batch[i] = i % 256;
    }
    for (uint64_t rg = 0; rg < g_config.row_groups; rg++) {
        parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();
        parquet::Int32Writer *int32_writer = static_cast<parquet::Int32Writer *>(rg_writer->NextColumn());
        for (int64_t k = 0; k < g_config.rows; k += std::size(batch)) {
            int32_writer->WriteBatch(std::size(batch), nullptr, nullptr, batch);
        }
        int32_writer->Close();
        rg_writer->Close();
    }
}

void WriteInt64(std::shared_ptr<parquet::ParquetFileWriter> file_writer) {
    int64_t batch[1024];
    for (int i = 0; i < std::size(batch); ++i) {
        batch[i] = i % 256;
    }
    for (uint64_t rg = 0; rg < g_config.row_groups; rg++) {
        parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();
        parquet::Int64Writer *int64_writer = static_cast<parquet::Int64Writer *>(rg_writer->NextColumn());
        for (int64_t k = 0; k < g_config.rows; k += std::size(batch)) {
            int64_writer->WriteBatch(std::size(batch), nullptr, nullptr, batch);
        }
        int64_writer->Close();
        rg_writer->Close();
    }
}

void WriteString(std::shared_ptr<parquet::ParquetFileWriter> file_writer) {
    std::basic_string<uint8_t> strings[256];
    for (int i = 0; i < std::size(strings); ++i) {
        strings[i] = std::basic_string<uint8_t>(g_config.string_length, 0);
        strings[i][0] = (uint8_t)(i % 256);
    }
    parquet::ByteArray batch[1024];
    for (int i = 0; i < std::size(batch); ++i) {
        batch[i] = parquet::ByteArray(strings[i%256].size(), strings[i%256].data());
    }
    for (uint64_t rg = 0; rg < g_config.row_groups; rg++) {
        parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();
        parquet::ByteArrayWriter *byte_array_writer = static_cast<parquet::ByteArrayWriter *>(rg_writer->NextColumn());

        for (int64_t k = 0; k < g_config.rows; k += std::size(batch)) {
            byte_array_writer->WriteBatch(std::size(batch), nullptr, nullptr, batch);
        }

        byte_array_writer->Close();
        rg_writer->Close();
    }
}

std::shared_ptr<parquet::WriterProperties> SetupWriterProperties() {
    parquet::WriterProperties::Builder builder;

    builder.compression(g_config.compression)
            ->data_pagesize(g_config.page_size)
            ->dictionary_pagesize_limit(16*1024)
            ->max_row_group_length(g_config.rows)
            ->disable_statistics()
            ->created_by("scylla");
    if (g_config.plain) {
        builder.disable_dictionary();
    } else {
        builder.enable_dictionary();
    }
    return builder.build();
}

int main(int argc, char **argv) {
    try {
        parse_args(argc, argv);

        using FileClass = ::arrow::io::FileOutputStream;
        std::shared_ptr<FileClass> out_file;
        PARQUET_ASSIGN_OR_THROW(out_file, FileClass::Open(g_config.filename));

        std::shared_ptr<GroupNode> schema = SetupSchema();
        std::shared_ptr<parquet::WriterProperties> props = SetupWriterProperties();
        std::shared_ptr<parquet::ParquetFileWriter> file_writer =
                parquet::ParquetFileWriter::Open(out_file, schema, props);

        switch (g_config.filetype) {
            case FileType::int32:
                WriteInt32(file_writer);
                break;
            case FileType::int64:
                WriteInt64(file_writer);
                break;
            case FileType::string:
                WriteString(file_writer);
                break;
        }

        file_writer->Close();

        DCHECK(out_file->Close().ok());
    } catch (const std::exception &e) {
        std::cerr << "Parquet write error: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}
