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
    boolean,
    double_precision,
    floating_point,
    int8,
    int16,
    int32,
    int64,
    string,
    flba_blob
};

template<enum DataType>
void write_column(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length);

template<>
void write_column<DataType::boolean>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::BoolWriter *bool_writer =
            static_cast<parquet::BoolWriter *>(rg_writer->NextColumn());
    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        bool value = k % 2 == 1;
        bool_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    bool_writer->Close();
}

template<>
void write_column<DataType::double_precision>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::DoubleWriter *double_writer =
            static_cast<parquet::DoubleWriter *>(rg_writer->NextColumn());
    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        double value = i * 1.1111111;
        double_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    double_writer->Close();
}

template<>
void write_column<DataType::floating_point>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::FloatWriter *float_writer =
            static_cast<parquet::FloatWriter *>(rg_writer->NextColumn());
    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        float value = i * 1.1111111;
        float_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    float_writer->Close();
}

template<>
void write_column<DataType::int8>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::Int32Writer *int8_writer =
            static_cast<parquet::Int32Writer *>(rg_writer->NextColumn());
    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        int32_t value = (int8_t) i;
        int8_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    int8_writer->Close();
}

template<>
void write_column<DataType::int16>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::Int32Writer *int16_writer =
            static_cast<parquet::Int32Writer *>(rg_writer->NextColumn());
    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        int32_t value = (int16_t) i;
        int16_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    int16_writer->Close();
}

template<>
void write_column<DataType::int32>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::Int32Writer *int32_writer =
            static_cast<parquet::Int32Writer *>(rg_writer->NextColumn());
    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        int32_t value = (int32_t) i;
        int32_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    int32_writer->Close();
}

template<>
void write_column<DataType::int64>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::Int64Writer *int64_writer =
            static_cast<parquet::Int64Writer *>(rg_writer->NextColumn());
    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        int64_t value = (int64_t) i;
        int64_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    int64_writer->Close();
}

template<>
void write_column<DataType::string>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::ByteArrayWriter *byte_array_writer =
            static_cast<parquet::ByteArrayWriter *>(rg_writer->NextColumn());

    std::vector<int8_t> bytes(g_config.string_length, 0);
    parquet::ByteArray value;
    value.len = g_config.string_length;
    value.ptr = reinterpret_cast<const uint8_t *>(bytes.data());

    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        bytes[0] = (int8_t) i;

        byte_array_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    byte_array_writer->Close();
}

template<>
void write_column<DataType::flba_blob>(parquet::RowGroupWriter *rg_writer, uint64_t rg, int16_t *record_defs, int16_t *record_reps, int16_t record_length) {
    parquet::FixedLenByteArrayWriter *flba_writer =
            static_cast<parquet::FixedLenByteArrayWriter *>(rg_writer->NextColumn());

    std::vector<int8_t> bytes(g_config.flba_length, 0);
    parquet::FixedLenByteArray value;
    value.ptr = reinterpret_cast<const uint8_t *>(bytes.data());

    for (int64_t k = record_length * rg * g_config.rows; k < record_length * (rg + 1) * g_config.rows; k++) {
        int64_t i = k % 256;
        bytes[0] = (int8_t) i;

        flba_writer->WriteBatch(1, record_defs + (k % record_length), record_reps + (k % record_length), &value);
    }
    flba_writer->Close();
}

std::shared_ptr<GroupNode> DremelSchema() {
    parquet::schema::NodeVector fields;

    fields.push_back(PrimitiveNode::Make("DocId",
                                         Repetition::REQUIRED, LogicalType::Int(64, true), Type::INT64));

    parquet::schema::NodeVector links;
    links.push_back(PrimitiveNode::Make("Backward",
                                        Repetition::REPEATED, LogicalType::Int(64, true), Type::INT64));
    links.push_back(PrimitiveNode::Make("Forward",
                                        Repetition::REPEATED, LogicalType::Int(64, true), Type::INT64));
    fields.push_back(GroupNode::Make("Links", Repetition::OPTIONAL, links));

    parquet::schema::NodeVector name, language;
    language.push_back(PrimitiveNode::Make("Code",
                                           Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY));
    language.push_back(PrimitiveNode::Make("Country",
                                           Repetition::OPTIONAL, LogicalType::String(), Type::BYTE_ARRAY));
    name.push_back(GroupNode::Make("Language", Repetition::REPEATED, language));
    name.push_back(PrimitiveNode::Make("Url",
                                       Repetition::OPTIONAL, LogicalType::String(), Type::BYTE_ARRAY));

    fields.push_back(GroupNode::Make("Name", Repetition::REPEATED, name));

    return std::static_pointer_cast<GroupNode>(
            GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::shared_ptr<GroupNode> StringSchema() {
    parquet::schema::NodeVector fields;

    fields.push_back(PrimitiveNode::Make("string",
                                         Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY));

    return std::static_pointer_cast<GroupNode>(
            GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::shared_ptr<GroupNode> MixedSchema() {
    parquet::schema::NodeVector fields;

    fields.push_back(PrimitiveNode::Make("bool",
                                         Repetition::REQUIRED, Type::BOOLEAN, ConvertedType::NONE));

    fields.push_back(PrimitiveNode::Make("int32",
                                         Repetition::REQUIRED, LogicalType::Int(32, true), Type::INT32));

    fields.push_back(PrimitiveNode::Make("int64",
                                         Repetition::REQUIRED, LogicalType::Int(64, true), Type::INT64));

    fields.push_back(PrimitiveNode::Make("blob",
                                         Repetition::REQUIRED, LogicalType::None(), Type::FIXED_LEN_BYTE_ARRAY,
                                         g_config.flba_length));

    return std::static_pointer_cast<GroupNode>(
            GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::shared_ptr<GroupNode> NumericalSchema() {
    parquet::schema::NodeVector fields;

    fields.push_back(PrimitiveNode::Make("int16",
                                         Repetition::REQUIRED, LogicalType::Int(16, true), Type::INT32));

    fields.push_back(PrimitiveNode::Make("int32",
                                         Repetition::REQUIRED, LogicalType::Int(32, true), Type::INT32));

    fields.push_back(PrimitiveNode::Make("int64",
                                         Repetition::REQUIRED, LogicalType::Int(64, true), Type::INT64));

    return std::static_pointer_cast<GroupNode>(
            GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::shared_ptr<GroupNode> SetupSchema() {
    switch (g_config.filetype) {
        case FileType::numerical:return NumericalSchema();
        case FileType::mixed:return MixedSchema();
        case FileType::nested:return DremelSchema();
        case FileType::strings:return StringSchema();
    }
}

void WriteStrings(std::shared_ptr<parquet::ParquetFileWriter> file_writer) {
    for (uint64_t rg = 0; rg < g_config.row_groups; rg++) {
        parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();
        write_column<DataType::string>(rg_writer, rg, nullptr, nullptr, 1);
        rg_writer->Close();
    }
}

void WriteNested(std::shared_ptr<parquet::ParquetFileWriter> file_writer) {
    for (uint64_t rg = 0; rg < g_config.row_groups; rg++) {
        parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();

        write_column<DataType::int64>(rg_writer, rg, nullptr, nullptr, 1);

        int16_t backward_defs[4] = {2, 2, 2, 2};
        int16_t backward_reps[4] = {0, 1, 1, 1};
        write_column<DataType::int64>(rg_writer, rg, &(*backward_defs), &(*backward_reps), 4);

        int16_t forward_defs[1] = {1};
        int16_t forward_reps[1] = {0};
        write_column<DataType::int64>(rg_writer, rg, &(*forward_defs), &(*forward_reps), 1);

        int16_t code_defs[4] = {2, 2, 1, 2};
        int16_t code_reps[4] = {0, 2, 1, 1};
        write_column<DataType::string>(rg_writer, rg, &(*code_defs), &(*code_reps), 4);

        int16_t country_defs[4] = {3, 2, 1, 3};
        int16_t country_reps[4] = {0, 2, 1, 1};
        write_column<DataType::string>(rg_writer, rg, &(*country_defs), &(*country_reps), 4);

        int16_t url_defs[3] = {2, 2, 1};
        int16_t url_reps[3] = {0, 1, 1};
        write_column<DataType::string>(rg_writer, rg, &(*url_defs), &(*url_reps), 3);

        rg_writer->Close();
    }
}

void WriteMixed(std::shared_ptr<parquet::ParquetFileWriter> file_writer) {
    for (uint64_t rg = 0; rg < g_config.row_groups; rg++) {
        parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();
        write_column<DataType::boolean>(rg_writer, rg, nullptr, nullptr, 1);
        write_column<DataType::int32>(rg_writer, rg, nullptr, nullptr, 1);
        write_column<DataType::int64>(rg_writer, rg, nullptr, nullptr, 1);
        write_column<DataType::flba_blob>(rg_writer, rg, nullptr, nullptr, 1);
        rg_writer->Close();
    }
}

void WriteNumerical(std::shared_ptr<parquet::ParquetFileWriter> file_writer) {
    for (uint64_t rg = 0; rg < g_config.row_groups; rg++) {
        parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();
        write_column<DataType::int16>(rg_writer, rg, nullptr, nullptr, 1);
        write_column<DataType::int32>(rg_writer, rg, nullptr, nullptr, 1);
        write_column<DataType::int64>(rg_writer, rg, nullptr, nullptr, 1);
        rg_writer->Close();
    }
}

std::shared_ptr<parquet::WriterProperties> SetupWriterProperties() {
    parquet::WriterProperties::Builder builder;

    builder.compression(g_config.compression)
            ->data_pagesize(g_config.page_size)
            ->dictionary_pagesize_limit(9223372036854775807)
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
            case FileType::numerical:WriteNumerical(file_writer);
                break;
            case FileType::mixed:WriteMixed(file_writer);
                break;
            case FileType::nested:WriteNested(file_writer);
                break;
            case FileType::strings:WriteStrings(file_writer);
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
