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

#include <parquet4seastar/record_reader.hh>
#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/overloaded.hh>

namespace parquet4seastar::record {

seastar::future<field_reader> field_reader::make(file_reader& fr, const reader_schema::node& node_variant, int row_group) {
    return std::visit(overloaded {
        [&] (const reader_schema::primitive_node& node) -> seastar::future<field_reader> {
            return std::visit([&] (auto lt) {
                return fr.open_column_chunk_reader<lt.physical_type>(row_group, node.column_index).then(
                [&node] (column_chunk_reader<lt.physical_type> ccr) {
                    return field_reader{typed_primitive_reader<decltype(lt)>{node, std::move(ccr)}};
                });
            }, node.logical_type);
        },
        [&] (const reader_schema::list_node& node) {
            return field_reader::make(fr, *node.element, row_group).then([&node] (field_reader child) {
                return field_reader{list_reader{node, std::make_unique<field_reader>(std::move(child))}};
            });
        },
        [&] (const reader_schema::optional_node& node) {
            return field_reader::make(fr, *node.child, row_group).then([&node] (field_reader child) {
                return field_reader{optional_reader{node, std::make_unique<field_reader>(std::move(child))}};
            });
        },
        [&] (const reader_schema::map_node& node) {
            return seastar::when_all_succeed(
                    field_reader::make(fr, *node.key, row_group),
                    field_reader::make(fr, *node.value, row_group)
            ).then([&node] (field_reader key, field_reader value) {
                return field_reader{map_reader{
                        node,
                        std::make_unique<field_reader>(std::move(key)),
                        std::make_unique<field_reader>(std::move(value))}};
            });
        },
        [&] (const reader_schema::struct_node& node) {
            std::vector<seastar::future<field_reader>> field_readers;
            field_readers.reserve(node.fields.size());
            for (const reader_schema::node& child : node.fields) {
                field_readers.push_back(field_reader::make(fr, child, row_group));
            }
            return seastar::when_all_succeed(field_readers.begin(), field_readers.end()).then(
            [&node] (std::vector<field_reader> field_readers) {
                return field_reader{struct_reader{node, std::move(field_readers)}};
            });
        }
    }, node_variant);
}

seastar::future<record_reader> record_reader::make(file_reader& fr, int row_group) {
    std::vector<seastar::future<field_reader>> field_readers;
    for (const reader_schema::node& field_node : fr.schema().fields) {
        field_readers.push_back(field_reader::make(fr, field_node, row_group));
    }
    return seastar::when_all_succeed(field_readers.begin(), field_readers.end()).then(
    [&fr] (std::vector<field_reader> field_readers) {
        return record_reader{fr.schema(), std::move(field_readers)};
    });
}

} // namespace parquet4seastar::record
