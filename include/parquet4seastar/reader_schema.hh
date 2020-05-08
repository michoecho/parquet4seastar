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

#pragma once

#include <parquet4seastar/logical_type.hh>
#include <variant>

namespace parquet4seastar::reader_schema {

struct raw_node {
    const format::SchemaElement& info;
    std::vector<raw_node> children;
    std::vector<std::string> path;
    uint32_t column_index; // Unused for non-primitive nodes
    uint32_t def_level;
    uint32_t rep_level;
};

struct raw_schema {
    raw_node root;
    std::vector<const raw_node*> leaves;
};

using node = std::variant<
    struct primitive_node,
    struct optional_node,
    struct struct_node,
    struct list_node,
    struct map_node
>;

struct node_base {
    const format::SchemaElement& info;
    std::vector<std::string> path;
    uint32_t def_level;
    uint32_t rep_level;
};

struct primitive_node : node_base {
    logical_type::logical_type logical_type;
    uint32_t column_index;
};

struct list_node : node_base {
    std::unique_ptr<node> element;
};

struct map_node : node_base {
    std::unique_ptr<node> key;
    std::unique_ptr<node> value;
};

struct struct_node : node_base {
    std::vector<node> fields;
};

struct optional_node : node_base {
    std::unique_ptr<node> child;
};

struct schema {
    const format::SchemaElement &info;
    std::vector<node> fields;
    std::vector<const primitive_node*> leaves;
};

raw_schema flat_schema_to_raw_schema(const std::vector<format::SchemaElement>& flat_schema);
schema raw_schema_to_schema(raw_schema raw_root);

} // namespace parquet4seastar::reader_schema
