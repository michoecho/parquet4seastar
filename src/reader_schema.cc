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

#include <parquet4seastar/reader_schema.hh>
#include <parquet4seastar/exception.hh>
#include <parquet4seastar/overloaded.hh>
#include <parquet4seastar/y_combinator.hh>

namespace parquet4seastar::reader_schema {

namespace {

// The schema tree is stored as a flat vector in the metadata (obtained by walking
// the tree in preorder, because Thrift doesn't support recursive structures.
// We recover the tree structure by using the num_children attribute.
raw_schema compute_shape(const std::vector<format::SchemaElement>& flat_schema) {
    size_t index = 0;
    raw_node root = y_combinator{[&] (auto&& convert) -> raw_node {
        if (index >= flat_schema.size()) {
            throw parquet_exception::corrupted_file("Could not build schema tree: unexpected end of flat schema");
        }
        const format::SchemaElement &current = flat_schema[index];
        ++index;
        if (current.__isset.num_children) {
            // Group node
            if (current.num_children < 0) {
                throw parquet_exception::corrupted_file("Could not build schema tree: negative num_children");
            }
            std::vector<raw_node> children;
            children.reserve(current.num_children);
            for (int i = 0; i < current.num_children; ++i) {
                children.push_back(convert());
            }
            return raw_node{current, std::move(children)};
        } else {
            // Primitive node
            return raw_node{current, std::vector<raw_node>()};
        }
    }}();
    return {std::move(root)};
}

// Assign the column_index to each primitive (leaf) node of the schema.
void compute_leaves(raw_schema& raw_schema) {
    y_combinator{[&] (auto&& compute, raw_node& r) -> void {
        if (r.children.empty()) {
            // Primitive node
            r.column_index = raw_schema.leaves.size();
            raw_schema.leaves.push_back(&r);
        } else {
            // Group node
            r.column_index = -1;
            for (raw_node& child : r.children) {
                compute(child);
            }
        }
    }}(raw_schema.root);
}

// Recursively compute the definition and repetition levels of every node.
void compute_levels(raw_schema& raw_schema) {
    y_combinator{[&] (auto&& compute, raw_node& r, uint32_t def, uint32_t rep) -> void {
        if (r.info.repetition_type == format::FieldRepetitionType::REPEATED) {
            ++def;
            ++rep;
        } else if (r.info.repetition_type == format::FieldRepetitionType::OPTIONAL) {
            ++def;
        }
        r.def_level = def;
        r.rep_level = rep;
        for (raw_node& child : r.children) {
            compute(child, def, rep);
        }
    }}(raw_schema.root, 0, 0);
}

void compute_path(raw_schema& raw_schema) {
    auto compute = y_combinator{[&] (auto&& compute, raw_node& r, std::vector<std::string> path) -> void {
        path.push_back(r.info.name);
        for (raw_node& child : r.children) {
            compute(child, path);
        }
        r.path = std::move(path);
    }};
    for (raw_node& child : raw_schema.root.children) {
        compute(child, std::vector<std::string>());
    }
}

node build_logical_node(const raw_node& raw_schema);

primitive_node build_primitive_node(const raw_node& r) {
    try {
        return primitive_node{{r.info, r.path, r.def_level, r.rep_level}, logical_type::read_logical_type(r.info),
                              r.column_index};
    } catch (const std::exception& e) {
        throw parquet_exception(seastar::format("Error while processing schema node {}: {}",
                r.path, e.what()));
    }
}

list_node build_list_node(const raw_node& r) {
    if (r.children.size() != 1 || r.info.repetition_type == format::FieldRepetitionType::REPEATED) {
        throw parquet_exception::corrupted_file(seastar::format("Invalid list node: {}", r.info));
    }

    const raw_node& repeated_node = r.children[0];
    if (repeated_node.info.repetition_type != format::FieldRepetitionType::REPEATED) {
        throw parquet_exception::corrupted_file(seastar::format("Invalid list element node: {}", r.info));
    }

    if ((repeated_node.children.size() != 1)
        || (repeated_node.info.name == "array")
        || (repeated_node.info.name == (r.info.name + "_tuple"))) {
        // Legacy 2-level list
        return list_node{
                {r.info, r.path, r.def_level, r.rep_level},
                std::make_unique<node>(build_logical_node(repeated_node))};
    } else {
        // Standard 3-level list
        const raw_node& element_node = repeated_node.children[0];
        return list_node{
                {r.info, r.path, r.def_level, r.rep_level},
                std::make_unique<node>(build_logical_node(element_node))};
    }
}

map_node build_map_node(const raw_node& r) {
    if (r.children.size() != 1) {
        throw parquet_exception(seastar::format("Invalid map node: {}", r.info));
    }

    const raw_node& repeated_node = r.children[0];
    if (repeated_node.children.size() != 2
        || repeated_node.info.repetition_type != format::FieldRepetitionType::REPEATED) {
        throw parquet_exception(seastar::format("Invalid map node: {}", r.info));
    }

    const raw_node& key_node = repeated_node.children[0];
    const raw_node& value_node = repeated_node.children[1];
    if (!key_node.children.empty()) {
        throw parquet_exception(seastar::format("Invalid map node: {}", r.info));
    }

    return map_node{
            {r.info, r.path, r.def_level, r.rep_level},
            std::make_unique<node>(build_logical_node(key_node)),
            std::make_unique<node>(build_logical_node(value_node))};
}

struct_node build_struct_node(const raw_node& r) {
    std::vector<node> fields;
    fields.reserve(r.children.size());
    for (const raw_node& child : r.children) {
        fields.push_back(build_logical_node(child));
    }
    return struct_node{{r.info, r.path, r.def_level, r.rep_level}, std::move(fields)};
}

enum class node_type { MAP, LIST, STRUCT, PRIMITIVE };
node_type determine_node_type(const raw_node& r) {
    if (r.children.empty()) {
        return node_type::PRIMITIVE;
    }
    if (r.info.__isset.converted_type) {
        if (r.info.converted_type == format::ConvertedType::MAP
                || r.info.converted_type == format::ConvertedType::MAP_KEY_VALUE) {
            return node_type::MAP;
        } else if (r.info.converted_type == format::ConvertedType::LIST) {
            return node_type::LIST;
        }
    }
    return node_type::STRUCT;
}

node build_logical_node(const raw_node& r) {
    auto build_unwrapped_node = [&r] () -> node {
        switch (determine_node_type(r)) {
        case node_type::MAP: return build_map_node(r);
        case node_type::LIST: return build_list_node(r);
        case node_type::STRUCT: return build_struct_node(r);
        case node_type::PRIMITIVE: return build_primitive_node(r);
        default: throw;
        }
    };

    if (r.info.repetition_type == format::FieldRepetitionType::OPTIONAL) {
        return optional_node{
                {r.info, r.path,  r.def_level - 1, r.rep_level},
                std::make_unique<node>(build_unwrapped_node())};
    } else if (r.info.repetition_type == format::FieldRepetitionType::REPEATED) {
        return list_node{
                {r.info, r.path, r.def_level - 1, r.rep_level - 1},
                std::make_unique<node>(build_unwrapped_node())};
    } else {
        return build_unwrapped_node();
    }
}

schema compute_shape(const raw_schema& raw) {
    std::vector<node> fields;
    fields.reserve(raw.root.children.size());
    for (const raw_node& child : raw.root.children) {
        fields.push_back(build_logical_node(child));
    }
    return schema{raw.root.info, std::move(fields)};
}

void compute_leaves(schema& root) {
    auto collect = y_combinator{[&](auto&& collect, const node& x_variant) -> void {
        std::visit(overloaded {
            [&] (const optional_node& x) { collect(*x.child); },
            [&] (const list_node& x) { collect(*x.element); },
            [&] (const map_node& x) {
                collect(*x.key);
                collect(*x.value);
            },
            [&] (const struct_node& x) {
                for (const node& child : x.fields) {
                    collect(child);
                }
            },
            [&] (const primitive_node& y) {
                root.leaves.push_back(&y);
            }
        }, x_variant);
    }};
    for (const node& field : root.fields) {
        collect(field);
    }
}

} // namespace

raw_schema flat_schema_to_raw_schema(const std::vector<format::SchemaElement>& flat_schema) {
    raw_schema raw_schema = compute_shape(flat_schema);
    compute_leaves(raw_schema);
    compute_levels(raw_schema);
    compute_path(raw_schema);
    return raw_schema;
}

schema raw_schema_to_schema(raw_schema raw) {
    schema root = compute_shape(raw);
    compute_leaves(root);
    return root;
}

} // namespace parquet4seastar::reader_schema
