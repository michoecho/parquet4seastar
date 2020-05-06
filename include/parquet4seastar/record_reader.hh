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

#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/reader_schema.hh>
#include <limits>

namespace parquet4seastar::record {

struct field_reader;

template <typename LogicalType>
class typed_primitive_reader {
public:
    using output_type = typename column_chunk_reader<LogicalType::physical_type>::output_type;
    static constexpr int64_t DEFAULT_BATCH_SIZE = 1024;
private:
    column_chunk_reader<LogicalType::physical_type> _source;
    uint32_t _def_level;
    uint32_t _rep_level;
    std::string _name;
    LogicalType _logical_type;
    std::vector<int32_t> _rep_levels;
    std::vector<int32_t> _def_levels;
    std::vector<output_type> _values;
    size_t _levels_offset = 0;
    size_t _values_offset = 0;
    size_t _levels_buffered = 0;
    size_t _values_buffered = 0;

    struct triplet {
        int16_t def_level;
        int16_t rep_level;
        std::optional<output_type> value;
    };
public:
    explicit typed_primitive_reader(
            const reader_schema::primitive_node& node,
            column_chunk_reader<LogicalType::physical_type>&& source,
            int64_t batch_size = DEFAULT_BATCH_SIZE)
        : _source{std::move(source)}
        , _def_level{node.def_level}
        , _rep_level{node.rep_level}
        , _name{node.info.name}
        , _logical_type(std::get<LogicalType>(node.logical_type))
        , _rep_levels(batch_size)
        , _def_levels(batch_size)
        , _values(batch_size) {
        if (_def_level > static_cast<uint32_t>(std::numeric_limits<int16_t>::max())
                || _rep_level > static_cast<uint32_t>(std::numeric_limits<int16_t>::max())) {
            throw parquet_exception(seastar::format(
                    "Levels greater than {} are not supported", std::numeric_limits<int16_t>::max()));
        }
    }

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
    const std::string& name() const { return _name; };

private:
    int current_def_level();
    int current_rep_level();
    seastar::future<> refill_when_empty();
    seastar::future<std::optional<triplet>> next();
};

class struct_reader {
    std::vector<field_reader> _readers;
    uint32_t _def_level;
    uint32_t _rep_level;
    std::string _name;
public:
    explicit struct_reader(
            const reader_schema::struct_node& node,
            std::vector<field_reader>&& readers)
        : _readers(std::move(readers))
        , _def_level{node.def_level}
        , _rep_level{node.rep_level}
        , _name(node.info.name) {
    }

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
    const std::string& name() const { return _name; };
};

class list_reader {
    std::unique_ptr<field_reader> _reader;
    uint32_t _def_level;
    uint32_t _rep_level;
    std::string _name;
public:
    explicit list_reader(
            const reader_schema::list_node& node,
            std::unique_ptr<field_reader> reader)
        : _reader(std::move(reader))
        , _def_level{node.def_level}
        , _rep_level{node.rep_level}
        , _name(node.info.name) {}

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
    const std::string& name() const { return _name; };
};

class optional_reader {
    std::unique_ptr<field_reader> _reader;
    uint32_t _def_level;
    uint32_t _rep_level;
    std::string _name;
public:
    explicit optional_reader(
            const reader_schema::optional_node &node,
            std::unique_ptr<field_reader> reader)
        : _reader(std::move(reader))
        , _def_level{node.def_level}
        , _rep_level{node.rep_level}
        , _name(node.info.name) {}

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
    const std::string& name() const { return _name; };
};

class map_reader {
    std::unique_ptr<field_reader> _key_reader;
    std::unique_ptr<field_reader> _value_reader;
    uint32_t _def_level;
    uint32_t _rep_level;
    std::string _name;
public:
    explicit map_reader(
            const reader_schema::map_node& node,
            std::unique_ptr<field_reader> key_reader,
            std::unique_ptr<field_reader> value_reader)
        : _key_reader(std::move(key_reader))
        , _value_reader(std::move(value_reader))
        , _def_level{node.def_level}
        , _rep_level{node.rep_level}
        , _name(node.info.name) {}

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
    const std::string& name() const { return _name; };
private:
    template <typename Consumer>
    seastar::future<> read_pair(Consumer& c);
};

struct field_reader {
    std::variant<
            class optional_reader,
            class struct_reader,
            class list_reader,
            class map_reader,
            class typed_primitive_reader<logical_type::BOOLEAN>,
            class typed_primitive_reader<logical_type::INT32>,
            class typed_primitive_reader<logical_type::INT64>,
            class typed_primitive_reader<logical_type::INT96>,
            class typed_primitive_reader<logical_type::FLOAT>,
            class typed_primitive_reader<logical_type::DOUBLE>,
            class typed_primitive_reader<logical_type::BYTE_ARRAY>,
            class typed_primitive_reader<logical_type::FIXED_LEN_BYTE_ARRAY>,
            class typed_primitive_reader<logical_type::STRING>,
            class typed_primitive_reader<logical_type::ENUM>,
            class typed_primitive_reader<logical_type::UUID>,
            class typed_primitive_reader<logical_type::INT8>,
            class typed_primitive_reader<logical_type::INT16>,
            class typed_primitive_reader<logical_type::UINT8>,
            class typed_primitive_reader<logical_type::UINT16>,
            class typed_primitive_reader<logical_type::UINT32>,
            class typed_primitive_reader<logical_type::UINT64>,
            class typed_primitive_reader<logical_type::DECIMAL_INT32>,
            class typed_primitive_reader<logical_type::DECIMAL_INT64>,
            class typed_primitive_reader<logical_type::DECIMAL_BYTE_ARRAY>,
            class typed_primitive_reader<logical_type::DECIMAL_FIXED_LEN_BYTE_ARRAY>,
            class typed_primitive_reader<logical_type::DATE>,
            class typed_primitive_reader<logical_type::TIME_INT32>,
            class typed_primitive_reader<logical_type::TIME_INT64>,
            class typed_primitive_reader<logical_type::TIMESTAMP>,
            class typed_primitive_reader<logical_type::INTERVAL>,
            class typed_primitive_reader<logical_type::JSON>,
            class typed_primitive_reader<logical_type::BSON>,
            class typed_primitive_reader<logical_type::UNKNOWN>
    > _reader;

    const std::string& name() {
        return *std::visit([](const auto& x) {return &x.name();}, _reader);
    }
    template <typename Consumer>
    seastar::future<> read_field(Consumer& c) {
        return std::visit([&](auto& x) {return x.read_field(c);}, _reader);
    }
    seastar::future<> skip_field() {
        return std::visit([](auto& x) {return x.skip_field();}, _reader);
    }
    seastar::future<int, int> current_levels() {
        return std::visit([](auto& x) {return x.current_levels();}, _reader);
    }
    static seastar::future<field_reader>
    make(file_reader& file, const reader_schema::node& node_variant, int row_group);
};

class record_reader {
    const reader_schema::schema& _schema;
    std::vector<field_reader> _field_readers;
    explicit record_reader(
            const reader_schema::schema& schema,
            std::vector<field_reader>&& field_readers)
        : _schema(schema), _field_readers(std::move(field_readers)) {
    }
public:
    template <typename Consumer> seastar::future<> read_one(Consumer& c);
    template <typename Consumer> seastar::future<> read_all(Consumer& c);
    seastar::future<int, int> current_levels();
    static seastar::future<record_reader> make(file_reader& fr, int row_group);
};

template <typename L>
template <typename Consumer>
inline seastar::future<> typed_primitive_reader<L>::read_field(Consumer& c) {
    return next().then([this, &c] (std::optional<triplet> t) {
        if (!t) {
            throw parquet_exception("No more values buffered");
        } else if (t->value) {
            c.append_value(_logical_type, std::move(*t->value));
        }
    });
}

template <typename Consumer>
inline seastar::future<> struct_reader::read_field(Consumer& c) {
    c.start_struct();
    return seastar::do_for_each(_readers, [this, &c] (auto& child) mutable {
        c.start_field(child.name());
        return child.read_field(c);
    }).then([&c] {
        c.end_struct();
    });
}

template <typename Consumer>
inline seastar::future<> list_reader::read_field(Consumer& c) {
    c.start_list();
    return current_levels().then([this, &c] (int def, int) {
        if (def > static_cast<int>(_def_level)) {
            return _reader->read_field(c).then([this, &c] {
                return seastar::repeat([this, &c] {
                    return current_levels().then([this, &c] (int def, int rep) {
                        if (rep > static_cast<int>(_rep_level)) {
                            c.separate_list_values();
                            return _reader->read_field(c).then([] {
                                return seastar::stop_iteration::no;
                            });
                        } else {
                            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                        }
                    });
                });
            });
        } else {
            return _reader->skip_field();
        }
    }).then([&c] {
        c.end_list();
    });
}

template <typename Consumer>
inline seastar::future<> optional_reader::read_field(Consumer& c) {
    return current_levels().then([this, &c] (int def, int) {
        if (def > static_cast<int>(_def_level)) {
            return _reader->read_field(c);
        } else {
            c.append_null();
            return _reader->skip_field();
        }
    });
}

template <typename Consumer>
inline seastar::future<> map_reader::read_field(Consumer& c) {
    c.start_map();
    return current_levels().then([this, &c] (int def, int) {
        if (def > static_cast<int>(_def_level)) {
            return read_pair<Consumer>(c).then([this, &c] {
                return seastar::repeat([this, &c] {
                    return current_levels().then([this, &c] (int def, int rep) {
                        if (rep > static_cast<int>(_rep_level)) {
                            c.separate_map_values();
                            return read_pair<Consumer>(c).then([this, &c] {
                                return seastar::stop_iteration::no;
                            });
                        } else {
                            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                        }
                    });
                });
            });
        } else {
            return skip_field();
        }
    }).then([&c] {
        c.end_map();
    });
}

template <typename Consumer>
inline seastar::future<> map_reader::read_pair(Consumer& c) {
    return _key_reader->read_field(c).then([this, &c] {
        c.separate_key_value();
        return _value_reader->read_field(c);
    });
}

inline seastar::future<> struct_reader::skip_field() {
    return seastar::do_for_each(_readers.begin(), _readers.end(), [] (auto& child) {
        return child.skip_field();
    });
}

inline seastar::future<int, int> struct_reader::current_levels() {
    if (_readers.empty()) {
        return seastar::make_ready_future<int, int>(-1, -1);
    }
    return _readers[0].current_levels();
}
inline seastar::future<> list_reader::skip_field() {
    return _reader->skip_field();
}

inline seastar::future<int, int> list_reader::current_levels() {
    return _reader->current_levels();
}

inline seastar::future<> optional_reader::skip_field() {
    return _reader->skip_field();
}

inline seastar::future<int, int> optional_reader::current_levels() {
    return _reader->current_levels();
}

inline seastar::future<> map_reader::skip_field() {
    return seastar::when_all_succeed(
            _key_reader->skip_field(),
            _value_reader->skip_field());
}

inline seastar::future<int, int> map_reader::current_levels() {
    return _key_reader->current_levels();
}

inline seastar::future<int, int> record_reader::current_levels() {
    if (_field_readers.empty()) {
        return seastar::make_ready_future<int, int>(-1, -1);
    }
    return _field_readers[0].current_levels();
}


template <typename L>
inline seastar::future<> typed_primitive_reader<L>::skip_field() {
    return next().then([this] (std::optional<triplet> triplet) {
        if (!triplet) {
            throw parquet_exception("No more values buffered");
        }
    });
}

template <typename L>
inline seastar::future<int, int> typed_primitive_reader<L>::current_levels() {
    return refill_when_empty().then([this] {
        if (!_levels_buffered) {
            return seastar::make_ready_future<int, int>(-1, -1);
        } else {
            return seastar::make_ready_future<int, int>(current_def_level(), current_rep_level());
        }
    });
}

template <typename L>
inline int typed_primitive_reader<L>::current_def_level() {
    return _def_level > 0 ? _def_levels[_levels_offset] : 0;
}

template <typename L>
inline int typed_primitive_reader<L>::current_rep_level() {
    return _rep_level > 0 ? _rep_levels[_levels_offset] : 0;
}

template <typename L>
inline seastar::future<> typed_primitive_reader<L>::refill_when_empty() {
    if (_levels_offset == _levels_buffered) {
        return _source.read_batch(
                _def_levels.size(),
                _def_levels.data(),
                _rep_levels.data(),
                _values.data()
        ).then([this] (size_t levels_read) {
            _levels_buffered = levels_read;
            _values_buffered = 0;
            for (size_t i = 0; i < levels_read; ++i) {
                if (_def_levels[i] == static_cast<int>(_def_level)) {
                    ++_values_buffered;
                }
            }
            _values_offset = 0;
            _levels_offset = 0;
        }).handle_exception_type([this] (const std::exception& e){
            throw parquet_exception(seastar::format(
                        "In column {}: {}", _name, e.what()));
        });
    }
    return seastar::make_ready_future<>();
}

template <typename L>
inline seastar::future<std::optional<typename typed_primitive_reader<L>::triplet>> typed_primitive_reader<L>::next() {
    return refill_when_empty().then([this] {
        if (!_levels_buffered) {
            return std::optional<triplet>{};
        }
        int16_t def_level = current_def_level();
        int16_t rep_level = current_rep_level();
        _levels_offset++;
        bool is_null = def_level < static_cast<int>(_def_level);
        if (is_null) {
            return std::optional<triplet>{{def_level, rep_level, std::nullopt}};
        }
        if (_values_offset == _values_buffered) {
            throw parquet_exception("Value was non-null, but has not been buffered");
        }
        output_type& val = _values[_values_offset++];
        return std::optional<triplet>{{def_level, rep_level, std::move(val)}};
    });
}

template <typename Consumer>
inline seastar::future<> record_reader::read_one(Consumer& c) {
    c.start_record();
    return seastar::do_for_each(_field_readers, [this, &c] (auto& child) {
        return child.current_levels().then([this, &c, &child] (int def, int) {
            c.start_column(child.name());
            return std::visit(overloaded {
                [this, def, &c] (optional_reader& typed_child) {
                    if (def > 0) {
                        return typed_child.read_field(c);
                    } else {
                        c.append_null();
                        return typed_child.skip_field();
                    }
                },
                [this, &c] (auto& typed_child) {
                    return typed_child.read_field(c);
                }
            }, child._reader);
        });
    }).then([&c] {
        c.end_record();
    });
}

template <typename Consumer>
inline seastar::future<> record_reader::read_all(Consumer& c) {
    return seastar::repeat([this, &c] {
        return current_levels().then([this, &c] (int def, int) {
            if (def < 0) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            } else {
                return read_one<Consumer>(c).then([] { return seastar::stop_iteration::no; });
            }
        });
    });
}

} // namespace parquet4seastar::record
