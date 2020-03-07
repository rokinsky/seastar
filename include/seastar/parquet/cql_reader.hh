#pragma once

#include <boost/iterator/counting_iterator.hpp>
#include <seastar/parquet/file_reader.hh>
#include <seastar/parquet/schema.hh>
#include <ostream>

namespace parquet::cql {

struct field_reader;

template <typename format::Type::type T, typename LogicalType>
class typed_primitive_reader {
public:
    using output_type = typename value_decoder_traits<T>::output_type;
    static constexpr int64_t DEFAULT_BATCH_SIZE = 1024;
    const schema::primitive_node& _node;
    LogicalType _logical_type;
private:
    column_chunk_reader<T> _source;
    std::vector<int16_t> _rep_levels;
    std::vector<int16_t> _def_levels;
    std::vector<output_type> _values;
    int _levels_offset = 0;
    int _values_offset = 0;
    int _levels_buffered = 0;
    int _values_buffered = 0;

    struct triplet {
        int16_t def_level;
        int16_t rep_level;
        std::optional<output_type> value;
    };
public:
    explicit typed_primitive_reader(
            const schema::primitive_node& node,
            column_chunk_reader<T> source,
            int64_t batch_size = DEFAULT_BATCH_SIZE)
        : _node{node}
        , _logical_type(std::get<LogicalType>(_node.logical_type))
        , _source{std::move(source)}
        , _rep_levels(batch_size)
        , _def_levels(batch_size)
        , _values(batch_size)
        {}

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();

private:
    int current_def_level();
    int current_rep_level();
    seastar::future<> refill_when_empty();
    seastar::future<std::optional<triplet>> next();
    template <typename Consumer>
    void offer_value(output_type v);
};

class struct_reader {
    const char* _separator = "";
    std::vector<field_reader> _readers;
public:
    const schema::struct_node& _node;

    explicit struct_reader(
            const schema::struct_node& node,
            std::vector<field_reader> readers)
        : _readers(std::move(readers))
        , _node(node) {
        assert(_readers.size() > 0);
    }

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
};

class list_reader {
    std::unique_ptr<field_reader> _reader;
public:
    const schema::list_node& _node;
    explicit list_reader(
            const schema::list_node& node,
            std::unique_ptr<field_reader> reader)
        : _reader(std::move(reader))
        , _node(node) {}

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
};

class optional_reader {
    std::unique_ptr<field_reader> _reader;
public:
    const schema::optional_node &_node;
    explicit optional_reader(
            const schema::optional_node &node,
            std::unique_ptr<field_reader> reader)
        : _reader(std::move(reader))
        , _node(node) {}

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
};

class map_reader {
    std::unique_ptr<field_reader> _key_reader;
    std::unique_ptr<field_reader> _value_reader;
public:
    const schema::map_node& _node;
    explicit map_reader(
            const schema::map_node& node,
            std::unique_ptr<field_reader> key_reader,
            std::unique_ptr<field_reader> value_reader)
        : _key_reader(std::move(key_reader))
        , _value_reader(std::move(value_reader))
        , _node(node) {}

    template <typename Consumer>
    seastar::future<> read_field(Consumer& c);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
private:
    template <typename Consumer>
    seastar::future<> read_pair(Consumer& c);
};

extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT8>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT16>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT32>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT8>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT16>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT32>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::DATE>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::TIME>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::DECIMAL>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UNKNOWN>;
extern template class typed_primitive_reader<format::Type::INT32, schema::logical_type::NONE>;
extern template class typed_primitive_reader<format::Type::INT64, schema::logical_type::INT64>;
extern template class typed_primitive_reader<format::Type::INT64, schema::logical_type::UINT64>;
extern template class typed_primitive_reader<format::Type::INT64, schema::logical_type::TIME>;
extern template class typed_primitive_reader<format::Type::INT64, schema::logical_type::TIMESTAMP>;
extern template class typed_primitive_reader<format::Type::INT64, schema::logical_type::DECIMAL>;
extern template class typed_primitive_reader<format::Type::INT64, schema::logical_type::NONE>;
extern template class typed_primitive_reader<format::Type::INT96, schema::logical_type::NONE>;
extern template class typed_primitive_reader<format::Type::FLOAT, schema::logical_type::NONE>;
extern template class typed_primitive_reader<format::Type::DOUBLE, schema::logical_type::NONE>;
extern template class typed_primitive_reader<format::Type::BOOLEAN, schema::logical_type::NONE>;
extern template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::STRING>;
extern template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::ENUM>;
extern template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::DECIMAL>;
extern template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::JSON>;
extern template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::BSON>;
extern template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::NONE>;
extern template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::STRING>;
extern template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::UUID>;
extern template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::INTERVAL>;
extern template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::NONE>;

struct field_reader {
    std::variant<
            class optional_reader,
            class struct_reader,
            class list_reader,
            class map_reader,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT8>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT16>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT32>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT8>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT16>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT32>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::DATE>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::TIME>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::DECIMAL>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::UNKNOWN>,
            class typed_primitive_reader<format::Type::INT32, schema::logical_type::NONE>,
            class typed_primitive_reader<format::Type::INT64, schema::logical_type::INT64>,
            class typed_primitive_reader<format::Type::INT64, schema::logical_type::UINT64>,
            class typed_primitive_reader<format::Type::INT64, schema::logical_type::TIME>,
            class typed_primitive_reader<format::Type::INT64, schema::logical_type::TIMESTAMP>,
            class typed_primitive_reader<format::Type::INT64, schema::logical_type::DECIMAL>,
            class typed_primitive_reader<format::Type::INT64, schema::logical_type::NONE>,
            class typed_primitive_reader<format::Type::INT96, schema::logical_type::NONE>,
            class typed_primitive_reader<format::Type::FLOAT, schema::logical_type::NONE>,
            class typed_primitive_reader<format::Type::DOUBLE, schema::logical_type::NONE>,
            class typed_primitive_reader<format::Type::BOOLEAN, schema::logical_type::NONE>,
            class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::STRING>,
            class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::ENUM>,
            class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::DECIMAL>,
            class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::JSON>,
            class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::BSON>,
            class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::NONE>,
            class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::STRING>,
            class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::UUID>,
            class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::INTERVAL>,
            class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::NONE>
    > _reader;

    const std::string& name() {
        return *std::visit([](const auto& x) {return &x._node.info.name;}, _reader);
    }
    template <typename Consumer>
    seastar::future<> read_field(Consumer& c) {
        return std::visit([&](auto& x) {return x.template read_field<Consumer>(c);}, _reader);
    }
      seastar::future<> skip_field() {
        return std::visit([](auto& x) {return x.skip_field();}, _reader);
    }
      seastar::future<int, int> current_levels() {
        return std::visit([](auto& x) {return x.current_levels();}, _reader);
    }
    static seastar::future<field_reader>
    make(const file_reader& file, const schema::node& node_variant, int row_group_index);
};

class record_reader {
    const schema::schema& _schema;
    std::vector<field_reader> _field_readers;
    const char* _separator = "";
    explicit record_reader(
            const schema::schema& schema,
            std::vector<field_reader> field_readers)
        : _schema(schema), _field_readers(std::move(field_readers)) {
        assert(_field_readers.size() > 0);
    }
public:
    template <typename Consumer> seastar::future<> read_one(Consumer& c);
    template <typename Consumer> seastar::future<> read_all(Consumer& c);
    seastar::future<int, int> current_levels();
    static seastar::future<record_reader> make(const file_reader& fr, int row_group_index);
};

template <typename format::Type::type T, typename L>
template <typename Consumer>
seastar::future<> typed_primitive_reader<T, L>::read_field(Consumer& c) {
    return next().then([this, &c] (std::optional<triplet> t) {
        if (!t) {
            throw parquet_exception("No more values buffered");
        } else if (t->value) {
            c.append_value(_logical_type, std::move(*t->value));
        }
    });
}

template <typename Consumer>
seastar::future<> struct_reader::read_field(Consumer& c) {
    c.start_struct();
    return seastar::do_for_each(_readers, [this, &c] (auto& child) mutable {
        c.start_field(child.name());
        return child.template read_field<Consumer>(c);
    }).then([&c] {
        c.end_struct();
    });
}

template <typename Consumer>
seastar::future<> list_reader::read_field(Consumer& c) {
    c.start_list();
    return current_levels().then([this, &c] (int def, int) {
        if (def > _node.def_level) {
            return _reader->read_field<Consumer>(c).then([this, &c] {
                return seastar::repeat([this, &c] {
                    return current_levels().then([this, &c] (int def, int rep) {
                        if (rep > _node.rep_level) {
                            c.separate_list_values();
                            return _reader->read_field<Consumer>(c).then([] {
                                return seastar::stop_iteration::no;
                            });
                        } else {
                            return seastar::make_ready_future<seastar::stop_iteration>(
                                    seastar::stop_iteration::yes);
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
        if (def > _node.def_level) {
            return _reader->read_field<Consumer>(c);
        } else {
            c.append_null();
            return _reader->skip_field();
        }
    });
}

template <typename Consumer>
seastar::future<> map_reader::read_field(Consumer& c) {
    c.start_map();
    return current_levels().then([this, &c] (int def, int) {
        if (def > _node.def_level) {
            return read_pair<Consumer>(c).then([this, &c] {
                return seastar::repeat([this, &c] {
                    return current_levels().then([this, &c] (int def, int rep) {
                        if (rep > _node.rep_level) {
                            c.separate_map_values();
                            return read_pair<Consumer>(c).then([this, &c] {
                                return seastar::stop_iteration::no;
                            });
                        } else {
                            return seastar::make_ready_future<seastar::stop_iteration>(
                                    seastar::stop_iteration::yes);
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
seastar::future<> map_reader::read_pair(Consumer& c) {
    return _key_reader->read_field<Consumer>(c).then([this, &c] {
        c.separate_key_value();
        return _value_reader->read_field<Consumer>(c);
    });
}

template <typename Consumer>
seastar::future<> record_reader::read_one(Consumer& c) {
    c.start_record();
    return seastar::do_for_each(_field_readers, [this, &c] (auto& child) {
        return child.current_levels().then([this, &c, &child] (int def, int) {
            c.start_column(child.name());
            return std::visit(overloaded {
                [this, def, &c] (optional_reader& typed_child) {
                    if (def > 0) {
                        return typed_child.read_field<Consumer>(c);
                    } else {
                        c.append_null();
                        return typed_child.skip_field();
                    }
                },
                [this, &c] (auto& typed_child) {
                    return typed_child.template read_field<Consumer>(c);
                }
            }, child._reader);
        });
    }).then([&c] {
        c.end_record();
    });
}

template <typename Consumer>
seastar::future<> record_reader::read_all(Consumer& c) {
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

class cql_consumer {
    const char* _separator = "";
    std::ostream& _out;
public:
    explicit cql_consumer(std::ostream& out) : _out(out) {}
    void start_record() {
        _out << '{';
        _separator = "";
    }
    void end_record() {
        _out << "}\n";
        _separator = ", ";
    }
    void start_column(const std::string& s) {
        _out << _separator << s << ": ";
        _separator = ", ";
    }
    void start_struct() {
        _out << '{';
        _separator = "";
    }
    void end_struct() {
        _out << '}';
        _separator = ", ";
    }
    void start_field(const std::string& s) {
        _out  << _separator << '"' << s << '"' << ": ";
        _separator = ", ";
    }
    void start_list() {
        _out << '[';
    }
    void end_list() {
        _out << ']';
    }
    void start_map() {
        _out << '{';
    }
    void end_map() {
        _out << '}';
    }
    void separate_key_value() {
        _out << ": ";
    }
    void separate_list_values() {
        _out << ", ";
    }
    void separate_map_values() {
        _out << ", ";
    }
    void append_null() {
        _out << "null";
    }
    void append_value(schema::logical_type::STRING, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
    void append_value(schema::logical_type::ENUM, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
    void append_value(schema::logical_type::UUID, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
    void append_value(schema::logical_type::INT8, int32_t v) { _out << static_cast<int8_t>(v); }
    void append_value(schema::logical_type::INT16, int32_t v) { _out << static_cast<int16_t>(v); }
    void append_value(schema::logical_type::INT32, int32_t v) { _out << static_cast<int32_t>(v); }
    void append_value(schema::logical_type::INT64, int64_t v) { _out << static_cast<int64_t>(v); }
    void append_value(schema::logical_type::UINT8, int32_t v) { _out << static_cast<uint8_t>(v); }
    void append_value(schema::logical_type::UINT16, int32_t v) { _out << static_cast<uint16_t>(v); }
    void append_value(schema::logical_type::UINT32, int32_t v) { _out << static_cast<uint32_t>(v); }
    void append_value(schema::logical_type::UINT64, int64_t v) { _out << static_cast<uint64_t>(v); }
    void append_value(schema::logical_type::DECIMAL t, int32_t v) { _out << v; }
    void append_value(schema::logical_type::DECIMAL t, int64_t v) { _out << v; }
    void append_value(schema::logical_type::DECIMAL t, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
    void append_value(schema::logical_type::DATE, int32_t v) { _out << v; }
    void append_value(schema::logical_type::TIME t, int32_t v) { _out << v; }
    void append_value(schema::logical_type::TIME t, int64_t v) { _out << v; }
    void append_value(schema::logical_type::TIMESTAMP t, int64_t v) { _out << v; }
    void append_value(schema::logical_type::INTERVAL, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
    void append_value(schema::logical_type::JSON, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
    void append_value(schema::logical_type::BSON, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
    void append_value(schema::logical_type::UNKNOWN, int32_t) {
        _out << 666;
    }
    void append_value(schema::logical_type::NONE, int32_t v) { _out << v; }
    void append_value(schema::logical_type::NONE, int64_t v) { _out << v; }
    void append_value(schema::logical_type::NONE, std::array<int32_t, 3> v) {
        _out
            << static_cast<uint32_t>(v[0]) << ' '
            << static_cast<uint32_t>(v[1]) << ' '
            << static_cast<uint32_t>(v[2]);
    }
    void append_value(schema::logical_type::NONE, float v) { _out << v; }
    void append_value(schema::logical_type::NONE, double v) { _out << v; }
    void append_value(schema::logical_type::NONE, uint8_t v) {
        _out << (v ? "true" : "false");
    }
    void append_value(schema::logical_type::NONE, seastar::temporary_buffer<uint8_t> v) {
        _out << '\'' << std::string_view{reinterpret_cast<const char*>(v.get()), v.size()} << '\'';
    }
};

} // namespace parquet::cql
