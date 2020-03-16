#include <seastar/parquet/record_reader.hh>
#include <seastar/parquet/cql_reader.hh>
#include <seastar/parquet/overloaded.hh>
#include <boost/iterator/counting_iterator.hpp>

namespace parquet::cql {

namespace {

class cql_consumer {
    bool first_field = true;
    std::ostream& _out;
    std::string _column_selector;
    int _row_number = 0;
    void print_quoted_identifier(const std::string& s) {
        _out << '"';
        for (char c : s) {
            if (c == '"') {
                _out << c;
                _out << c;
            } else {
                _out << c;
            }
        }
        _out << '"';
    }
    void print_quoted_string(const seastar::temporary_buffer<uint8_t>& s) {
        _out << '\'';
        for (unsigned char c : s) {
            if (c == '\'') {
                _out << c;
                _out << c;
            } else {
                _out << c;
            }
        }
        _out << '\'';
    }
    void print_blob(const seastar::temporary_buffer<uint8_t>& s) {
        _out << "0x";
        for (uint8_t c : s) {
            print_hex_byte(c);
        }
    }
    void print_hex_byte(uint8_t b) {
        static const char table[] = "0123456789ABCDEF";
        _out << table[b >> 4];
        _out << table[b & 0x0F];
    }
public:
    explicit cql_consumer(std::ostream& out, std::string column_selector)
        : _out{out}
        , _column_selector{std::move(column_selector)} {}
    void start_record() {
        _out << "INSERT INTO ";
        _out << _column_selector;
        _out << " VALUES(";
        _out << _row_number;
    }
    void end_record() {
        ++_row_number;
        _out << ");\n";
    }
    void start_column(const std::string& s) {
        _out << ", ";
    }
    void start_struct() {
        _out << '{';
        first_field = true;
    }
    void end_struct() {
        _out << '}';
        first_field = false;
    }
    void start_field(const std::string& s) {
        if (first_field) {
            first_field = false;
        } else {
            _out << ", ";
        }
        print_quoted_identifier(s);
        _out << ": ";
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
    void append_value(schema::logical_type::STRING, seastar::temporary_buffer<uint8_t> v) { print_quoted_string(v); }
    void append_value(schema::logical_type::ENUM, seastar::temporary_buffer<uint8_t> v) { print_quoted_string(v); }
    void append_value(schema::logical_type::UUID, seastar::temporary_buffer<uint8_t> v) {
        assert(v.size() == 16);
        for (int i = 0; i < 16; ++i) {
            if (i == 4 || i == 6 || i == 8 || i == 10) {
                _out << '-';
            }
            print_hex_byte(v[i]);
        }
    }
    void append_value(schema::logical_type::INT8, int32_t v) { _out << static_cast<int8_t>(v); }
    void append_value(schema::logical_type::INT16, int32_t v) { _out << static_cast<int16_t>(v); }
    void append_value(schema::logical_type::INT32, int32_t v) { _out << static_cast<int32_t>(v); }
    void append_value(schema::logical_type::INT64, int64_t v) { _out << static_cast<int64_t>(v); }
    void append_value(schema::logical_type::UINT8, int32_t v) { _out << static_cast<uint8_t>(v); }
    void append_value(schema::logical_type::UINT16, int32_t v) { _out << static_cast<uint16_t>(v); }
    void append_value(schema::logical_type::UINT32, int32_t v) { _out << static_cast<uint32_t>(v); }
    void append_value(schema::logical_type::UINT64, int64_t v) { _out << static_cast<uint64_t>(v); }
    void append_value(schema::logical_type::DECIMAL_INT32 t, int32_t v) { _out << v << "e-" << t.scale; }
    void append_value(schema::logical_type::DECIMAL_INT64 t, int64_t v) { _out << v << "e-" << t.scale; }
    void append_value(schema::logical_type::DECIMAL_BYTE_ARRAY t, seastar::temporary_buffer<uint8_t> v) {
        boost::multiprecision::cpp_int x;
        import_bits(x, v.begin(), v.end(), 8);
        _out << x << "e-" << t.scale;
    }
    void append_value(schema::logical_type::DECIMAL_FIXED_LEN_BYTE_ARRAY t, seastar::temporary_buffer<uint8_t> v) {
        boost::multiprecision::cpp_int x;
        import_bits(x, v.begin(), v.end(), 8);
        _out << x << "e-" << t.scale;
    }
    void append_value(schema::logical_type::DATE, int32_t v) { _out << static_cast<uint32_t>(v) + (1u << 31u); }
    void append_value(schema::logical_type::TIME_INT32 t, int32_t v) { _out << static_cast<int64_t>(v) * 1000000; }
    void append_value(schema::logical_type::TIME_INT64 t, int64_t v) { _out << ((t.unit == t.NANOS) ? v : v * 1000); }
    void append_value(schema::logical_type::TIMESTAMP t, int64_t v) { _out << v; }
    void append_value(schema::logical_type::INTERVAL, seastar::temporary_buffer<uint8_t> v) {
        assert(v.size() == 12);
        uint32_t buf[3];
        static_assert(sizeof(buf) == 12);
        memcpy(buf, v.get(), 12);
        _out << buf[0] << "mo" << buf[1] << "d" << buf[2] << "ms";
    }
    void append_value(schema::logical_type::JSON, seastar::temporary_buffer<uint8_t> v) { print_quoted_string(v); }
    void append_value(schema::logical_type::BSON, seastar::temporary_buffer<uint8_t> v) { print_blob(v); }
    void append_value(schema::logical_type::UNKNOWN, int32_t) { append_null(); }
    void append_value(schema::logical_type::INT96, std::array<int32_t, 3> v) {
        boost::multiprecision::int128_t x = v[0];
        x <<= 32;
        x += static_cast<uint32_t>(v[1]);
        x <<= 32;
        x += static_cast<uint32_t>(v[2]);
        _out << x;
    }
    void append_value(schema::logical_type::FLOAT, float v) { _out << std::scientific << v; }
    void append_value(schema::logical_type::DOUBLE, double v) { _out << std::scientific << v; }
    void append_value(schema::logical_type::BOOLEAN, uint8_t v) { _out << (v ? "true" : "false"); }
    void append_value(schema::logical_type::BYTE_ARRAY, seastar::temporary_buffer<uint8_t> v) { print_blob(v); }
    void append_value(schema::logical_type::FIXED_LEN_BYTE_ARRAY, seastar::temporary_buffer<uint8_t> v) { print_blob(v); }

};

struct node {
    const schema::node& parquet_node;
    std::string cql_type;
    std::string identifier;
    std::vector<node> children;
};

struct cql_schema {
    const schema::schema& parquet_schema;
    std::vector<node> columns;
};

const char* primitive_cql_type(const schema::primitive_node& leaf) {
    using namespace schema::logical_type;
    return std::visit(overloaded {
        [] (const STRING&) { return "text"; },
        [] (const ENUM&) { return "blob"; },
        [] (const UUID&) { return "uuid"; },
        [] (const INT8&) { return "tinyint"; },
        [] (const INT16&) { return "smallint"; },
        [] (const INT32&) { return "int"; },
        [] (const INT64&) { return "bigint"; },
        [] (const UINT8&) { return "tinyint"; },
        [] (const UINT16&) { return "smallint"; },
        [] (const UINT32&) { return "int"; },
        [] (const UINT64&) { return "bigint"; },
        [] (const DECIMAL_INT32&) { return "decimal"; },
        [] (const DECIMAL_INT64&) { return "decimal"; },
        [] (const DECIMAL_BYTE_ARRAY&) { return "decimal"; },
        [] (const DECIMAL_FIXED_LEN_BYTE_ARRAY&) { return "decimal"; },
        [] (const DATE&) { return "date"; },
        [] (const TIME_INT32&) { return "time"; },
        [] (const TIME_INT64&) { return "time"; },
        [] (const TIMESTAMP& t) { return t.unit == TIMESTAMP::MILLIS ? "timestamp" : "bigint"; },
        [] (const INTERVAL&) { return "duration"; },
        [] (const JSON&) { return "text"; },
        [] (const BSON&) { return "blob"; },
        [] (const FLOAT&) { return "float"; },
        [] (const DOUBLE&) { return "double"; },
        [] (const BYTE_ARRAY&) { return "blob"; },
        [] (const FIXED_LEN_BYTE_ARRAY&) { return "blob"; },
        [] (const INT96&) { return "varint"; },
        [] (const BOOLEAN&) { return "boolean"; },
        [] (const UNKNOWN&) { return "blob"; },
        [] (const auto&) {
            throw parquet_exception("unreachable code");
            return "";
        }
    }, leaf.logical_type);
}

void print_udt_create_statements(const cql_schema& cql_schema, std::string& out) {
    auto print = y_combinator{[&out] (auto&& print, const node& x) -> void {
        for (const node& child : x.children) {
            print(child);
        }
        if (std::holds_alternative<schema::struct_node>(x.parquet_node)) {
            out += "CREATE TYPE ";
            out += x.cql_type;
            out += " (";
            const char *separator = "";
            for (const node& child : x.children) {
                out += separator;
                separator = ", ";
                out += child.identifier;
                out += ": ";
                out += child.cql_type;
            }
            out += ");\n";
        }
    }};
    for (const auto& column : cql_schema.columns) {
        print(column);
    }
}

std::string quote_identifier(const std::string& x) {
    std::string quoted;
    quoted.reserve((x.size() + 2));
    quoted.push_back('"');
    for (char c : x) {
        if (c == '"') {
            quoted.push_back(c);
            quoted.push_back(c);
        } else {
            quoted.push_back(c);
        }
    }
    quoted.push_back('"');
    return quoted;
}


cql_schema parquet_schema_to_cql_schema(const schema::schema& parquet_schema) {
    int udt_index = 0;
    auto convert = y_combinator{[&udt_index] (auto&& convert, const schema::node& parquet_node) -> node {
        return std::visit(overloaded {
            [&] (const schema::primitive_node& x) {
                return node{parquet_node, primitive_cql_type(x), quote_identifier(x.info.name)};
            },
            [&] (const schema::list_node& x) {
                node element = convert(*x.element);
                std::string type = "frozen<list<" + element.cql_type + ">>";
                return node{parquet_node, std::move(type), quote_identifier(x.info.name), {std::move(element)}};
            },
            [&] (const schema::map_node& x) {
                node key = convert(*x.key);
                node value = convert(*x.value);
                std::string type = "frozen<map<" + key.cql_type + ", " + value.cql_type + ">>";
                return node{parquet_node, std::move(type), quote_identifier(x.info.name),
                            {std::move(key), std::move(value)}};
            },
            [&] (const schema::optional_node& x) {
                node child = convert(*x.child);
                std::string type = child.cql_type;
                return node{parquet_node, child.cql_type, quote_identifier(x.info.name), {std::move(child)}};
            },
            [&] (const schema::struct_node& x) {
                std::vector<node> children;
                children.reserve(x.fields.size());
                for (const auto& field : x.fields) {
                    children.push_back(convert(field));
                }
                std::string type = "udt_" + std::to_string(udt_index);
                ++udt_index;
                return node{parquet_node, std::move(type), quote_identifier(x.info.name), std::move(children)};
            },
        }, parquet_node);
    }};
    std::vector<node> columns;
    columns.reserve(parquet_schema.fields.size());
    for (const schema::node& field : parquet_schema.fields) {
        columns.push_back(convert(field));
    }
    return cql_schema{parquet_schema, std::move(columns)};
}

const char default_primary_key_name[] = "\"row_number\"";

std::string cql_schema_to_cql_create(const cql_schema& schema, const std::string& table_name) {
    std::string out;
    print_udt_create_statements(schema, out);

    out += "CREATE TABLE ";
    out += table_name;
    out += '(';
    out += default_primary_key_name;
    out += " bigint PRIMARY KEY";
    for (const node& child : schema.columns) {
        out += ", ";
        out += child.identifier;
        out += " ";
        out += child.cql_type;
    }
    out += ");\n";
    return out;
}

std::string cql_schema_to_cql_column_name_tuple(const cql_schema& schema) {
    std::string out;
    out += '(';
    out += default_primary_key_name;
    for (const node& child : schema.columns) {
        out += ", ";
        out += child.identifier;
    }
    out += ")";
    return out;
}

} // namespace

seastar::future<> parquet_to_cql(file_reader& fr, std::ostream& out) {
    cql_schema schema = parquet_schema_to_cql_schema(fr.schema());
    const char table_name[] = "parquet";
    out << cql_schema_to_cql_create(schema, table_name);
    return seastar::do_with(cql_consumer{out, table_name + cql_schema_to_cql_column_name_tuple(schema)},
    [&fr] (cql_consumer& consumer) {
        return seastar::do_for_each(
        boost::counting_iterator<int>(0),
        boost::counting_iterator<int>(fr.metadata().row_groups.size()),
        [&fr, &consumer](int row_group) {
            return record::record_reader::make(fr, row_group).then(
            [&consumer](record::record_reader rr) {
                return seastar::do_with(std::move(rr),
                [&consumer](record::record_reader &rr) {
                    return rr.read_all(consumer);
                });
            });
        });
    });
}

} // namespace parquet::cql

