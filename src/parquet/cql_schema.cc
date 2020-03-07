#include <seastar/parquet/cql_schema.hh>
#include <seastar/parquet/exception.hh>
#include <seastar/parquet/overloaded.hh>

namespace parquet::cql {

namespace {

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
        [] (const DECIMAL&) { return "decimal"; },
        [] (const DATE&) { return "date"; },
        [] (const TIME&) { return "time"; },
        [] (const TIMESTAMP& t) { return t.unit == TIMESTAMP::MILLIS ? "timestamp" : "bigint"; },
        [] (const INTERVAL&) { return "blob"; },
        [] (const JSON&) { return "text"; },
        [] (const BSON&) { return "blob"; },
        [] (const UNKNOWN&) { return "blob"; },
        [&leaf] (const NONE&) {
            switch (leaf.info.type) {
            case format::Type::INT32: return "int";
            case format::Type::INT64: return "bigint";
            case format::Type::INT96: return "varint";
            case format::Type::BOOLEAN: return "boolean";
            case format::Type::BYTE_ARRAY:
            case format::Type::FIXED_LEN_BYTE_ARRAY: return "blob";
            case format::Type::FLOAT: return "float";
            case format::Type::DOUBLE: return "double";
            default:
                throw parquet_exception::nyi(std::string("Unsupported physical type: ") + std::to_string(leaf.info.type));
                return "";
            }
        },
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

std::string quote_string(const std::string& x) {
    std::string quoted;
    quoted.reserve((x.size() + 2));
    quoted.push_back('"');
    for (char c : x) {
        if (c == '"') {
            quoted.push_back(c);
            quoted.push_back(c);
        } else if (c == '\0') {
        } else {
            quoted.push_back(c);
        }
    }
    quoted.push_back('"');
    return quoted;
}

} // namespace

cql_schema build_cql_schema(const schema::schema& parquet_schema) {
    int udt_index = 0;
    auto convert = y_combinator{[&udt_index] (auto&& convert, const schema::node& parquet_node) -> node {
        return std::visit(overloaded {
            [&] (const schema::primitive_node& x) {
                return node{parquet_node, primitive_cql_type(x), quote_string(x.info.name)};
            },
            [&] (const schema::list_node& x) {
                node element = convert(*x.element);
                std::string type = "frozen<list<" + element.cql_type + ">>";
                return node{parquet_node, std::move(type), quote_string(x.info.name), {std::move(element)}};
            },
            [&] (const schema::map_node& x) {
                node key = convert(*x.key);
                node value = convert(*x.value);
                std::string type = "frozen<map<" + key.cql_type + ", " + value.cql_type + ">>";
                return node{parquet_node, std::move(type), quote_string(x.info.name),
                            {std::move(key), std::move(value)}};
            },
            [&] (const schema::optional_node& x) {
                node child = convert(*x.child);
                std::string type = child.cql_type;
                return node{parquet_node, child.cql_type, quote_string(x.info.name), {std::move(child)}};
            },
            [&] (const schema::struct_node& x) {
                std::vector<node> children;
                children.reserve(x.fields.size());
                for (const auto& field : x.fields) {
                    children.push_back(convert(field));
                }
                std::string type = "udt_" + std::to_string(udt_index);
                ++udt_index;
                return node{parquet_node, std::move(type), quote_string(x.info.name), std::move(children)};
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

std::string cql_schema_from_logical_schema(const schema::schema& schema) {
    cql_schema cql_schema = build_cql_schema(schema);
    std::string out;
    print_udt_create_statements(cql_schema, out);

    out += "CREATE TABLE parquet (\"pk\" timeuuid PRIMARY KEY";
    for (const node& child : cql_schema.columns) {
        out += ", ";
        out += child.identifier;
        out += " ";
        out += child.cql_type;
    }
    out += ");\n";
    return out;
}

} // namespace parquet

