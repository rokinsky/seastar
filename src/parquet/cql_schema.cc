#include <seastar/parquet/cql_schema.hh>
#include <seastar/parquet/exception.hh>
#include <seastar/parquet/overloaded.hh>
#include <boost/algorithm/string/join.hpp>
#include <sstream>
#include <iterator>

namespace parquet::cql {

namespace {

std::string dotted_path(const schema::node_base& node) {
    return boost::algorithm::join(node.path, ".");
}

const char* cql_from_physical_type(const format::Type::type& type) {
    switch (type) {
        case format::Type::BOOLEAN:
            return "boolean";
        case format::Type::INT32:
            return "int";
        case format::Type::INT64:
            return "bigint";
        case format::Type::INT96:
            return "varint";
        case format::Type::FLOAT:
            return "float";
        case format::Type::DOUBLE:
            return "double";
        case format::Type::BYTE_ARRAY:
            return "blob";
        case format::Type::FIXED_LEN_BYTE_ARRAY:
            return "blob";
        default:
            throw parquet_exception("Unexpected physical type");
    }
}

void print_cql_type(const schema::node& node, std::string& out) {
    std::visit(overloaded {
        [&] (const schema::primitive_node& node) { out += cql_from_physical_type(node.physical_type); },
        [&] (const schema::list_node& node) {
            out += "frozen<list<";
            print_cql_type(*node.element_node, out);
            out += ">>";
        },
        [&] (const schema::optional_node& node) {
            print_cql_type(*node.child_node, out);
        },
        [&] (const schema::map_node& node) {
            out += "frozen<map<";
            print_cql_type(*node.key_node, out);
            out += ", ";
            print_cql_type(*node.value_node, out);
            out += ">>";
        },
        [&] (const schema::struct_node& node) {
            out += dotted_path(node);
        },
    }, node);
}

void create_udt(const schema::node& node, std::string& out) {
    std::visit(overloaded {
        [&] (const schema::primitive_node& node) {},
        [&] (const schema::list_node& node) { create_udt(*node.element_node, out); },
        [&] (const schema::optional_node& node) { create_udt(*node.child_node, out); },
        [&] (const schema::map_node& node) {
            create_udt(*node.key_node, out);
            create_udt(*node.value_node, out);
        },
        [&] (const schema::struct_node& node) {
            for (const schema::node& child : node.field_nodes) {
                create_udt(child, out);
            }
            out += "CREATE TYPE ";
            out += dotted_path(node);
            out += "(";
            const char *separator = "";
            for (const schema::node& child : node.field_nodes) {
                out += separator;
                separator = ", ";
                std::visit([&](const auto& x) { out += x.info.name; }, child);
                out += ": ";
                print_cql_type(child, out);
            }
            out += ");\n";
        },
    }, node);
}

} // namespace

std::string cql_schema_from_logical_schema(const schema::root_node& schema) {
    std::string out;
    for (const schema::node& child : schema.field_nodes) {
        create_udt(child, out);
    }

    out += "CREATE TABLE parquet (id timeuuid PRIMARY KEY";
    for (const schema::node& child : schema.field_nodes) {
        out += ", ";
        std::visit([&](const auto& x) { out += x.info.name; }, child);
        out += " ";
        print_cql_type(child, out);
    }
    out += ");\n";
    return out;
}

} // namespace parquet

