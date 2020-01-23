#include <seastar/parquet/parquet/exception.h>
#include <seastar/parquet/parquet/cql_schema.h>

namespace parquet::seastarized::cql {

namespace {

using namespace logical_schema;

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

const char* CqlFromPhysicalType(const Type::type& type) {
  switch (type) {
    case parquet::Type::BOOLEAN:
      return "boolean";
    case parquet::Type::INT32:
      return "int";
    case parquet::Type::INT64:
      return "bigint";
    case parquet::Type::INT96:
      return "varint";
    case parquet::Type::FLOAT:
      return "float";
    case parquet::Type::DOUBLE:
      return "double";
    case parquet::Type::BYTE_ARRAY:
      return "blob";
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return "blob";
    default:
      throw ParquetException("Unexpected physical type");
  }
}

void PrintCqlType(const Node& node, std::string& out) {
  std::visit(overloaded {
    [&] (const PrimitiveNode& node) { out += CqlFromPhysicalType(node.physical_type); },
    [&] (const ListNode& node) {
      out += "frozen<list<";
      PrintCqlType(*node.element_node, out);
      out += ">>";
    },
    [&] (const OptionalNode& node) {
      PrintCqlType(*node.child_node, out);
    },
    [&] (const MapNode& node) {
      out += "frozen<map<";
      PrintCqlType(*node.key_node, out);
      out += ", ";
      PrintCqlType(*node.value_node, out);
      out += ">>";
    },
    [&] (const StructNode& node) {
      out += node.path;
    },
  }, node);
}

void CreateUDT(const Node& node, std::string& out) {
  std::visit(overloaded {
    [&] (const PrimitiveNode& node) {},
    [&] (const ListNode& node) { CreateUDT(*node.element_node, out); },
    [&] (const OptionalNode& node) { CreateUDT(*node.child_node, out); },
    [&] (const MapNode& node) {
      CreateUDT(*node.key_node, out);
      CreateUDT(*node.value_node, out);
    },
    [&] (const StructNode& node) {
      for (const Node& child : node.field_nodes) {
        CreateUDT(child, out);
      }
      out += "CREATE TYPE ";
      out += node.path;
      out += "(";
      const char *separator = "";
      for (const Node& child : node.field_nodes) {
        out += separator;
        separator = ", ";
        std::visit([&](const auto& x) { out += x.name; }, child);
        out += ": ";
        PrintCqlType(child, out);
      }
      out += ");\n";
    },
  }, node);
}

} // namespace

std::string CqlSchemaFromLogicalSchema(const RootNode& schema) {
  std::string out;
  for (const Node& child : schema.field_nodes) {
    CreateUDT(child, out);
  }

  out += "CREATE TABLE parquet (id timeuuid PRIMARY KEY";
  for (const Node& child : schema.field_nodes) {
    out += ", ";
    std::visit([&](const auto& x) { out += x.name; }, child);
    out += " ";
    PrintCqlType(child, out);
  }
  out += ");\n";
  return out;
}

} // namespace parquet::seastarized::cql

