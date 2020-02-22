#pragma once

#include <seastar/parquet/parquet_types.h>
#include <variant>
#include <memory>
#include <vector>

namespace parquet::schema {

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
    int def_level;
    int rep_level;
};

struct primitive_node : node_base {
    format::Type::type physical_type;
    int column_index;
};

struct list_node : node_base {
    std::unique_ptr<node> element_node;
};

struct map_node : node_base {
    std::unique_ptr<node> key_node;
    std::unique_ptr<node> value_node;
};

struct struct_node : node_base {
    std::vector<node> field_nodes;
};

struct optional_node : node_base {
    std::unique_ptr<node> child_node;
};

struct root_node {
    const format::SchemaElement &info;
    std::vector<node> field_nodes;
    std::vector<const primitive_node*> leaves;
};

root_node build_logical_schema(const format::FileMetaData& file_metadata);

} // namespace parquet::schema
