#pragma once

#include <seastar/parquet/parquet_types.h>

namespace parquet::schema {

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

raw_schema flat_schema_to_raw_schema(const std::vector<format::SchemaElement>& flat_schema);

} // namespace parquet::schema
