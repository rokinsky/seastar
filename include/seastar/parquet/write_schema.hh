#pragma once

#include <seastar/parquet/schema.hh>

namespace parquet::write_schema {

using node = std::variant<
    struct primitive_node,
    struct struct_node,
    struct list_node,
    struct map_node
>;

struct primitive_node {
    std::string name;
    bool optional;
    schema::logical_type::logical_type logical_type;
    std::optional<uint32_t> type_length;
};

struct list_node {
    std::string name;
    bool optional;
    std::unique_ptr<node> element;
};

struct map_node {
    std::string name;
    bool optional;
    std::unique_ptr<node> key;
    std::unique_ptr<node> value;
};

struct struct_node {
    std::string name;
    bool optional;
    std::vector<node> fields;
};

struct schema {
    std::vector<node> fields;
};

} // namespace parquet::write_schema
