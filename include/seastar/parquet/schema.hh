#pragma once

#include <seastar/parquet/parquet_types.h>
#include <variant>
#include <memory>
#include <vector>

namespace parquet::schema {

namespace logical_type {
struct STRING {};
struct ENUM {};
struct UUID {};
struct INT8 {};
struct INT16 {};
struct INT32 {};
struct INT64 {};
struct UINT8 {};
struct UINT16 {};
struct UINT32 {};
struct UINT64 {};
struct DECIMAL { int32_t scale; int32_t precision; };
struct DATE {};
struct TIME { bool utc_adjustment; enum precision {MILLIS, MICROS, NANOS} unit; };
struct TIMESTAMP { bool utc_adjustment; enum precision {MILLIS, MICROS, NANOS} unit; };
struct INTERVAL {};
struct JSON {};
struct BSON {};
struct LIST {};
struct MAP {};
struct UNKNOWN {};
struct NONE {};

using logical_type = std::variant<
        STRING,
        ENUM,
        UUID,
        INT8,
        INT16,
        INT32,
        INT64,
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        DECIMAL,
        DATE,
        TIME,
        TIMESTAMP,
        INTERVAL,
        JSON,
        BSON,
        LIST,
        MAP,
        UNKNOWN,
        NONE
>;
} // namespace logical_type

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
    logical_type::logical_type logical_type;
    int column_index;
};

struct list_node : node_base {
    std::unique_ptr<node> element;
};

struct map_node : node_base {
    std::unique_ptr<node> key;
    std::unique_ptr<node> value;
};

struct struct_node : node_base {
    std::vector<node> fields;
};

struct optional_node : node_base {
    std::unique_ptr<node> child;
};

struct schema {
    const format::SchemaElement &info;
    std::vector<node> fields;
    std::vector<const primitive_node*> leaves;
};

schema build_logical_schema(const format::FileMetaData& file_metadata);

} // namespace parquet::schema
