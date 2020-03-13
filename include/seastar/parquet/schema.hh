#pragma once

#include <seastar/parquet/parquet_types.h>
#include <variant>
#include <memory>
#include <vector>

namespace parquet::schema {

namespace logical_type {
struct BOOLEAN { static constexpr format::Type::type physical_type = format::Type::BOOLEAN; };
struct INT32 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct INT64 { static constexpr format::Type::type physical_type = format::Type::INT64; };
struct INT96 { static constexpr format::Type::type physical_type = format::Type::INT96; };
struct FLOAT { static constexpr format::Type::type physical_type = format::Type::FLOAT; };
struct DOUBLE { static constexpr format::Type::type physical_type = format::Type::DOUBLE; };
struct BYTE_ARRAY { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct FIXED_LEN_BYTE_ARRAY { static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY; };
struct STRING { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct ENUM { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct UUID { static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY; };
struct INT8 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct INT16 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT8 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT16 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT32 { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct UINT64 { static constexpr format::Type::type physical_type = format::Type::INT64; };
struct DECIMAL_INT32 {
    static constexpr format::Type::type physical_type = format::Type::INT32;
    int32_t scale;
    int32_t precision;
};
struct DECIMAL_INT64 {
    static constexpr format::Type::type physical_type = format::Type::INT64;
    int32_t scale;
    int32_t precision;
};
struct DECIMAL_BYTE_ARRAY {
    static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY;
    int32_t scale;
    int32_t precision;
};
struct DECIMAL_FIXED_LEN_BYTE_ARRAY {
    static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY;
    int32_t scale;
    int32_t precision;
};
struct DATE { static constexpr format::Type::type physical_type = format::Type::INT32; };
struct TIME_INT32 {
    static constexpr format::Type::type physical_type = format::Type::INT32;
    bool utc_adjustment;
};
struct TIME_INT64 {
    static constexpr format::Type::type physical_type = format::Type::INT64;
    bool utc_adjustment;
    enum {MICROS, NANOS} unit;
};
struct TIMESTAMP {
    static constexpr format::Type::type physical_type = format::Type::INT64;
    bool utc_adjustment;
    enum {MILLIS, MICROS, NANOS} unit;
};
struct INTERVAL { static constexpr format::Type::type physical_type = format::Type::FIXED_LEN_BYTE_ARRAY; };
struct JSON { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct BSON { static constexpr format::Type::type physical_type = format::Type::BYTE_ARRAY; };
struct UNKNOWN { static constexpr format::Type::type physical_type = format::Type::INT32; };

using logical_type = std::variant<
        BOOLEAN,
        INT32,
        INT64,
        INT96,
        FLOAT,
        DOUBLE,
        BYTE_ARRAY,
        FIXED_LEN_BYTE_ARRAY,
        STRING,
        ENUM,
        UUID,
        INT8,
        INT16,
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        DECIMAL_INT32,
        DECIMAL_INT64,
        DECIMAL_BYTE_ARRAY,
        DECIMAL_FIXED_LEN_BYTE_ARRAY,
        DATE,
        TIME_INT32,
        TIME_INT64,
        TIMESTAMP,
        INTERVAL,
        JSON,
        BSON,
        UNKNOWN
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

schema file_metadata_to_schema(const format::FileMetaData& file_metadata);

} // namespace parquet::schema
