#include <seastar/parquet/schema.hh>
#include <seastar/parquet/exception.hh>
#include <seastar/parquet/parquet_types.h>
#include <seastar/parquet/overloaded.hh>

namespace parquet::schema {

namespace {

struct raw_node {
    std::vector<raw_node> children;
    const format::SchemaElement& info;
    std::vector<std::string> path;
    int column_index;
    int def_level;
    int rep_level;
};

raw_node compute_shape(const std::vector<format::SchemaElement>& flat_schema) {
    size_t index = 0;
    return y_combinator{[&] (auto&& convert) -> raw_node {
        if (index >= flat_schema.size()) {
            throw parquet_exception::corrupted_file("invalid number of children in schema");
        }
        const format::SchemaElement &current = flat_schema[index];
        ++index;
        if (current.__isset.num_children) {
            if (current.num_children < 0) {
                throw parquet_exception::corrupted_file("negative num_children");
            }
            std::vector<raw_node> children;
            children.reserve(current.num_children);
            for (int i = 0; i < current.num_children; ++i) {
                children.push_back(convert());
            }
            return raw_node{std::move(children), current};
        } else {
            return raw_node{std::vector<raw_node>(), current};
        }
    }}();
}

void compute_column_index(raw_node& raw_schema) {
    int column_index = 0;
    y_combinator{[&] (auto&& compute, raw_node& r) -> void {
        if (r.children.empty()) {
            r.column_index = column_index;
            ++column_index;
        } else {
            r.column_index = -1;
            for (raw_node& child : r.children) {
                compute(child);
            }
        }
    }}(raw_schema);
}

void compute_levels(raw_node& raw_schema) {
    y_combinator{[&] (auto&& compute, raw_node& r, int def, int rep) -> void {
        if (r.info.repetition_type == format::FieldRepetitionType::REPEATED) {
            ++def;
            ++rep;
        } else if (r.info.repetition_type == format::FieldRepetitionType::OPTIONAL) {
            ++def;
        }
        r.def_level = def;
        r.rep_level = rep;
        for (raw_node& child : r.children) {
            compute(child, def, rep);
        }
    }}(raw_schema, 0, 0);
}

void compute_path(raw_node& raw_schema) {
    auto compute = y_combinator{[&] (auto&& compute, raw_node& r, std::vector<std::string> path) -> void {
        path.push_back(r.info.name);
        for (raw_node& child : r.children) {
            compute(child, path);
        }
        r.path = std::move(path);
    }};
    for (raw_node& child : raw_schema.children) {
        compute(child, std::vector<std::string>());
    }
}

logical_type::logical_type determine_logical_type(const format::SchemaElement& x) {
    static auto verify = [] (bool condition, const std::string& error) {
        if (!condition) { throw parquet_exception::corrupted_file(error); }
    };
    if (!x.__isset.type) {
        return logical_type::UNKNOWN{};
    }
    if (x.__isset.logicalType) {
        if (x.logicalType.__isset.TIME) {
            if (x.logicalType.TIME.unit.__isset.MILLIS) {
                verify(x.type == format::Type::INT32, "TIME MILLIS must annotate the INT32 physical type");
                return logical_type::TIME_INT32{x.logicalType.TIME.isAdjustedToUTC};
            } else if (x.logicalType.TIME.unit.__isset.MICROS) {
                verify(x.type == format::Type::INT64, "TIME MICROS must annotate the INT64 physical type");
                return logical_type::TIME_INT64{x.logicalType.TIME.isAdjustedToUTC, logical_type::TIME_INT64::MICROS};
            } else if (x.logicalType.TIME.unit.__isset.NANOS) {
                verify(x.type == format::Type::INT64, "TIME NANOS must annotate the INT64 physical type");
                return logical_type::TIME_INT64{x.logicalType.TIME.isAdjustedToUTC, logical_type::TIME_INT64::NANOS};
            }
        } else if (x.logicalType.__isset.TIMESTAMP) {
            verify(x.type == format::Type::INT64, "TIMESTAMP must annotate the INT64 physical type");
            if (x.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
                return logical_type::TIMESTAMP{x.logicalType.TIMESTAMP.isAdjustedToUTC, logical_type::TIMESTAMP::MILLIS};
            } else if (x.logicalType.TIMESTAMP.unit.__isset.MICROS) {
                return logical_type::TIMESTAMP{x.logicalType.TIMESTAMP.isAdjustedToUTC, logical_type::TIMESTAMP::MICROS};
            } else if (x.logicalType.TIMESTAMP.unit.__isset.NANOS) {
                return logical_type::TIMESTAMP{x.logicalType.TIMESTAMP.isAdjustedToUTC, logical_type::TIMESTAMP::NANOS};
            }
        } else if (x.logicalType.__isset.UUID) {
            verify(x.type == format::Type::FIXED_LEN_BYTE_ARRAY && x.type_length == 16,
                   "UUID must annotate the 16-byte fixed-length binary type");
            return logical_type::UUID{};
        } else if (x.logicalType.__isset.UNKNOWN) {
            return logical_type::UNKNOWN{};
        }
    }
    if (x.__isset.converted_type) {
        if (x.converted_type == format::ConvertedType::UTF8) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "UTF8 must annotate the binary physical type");
            return logical_type::STRING{};
        } else if (x.converted_type == format::ConvertedType::ENUM) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "ENUM must annotate the binary physical type");
            return logical_type::ENUM{};
        } else if (x.converted_type == format::ConvertedType::INT_8) {
            verify(x.type == format::Type::INT32, "INT_8 must annotate the INT32 physical type");
            return logical_type::INT8{};
        } else if (x.converted_type == format::ConvertedType::INT_16) {
            verify(x.type == format::Type::INT32, "INT_16 must annotate the INT32 physical type");
            return logical_type::INT16{};
        } else if (x.converted_type == format::ConvertedType::INT_32) {
            verify(x.type == format::Type::INT32, "INT_32 must annotate the INT32 physical type");
            return logical_type::INT32{};
        } else if (x.converted_type == format::ConvertedType::INT_64) {
            verify(x.type == format::Type::INT64, "INT_64 must annotate the INT64 physical type");
            return logical_type::INT64{};
        } else if (x.converted_type == format::ConvertedType::UINT_8) {
            verify(x.type == format::Type::INT32, "UINT_8 must annotate the INT32 physical type");
            return logical_type::UINT8{};
        } else if (x.converted_type == format::ConvertedType::UINT_16) {
            verify(x.type == format::Type::INT32, "UINT_16 must annotate the INT32 physical type");
            return logical_type::UINT16{};
        } else if (x.converted_type == format::ConvertedType::UINT_32) {
            verify(x.type == format::Type::INT32, "UINT_32 must annotate the INT32 physical type");
            return logical_type::UINT32{};
        } else if (x.converted_type == format::ConvertedType::UINT_64) {
            verify(x.type == format::Type::INT64, "UINT_64 must annotate the INT64 physical type");
            return logical_type::UINT64{};
        } else if (x.converted_type == format::ConvertedType::DECIMAL) {
            verify(x.__isset.precision && x.__isset.scale, "precision and scale must be set for DECIMAL");
            int precision = x.precision;
            int scale = x.scale;
            if (x.type == format::Type::INT32) {
                verify(1 <= precision && precision <= 9,
                        "precision " + std::to_string(precision) + " out of bounds for INT32 decimal");
                return logical_type::DECIMAL_INT32{scale, precision};
            } else if (x.type == format::Type::INT64) {
                verify(1 <= precision && precision <= 18,
                        "precision " + std::to_string(precision) + " out of bounds for INT64 decimal");
                return logical_type::DECIMAL_INT64{scale, precision};
            } else if (x.type == format::Type::BYTE_ARRAY) {
                verify(precision > 0,
                        "precision " + std::to_string(precision) + " out of bounds for BYTE_ARRAY decimal");
                return logical_type::DECIMAL_BYTE_ARRAY{scale, precision};
            } else if (x.type == format::Type::FIXED_LEN_BYTE_ARRAY) {
                verify(precision > 0,
                        "precision " + std::to_string(precision) + " out of bounds for FIXED_LEN_BYTE_ARRAY decimal");
                return logical_type::DECIMAL_FIXED_LEN_BYTE_ARRAY{scale, precision};
            } else {
                verify(false,"DECIMAL must annotate INT32, INT64, BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY");
                return logical_type::UNKNOWN{}; // Unreachable
            }
        } else if (x.converted_type == format::ConvertedType::DATE) {
            verify(x.type == format::Type::INT32, "DATE must annotate the INT32 physical type");
        } else if (x.converted_type == format::ConvertedType::TIME_MILLIS) {
            verify(x.type == format::Type::INT32, "TIME_MILLIS must annotate the INT32 physical type");
            return logical_type::TIME_INT32{true};
        } else if (x.converted_type == format::ConvertedType::TIME_MICROS) {
            verify(x.type == format::Type::INT64, "TIME_MICROS must annotate the INT64 physical type");
            return logical_type::TIME_INT64{true, logical_type::TIME_INT64::MICROS};
        } else if (x.converted_type == format::ConvertedType::TIMESTAMP_MILLIS) {
            verify(x.type == format::Type::INT64, "TIMESTAMP_MILLIS must annotate the INT64 physical type");
            return logical_type::TIMESTAMP{true, logical_type::TIMESTAMP::MILLIS};
        } else if (x.converted_type == format::ConvertedType::TIMESTAMP_MICROS) {
            verify(x.type == format::Type::INT64, "TIMESTAMP_MICROS must annotate the INT64 physical type");
            return logical_type::TIMESTAMP{true, logical_type::TIMESTAMP::MICROS};
        } else if (x.converted_type == format::ConvertedType::INTERVAL) {
            verify(x.type == format::Type::FIXED_LEN_BYTE_ARRAY && x.type_length == 12,
                   "INTERVAL must annotate the INT32 physical type");
            return logical_type::INTERVAL{};
        } else if (x.converted_type == format::ConvertedType::JSON) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "JSON must annotate the binary physical type");
            return logical_type::JSON{};
        } else if (x.converted_type == format::ConvertedType::BSON) {
            verify(x.type == format::Type::BYTE_ARRAY || x.type == format::Type::FIXED_LEN_BYTE_ARRAY,
                   "BSON must annotate the binary physical type");
            return logical_type::BSON{};
        }
    }
    switch (x.type) {
    case format::Type::BOOLEAN: return logical_type::BOOLEAN{};
    case format::Type::INT32: return logical_type::INT32{};
    case format::Type::INT64: return logical_type::INT64{};
    case format::Type::INT96: return logical_type::INT96{};
    case format::Type::FLOAT: return logical_type::FLOAT{};
    case format::Type::DOUBLE: return logical_type::DOUBLE{};
    case format::Type::BYTE_ARRAY: return logical_type::BYTE_ARRAY{};
    case format::Type::FIXED_LEN_BYTE_ARRAY: return logical_type::FIXED_LEN_BYTE_ARRAY{};
    default: return logical_type::UNKNOWN{};
    }
}

raw_node flat_schema_to_raw_schema(const std::vector<format::SchemaElement>& flat_schema) {
    raw_node raw_schema = compute_shape(flat_schema);
    compute_column_index(raw_schema);
    compute_levels(raw_schema);
    compute_path(raw_schema);
    return raw_schema;
}

node build_logical_node(const raw_node& raw_schema);

primitive_node build_primitive_node(const raw_node& r) {
    return primitive_node{{r.info, r.path, r.def_level, r.rep_level}, determine_logical_type(r.info), r.column_index};
}

list_node build_list_node(const raw_node& r) {
    if (r.children.size() != 1 || r.info.repetition_type == format::FieldRepetitionType::REPEATED) {
        throw parquet_exception::corrupted_file(std::string("Invalid list node: ") + r.info.name);
    }

    const raw_node& repeated_node = r.children[0];
    if (repeated_node.info.repetition_type != format::FieldRepetitionType::REPEATED) {
        throw parquet_exception::corrupted_file(std::string("Invalid list element: ") + r.info.name);
    }

    if ((repeated_node.children.size() != 1)
        || (repeated_node.info.name == "array")
        || (repeated_node.info.name == (r.info.name + "_tuple"))) {
        // Legacy 2-level list
        return list_node{
                {r.info, r.path, r.def_level, r.rep_level},
                std::make_unique<node>(build_logical_node(repeated_node))};
    } else {
        // Standard 3-level list
        const raw_node& element_node = repeated_node.children[0];
        return list_node{
                {r.info, r.path, r.def_level, r.rep_level},
                std::make_unique<node>(build_logical_node(element_node))};
    }
}

map_node build_map_node(const raw_node& r) {
    if (r.children.size() != 1) {
        throw parquet_exception(std::string("Invalid map node: ") + r.info.name);
    }

    const raw_node& repeated_node = r.children[0];
    if (repeated_node.children.size() != 2
        || repeated_node.info.repetition_type != format::FieldRepetitionType::REPEATED) {
        throw parquet_exception(std::string("Invalid map node: ") + r.info.name);
    }

    const raw_node& key_node = repeated_node.children[0];
    const raw_node& value_node = repeated_node.children[1];
    if (!key_node.children.empty()) {
        throw parquet_exception(std::string("Invalid map node: ") + r.info.name);
    }

    return map_node{
            {r.info, r.path, r.def_level, r.rep_level},
            std::make_unique<node>(build_logical_node(key_node)),
            std::make_unique<node>(build_logical_node(value_node))};
}

struct_node build_struct_node(const raw_node& r) {
    std::vector<node> fields;
    fields.reserve(r.children.size());
    for (const raw_node& child : r.children) {
        fields.push_back(build_logical_node(child));
    }
    return struct_node{{r.info, r.path, r.def_level, r.rep_level}, std::move(fields)};
}

enum class node_type { MAP, LIST, STRUCT, PRIMITIVE };
node_type determine_node_type(const raw_node& r) {
    if (r.children.empty()) {
        return node_type::PRIMITIVE;
    }
    if (r.info.__isset.converted_type) {
        if (r.info.converted_type == format::ConvertedType::MAP
                || r.info.converted_type == format::ConvertedType::MAP_KEY_VALUE) {
            return node_type::MAP;
        } else if (r.info.converted_type == format::ConvertedType::LIST) {
            return node_type::LIST;
        }
    }
    return node_type::STRUCT;
}

node build_logical_node(const raw_node& r) {
    auto build_unwrapped_node = [&r] () -> node {
        switch (determine_node_type(r)) {
        case node_type::MAP: return build_map_node(r);
        case node_type::LIST: return build_list_node(r);
        case node_type::STRUCT: return build_struct_node(r);
        case node_type::PRIMITIVE: return build_primitive_node(r);
        default: throw;
        }
    };

    if (r.info.repetition_type == format::FieldRepetitionType::OPTIONAL) {
        return optional_node{
                {r.info, r.path,  r.def_level - 1, r.rep_level},
                std::make_unique<node>(build_unwrapped_node())};
    } else if (r.info.repetition_type == format::FieldRepetitionType::REPEATED) {
        return list_node{
                {r.info, r.path, r.def_level - 1, r.rep_level - 1},
                std::make_unique<node>(build_unwrapped_node())};
    } else {
        return build_unwrapped_node();
    }
}

schema compute_shape(const raw_node& raw_schema) {
    std::vector<node> fields;
    fields.reserve(raw_schema.children.size());
    for (const raw_node& child : raw_schema.children) {
        fields.push_back(build_logical_node(child));
    }
    return schema{raw_schema.info, std::move(fields)};
}

void compute_leaves(schema& root) {
    auto collect = y_combinator{[&](auto&& collect, const node& x_variant) -> void {
        std::visit(overloaded {
            [&] (const optional_node& x) { collect(*x.child); },
            [&] (const list_node& x) { collect(*x.element); },
            [&] (const map_node& x) {
                collect(*x.key);
                collect(*x.value);
            },
            [&] (const struct_node& x) {
                for (const node& child : x.fields) {
                    collect(child);
                }
            },
            [&] (const primitive_node& y) {
                root.leaves.push_back(&y);
            }
        }, x_variant);
    }};
    for (const node& field : root.fields) {
        collect(field);
    }
}

} // namespace

schema file_metadata_to_schema(const format::FileMetaData& metadata) {
    raw_node raw_root = flat_schema_to_raw_schema(metadata.schema);
    schema root = compute_shape(raw_root);
    compute_leaves(root);
    return root;
}

} // namespace parquet::schema
