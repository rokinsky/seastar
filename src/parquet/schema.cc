#include <seastar/parquet/schema.hh>
#include <seastar/parquet/exception.hh>
#include <seastar/parquet/parquet_types.h>
#include <seastar/parquet/overloaded.hh>

namespace parquet::schema {

namespace {

constexpr format::FieldRepetitionType::type REPEATED = format::FieldRepetitionType::REPEATED;
constexpr format::FieldRepetitionType::type OPTIONAL = format::FieldRepetitionType::OPTIONAL;

struct raw_node {
    std::vector<raw_node> children;
    const format::SchemaElement& info;
    int column_index;
};

raw_node flat_schema_to_tree_recursive(
        const std::vector<format::SchemaElement>& flat_schema,
        int& schema_index,
        int& column_index
) {
    if (schema_index >= static_cast<int>(flat_schema.size())) {
        throw parquet_exception::corrupted_file("Invalid schema");
    }
    const format::SchemaElement& current = flat_schema[schema_index];
    ++schema_index;
    std::vector<raw_node> children;
    if (current.__isset.num_children) {
        for (int i = 0; i < current.num_children; ++i) {
            auto child = flat_schema_to_tree_recursive(flat_schema, schema_index, column_index);
            children.push_back(std::move(child));
        }
        return raw_node{std::move(children), current, -1};
    } else {
        column_index += 1;
        return raw_node{std::move(children), current, column_index - 1};
    }
}

raw_node flat_schema_to_tree(const std::vector<format::SchemaElement>& flat_schema) {
    int schema_index = 0;
    int column_index = 0;
    return flat_schema_to_tree_recursive(flat_schema, schema_index, column_index);
}

node build_logical_node(const raw_node& r, std::vector<std::string>, int def_level, int rep_level);

node build_logical_inner_node(
        const raw_node& r,
        std::vector<std::string> path,
        int def_level,
        int rep_level
) {
    node_base node_base{r.info, path, def_level, rep_level};

    if (r.children.size() == 0) {
        return primitive_node{std::move(node_base), r.info.type, r.column_index};
    } else {
        if ((r.info.__isset.logicalType && r.info.logicalType.__isset.LIST)
                || (r.info.__isset.converted_type && r.info.converted_type == format::ConvertedType::LIST)) {
            if (r.children.size() != 1 || r.info.repetition_type == REPEATED) {
                throw parquet_exception::corrupted_file(
                        std::string("Invalid list group_node: ") + r.info.name);
            }

            const raw_node& repeated_node = r.children[0];
            if (repeated_node.info.repetition_type != REPEATED) {
                throw parquet_exception::corrupted_file(
                        std::string("Invalid list node: ") + r.info.name);
            }

            if (repeated_node.children.size() == 0
                    || repeated_node.children.size() > 1
                    || repeated_node.info.name.compare("array") == 0
                    || repeated_node.info.name.compare(r.info.name + "_tuple") == 0) {
                // Legacy 2-level list
                return list_node{
                    std::move(node_base),
                    std::make_unique<node>(build_logical_node(repeated_node, std::move(path),
                            def_level, rep_level))};
            } else {
                // Standard 3-level list
                const raw_node& element_node = repeated_node.children[0];
                path.push_back(repeated_node.info.name);
                return list_node{
                    std::move(node_base),
                    std::make_unique<node>(build_logical_node(element_node, std::move(path),
                            def_level + 1, rep_level + 1))};
            }
        } else if ((r.info.__isset.logicalType && r.info.logicalType.__isset.MAP)
                || (r.info.__isset.converted_type && r.info.converted_type == format::ConvertedType::MAP)) {
            if (r.children.size() != 1) {
                throw parquet_exception(std::string("Invalid map node: ") + r.info.name);
            }

            const raw_node& repeated_node = r.children[0];
            if (repeated_node.children.size() != 2
                    || repeated_node.info.repetition_type != REPEATED) {
                throw parquet_exception(std::string("Invalid map node: ") + r.info.name);
            }

            const raw_node& key_node = repeated_node.children[0];
            const raw_node& value_node = repeated_node.children[1];

            if (key_node.children.size() != 0) {
                throw parquet_exception(std::string("Invalid map node: ") + r.info.name);
            }

            path.push_back(repeated_node.info.name);
            return map_node{
                std::move(node_base),
                std::make_unique<node>(build_logical_node(key_node,
                        path, def_level + 1, rep_level + 1)),
                std::make_unique<node>(build_logical_node(value_node,
                        path, def_level + 1, rep_level + 1))};
        } else {
            std::vector<node> fields;
            fields.reserve(r.children.size());
            for (size_t i = 0; i < r.children.size(); ++i) {
                fields.push_back(build_logical_node(r.children[i], path, def_level, rep_level));
            }
            return struct_node{
                std::move(node_base),
                std::move(fields)};
        }
    }
}

node build_logical_node(
        const raw_node& r,
        std::vector<std::string> path,
        int def_level,
        int rep_level
) {
    def_level = def_level + (r.info.repetition_type == REPEATED || r.info.repetition_type == OPTIONAL);
    rep_level = rep_level + (r.info.repetition_type == REPEATED);
    path.push_back(r.info.name);

    node inner_node = build_logical_inner_node(r, path, def_level, rep_level);

    if (r.info.repetition_type == OPTIONAL) {
        return optional_node{
            {r.info, std::move(path), def_level - 1, rep_level},
            std::make_unique<node>(std::move(inner_node))};
    } else if (r.info.repetition_type == REPEATED) {
        return list_node{
            {r.info, std::move(path), def_level - 1, rep_level - 1},
            std::make_unique<node>(std::move(inner_node))};
    } else {
        return inner_node;
    }
}

} // namespace

void collect_leaves(const node& x, std::vector<const primitive_node*>& leaves) {
    std::visit(overloaded {
        [&] (const optional_node& x) { collect_leaves(*x.child_node, leaves); },
        [&] (const list_node& x) { collect_leaves(*x.element_node, leaves); },
        [&] (const map_node& x) {
            collect_leaves(*x.key_node, leaves);
            collect_leaves(*x.value_node, leaves);
        },
        [&] (const struct_node& x) {
            for (const node& child : x.field_nodes) {
                collect_leaves(child, leaves);
            }
        },
        [&] (const primitive_node& y) {
            leaves.push_back(&y);
        }
    }, x);
}

root_node build_logical_schema(const format::FileMetaData& metadata) {
    const raw_node& raw_root = flat_schema_to_tree(metadata.schema);
    std::vector<node> fields;
    fields.reserve(raw_root.children.size());

    for (size_t i = 0; i < raw_root.children.size(); ++i) {
        fields.push_back(build_logical_node(raw_root.children[i], std::vector<std::string>(), 0, 0));
    }

    std::vector<const primitive_node*> leaves;
    for (const node& x : fields) {
        collect_leaves(x, leaves);
    }

    return root_node{raw_root.info, std::move(fields), std::move(leaves)};
}

} // namespace parquet::schema
