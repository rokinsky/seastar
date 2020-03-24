#include <seastar/parquet/untyped_schema.hh>
#include <seastar/parquet/exception.hh>
#include <seastar/parquet/overloaded.hh>
#include <seastar/core/print.hh>

namespace parquet::schema {

namespace {

// The schema tree is stored as a flat vector in the metadata (obtained by walking
// the tree in preorder, because Thrift doesn't support recursive structures.
// We recover the tree structure by using the num_children attribute.
raw_schema compute_shape(const std::vector<format::SchemaElement>& flat_schema) {
    size_t index = 0;
    raw_node root = y_combinator{[&] (auto&& convert) -> raw_node {
        if (index >= flat_schema.size()) {
            throw parquet_exception::corrupted_file("Could not build schema tree: unexpected end of flat schema");
        }
        const format::SchemaElement &current = flat_schema[index];
        ++index;
        if (current.__isset.num_children) {
            // Group node
            if (current.num_children < 0) {
                throw parquet_exception::corrupted_file("Could not build schema tree: negative num_children");
            }
            std::vector<raw_node> children;
            children.reserve(current.num_children);
            for (int i = 0; i < current.num_children; ++i) {
                children.push_back(convert());
            }
            return raw_node{current, std::move(children)};
        } else {
            // Primitive node
            return raw_node{current, std::vector<raw_node>()};
        }
    }}();
    return {std::move(root)};
}

// Assign the column_index to each primitive (leaf) node of the schema.
void compute_leaves(raw_schema& raw_schema) {
    y_combinator{[&] (auto&& compute, raw_node& r) -> void {
        if (r.children.empty()) {
            // Primitive node
            r.column_index = raw_schema.leaves.size();
            raw_schema.leaves.push_back(&r);
        } else {
            // Group node
            r.column_index = -1;
            for (raw_node& child : r.children) {
                compute(child);
            }
        }
    }}(raw_schema.root);
}

// Recursively compute the definition and repetition levels of every node.
void compute_levels(raw_schema& raw_schema) {
    y_combinator{[&] (auto&& compute, raw_node& r, uint32_t def, uint32_t rep) -> void {
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
    }}(raw_schema.root, 0, 0);
}

void compute_path(raw_schema& raw_schema) {
    auto compute = y_combinator{[&] (auto&& compute, raw_node& r, std::vector<std::string> path) -> void {
        path.push_back(r.info.name);
        for (raw_node& child : r.children) {
            compute(child, path);
        }
        r.path = std::move(path);
    }};
    for (raw_node& child : raw_schema.root.children) {
        compute(child, std::vector<std::string>());
    }
}

} // namespace

raw_schema flat_schema_to_raw_schema(const std::vector<format::SchemaElement>& flat_schema) {
    raw_schema raw_schema = compute_shape(flat_schema);
    compute_leaves(raw_schema);
    compute_levels(raw_schema);
    compute_path(raw_schema);
    return raw_schema;
}

} // namespace parquet::schema
