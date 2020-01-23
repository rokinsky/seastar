#pragma once

#include <seastar/parquet/parquet/schema.h>
#include <variant>
#include <memory>
#include <vector>

namespace parquet::seastarized::logical_schema {

using Node = std::variant<
  struct PrimitiveNode,
  struct OptionalNode,
  struct StructNode,
  struct ListNode,
  struct MapNode
>;

struct NodeBase {
  std::string name;
  std::string path;
  int def_level;
  int rep_level;
};

struct PrimitiveNode : NodeBase {
  Type::type physical_type;
  int column_index;
};

struct ListNode : NodeBase {
  std::unique_ptr<Node> element_node;
};

struct MapNode : NodeBase {
  std::unique_ptr<Node> key_node;
  std::unique_ptr<Node> value_node;
};

struct StructNode : NodeBase {
  std::vector<Node> field_nodes;
};

struct OptionalNode : NodeBase {
  std::unique_ptr<Node> child_node;
};

struct RootNode {
  std::vector<Node> field_nodes;
};

RootNode BuildLogicalSchema(const SchemaDescriptor* schema);

} // namespace parquet::seastarized::logical_schema
