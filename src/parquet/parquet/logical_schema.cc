#include <seastar/parquet/parquet/logical_schema.h>
#include <seastar/parquet/parquet/exception.h>

namespace parquet::seastarized::logical_schema {

namespace {

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

Node BuildLogicalNode(
    const SchemaDescriptor* schema,
    const schema::Node* node,
    std::string path,
    int def_level,
    int rep_level
) {
  def_level = def_level + (node->is_repeated() || node->is_optional() ? 1 : 0);
  rep_level = rep_level + (node->is_repeated() ? 1 : 0);
  path += ".";
  path += node->name();

  Node result = [path, def_level, rep_level, node, schema] () -> Node {
    NodeBase node_base{node->name(), path, def_level, rep_level};

    if (node->is_primitive()) {
      return PrimitiveNode{
        std::move(node_base),
        static_cast<const schema::PrimitiveNode*>(node)->physical_type(),
        schema->GetColumnIndex(*static_cast<const schema::PrimitiveNode*>(node))};
    } else {
      const schema::GroupNode* group_node = static_cast<const schema::GroupNode*>(node);

      if (group_node->logical_type() && group_node->logical_type()->is_list()) {
        if (group_node->field_count() != 1 || group_node->is_repeated()) {
          throw ParquetException(std::string("Invalid list group_node: ") + group_node->name());
        }

        const schema::Node* repeated_node = group_node->field(0).get();
        if (!repeated_node->is_repeated()) {
          throw ParquetException(std::string("Invalid list node: ") + group_node->name());
        }

        if (repeated_node->is_primitive()
            || static_cast<const schema::GroupNode*>(group_node)->field_count() > 1
            || repeated_node->name().compare("array") == 0
            || repeated_node->name().compare(group_node->name() + "_tuple") == 0) {
          // Legacy 2-level list
          return ListNode{
            std::move(node_base),
            std::make_unique<Node>(BuildLogicalNode(schema, repeated_node, std::move(path),
                def_level, rep_level))};
        } else {
          // 3-level list
          const schema::Node* element_node =
              static_cast<const schema::GroupNode*>(repeated_node)->field(0).get();
          return ListNode{
            std::move(node_base),
            std::make_unique<Node>(BuildLogicalNode(schema, element_node, path + "." + repeated_node->name(),
                def_level + 1, rep_level + 1))};
        }
      } else if (group_node->logical_type() && group_node->logical_type()->is_map()) {
        if (group_node->field_count() != 1) {
          throw ParquetException(std::string("Invalid map node: ") + group_node->name());
        }

        const schema::Node* repeated_node = group_node->field(0).get();
        if (!repeated_node->is_group()
            || !repeated_node->is_repeated()
            || static_cast<const schema::GroupNode*>(repeated_node)->field_count() != 2) {
          throw ParquetException(std::string("Invalid map node: ") + group_node->name());
        }

        const schema::Node* key_node =
          static_cast<const schema::GroupNode*>(repeated_node)->field(0).get();
        const schema::Node* value_node =
          static_cast<const schema::GroupNode*>(repeated_node)->field(1).get();

        if (!key_node->is_primitive()) {
          throw ParquetException(std::string("Invalid map node: ") + group_node->name());
        }

        return MapNode{
          std::move(node_base),
          std::make_unique<Node>(BuildLogicalNode(schema, key_node, path + "." + repeated_node->name(),
              def_level + 1, rep_level + 1)),
          std::make_unique<Node>(BuildLogicalNode(schema, value_node, path + "." + repeated_node->name(),
              def_level + 1, rep_level + 1))};
      } else {
        std::vector<Node> fields;
        fields.reserve(group_node->field_count());
        for (int i = 0; i < group_node->field_count(); ++i) {
          fields.push_back(BuildLogicalNode(
              schema, group_node->field(i).get(), path, def_level, rep_level));
        }
        return StructNode{
          std::move(node_base),
          std::move(fields)};
      }
    }
  }();


  if (node->is_optional()) {
    return OptionalNode{
      {node->name(), std::move(path), def_level - 1, rep_level},
      std::make_unique<Node>(std::move(result))};
  } else if (node->is_repeated()) {
    return ListNode{
      {node->name(), std::move(path), def_level - 1, rep_level - 1},
      std::make_unique<Node>(std::move(result))};
  } else {
    return result;
  }
}

} // namespace

RootNode BuildLogicalSchema(const SchemaDescriptor *schema) {
  const schema::GroupNode* root_node = schema->group_node();
  int n_fields = root_node->field_count();
  std::vector<Node> fields;
  fields.reserve(n_fields);

  for (int i = 0; i < n_fields; ++i) {
    fields.push_back(BuildLogicalNode(schema, root_node->field(i).get(), std::string(), 0, 0));
  }

  return RootNode{std::move(fields)};
}

} // namespace parquet::seastarized::logical_schema
