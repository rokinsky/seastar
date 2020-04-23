/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2020 ScyllaDB
 */

#include <seastar/parquet/writer_schema.hh>

namespace parquet::writer_schema {

std::vector<format::SchemaElement> write_schema(const schema &root) {
    std::vector<format::SchemaElement> flat_schema;

    format::SchemaElement root_element;
    root_element.__set_num_children(root.fields.size());
    root_element.__set_name("schema");
    flat_schema.push_back(root_element);

    auto convert = y_combinator{[&](auto&& convert, const node& node_variant, std::vector<std::string> path_in_schema) -> void {
        format::FieldRepetitionType::type REQUIRED = format::FieldRepetitionType::REQUIRED;
        format::FieldRepetitionType::type OPTIONAL = format::FieldRepetitionType::OPTIONAL;
        format::FieldRepetitionType::type REPEATED = format::FieldRepetitionType::REPEATED;
        std::visit(overloaded {
                [&] (const list_node& x) {
                    format::SchemaElement group_element;
                    group_element.__set_num_children(1);
                    group_element.__set_name(*path_in_schema.rbegin());
                    group_element.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    group_element.__set_converted_type(format::ConvertedType::LIST);
                    format::LogicalType logical_type;
                    logical_type.__set_LIST({});
                    group_element.__set_logicalType(logical_type);
                    flat_schema.push_back(group_element);

                    path_in_schema.emplace_back("list");
                    format::SchemaElement repeated_element;
                    repeated_element.__set_num_children(1);
                    repeated_element.__set_name(*path_in_schema.rbegin());
                    repeated_element.__set_repetition_type(REPEATED);
                    flat_schema.push_back(repeated_element);

                    path_in_schema.emplace_back("element");
                    convert(*x.element, std::move(path_in_schema));
                },
                [&] (const map_node& x) {
                    format::SchemaElement group_element;
                    group_element.__set_num_children(1);
                    group_element.__set_name(*path_in_schema.rbegin());
                    group_element.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    group_element.__set_converted_type(format::ConvertedType::MAP);
                    format::LogicalType logical_type;
                    logical_type.__set_MAP({});
                    group_element.__set_logicalType(logical_type);
                    flat_schema.push_back(group_element);

                    path_in_schema.emplace_back("key_value");
                    format::SchemaElement repeated_element;
                    repeated_element.__set_num_children(2);
                    repeated_element.__set_name(*path_in_schema.rbegin());
                    repeated_element.__set_repetition_type(REPEATED);
                    flat_schema.push_back(repeated_element);

                    bool key_is_optional = std::visit([](auto& k){return k.optional;}, *x.key);
                    if (key_is_optional) {
                        throw parquet_exception("Map key must not be optional");
                    };
                    path_in_schema.emplace_back("key");
                    convert(*x.key, path_in_schema);

                    path_in_schema.pop_back();
                    path_in_schema.emplace_back("value");
                    convert(*x.value, std::move(path_in_schema));
                },
                [&] (const struct_node& x) {
                    format::SchemaElement group_element;
                    group_element.__set_num_children(x.fields.size());
                    group_element.__set_name(*path_in_schema.rbegin());
                    group_element.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    flat_schema.push_back(group_element);

                    for (const node& child : x.fields) {
                        path_in_schema.push_back(std::visit([] (auto& x) {return x.name;}, child));
                        convert(child, path_in_schema);
                        path_in_schema.pop_back();
                    }
                },
                [&] (const primitive_node& x) {
                    format::SchemaElement leaf;
                    leaf.__set_type(std::visit([] (auto y) {return decltype(y)::physical_type;}, x.logical_type));
                    leaf.__set_name(*path_in_schema.rbegin());
                    if (x.type_length) { leaf.__set_type_length(*x.type_length); }
                    leaf.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    logical_type::write_logical_type(x.logical_type, leaf);
                    flat_schema.push_back(leaf);
                }
        }, node_variant);
    }};

    for (const node& field : root.fields) {
        std::string name = std::visit([](auto& x){return x.name;}, field);
        convert(field, std::vector<std::string>{name});
    }

    return flat_schema;
}

} // parquet::writer_schema
