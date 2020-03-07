#pragma once

#include <seastar/parquet/schema.hh>

namespace parquet::cql {

struct node {
    const schema::node& parquet_node;
    std::string cql_type;
    std::string identifier;
    std::vector<node> children;
};

struct cql_schema {
    const schema::schema& parquet_schema;
    std::vector<node> columns;
};

cql_schema build_cql_schema(const schema::schema& parquet_schema);
std::string cql_schema_from_logical_schema(const schema::schema& schema);

} // namespace
