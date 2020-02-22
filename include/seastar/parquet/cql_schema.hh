#pragma once

#include <seastar/parquet/schema.hh>

namespace parquet::cql {

std::string cql_schema_from_logical_schema(const schema::root_node& schema);

} // namespace
