#pragma once

#include <seastar/parquet/parquet/logical_schema.h>

namespace parquet::seastarized::cql {

std::string CqlSchemaFromLogicalSchema(const logical_schema::RootNode& schema);

} // namespace parquet::seastarized::cql
