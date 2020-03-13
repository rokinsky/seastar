#pragma once

#include <seastar/parquet/file_reader.hh>
#include <ostream>

namespace parquet::cql {
seastar::future<> parquet_to_cql(file_reader& fr, std::ostream& out);
} // namespace parquet::cql
