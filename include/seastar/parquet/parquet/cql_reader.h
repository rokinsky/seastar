#pragma once

#include <boost/iterator/counting_iterator.hpp>
#include <seastar/parquet/parquet/file_reader.h>
#include <seastar/parquet/parquet/schema.h>
#include <seastar/parquet/parquet/logical_schema.h>
#include <ostream>

namespace parquet::seastarized::cql {

class FieldReader;

class RecordReader {
 public:
  ~RecordReader();
  seastar::future<> ReadOne(std::ostream &out);
  seastar::future<> ReadAll(std::ostream &out);
  seastar::future<int, int> CurrentLevels();
  static RecordReader Make(
      RowGroupReader *row_group_reader,
      std::shared_ptr<const logical_schema::RootNode> schema);

 private:
  explicit RecordReader(
      std::shared_ptr<const logical_schema::RootNode> schema,
      std::vector<std::unique_ptr<FieldReader>> field_readers);

  std::shared_ptr<const logical_schema::RootNode> schema_;
  std::vector<std::unique_ptr<FieldReader>> field_readers_;
  const char* separator_ = "";
};

} // namespace seastarized::parquet
