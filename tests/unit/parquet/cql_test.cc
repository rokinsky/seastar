#include "gtest_util.h"
#include "test_util.h"
#include <seastar/parquet/parquet/file_reader.h>
#include <seastar/parquet/parquet/logical_schema.h>
#include <seastar/parquet/parquet/cql_schema.h>
#include <seastar/parquet/parquet/cql_reader.h>
#include <map>
#include <sstream>

namespace parquet {

std::string data_file(const char* file) {
  std::string dir_string(test::get_data_dir());
  std::stringstream ss;
  ss << dir_string << "/" << file;
  return ss.str();
}

TEST(TestCQL, DumpOutput) {
  for (const auto& [filename, output] : std::map<std::string, std::string>{
    {"nested_maps.snappy.parquet", R"###(
CREATE TABLE parquet (id timeuuid PRIMARY KEY, a frozen<map<blob, frozen<map<int, boolean>>>>, b int, c double);
{a: {"a": {1: 1, 2: 0}}, b: 1, c: 1}
{a: {"b": {1: 1}}, b: 1, c: 1}
{a: {"c": null}, b: 1, c: 1}
{a: {"d": {}}, b: 1, c: 1}
{a: {"e": {1: 1}}, b: 1, c: 1}
{a: {"f": {3: 1, 4: 0, 5: 1}}, b: 1, c: 1}
)###"},
    {"nested_lists.snappy.parquet", R"###(
CREATE TABLE parquet (id timeuuid PRIMARY KEY, a frozen<list<frozen<list<frozen<list<blob>>>>>>, b int);
{a: [[["a", "b"], ["c"]], [null, ["d"]]], b: 1}
{a: [[["a", "b"], ["c", "d"]], [null, ["e"]]], b: 1}
{a: [[["a", "b"], ["c", "d"], ["e"]], [null, ["f"]]], b: 1}
)###"},
    {"alltypes_plain.snappy.parquet", R"###(
CREATE TABLE parquet (id timeuuid PRIMARY KEY, id int, bool_col boolean, tinyint_col int, smallint_col int, int_col int, bigint_col bigint, float_col float, double_col double, date_string_col blob, string_col blob, timestamp_col varint);
{id: 6, bool_col: 1, tinyint_col: 0, smallint_col: 0, int_col: 0, bigint_col: 0, float_col: 0, double_col: 0, date_string_col: "04/01/09", string_col: "0", timestamp_col: 0 0 2454923 }
{id: 7, bool_col: 0, tinyint_col: 1, smallint_col: 1, int_col: 1, bigint_col: 10, float_col: 1.1, double_col: 10.1, date_string_col: "04/01/09", string_col: "1", timestamp_col: 4165425152 13 2454923 }
)###"},
    {"repeated_no_annotation.parquet", R"###(
CREATE TYPE .phoneNumbers.phone(number: bigint, kind: blob);
CREATE TYPE .phoneNumbers(phone: frozen<list<.phoneNumbers.phone>>);
CREATE TABLE parquet (id timeuuid PRIMARY KEY, id int, phoneNumbers .phoneNumbers);
{id: 1}
{id: 2}
{id: 3, phoneNumbers: {phone: []}}
{id: 4, phoneNumbers: {phone: [{number: 5555555555, kind: null}]}}
{id: 5, phoneNumbers: {phone: [{number: 1111111111, kind: "home"}]}}
{id: 6, phoneNumbers: {phone: [{number: 1111111111, kind: "home"}, {number: 2222222222, kind: null}, {number: 3333333333, kind: "mobile"}]}}
)###"},
    {"int64_decimal.parquet", R"###(
CREATE TABLE parquet (id timeuuid PRIMARY KEY, value bigint);
{value: 100}
{value: 200}
{value: 300}
{value: 400}
{value: 500}
{value: 600}
{value: 700}
{value: 800}
{value: 900}
{value: 1000}
{value: 1100}
{value: 1200}
{value: 1300}
{value: 1400}
{value: 1500}
{value: 1600}
{value: 1700}
{value: 1800}
{value: 1900}
{value: 2000}
{value: 2100}
{value: 2200}
{value: 2300}
{value: 2400}
)###"},
  }) {
    std::stringstream ss;
    ss << '\n';
    auto source = seastarized::RandomAccessFile::Open(data_file(filename.c_str())).get0();
    auto file_reader = seastarized::ParquetFileReader::Open(source).get0();
    auto logical_schema = std::make_shared<seastarized::logical_schema::RootNode>(
        seastarized::logical_schema::BuildLogicalSchema(file_reader->metadata()->schema()));
    ss << seastarized::cql::CqlSchemaFromLogicalSchema(*logical_schema);
    auto row_group_reader = file_reader->RowGroup(0);
    auto record_reader = seastarized::cql::RecordReader::Make(row_group_reader.get(), logical_schema);
    record_reader.ReadAll(ss).get();
    ASSERT_EQ(ss.str(), output);
  }
}

} // namespace parquet
