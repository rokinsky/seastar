#include <seastar/testing/test_case.hh>
#include <seastar/core/thread.hh>
#include <seastar/parquet/file_reader.hh>
#include <seastar/parquet/schema.hh>
#include <seastar/parquet/cql_schema.hh>
#include <seastar/parquet/cql_reader.hh>
#include <map>
#include <sstream>

namespace parquet {

const char* get_data_dir() {
  const char* result = std::getenv("PARQUET_TEST_DATA");
  if (!result || !result[0]) {
    throw parquet_exception(
      "Please point the PARQUET_TEST_DATA environment variable to the test data directory");
  }
  return result;
}

std::string get_data_path(const std::string& filename) {
  std::stringstream ss;
  ss << get_data_dir() << "/" << filename;
  return ss.str();
}

SEASTAR_TEST_CASE(parquet_to_cql) {
    return seastar::async([] {
        std::map<std::string, std::string> test_cases = {
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
{id: 6, bool_col: 1, tinyint_col: 0, smallint_col: 0, int_col: 0, bigint_col: 0, float_col: 0, double_col: 0, date_string_col: "04/01/09", string_col: "0", timestamp_col: 0 0 2454923}
{id: 7, bool_col: 0, tinyint_col: 1, smallint_col: 1, int_col: 1, bigint_col: 10, float_col: 1.1, double_col: 10.1, date_string_col: "04/01/09", string_col: "1", timestamp_col: 4165425152 13 2454923}
)###"},
        {"repeated_no_annotation.parquet", R"###(
CREATE TYPE phoneNumbers.phone(number: bigint, kind: blob);
CREATE TYPE phoneNumbers(phone: frozen<list<phoneNumbers.phone>>);
CREATE TABLE parquet (id timeuuid PRIMARY KEY, id int, phoneNumbers phoneNumbers);
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
        }; // test_cases
        for (const auto& [filename, output] : test_cases) {
            std::stringstream ss;
            ss << '\n';
            auto reader = file_reader::open(get_data_path(filename)).get0();
            ss << cql::cql_schema_from_logical_schema(reader.schema());
            auto record_reader = cql::record_reader::make(reader, 0);
            record_reader.read_all(ss).get();
            BOOST_CHECK_EQUAL(ss.str(), output);
        }
    });
}

} // namespace parquet
