#include <seastar/testing/test_case.hh>
#include <seastar/core/thread.hh>
#include <seastar/parquet/file_reader.hh>
#include <seastar/parquet/schema.hh>
#include <seastar/parquet/cql_schema.hh>
#include <seastar/parquet/cql_reader.hh>
#include <seastar/parquet/exception.hh>

namespace parquet {

const char* get_data_dir() {
  const char* result = std::getenv("PARQUET_TEST_DATA");
  if (!result || !result[0]) {
    throw parquet_exception(
      "Please point the PARQUET_TEST_DATA environment variable to the test data directory");
  }
  return result;
}

std::string get_data_file(const std::string& filename) {
  std::stringstream ss;
  ss << get_data_dir() << "/" << filename;
  return ss.str();
}

std::ostream& operator<<(std::ostream& out, const seastar::temporary_buffer<uint8_t>& b) {
    return out << std::string_view{reinterpret_cast<const char*>(b.get()), b.size()};
}

std::ostream& operator<<(std::ostream& out, const std::array<int32_t, 3>& a) {
    return out << a[0] << ':' << a[1] << ':' << a[2];
}

#if 0
template<format::Type::type T>
void print_column_chunk(column_chunk_reader<T> r) {
        std::ostream& out = std::cout;
        cql::typed_primitive_reader<T> xd{r._schema_node, std::move(r)};
        seastar::repeat([&] {
            return xd.current_levels().then([&] (int def, int) {
                if (def < 0) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                } else {
                    return xd.template read_field<cql::cql_consumer>(out).then([] { return seastar::stop_iteration::no; });
                }
            });
        }).get();
}
#endif

SEASTAR_TEST_CASE(parquet_file_reader) {
    return seastar::async([] {
        {
            std::string test_name = get_data_file("alltypes_plain.parquet");
            parquet::file_reader file_reader = file_reader::open(test_name).get0();
            BOOST_REQUIRE(file_reader.metadata().num_rows == 8);
            const format::ColumnMetaData& column_metadata =
                    file_reader.metadata().row_groups[0].columns[0].meta_data;
            page_reader page_reader{
                seastar::make_file_input_stream(
                        file_reader.file(),
                        column_metadata.dictionary_page_offset,
                        column_metadata.total_compressed_size)};
            page page = *page_reader.next_page().get0();
            std::cout << *page.header << '\n';
            for (int i = 0; i < page.header->compressed_page_size; ++i) {
                std::cout << (int)page.contents[i] << ' ';
            }
            std::cout << '\n';
            page = *page_reader.next_page().get0();
            std::cout << *page.header << '\n';
            for (int i = 0; i < page.header->compressed_page_size; ++i) {
                std::cout << (int)page.contents[i] << ' ';
            }
            std::cout << '\n';
            schema::schema root = schema::build_logical_schema(file_reader.metadata());
            std::cout << cql::cql_schema_from_logical_schema(root) << '\n';
        }
        {
            std::string test_name = get_data_file("nested_maps.snappy.parquet");
            parquet::file_reader file_reader = file_reader::open(test_name).get0();
            schema::schema root = schema::build_logical_schema(file_reader.metadata());
            std::cout << cql::cql_schema_from_logical_schema(root) << '\n';
        }
        {
            std::string test_name = get_data_file("nested_lists.snappy.parquet");
            parquet::file_reader file_reader = file_reader::open(test_name).get0();
            schema::schema root = schema::build_logical_schema(file_reader.metadata());
            std::cout << cql::cql_schema_from_logical_schema(root) << '\n';
        }
#if 0
        {
            std::string test_name = get_data_file("nested_maps.snappy.parquet");
            parquet::file_reader file_reader = file_reader::open(test_name).get0();
            schema::schema root = schema::build_logical_schema(file_reader.metadata());
            std::cout << file_reader.metadata();
            std::cout << cql::cql_schema_from_logical_schema(root) << '\n';
            for (size_t j = 0; j < file_reader.metadata().row_groups[0].columns.size(); ++j) {
                for (size_t i = 0; i < file_reader.metadata().row_groups.size(); ++i) {
                    const format::ColumnMetaData& column_metadata =
                            file_reader.metadata().row_groups[i].columns[j].meta_data;
                    int64_t file_offset = column_metadata.__isset.dictionary_page_offset
                            ? column_metadata.dictionary_page_offset
                            : column_metadata.data_page_offset;
                    page_reader page_reader{
                        seastar::make_file_input_stream(
                                file_reader.file(),
                                file_offset,
                                column_metadata.total_compressed_size)};
                    page_decompressor pd{std::move(page_reader), column_metadata.codec};
                    std::cout << root.leaves[j]->info << '\n';
                    const schema::primitive_node& pn = *root.leaves[j];
                    switch (pn.info.type) {
                    case format::Type::INT32: {
                        column_chunk_reader<format::Type::INT32> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    case format::Type::INT64: {
                        column_chunk_reader<format::Type::INT64> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    case format::Type::INT96: {
                        column_chunk_reader<format::Type::INT96> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    case format::Type::FLOAT: {
                        column_chunk_reader<format::Type::FLOAT> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    case format::Type::DOUBLE: {
                        column_chunk_reader<format::Type::DOUBLE> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    case format::Type::BOOLEAN: {
                        column_chunk_reader<format::Type::BOOLEAN> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    case format::Type::BYTE_ARRAY: {
                        column_chunk_reader<format::Type::BYTE_ARRAY> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    case format::Type::FIXED_LEN_BYTE_ARRAY: {
                        column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY> r(pn, std::move(pd));
                        print_column_chunk(std::move(r));
                        break;
                    }
                    }
                }
            }
        }
#endif
        {
            std::string test_name = get_data_file("repeated_no_annotation.parquet");
            parquet::file_reader file_reader = file_reader::open(test_name).get0();
            std::cout << file_reader.metadata();
            std::cout << cql::cql_schema_from_logical_schema(file_reader.schema()) << '\n';
            cql::record_reader r = cql::record_reader::make(file_reader, 0).get0();
            cql::cql_consumer cc{std::cout};
            r.read_all<cql::cql_consumer>(cc).get();
        }
    });
}

} // namespace parquet
