#pragma once

#include <seastar/parquet/column_chunk_reader.hh>
#include <seastar/parquet/schema.hh>
#include <seastar/core/file.hh>

namespace parquet {

// The entry class of this library. All other objects are spawned from the file_reader,
// and are forbidden from outliving their parent file_reader.
class file_reader {
    std::string _path;
    seastar::file _file;
    std::unique_ptr<format::FileMetaData> _metadata;
    std::unique_ptr<schema::schema> _schema;
    std::unique_ptr<schema::raw_schema> _raw_schema;
private:
    file_reader() {};
    static seastar::future<std::unique_ptr<format::FileMetaData>> read_file_metadata(seastar::file file);
    template <format::Type::type T>
    seastar::future<column_chunk_reader<T>>
    open_column_chunk_reader_internal(uint32_t row_group, uint32_t column);
public:
    // The entry point to this library.
    static seastar::future<file_reader> open(std::string path);
    seastar::future<> close() { return _file.close(); };
    const std::string& path() const { return _path; }
    seastar::file file() const { return _file; }
    const format::FileMetaData& metadata() const { return *_metadata; }
    // The schemata are computed lazily (not on open) for robustness.
    // This way lower-level operations (i.e. inspecting metadata,
    // reading raw data with column_chunk_reader) can be done even if
    // higher level metadata cannot be understood/validated by our reader.
    const schema::raw_schema& raw_schema() {
        if (!_raw_schema) {
            _raw_schema = std::make_unique<schema::raw_schema>(schema::flat_schema_to_raw_schema(metadata().schema));
        }
        return *_raw_schema;
    }
    const schema::schema& schema() {
        if (!_schema) {
            _schema = std::make_unique<schema::schema>(schema::raw_schema_to_schema(raw_schema()));
        }
        return *_schema;
    }

    // Open a column_chunk_reader. The child column_chunk_reader MUST NOT outlive the parent file_reader.
    template <format::Type::type T>
    seastar::future<column_chunk_reader<T>> open_column_chunk_reader(uint32_t row_group, uint32_t column);
};

extern template seastar::future<column_chunk_reader<format::Type::INT32>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::INT64>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::INT96>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::FLOAT>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::DOUBLE>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::BOOLEAN>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::BYTE_ARRAY>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);

} // namespace parquet
