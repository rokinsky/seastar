#pragma once

#include <seastar/parquet/exception.hh>
#include <seastar/parquet/schema.hh>
#include <seastar/parquet/parquet_types.h>
#include <seastar/parquet/rle_encoding.hh>
#include <seastar/parquet/overloaded.hh>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>

#include <optional>
#include <variant>

namespace parquet {

////////////////////////////////////////////////////////////////////////////////

class file_reader {
    std::string _path;
    seastar::file _file;
    std::unique_ptr<format::FileMetaData> _metadata;
    std::unique_ptr<schema::root_node> _schema;
private:
    file_reader() {};
    static seastar::future<std::unique_ptr<format::FileMetaData>>
    read_file_metadata(seastar::file file);
public:
    static seastar::future<file_reader> open(std::string path);
    seastar::future<> close() { return _file.close(); };
    const std::string& path() const { return _path; }
    seastar::file file() const { return _file; }
    const format::FileMetaData& metadata() const { return *_metadata; }
    const schema::root_node& schema() const { return *_schema; }
};

////////////////////////////////////////////////////////////////////////////////

class peekable_stream {
    seastar::input_stream<char> _source;
    std::unique_ptr<uint8_t[]> _buffer;
    size_t _buffer_size = 0;
    size_t _buffer_start = 0;
    size_t _buffer_end = 0;
private:
    void ensure_space(size_t n);
    seastar::future<> read_exactly(size_t n);
public:
    explicit peekable_stream(seastar::input_stream<char> source)
        : _source{std::move(source)} {};
    seastar::future<std::basic_string_view<uint8_t>> peek(size_t n);
    seastar::future<> advance(size_t n);
};

////////////////////////////////////////////////////////////////////////////////

struct page {
    const format::PageHeader* header;
    std::basic_string_view<uint8_t> contents;
};

class page_reader {
    peekable_stream _source;
    std::unique_ptr<format::PageHeader> _latest_header;
    static constexpr uint32_t _default_expected_header_size = 1024;
    static constexpr uint32_t _max_allowed_header_size = 16 * 1024 * 1024;
public:
    explicit page_reader(seastar::input_stream<char> source)
        : _source{std::move(source)}
        , _latest_header{std::make_unique<format::PageHeader>()} {};
    seastar::future<std::optional<page>>
    next_page(uint32_t expected_header_size=_default_expected_header_size);
};

////////////////////////////////////////////////////////////////////////////////

class page_decompressor {
    page_reader _source;
    const format::CompressionCodec::type _codec;
    std::unique_ptr<uint8_t[]> _buffer;
    size_t _buffer_size = 0;
public:
    explicit page_decompressor(page_reader source, format::CompressionCodec::type codec)
        : _source{std::move(source)}
        , _codec{codec} {}
    seastar::future<std::optional<page>> next_page();
};

////////////////////////////////////////////////////////////////////////////////

class level_decoder {
    std::variant<RleDecoder, BitReader> _decoder;
    int _bit_width;
    int _num_values;
    int _values_read;
public:
    size_t reset(
            std::basic_string_view<uint8_t> buffer,
            format::Encoding::type encoding,
            int num_values,
            int max_level);

    template <typename T>
    size_t read_batch(int n, T *out) {
        n = std::min(n, _num_values - _values_read);
        if (_bit_width == 0) {
            std::fill(out, out + n, 0);
            _values_read += n;
            return n;
        }
        return std::visit(overloaded {
            [this, n, out] (BitReader& r) {
                size_t n_read = r.GetBatch(_bit_width, out, n);
                _values_read += n_read;
                return n_read;
            },
            [this, n, out] (RleDecoder& r) {
                size_t n_read = r.GetBatch(out, n);
                _values_read += n_read;
                return n_read;
            },
        }, _decoder);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class plain_decoder_trivial {
    std::basic_string_view<uint8_t> _buffer;
public:
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, T out[]);
};

class plain_decoder_boolean {
    BitReader _decoder;
public:
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, uint8_t out[]);
};

class plain_decoder_byte_array {
    seastar::temporary_buffer<uint8_t> _buffer;
public:
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]);
};

class plain_decoder_fixed_len_byte_array {
    size_t _fixed_len;
    seastar::temporary_buffer<uint8_t> _buffer;
public:
    explicit plain_decoder_fixed_len_byte_array(size_t fixed_len=0)
        : _fixed_len(fixed_len) {}
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]);
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class dict_decoder {
    T* _dict;
    size_t _dict_size;
    RleDecoder _rle_decoder;
public:
    explicit dict_decoder(T* dict, size_t dict_size)
        : _dict(dict)
        , _dict_size(dict_size) {};
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, T out[]);
};

////////////////////////////////////////////////////////////////////////////////

class rle_decoder_boolean {
    RleDecoder _rle_decoder;
public:
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, uint8_t out[]);
};

////////////////////////////////////////////////////////////////////////////////

template<format::Type::type T>
struct value_decoder_traits;

template<> struct value_decoder_traits<format::Type::INT32> {
    using output_type = int32_t;
    using decoder_type = std::variant<
        plain_decoder_trivial<output_type>,
        dict_decoder<output_type>
        // TODO DELTA_BINARY_PACKED
    >;
};

template<> struct value_decoder_traits<format::Type::INT64> {
    using output_type = int64_t;
    using decoder_type = std::variant<
        plain_decoder_trivial<output_type>,
        dict_decoder<output_type>
        // TODO DELTA_BINARY_PACKED
    >;
};

template<> struct value_decoder_traits<format::Type::INT96> {
    using output_type = std::array<int32_t, 3>;
    using decoder_type = std::variant<
        plain_decoder_trivial<output_type>,
        dict_decoder<output_type>
    >;
    static_assert(sizeof(output_type) == 12);
};

template<> struct value_decoder_traits<format::Type::FLOAT> {
    using output_type = float;
    using decoder_type = std::variant<
        plain_decoder_trivial<output_type>,
        dict_decoder<output_type>
        // TODO BYTE_STREAM_SPLIT
    >;
};

template<> struct value_decoder_traits<format::Type::DOUBLE> {
    using output_type = double;
    using decoder_type = std::variant<
        plain_decoder_trivial<output_type>,
        dict_decoder<output_type>
        // TODO BYTE_STREAM_SPLIT
    >;
};

template<> struct value_decoder_traits<format::Type::BOOLEAN> {
    using output_type = uint8_t;
    using decoder_type = std::variant<
        plain_decoder_boolean,
        rle_decoder_boolean,
        dict_decoder<output_type>
    >;
};

template<> struct value_decoder_traits<format::Type::BYTE_ARRAY> {
    using output_type = seastar::temporary_buffer<uint8_t>;
    using decoder_type = std::variant<
        plain_decoder_byte_array,
        dict_decoder<output_type>
        // TODO DELTA_BYTE_ARRAY
        // TODO DELTA_LENGTH_BYTE_ARRAY
    >;
};

template<> struct value_decoder_traits<format::Type::FIXED_LEN_BYTE_ARRAY> {
    using output_type = seastar::temporary_buffer<uint8_t>;
    using decoder_type = std::variant<
        plain_decoder_fixed_len_byte_array,
        dict_decoder<output_type>
    >;
};

////////////////////////////////////////////////////////////////////////////////

template<format::Type::type T>
class value_decoder {
public:
    using output_type = typename value_decoder_traits<T>::output_type;
    using decoder_type = typename value_decoder_traits<T>::decoder_type;
private:
    decoder_type _decoder;
public:
    void reset(
            std::basic_string_view<uint8_t> buf,
            format::Encoding::type encoding,
            int type_length,
            output_type dictionary[],
            size_t dictionary_size);
    size_t read_batch(size_t n, output_type out[]);
};

////////////////////////////////////////////////////////////////////////////////

template<format::Type::type T>
class column_chunk_reader {
public:
    using output_type = typename value_decoder_traits<T>::output_type;
    const schema::primitive_node& _schema_node;
private:
    page_decompressor _source;
    level_decoder _def_decoder;
    level_decoder _rep_decoder;
    value_decoder<T> _val_decoder;
    std::optional<std::vector<output_type>> _dict;
    bool _initialized = false;
    bool _eof = false;
private:
    seastar::future<> load_next_page();
    void load_dictionary_page(const page p);
    void load_data_page(const page p);
    void load_data_page_v2(const page p);
public:
    explicit column_chunk_reader(const schema::primitive_node& schema_node, page_decompressor source)
        : _schema_node(schema_node)
        , _source(std::move(source)) {};
    template<typename LevelT>
    seastar::future<size_t> read_batch(size_t n, LevelT def[], LevelT rep[], output_type val[]);
};

template<format::Type::type T>
template<typename LevelT>
seastar::future<size_t>
inline column_chunk_reader<T>::read_batch(size_t n, LevelT def[], LevelT rep[], output_type val[]) {
    if (_eof) {
        return seastar::make_ready_future<size_t>(0);
    }
    if (!_initialized) {
        return load_next_page().then([=] {
            return read_batch(n, def, rep, val);
        });
    }
    size_t def_levels_read = _def_decoder.read_batch(n, def);
    size_t rep_levels_read = _rep_decoder.read_batch(n, rep);
    if (def_levels_read != rep_levels_read) {
        throw parquet_exception::corrupted_file(
                "Number of definition levels does not equal the number of repetition levels");
    }
    if (def_levels_read == 0) {
        return load_next_page().then([=] {
            return read_batch(n, def, rep, val);
        });
    }
    size_t values_to_read = 0;
    for (size_t i = 0; i < def_levels_read; ++i) {
        if (def[i] < 0 || def[i] > _schema_node.def_level) {
            throw parquet_exception::corrupted_file("Definition level out of range");
        }
        if (def[i] == _schema_node.def_level) {
            ++values_to_read;
        }
    }
    size_t values_read = _val_decoder.read_batch(values_to_read, val);
    if (values_read != values_to_read) {
        throw parquet_exception::corrupted_file("Number of values in the page doesn't match the levels");
    }
    return seastar::make_ready_future<size_t>(def_levels_read);
}

} // namespace parquet
