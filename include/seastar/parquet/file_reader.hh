#pragma once

#include <seastar/parquet/exception.hh>
#include <seastar/parquet/schema.hh>
#include <seastar/parquet/parquet_types.h>
#include <seastar/parquet/rle_encoding.hh>
#include <seastar/parquet/overloaded.hh>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>

#include <optional>
#include <variant>

namespace parquet {

/* A dynamically sized buffer. Rounds up the size given in constructor to a power of 2.
 */
class buffer {
    size_t _size;
    std::unique_ptr<uint8_t[]> _data;
    static constexpr inline uint64_t next_power_of_2(uint64_t n) {
        if (n < 2) return n;
        return 1ull << seastar::log2ceil(n);
    }
public:
    explicit buffer(size_t size = 0)
        : _size(next_power_of_2(size))
        , _data(new uint8_t[_size]) {}
    uint8_t* data() { return _data.get(); }
    size_t size() { return _size; }
};

/* The problem: we need to read a stream of objects of unknown, variable size (page headers)
 * placing each of them into contiguous memory for deserialization. Because we can only learn the size
 * of a page header after we deserialize it, we will inevitably read too much from the source stream,
 * and then we have to move the leftovers around to keep them contiguous with future reads.
 * peekable_stream takes care of that.
 */
class peekable_stream {
    seastar::input_stream<char> _source;
    buffer _buffer;
    size_t _buffer_start = 0;
    size_t _buffer_end = 0;
private:
    void ensure_space(size_t n);
    seastar::future<> read_exactly(size_t n);
public:
    explicit peekable_stream(seastar::input_stream<char>&& source)
        : _source{std::move(source)} {};

    // Assuming there is k bytes remaining in stream, view the next unconsumed min(k, n) bytes.
    seastar::future<std::basic_string_view<uint8_t>> peek(size_t n);
    // Consume n bytes. If there is less than n bytes in stream, throw.
    seastar::future<> advance(size_t n);
};

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
    explicit page_reader(seastar::input_stream<char>&& source)
        : _source{std::move(source)}
        , _latest_header{std::make_unique<format::PageHeader>()} {};
    // View the next page. Returns an empty result on eof.
    seastar::future<std::optional<page>> next_page();
};

// A uniform interface to multiple compression algorithms.
class decompressor {
    const format::CompressionCodec::type _codec;
    buffer _buffer;
public:
    explicit decompressor(format::CompressionCodec::type codec) : _codec{codec} {}
    // View the decompressed data. Will throw if the size of decompressed data doesn't match decompressed_len.
    std::basic_string_view<uint8_t> operator()(std::basic_string_view<uint8_t> compressed, size_t decompressed_len);
};

/* There are two encodings used for definition and repetition levels: RLE and BIT_PACKED.
 * level_decoder provides a uniform interface to them.
 */
class level_decoder {
    std::variant<RleDecoder, BitReader> _decoder;
    uint32_t _bit_width;
    uint32_t _num_values;
    uint32_t _values_read;
    uint32_t bit_width(uint32_t max_n) {
        return (max_n == 0) ? 0 : seastar::log2floor(max_n) + 1;
    }
public:
    explicit level_decoder(uint32_t max_level) : _bit_width(bit_width(max_level)) {}

    // Set a new source of levels. V1 and V2 are for data pages V1 and V2 respectively.
    // In data pages V1 the size of levels is not specified in the metadata,
    // so reset_v1 receives a view of the full page and returns the number of bytes consumed.
    size_t reset_v1(std::basic_string_view<uint8_t> buffer, format::Encoding::type encoding, uint32_t num_values);
    // reset_v2 is passed a view of the levels only, because the size of levels is known in advance.
    void reset_v2(std::basic_string_view<uint8_t> encoded_levels, uint32_t num_values);

    // Read a batch of n levels (the last batch may be smaller than n).
    template <typename T>
    uint32_t read_batch(uint32_t n, T out[]) {
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

/* Refer to the parquet documentation for the description of supported encodings:
 * https://github.com/apache/parquet-format/blob/master/Encodings.md
 * doc/parquet/Encodings.md
 */

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

template <typename T>
class dict_decoder {
    T* _dict;
    size_t _dict_size;
    RleDecoder _rle_decoder;
public:
    explicit dict_decoder(T dict[], size_t dict_size)
        : _dict(dict)
        , _dict_size(dict_size) {};
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, T out[]);
};

class rle_decoder_boolean {
    RleDecoder _rle_decoder;
public:
    void reset(std::basic_string_view<uint8_t> data);
    size_t read_batch(size_t n, uint8_t out[]);
};

template <typename T>
class delta_binary_packed_decoder {
    BitReader _decoder;
    int32_t _values_current_block;
    int32_t _num_mini_blocks;
    uint64_t _values_per_mini_block;
    uint64_t _values_current_mini_block;
    int32_t _min_delta;
    size_t _mini_block_idx;
    buffer _delta_bit_widths;
    int _delta_bit_width;
    int32_t _last_value;
private:
    void init_block();
public:
    size_t read_batch(size_t n, T out[]);
    void reset(std::basic_string_view<uint8_t> data);
};

template<format::Type::type T>
struct value_decoder_traits;

// output_type = the c++ type which we will use to return the values in
// decoder_type = the variant of all supported decoders for a given encoding

template<> struct value_decoder_traits<format::Type::INT32> {
    using output_type = int32_t;
    using decoder_type = std::variant<
        plain_decoder_trivial<output_type>,
        dict_decoder<output_type>,
        delta_binary_packed_decoder<output_type>
    >;
};

template<> struct value_decoder_traits<format::Type::INT64> {
    using output_type = int64_t;
    using decoder_type = std::variant<
        plain_decoder_trivial<output_type>,
        dict_decoder<output_type>,
        delta_binary_packed_decoder<output_type>
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

// A uniform interface to all the various value decoders.
template<format::Type::type T>
class value_decoder {
public:
    using output_type = typename value_decoder_traits<T>::output_type;
    using decoder_type = typename value_decoder_traits<T>::decoder_type;
private:
    decoder_type _decoder;
    std::optional<uint32_t> _type_length;
    bool _dict_set = false;
    output_type* _dict = nullptr;
    size_t _dict_size = 0;
public:
    value_decoder(std::optional<uint32_t>(type_length))
        : _type_length(type_length) {
        if constexpr (T == format::Type::FIXED_LEN_BYTE_ARRAY) {
            if (!_type_length) {
                throw parquet_exception::corrupted_file("type_length not set for FIXED_LEN_BYTE_ARRAY");
            }
        }
    }
    // Set a new dictionary (to be used for decoding RLE_DICTIONARY) for this reader.
    void reset_dict(output_type* dictionary, size_t dictionary_size);
    // Set a new source of encoded data.
    void reset(std::basic_string_view<uint8_t> buf, format::Encoding::type encoding);
    // Read a batch of n values (the last batch may be smaller than n).
    size_t read_batch(size_t n, output_type out[]);
};

// The core low-level interface. Takes the relevant metadata and an input_stream set to the beginning of a column chunk
// and extracts batches of (repetition level, definition level, value (optional)) from it.
template<format::Type::type T>
class column_chunk_reader {
public:
    using output_type = typename value_decoder_traits<T>::output_type;
    const schema::primitive_node& _schema_node;
private:
    page_reader _source;
    decompressor _decompressor;
    level_decoder _rep_decoder;
    level_decoder _def_decoder;
    value_decoder<T> _val_decoder;
    std::optional<std::vector<output_type>> _dict;
    bool _initialized = false;
    bool _eof = false;
    int64_t _page_ordinal = -1; // Only used for error reporting.
private:
    seastar::future<> load_next_page();
    void load_dictionary_page(page p);
    void load_data_page(page p);
    void load_data_page_v2(page p);

    template<typename LevelT>
    seastar::future<size_t> read_batch_internal(size_t n, LevelT def[], LevelT rep[], output_type val[]);
public:
    explicit column_chunk_reader(
            const schema::primitive_node& schema_node,
            page_reader&& source,
            format::CompressionCodec::type codec)
        : _schema_node{schema_node}
        , _source{std::move(source)}
        , _decompressor{codec}
        , _rep_decoder{schema_node.rep_level}
        , _def_decoder{schema_node.def_level}
        , _val_decoder{_schema_node.info.type_length} {};
    // Read a batch of n (rep, def, value) triplets. The last batch may be smaller than n.
    // Return the number of triplets read. Note that null values are not read into the output array.
    // Example output: def == [1, 1, 0, 1, 0], rep = [0, 0, 0, 0, 0], val = ["a", "b", "d"].
    template<typename LevelT>
    seastar::future<size_t> read_batch(size_t n, LevelT def[], LevelT rep[], output_type val[]);
};

template<format::Type::type T>
template<typename LevelT>
seastar::future<size_t>
column_chunk_reader<T>::read_batch_internal(size_t n, LevelT def[], LevelT rep[], output_type val[]) {
    if (_eof) {
        return seastar::make_ready_future<size_t>(0);
    }
    if (!_initialized) {
        return load_next_page().then([=] {
            return read_batch_internal(n, def, rep, val);
        });
    }
    size_t def_levels_read = _def_decoder.read_batch(n, def);
    size_t rep_levels_read = _rep_decoder.read_batch(n, rep);
    if (def_levels_read != rep_levels_read) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Number of definition levels {} does not equal the number of repetition levels {} in batch",
                def_levels_read, rep_levels_read));
    }
    if (def_levels_read == 0) {
        _initialized = false;
        return read_batch_internal(n, def, rep, val);
    }
    for (size_t i = 0; i < def_levels_read; ++i) {
        if (def[i] < 0 || def[i] > static_cast<LevelT>(_schema_node.def_level)) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "Definition level ({}) out of range (0 to {})", def[i], _schema_node.def_level));
        }
        if (rep[i] < 0 || rep[i] > static_cast<LevelT>(_schema_node.rep_level)) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "Repetition level ({}) out of range (0 to {})", rep[i], _schema_node.rep_level));
        }
    }
    size_t values_to_read = 0;
    for (size_t i = 0; i < def_levels_read; ++i) {
        if (def[i] == static_cast<LevelT>(_schema_node.def_level)) {
            ++values_to_read;
        }
    }
    size_t values_read = _val_decoder.read_batch(values_to_read, val);
    if (values_read != values_to_read) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Number of values in batch {} is less than indicated by def levels {}", values_read, values_to_read));
    }
    return seastar::make_ready_future<size_t>(def_levels_read);
}

template<format::Type::type T>
template<typename LevelT>
seastar::future<size_t>
inline column_chunk_reader<T>::read_batch(size_t n, LevelT def[], LevelT rep[], output_type val[]) {
    return seastar::futurize_apply([=] {
        return read_batch_internal(n, def, rep, val);
    }).handle_exception([this] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            return seastar::make_exception_future<size_t>(parquet_exception(seastar::format(
                    "Error while reading page number {} (in parquet column {} (path = {})): {}",
                    _page_ordinal, _schema_node.column_index, _schema_node.path, e.what())));
        }
    });
}

// The entry class of this library. All other objects are spawned from the file_reader,
// and are forbidden from outliving their parent file_reader.
class file_reader {
    std::string _path;
    seastar::file _file;
    std::unique_ptr<format::FileMetaData> _metadata;
    std::unique_ptr<schema::schema> _schema;
private:
    file_reader() {};
    static seastar::future<std::unique_ptr<format::FileMetaData>> read_file_metadata(seastar::file file);
    template <format::Type::type T>
    seastar::future<column_chunk_reader<T>>
    open_column_chunk_reader_internal(uint32_t row_group, uint32_t column) const;
public:
    // The entry point to this library.
    static seastar::future<file_reader> open(std::string&& path);
    seastar::future<> close() { return _file.close(); };
    const std::string& path() const { return _path; }
    seastar::file file() const { return _file; }
    const format::FileMetaData& metadata() const { return *_metadata; }
    const schema::schema& schema() const { return *_schema; }

    // Open a column_chunk_reader. The child column_chunk_reader MUST NOT outlive the parent file_reader.
    template <format::Type::type T>
    seastar::future<column_chunk_reader<T>> open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
};

extern template class column_chunk_reader<format::Type::INT32>;
extern template class column_chunk_reader<format::Type::INT64>;
extern template class column_chunk_reader<format::Type::INT96>;
extern template class column_chunk_reader<format::Type::FLOAT>;
extern template class column_chunk_reader<format::Type::DOUBLE>;
extern template class column_chunk_reader<format::Type::BOOLEAN>;
extern template class column_chunk_reader<format::Type::BYTE_ARRAY>;
extern template class column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>;
extern template class value_decoder<format::Type::INT32>;
extern template class value_decoder<format::Type::INT64>;
extern template class value_decoder<format::Type::INT96>;
extern template class value_decoder<format::Type::FLOAT>;
extern template class value_decoder<format::Type::DOUBLE>;
extern template class value_decoder<format::Type::BOOLEAN>;
extern template class value_decoder<format::Type::BYTE_ARRAY>;
extern template class value_decoder<format::Type::FIXED_LEN_BYTE_ARRAY>;
extern template seastar::future<column_chunk_reader<format::Type::INT32>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
extern template seastar::future<column_chunk_reader<format::Type::INT64>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
extern template seastar::future<column_chunk_reader<format::Type::INT96>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
extern template seastar::future<column_chunk_reader<format::Type::FLOAT>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
extern template seastar::future<column_chunk_reader<format::Type::DOUBLE>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
extern template seastar::future<column_chunk_reader<format::Type::BOOLEAN>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
extern template seastar::future<column_chunk_reader<format::Type::BYTE_ARRAY>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;
extern template seastar::future<column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) const;

} // namespace parquet
