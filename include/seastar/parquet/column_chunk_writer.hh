#pragma once

#include <seastar/parquet/column_chunk_reader.hh>
#include <seastar/parquet/io.hh>
#include <boost/iterator/counting_iterator.hpp>
#include <unordered_map>
#include <vector>

namespace parquet {

using bytes = std::basic_string<uint8_t>;
using bytes_view = std::basic_string_view<uint8_t>;
using byte = bytes::value_type;

struct bytes_hasher {
    size_t operator()(const bytes& s) const {
        return std::hash<std::string_view>{}(std::string_view{reinterpret_cast<const char*>(s.data()), s.size()});
    }
};

constexpr uint32_t bit_width(uint32_t max_n) {
    return (max_n == 0) ? 0 : seastar::log2floor(max_n) + 1;
}

class rle_builder {
    size_t _buffer_offset = 0;
    bytes _buffer;
    uint32_t _bit_width;
    RleEncoder _encoder;
public:
    rle_builder(uint32_t bit_width)
        : _buffer(RleEncoder::MinBufferSize(bit_width), 0)
        , _bit_width{bit_width}
        , _encoder{_buffer.data(), static_cast<int>(_buffer.size()), static_cast<int>(_bit_width)}
        {};
    void put(uint64_t value) {
        while (!_encoder.Put(value)) {
            _encoder.Flush();
            _buffer_offset += _encoder.len();
            _buffer.resize(_buffer.size() * 2);
            _encoder = RleEncoder{
                    _buffer.data() + _buffer_offset,
                    static_cast<int>(_buffer.size() - _buffer_offset),
                    static_cast<int>(_bit_width)};
        }
    }
    void put_batch(uint64_t data[], size_t size) {
        for (size_t i = 0; i < size; ++i) {
            put(data[i]);
        }
    }
    void clear() {
        _buffer.clear();
        _buffer.resize(RleEncoder::MinBufferSize(_bit_width));
        _buffer_offset = 0;
        _encoder = RleEncoder{_buffer.data(), static_cast<int>(_buffer.size()), static_cast<int>(_bit_width)};
    }
    bytes_view view() {
        _encoder.Flush();
        return {_buffer.data(), _buffer_offset + _encoder.len()};
    }
    size_t max_encoded_size() const {
        return _buffer.size();
    }
};

template <format::Type::type ParquetType>
class dict_builder {
public:
    using input_type = typename value_decoder_traits<ParquetType>::output_type;
private:
    std::unordered_map<input_type, uint32_t> _accumulator;
    std::vector<input_type> _dict;
public:
    uint32_t put(input_type key) {
        auto [iter, was_new_key] = _accumulator.try_emplace(key, _dict.size());
        if (was_new_key) {
            _dict.push_back(key);
        }
        return iter->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const {
        size_t size = _dict.size() * sizeof(input_type);
        const uint8_t* data = reinterpret_cast<const uint8_t*>(_dict.data());
        return {data, size};
    }
};

template <>
class dict_builder<format::Type::BYTE_ARRAY> {
private:
    std::unordered_map<bytes, uint32_t, bytes_hasher> _accumulator;
    bytes _dict;
public:
    uint32_t put(bytes_view key) {
        auto [iter, was_new_key] = _accumulator.try_emplace(bytes{key}, _accumulator.size());
        if (was_new_key) {
            uint32_t size = static_cast<uint32_t>(key.size());
            _dict.insert(_dict.end(), reinterpret_cast<byte*>(&size), reinterpret_cast<byte*>(&size) + 4);
            _dict.insert(_dict.end(), key.begin(), key.end());
        }
        return iter->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return {_dict.data(), _dict.size()}; }
};

template <>
class dict_builder<format::Type::FIXED_LEN_BYTE_ARRAY> {
private:
    std::unordered_map<bytes, uint32_t, bytes_hasher> _accumulator;
    bytes _dict;
public:
    uint32_t put(bytes_view key) {
        auto [iter, was_new_key] = _accumulator.try_emplace(bytes{key}, _accumulator.size());
        if (was_new_key) {
            _dict.insert(_dict.end(), key.begin(), key.end());
        }
        return iter->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return {_dict.data(), _dict.size()}; }
};

template <format::Type::type ParquetType>
class value_encoder {
public:
    using input_type = typename value_decoder_traits<ParquetType>::input_type;
    virtual void put_batch(input_type data[], size_t size) = 0;
    virtual size_t max_encoded_size() const = 0;
    virtual size_t flush(uint8_t sink[]) = 0;
    virtual std::optional<std::basic_string_view<uint8_t>> view_dict() { return {}; };
    virtual uint64_t cardinality() { return 0; }
    virtual ~value_encoder() = default;
};

template <format::Type::type ParquetType>
class dict_encoder : public value_encoder<ParquetType> {
private:
    std::vector<uint32_t> _indices;
    dict_builder<ParquetType> _values;
private:
    int index_bit_width() const {
        return bit_width(_values.cardinality());
    }
public:
    using typename value_encoder<ParquetType>::input_type;
    void put_batch(input_type data[], size_t size) override {
        _indices.reserve(_indices.size() + size);
        for (size_t i = 0; i < size; ++i) {
            _indices.push_back(_values.put(data[i]));
        }
    }
    size_t max_encoded_size() const override {
        return 1 + std::max(
                RleEncoder::MinBufferSize(index_bit_width()),
                RleEncoder::MaxBufferSize(index_bit_width(), _indices.size()));
    }
    size_t flush(uint8_t sink[]) override {
        *sink = static_cast<uint8_t>(index_bit_width());
        RleEncoder encoder{sink + 1, static_cast<int>(max_encoded_size() - 1), index_bit_width()};
        for (uint32_t index : _indices) {
            encoder.Put(index);
        }
        encoder.Flush();
        _indices.clear();
        return 1 + encoder.len();
    }
    std::optional<std::basic_string_view<uint8_t>> view_dict() override {
        return _values.view();
    }
    uint64_t cardinality() override {
        return _values.cardinality();
    }
};

template <format::Type::type ParquetType>
class column_chunk_writer {
    thrift_serializer _thrift_serializer;
    rle_builder _rep_encoder;
    rle_builder _def_encoder;
    std::unique_ptr<value_encoder<ParquetType>> _val_encoder;
    std::vector<bytes> pages;
    std::vector<format::PageHeader> page_headers;
    uint64_t _levels_in_current_page = 0;
    uint64_t _values_in_current_page = 0;
    uint32_t _rep_level;
    uint32_t _def_level;
    uint64_t _rows_written = 0;
    size_t _estimated_chunk_size = 0;
public:
    using input_type = typename value_encoder<ParquetType>::input_type;

    column_chunk_writer(uint32_t def_level, uint32_t rep_level)
        : _rep_encoder{bit_width(rep_level)}
        , _def_encoder{bit_width(def_level)}
        , _val_encoder{new dict_encoder<ParquetType>{}}
        , _rep_level{rep_level}
        , _def_level{def_level}
        {}

    void put(uint32_t def_level, uint32_t rep_level, input_type val) {
        if (_rep_level > 0) {
            _rep_encoder.put(rep_level);
        }
        if (_rep_level == 0 || rep_level == 0) {
            ++_rows_written;
        }
        if (_def_level > 0) {
            _def_encoder.put(def_level);
        }
        if (_def_level == 0 || def_level == _def_level) {
            _val_encoder->put_batch(&val, 1);
        }
        ++_levels_in_current_page;
    }

    size_t current_page_max_size() const {
        size_t def_size = _def_level ? _def_encoder.max_encoded_size() : 0;
        size_t rep_size = _rep_level ? _rep_encoder.max_encoded_size() : 0;
        size_t value_size = _val_encoder->max_encoded_size();
        return def_size + rep_size + value_size;
    }

    void flush_page() {
        bytes page;
        size_t page_max_size = current_page_max_size();
        page.reserve(page_max_size);
        if (_rep_level > 0) {
            bytes_view levels = _rep_encoder.view();
            uint32_t size = levels.size();
            page.insert(page.end(), reinterpret_cast<uint8_t*>(&size), reinterpret_cast<uint8_t*>(&size) + 4);
            page.insert(page.end(), levels.begin(), levels.end());
        }
        if (_def_level > 0) {
            bytes_view levels = _def_encoder.view();
            uint32_t size = levels.size();
            page.insert(page.end(), reinterpret_cast<uint8_t*>(&size), reinterpret_cast<uint8_t*>(&size) + 4);
            page.insert(page.end(), levels.begin(), levels.end());
        }
        size_t data_offset = page.size();
        page.resize(page_max_size);
        size_t val_size = _val_encoder->flush(page.data() + data_offset);
        page.resize(data_offset + val_size);

        // TODO compress
        bytes compressed_page = page;

        format::DataPageHeader data_page_header;
        data_page_header.__set_num_values(_levels_in_current_page);
        // TODO other encodings
        data_page_header.__set_encoding(format::Encoding::RLE_DICTIONARY);
        data_page_header.__set_definition_level_encoding(format::Encoding::RLE);
        data_page_header.__set_repetition_level_encoding(format::Encoding::RLE);
        format::PageHeader page_header;
        page_header.__set_type(format::PageType::DATA_PAGE);
        page_header.__set_uncompressed_page_size(page.size());
        page_header.__set_compressed_page_size(compressed_page.size());
        page_header.__set_data_page_header(data_page_header);

        page_headers.push_back(page_header);
        pages.push_back(compressed_page);

        _def_encoder.clear();
        _rep_encoder.clear();
        _levels_in_current_page = 0;
        _values_in_current_page = 0;

        _estimated_chunk_size += compressed_page.size();
    }

    seastar::future<seastar::lw_shared_ptr<format::ColumnMetaData>> flush_chunk(seastar::output_stream<char>& sink) {
        if (_levels_in_current_page > 0) {
            flush_page();
        }
        auto metadata = seastar::make_lw_shared<format::ColumnMetaData>();
        metadata->__set_type(ParquetType);
        metadata->__set_encodings({format::Encoding::RLE, format::Encoding::RLE_DICTIONARY, format::Encoding::PLAIN});
        metadata->__set_codec(format::CompressionCodec::UNCOMPRESSED);
        metadata->__set_num_values(0);
        metadata->__set_total_compressed_size(0);
        metadata->__set_total_uncompressed_size(0);

        auto write_page = [this, metadata, &sink] (const format::PageHeader& header, bytes_view contents) {
            bytes_view serialized_header = _thrift_serializer.serialize(header);
            metadata->total_uncompressed_size += serialized_header.size() + header.uncompressed_page_size;
            metadata->total_compressed_size += serialized_header.size() + header.compressed_page_size;

            const char* data = reinterpret_cast<const char*>(serialized_header.data());
            return sink.write(data, serialized_header.size()).then([this, contents, &sink] {
                const char* data = reinterpret_cast<const char*>(contents.data());
                return sink.write(data, contents.size());
            });
        };

        return [this, metadata, write_page, &sink] {
            if (_val_encoder->view_dict()) {
                auto [header, contents] = dictionary_page();
                metadata->__set_dictionary_page_offset(metadata->total_compressed_size);
                return write_page(header, contents);
            } else {
                return seastar::make_ready_future<>();
            }
        }().then([this, write_page, metadata, &sink] {
            metadata->__set_data_page_offset(metadata->total_compressed_size);
            using it = boost::counting_iterator<size_t>;
            return seastar::do_for_each(it(0), it(page_headers.size()),
                [this, metadata, write_page, &sink] (size_t i) {
                metadata->num_values += page_headers[i].data_page_header.num_values;
                return write_page(page_headers[i], pages[i]);
            });
        }).then([this, metadata] {
            pages.clear();
            page_headers.clear();
            _estimated_chunk_size = 0;
            return metadata;
        });
    }

    size_t rows_written() const {
        return _rows_written;
    }

    size_t estimated_chunk_size() const {
        return _estimated_chunk_size;
    }

private:
    std::pair<format::PageHeader, bytes_view> dictionary_page() {
        bytes_view dict = *_val_encoder->view_dict();
        //TODO compress
        bytes_view compressed_dict = dict;

        format::DictionaryPageHeader dictionary_page_header;
        dictionary_page_header.__set_num_values(_val_encoder->cardinality());
        dictionary_page_header.__set_encoding(format::Encoding::PLAIN);
        dictionary_page_header.__set_is_sorted(false);
        format::PageHeader page_header;
        page_header.__set_type(format::PageType::DICTIONARY_PAGE);
        page_header.__set_uncompressed_page_size(dict.size());
        page_header.__set_compressed_page_size(compressed_dict.size());
        page_header.__set_dictionary_page_header(dictionary_page_header);

        return {page_header, compressed_dict};
    }
};

} // namespace parquet
