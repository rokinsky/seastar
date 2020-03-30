#include <seastar/parquet/column_chunk_reader.hh>
#include "compression.hh"

namespace parquet {

seastar::future<std::optional<page>> page_reader::next_page() {
    *_latest_header = format::PageHeader{}; // Thrift does not clear the structure by itself before writing to it.
    return read_thrift_from_stream(_source, *_latest_header).then([this] (bool read) {
        if (!read) {
            return seastar::make_ready_future<std::optional<page>>();
        }
        if (_latest_header->compressed_page_size < 0) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "Negative compressed_page_size in header: {}", *_latest_header));
        }
        size_t compressed_size = static_cast<uint32_t>(_latest_header->compressed_page_size);
        return _source.peek(compressed_size).then(
        [this, compressed_size] (std::basic_string_view<uint8_t> page_contents) {
            if (page_contents.size() < compressed_size) {
                throw parquet_exception::corrupted_file(seastar::format(
                        "Unexpected end of column chunk while reading compressed page contents (expected {}B, got {}B)",
                        compressed_size, page_contents.size()));
            }
            return _source.advance(compressed_size).then([this, page_contents] {
                return seastar::make_ready_future<std::optional<page>>(page{_latest_header.get(), page_contents});
            });
        });
    });
}

std::basic_string_view<uint8_t>
decompressor::operator()(std::basic_string_view<uint8_t> input, size_t decompressed_len) {
    if (_codec == format::CompressionCodec::UNCOMPRESSED) {
        return input;
    } else {
        if (decompressed_len > _buffer.size()) {
            _buffer = buffer{decompressed_len};
        }
        switch (_codec) {
        case format::CompressionCodec::SNAPPY:
            compression::snappy_decompress(input.data(), input.size(), _buffer.data(), decompressed_len);
            break;
        case format::CompressionCodec::GZIP:
            compression::zlib_decompress(input.data(), input.size(), _buffer.data(), decompressed_len);
            break;
        // TODO GZIP, LZO, BROTLI, LZ4, ZSTD
        default:
            throw parquet_exception::not_implemented(seastar::format("Unsupported compression ({})", _codec));
        }
        return {_buffer.data(), decompressed_len};
    }
}

size_t level_decoder::reset_v1(
        std::basic_string_view<uint8_t> buffer,
        format::Encoding::type encoding,
        uint32_t num_values) {
    _num_values = num_values;
    _values_read = 0;
    if (_bit_width == 0) {
        return 0;
    }
    if (encoding == format::Encoding::RLE) {
        if (buffer.size() < 4) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading levels (needed {}B, got {}B)", 4, buffer.size()));
        }
        int32_t len;
        std::memcpy(&len, buffer.data(), 4);
        if (len < 0) {
            throw parquet_exception::corrupted_file(seastar::format("Negative RLE levels length ({})", len));
        }
        if (static_cast<size_t>(len) > buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading levels (needed {}B, got {}B)", len, buffer.size()));
        }
        _decoder = RleDecoder{buffer.data() + 4, len, static_cast<int>(_bit_width)};
        return 4 + len;
    } else if (encoding == format::Encoding::BIT_PACKED) {
        uint64_t bit_len = static_cast<uint64_t>(num_values) * _bit_width;
        uint64_t byte_len = (bit_len + 7) >> 3;
        if (byte_len > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            throw parquet_exception::corrupted_file(seastar::format("BIT_PACKED length exceeds int ({}B)", byte_len));
        }
        if (byte_len > buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading levels (needed {}B, got {}B)", byte_len, buffer.size()));
        }
        _decoder = BitReader{buffer.data(), static_cast<int>(byte_len)};
        return byte_len;
    } else {
        throw parquet_exception::not_implemented(seastar::format("Unknown level encoding ({})", encoding));
    }
}

void level_decoder::reset_v2(std::basic_string_view<uint8_t> encoded_levels, uint32_t num_values) {
    _num_values = num_values;
    _values_read = 0;
    if (encoded_levels.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Levels length exceeds int ({}B)", encoded_levels.size()));
    }
    _decoder = RleDecoder{
        encoded_levels.data(),
        static_cast<int>(encoded_levels.size()),
        static_cast<int>(_bit_width)};
}

template <typename T>
void plain_decoder_trivial<T>::reset(std::basic_string_view<uint8_t> data) {
    _buffer = data;
}

void plain_decoder_boolean::reset(std::basic_string_view<uint8_t> data) {
    _decoder.Reset(const_cast<const uint8_t*>(data.data()), data.size());
}

void plain_decoder_byte_array::reset(std::basic_string_view<uint8_t> data) {
    _buffer = seastar::temporary_buffer<uint8_t>(data.size());
    std::memcpy(_buffer.get_write(), data.data(), data.size());
}

void plain_decoder_fixed_len_byte_array::reset(std::basic_string_view<uint8_t> data) {
    _buffer = seastar::temporary_buffer<uint8_t>(data.size());
    std::memcpy(_buffer.get_write(), data.data(), data.size());
}

template <typename T>
size_t plain_decoder_trivial<T>::read_batch(size_t n, T out[]) {
    size_t n_to_read = std::min(_buffer.size() / sizeof(T), n);
    size_t bytes_to_read = sizeof(T) * n_to_read;
    if (bytes_to_read > 0) {
        std::memcpy(out, _buffer.data(), bytes_to_read);
    }
    _buffer.remove_prefix(bytes_to_read);
    return n_to_read;
}

size_t plain_decoder_boolean::read_batch(size_t n, uint8_t out[]) {
    return _decoder.GetBatch(1, out, n);
}

size_t plain_decoder_byte_array::read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]) {
    for (size_t i = 0; i < n; ++i) {
        if (_buffer.size() == 0) {
            return i;
        }
        if (_buffer.size() < 4) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading BYTE_ARRAY length (needed {}B, got {}B)", 4, _buffer.size()));
        }
        uint32_t len;
        std::memcpy(&len, _buffer.get(), 4);
        _buffer.trim_front(4);
        if (len > _buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading BYTE_ARRAY (needed {}B, got {}B)", len, _buffer.size()));
        }
        out[i] = _buffer.share(0, len);
        _buffer.trim_front(len);
    }
    return n;
}

size_t plain_decoder_fixed_len_byte_array::read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]) {
    for (size_t i = 0; i < n; ++i) {
        if (_buffer.size() == 0) {
            return i;
        }
        if (_fixed_len > _buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
                    "End of page while reading FIXED_LEN_BYTE_ARRAY (needed {}B, got {}B)",
                    _fixed_len, _buffer.size()));
        }
        out[i] = _buffer.share(0, _fixed_len);
        _buffer.trim_front(_fixed_len);
    }
    return n;
}

template <typename T>
void dict_decoder<T>::reset(std::basic_string_view<uint8_t> data) {
    if (data.size() == 0) {
        _rle_decoder.Reset(data.data(), data.size(), 0);
    }
    int bit_width = data.data()[0];
    if (bit_width < 0 || bit_width > 32) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Illegal dictionary index bit width (should be 0 <= bit width <= 32, got {})", bit_width));
    }
    _rle_decoder.Reset(data.data() + 1, data.size() - 1, bit_width);
}

template <typename T>
size_t dict_decoder<T>::read_batch(size_t n, T out[]) {
    std::array<uint32_t, 1000> buf;
    size_t completed = 0;
    while (completed < n) {
        size_t n_to_read = std::min(n - completed, buf.size());
        size_t n_read = _rle_decoder.GetBatch(buf.data(), n_to_read);
        for (size_t i = 0; i < n_read; ++i) {
            if (buf[i] > _dict_size) {
                throw parquet_exception::corrupted_file(seastar::format(
                        "Dict index exceeds dict size (dict size = {}, index = {})", _dict_size, buf[i]));
            }
        }
        for (size_t i = 0; i < n_read; ++i) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                out[completed + i] = _dict[buf[i]];
            } else {
                // seastar::temporary_buffer<uint8_t>
                out[completed + i] = _dict[buf[i]].share();
            }
        }
        completed += n_read;
        if (n_read < n_to_read) {
            return completed;
        }
    }
    return n;
}

void rle_decoder_boolean::reset(std::basic_string_view<uint8_t> data) {
    _rle_decoder.Reset(data.data(), data.size(), 1);
}

size_t rle_decoder_boolean::read_batch(size_t n, uint8_t out[]) {
    return _rle_decoder.GetBatch(out, n);
}

template <typename T>
void delta_binary_packed_decoder<T>::reset(std::basic_string_view<uint8_t> data) {
    _decoder.Reset(data.data(), data.size());
    _values_current_block = 0;
    _values_current_mini_block = 0;
}

template <typename T>
void delta_binary_packed_decoder<T>::init_block() {
    uint32_t block_size;
    if (!_decoder.GetVlqInt(&block_size)) { throw; }
    if (!_decoder.GetVlqInt(&_num_mini_blocks)) { throw; }
    if (!_decoder.GetVlqInt(&_values_current_block)) { throw; }
    if (!_decoder.GetZigZagVlqInt(&_last_value)) { throw; }

    if (_delta_bit_widths.size() < static_cast<uint32_t>(_num_mini_blocks)) {
        _delta_bit_widths = buffer(_num_mini_blocks);
    }

    if (!_decoder.GetZigZagVlqInt(&_min_delta)) { throw; };
    for (uint32_t i = 0; i < _num_mini_blocks; ++i) {
        if (!_decoder.GetAligned<uint8_t>(1, _delta_bit_widths.data() + i)) {
            throw;
        }
    }
    _values_per_mini_block = block_size / _num_mini_blocks;
    _mini_block_idx = 0;
    _delta_bit_width = _delta_bit_widths.data()[0];
    _values_current_mini_block = _values_per_mini_block;
}

template <typename T>
size_t delta_binary_packed_decoder<T>::read_batch(size_t n, T out[]) {
    try {
        for (size_t i = 0; i < n; ++i) {
            if (__builtin_expect(_values_current_mini_block == 0, 0)) {
                ++_mini_block_idx;
                if (_mini_block_idx < static_cast<size_t>(_num_mini_blocks)) {
                    _delta_bit_width = _delta_bit_widths.data()[_mini_block_idx];
                    _values_current_mini_block = _values_per_mini_block;
                } else {
                    init_block();
                    out[i] = _last_value;
                    continue;
                }
            }

            // TODO: the key to this algorithm is to decode the entire miniblock at once.
            int64_t delta;
            if (!_decoder.GetValue(_delta_bit_width, &delta)) { throw; }
            delta += _min_delta;
            _last_value += static_cast<int32_t>(delta);
            out[i] = _last_value;
            --_values_current_mini_block;
        }
    } catch (...) {
        throw parquet_exception::corrupted_file("Could not decode DELTA_BINARY_PACKED batch");
    }
    return n;
}

template<format::Type::type T>
void value_decoder<T>::reset_dict(output_type dictionary[], size_t dictionary_size) {
    _dict = dictionary;
    _dict_size = dictionary_size;
    _dict_set = true;
};

template<format::Type::type T>
void value_decoder<T>::reset(std::basic_string_view<uint8_t> buf, format::Encoding::type encoding) {
    switch (encoding) {
    case format::Encoding::PLAIN:
        if constexpr (T == format::Type::BOOLEAN) {
            _decoder = plain_decoder_boolean{};
        } else if constexpr (T == format::Type::BYTE_ARRAY) {
            _decoder = plain_decoder_byte_array{};
        } else if constexpr (T == format::Type::FIXED_LEN_BYTE_ARRAY) {
            _decoder = plain_decoder_fixed_len_byte_array{static_cast<size_t>(*_type_length)};
        } else {
            _decoder = plain_decoder_trivial<output_type>{};
        }
        break;
    case format::Encoding::RLE_DICTIONARY:
    case format::Encoding::PLAIN_DICTIONARY:
        if (!_dict_set) {
            throw parquet_exception::corrupted_file("No dictionary page found before a dictionary-encoded page");
        }
        _decoder = dict_decoder<output_type>{_dict, _dict_size};
        break;
    case format::Encoding::RLE:
        if constexpr (T == format::Type::BOOLEAN) {
            _decoder = rle_decoder_boolean{};
        } else {
            throw parquet_exception::corrupted_file("RLE encoding is valid only for BOOLEAN values");
        }
        break;
    case format::Encoding::DELTA_BINARY_PACKED:
        if constexpr (T == format::Type::INT32 || T == format::Type::INT64) {
            _decoder = delta_binary_packed_decoder<output_type>{};
        } else {
            throw parquet_exception::corrupted_file("DELTA_BINARY_PACKED is valid only for INT32 and INT64");
        }
        break;
    default:
        throw parquet_exception::not_implemented("Unsupported encoding");
    }
    std::visit([&buf] (auto& dec) { dec.reset(buf); }, _decoder);
};

template<format::Type::type T>
size_t value_decoder<T>::read_batch(size_t n, output_type out[]) {
    return std::visit([n, out] (auto& d) { return d.read_batch(n, out); }, _decoder);
};

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page(page p) {
    if (!p.header->__isset.data_page_header) {
        throw parquet_exception::corrupted_file(seastar::format(
                "DataPageHeader not set for DATA_PAGE header: {}", *p.header));
    }
    const format::DataPageHeader& header = p.header->data_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative num_values in header: {}", header));
    }
    if (p.header->uncompressed_page_size < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative uncompressed_page_size in header: {}", *p.header));
    }
    std::basic_string_view<uint8_t> contents = _decompressor(
            p.contents, static_cast<uint32_t>(p.header->uncompressed_page_size));
    size_t n_read = 0;
    n_read = _rep_decoder.reset_v1(contents, header.repetition_level_encoding, header.num_values);
    contents.remove_prefix(n_read);
    n_read = _def_decoder.reset_v1(contents, header.definition_level_encoding, header.num_values);
    contents.remove_prefix(n_read);
    _val_decoder.reset(contents, header.encoding);
}

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page_v2(page p) {
    if (!p.header->__isset.data_page_header_v2) {
        throw parquet_exception::corrupted_file(seastar::format(
                "DataPageHeaderV2 not set for DATA_PAGE_V2 header: {}", *p.header));
    }
    const format::DataPageHeaderV2& header = p.header->data_page_header_v2;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative num_values in header: {}", header));
    }
    if (header.repetition_levels_byte_length < 0 || header.definition_levels_byte_length < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative levels byte length in header: {}", header));
    }
    if (p.header->uncompressed_page_size < 0) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Negative uncompressed_page_size in header: {}", *p.header));
    }
    std::basic_string_view<uint8_t> contents = p.contents;
    _rep_decoder.reset_v2(contents.substr(0, header.repetition_levels_byte_length), header.num_values);
    contents.remove_prefix(header.repetition_levels_byte_length);
    _def_decoder.reset_v2(contents.substr(0, header.definition_levels_byte_length), header.num_values);
    contents.remove_prefix(header.definition_levels_byte_length);
    std::basic_string_view<uint8_t> values = contents;
    if (header.__isset.is_compressed && header.is_compressed) {
        size_t n_read = header.repetition_levels_byte_length + header.definition_levels_byte_length;
        size_t uncompressed_values_size = static_cast<size_t>(p.header->uncompressed_page_size) - n_read;
        values = _decompressor(contents, uncompressed_values_size);
    }
    _val_decoder.reset(values, header.encoding);
}

template<format::Type::type T>
void column_chunk_reader<T>::load_dictionary_page(page p) {
    if (!p.header->__isset.dictionary_page_header) {
        throw parquet_exception::corrupted_file(seastar::format(
                "DictionaryPageHeader not set for DICTIONARY_PAGE header: {}", *p.header));
    }
    const format::DictionaryPageHeader& header = p.header->dictionary_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    if (p.header->uncompressed_page_size < 0) {
        throw parquet_exception::corrupted_file(
                seastar::format("Negative uncompressed_page_size in header: {}", *p.header));
    }
    _dict = std::vector<output_type>(header.num_values);
    std::basic_string_view<uint8_t> decompressed_values =
            _decompressor(p.contents, static_cast<size_t>(p.header->uncompressed_page_size));
    value_decoder<T> vd{_type_length};
    vd.reset(decompressed_values, format::Encoding::PLAIN);
    size_t n_read = vd.read_batch(_dict->size(), _dict->data());
    if (n_read < _dict->size()) {
        throw parquet_exception::corrupted_file(seastar::format(
                "Unexpected end of dictionary page (expected {} values, got {})", _dict->size(), n_read));
    }
    _val_decoder.reset_dict(_dict->data(), _dict->size());
}

template<format::Type::type T>
seastar::future<> column_chunk_reader<T>::load_next_page() {
    ++_page_ordinal;
    return _source.next_page().then([this] (std::optional<page> p) {
        if (!p) {
            _eof = true;
        } else {
            switch (p->header->type) {
            case format::PageType::DATA_PAGE:
                load_data_page(*p);
                _initialized = true;
                return;
            case format::PageType::DATA_PAGE_V2:
                load_data_page_v2(*p);
                _initialized = true;
                return;
            case format::PageType::DICTIONARY_PAGE:
                load_dictionary_page(*p);
                return;
            default:; // Unknown page types are to be skipped
            }
        }
    });
}

template class column_chunk_reader<format::Type::INT32>;
template class column_chunk_reader<format::Type::INT64>;
template class column_chunk_reader<format::Type::INT96>;
template class column_chunk_reader<format::Type::FLOAT>;
template class column_chunk_reader<format::Type::DOUBLE>;
template class column_chunk_reader<format::Type::BOOLEAN>;
template class column_chunk_reader<format::Type::BYTE_ARRAY>;
template class column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>;
/*
 * Explicit instantiation of value_decoder shouldn't be needed,
 * because column_chunk_reader<T> has a value_decoder<T> member.
 * Yet, without explicit instantiation of value_decoder<T>,
 * value_decoder<T>::read_batch is not generated. Why?
 *
 * Quote: When an explicit instantiation names a class template specialization,
 * it serves as an explicit instantiation of the same kind (declaration or definition)
 * of each of its non-inherited non-template members that has not been previously
 * explicitly specialized in the translation unit. If this explicit instantiation
 * is a definition, it is also an explicit instantiation definition only for
 * the members that have been defined at this point.
 * https://en.cppreference.com/w/cpp/language/class_template
 *
 * Has value_decoder<T> not been "defined at this point" or what?
 */
template class value_decoder<format::Type::INT32>;
template class value_decoder<format::Type::INT64>;
template class value_decoder<format::Type::INT96>;
template class value_decoder<format::Type::FLOAT>;
template class value_decoder<format::Type::DOUBLE>;
template class value_decoder<format::Type::BOOLEAN>;
template class value_decoder<format::Type::BYTE_ARRAY>;
template class value_decoder<format::Type::FIXED_LEN_BYTE_ARRAY>;

} // namespace parquet
