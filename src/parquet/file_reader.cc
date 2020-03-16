#include "compression.hh"

#include <seastar/parquet/file_reader.hh>
#include <seastar/parquet/exception.hh>
#include <seastar/parquet/overloaded.hh>

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <iostream>
#include <sstream>
#include <limits>

namespace parquet {

namespace {

template <typename DeserializedType>
void deserialize_thrift_msg(
        const uint8_t serialized_msg[],
        uint32_t* serialized_len,
        DeserializedType& deserialized_msg
) {
    using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;
    auto tmem_transport = std::make_shared<ThriftBuffer>(const_cast<uint8_t*>(serialized_msg), *serialized_len);
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
    deserialized_msg.read(tproto.get());
    uint32_t bytes_left = tmem_transport->available_read();
    *serialized_len = *serialized_len - bytes_left;
}

} // namespace

/* Assuming there is k bytes remaining in stream, append exactly min(k, n) bytes to the internal buffer.
 * seastar::input_stream has a read_exactly method of it's own, which does exactly what we want internally,
 * except instead of returning the k buffered bytes on eof, it discards all of it and returns an empty buffer.
 * Bummer. */
seastar::future<> peekable_stream::read_exactly(size_t n) {
    assert(_buffer.size() - _buffer_end >= n);
    if (n == 0) {
        return seastar::make_ready_future<>();
    }
    return _source.read_up_to(n).then([this, n] (seastar::temporary_buffer<char> newbuf) {
        if (newbuf.size() == 0) {
            return seastar::make_ready_future<>();
        } else {
            std::memcpy(_buffer.data() + _buffer_end, newbuf.get(), newbuf.size());
            _buffer_end += newbuf.size();
            return read_exactly(n - newbuf.size());
        }
    });
}

/* Ensure that there is at least n bytes of space after _buffer_end.
 * We want to strike a balance between rewinding the buffer and reallocating it.
 * If we are too stingy with reallocation, we might do a lot of pointless rewinding.
 * If we are too stingy with rewinding, we will allocate lots of unused memory too big a buffer.
 * Case in point: imagine that buffer.size() == 1024.
 * Then, imagine a peek(1024), advance(1), peek(1024), advance(1)... sequence.
 * If we never reallocate the buffer, we will have to move 1023 bytes every time we consume a byte.
 * If we never rewind the buffer, it will keep growing indefinitely, even though we only need 1024 contiguous
 * bytes.
 * Our strategy (rewind only when _buffer_start moves past half of buffer.size()) guarantees that
 * we will actively use at least 1/2 of allocated memory, and that any given byte is rewound at most once.
 */
void peekable_stream::ensure_space(size_t n) {
    if (_buffer.size() - _buffer_end >= n) {
        return;
    } else if (_buffer.size() > n + (_buffer_end - _buffer_start) && _buffer_start > _buffer.size() / 2) {
        // Rewind the buffer.
        std::memmove(_buffer.data(), _buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        _buffer_end -= _buffer_start;
        _buffer_start = 0;
    } else {
        // Allocate a bigger buffer and move unconsumed data into it.
        buffer b{_buffer_end + n};
        if (_buffer_end - _buffer_start > 0) {
            std::memcpy(b.data(), _buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        }
        _buffer = std::move(b);
        _buffer_end -= _buffer_start;
        _buffer_start = 0;
    }
}

// Assuming there is k bytes remaining in stream, view the next unconsumed min(k, n) bytes.
seastar::future<std::basic_string_view<uint8_t>> peekable_stream::peek(size_t n) {
    if (n == 0) {
        return seastar::make_ready_future<std::basic_string_view<uint8_t>>();
    } else if (_buffer_end - _buffer_start >= n) {
        return seastar::make_ready_future<std::basic_string_view<uint8_t>>(
                std::basic_string_view<uint8_t>{_buffer.data() +_buffer_start, n});
    } else {
        size_t bytes_needed = n - (_buffer_end - _buffer_start);
        ensure_space(bytes_needed);
        return read_exactly(bytes_needed).then([this] {
            return std::basic_string_view(_buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        });
    }
}

// Consume n bytes. If there is less than n bytes in stream, throw.
seastar::future<> peekable_stream::advance(size_t n) {
    if (_buffer_end - _buffer_start > n) {
        _buffer_start += n;
        return seastar::make_ready_future<>();
    } else {
        size_t remaining = n - (_buffer_end - _buffer_start);
        return _source.skip(remaining).then([this] {
            _buffer_end = 0;
            _buffer_start = 0;
        });
    }
}

seastar::future<std::optional<page>> page_reader::next_page(uint32_t expected_header_size) {
    *_latest_header = format::PageHeader{};
    return _source.peek(expected_header_size).then(
    [this, expected_header_size] (std::basic_string_view<uint8_t> peek) {
        if (peek.size() == 0) {
            return seastar::make_ready_future<std::optional<page>>();
        }
        uint32_t len = peek.size();
        try {
            deserialize_thrift_msg(peek.data(), &len, *_latest_header);
        } catch (std::exception& e) {
            if (expected_header_size > _max_allowed_header_size || expected_header_size > peek.size()) {
                std::stringstream ss;
                ss << e.what();
                ss << "Deserializing page header failed.\n";
                throw parquet_exception(ss.str());
            }
            return next_page(expected_header_size * 2);
        }
        return _source.advance(len).then([this] {
            return _source.peek(_latest_header->compressed_page_size);
        }).then([this] (std::basic_string_view<uint8_t> contents) {
            if (contents.size() < static_cast<size_t>(_latest_header->compressed_page_size)) {
                throw parquet_exception::eof();
            }
            return _source.advance(_latest_header->compressed_page_size).then(
            [this, contents=std::move(contents)] () mutable {
                page p{_latest_header.get(), std::move(contents)};
                return seastar::make_ready_future<std::optional<page>>(std::move(p));
            });
        });
    });
}

std::basic_string_view<uint8_t>
decompressor::operator()(std::basic_string_view<uint8_t> input, size_t decompressed_len) {
    if (_codec == format::CompressionCodec::UNCOMPRESSED) {
        return input;
    } else {
        if (decompressed_len == 0) {
            return {};
        }
        if (decompressed_len > _buffer.size()) {
            _buffer = buffer{decompressed_len};
        }
        switch (_codec) {
        case format::CompressionCodec::SNAPPY:
            compression::snappy_decompress(input.data(), input.size(), _buffer.data(), decompressed_len);
            break;
        // TODO GZIP, LZO, BROTLI, LZ4, ZSTD
        default:
            throw parquet_exception::nyi("Unsupported compression type");
        }
        return {_buffer.data(), decompressed_len};
    }
}

size_t level_decoder::reset_v1(
        std::basic_string_view<uint8_t> buffer,
        format::Encoding::type encoding,
        int num_values) {
    assert(num_values >= 0);
    _num_values = num_values;
    _values_read = 0;
    if (_bit_width == 0) {
        return 0;
    }
    if (encoding == format::Encoding::RLE) {
        if (buffer.size() < 4) {
            throw parquet_exception::corrupted_file("Unexpected end of page");
        }
        int32_t len;
        std::memcpy(&len, buffer.data(), 4);
        if (len < 0) {
            throw parquet_exception::corrupted_file("Negative rle encoding length");
        }
        if (static_cast<size_t>(len) > buffer.size()) {
            throw parquet_exception::corrupted_file("Unexpected end of page");
        }
        _decoder = RleDecoder{buffer.data() + 4, len, _bit_width};
        return 4 + len;
    } else if (encoding == format::Encoding::BIT_PACKED) {
        int64_t bit_len = static_cast<int64_t>(num_values) * _bit_width;
        int64_t byte_len = (bit_len + 7) >> 3;
        if (byte_len > std::numeric_limits<int>::max()) {
            throw parquet_exception::corrupted_file("BIT_PACKED length exceeds int");
        }
        if (static_cast<size_t>(byte_len) > buffer.size()) {
            throw parquet_exception::corrupted_file("Unexpected end of page");
        }
        _decoder = BitReader{buffer.data(), static_cast<int>(byte_len)};
        return byte_len;
    } else {
        throw parquet_exception::nyi("Unknown level encoding");
    }
}

void level_decoder::reset_v2(std::basic_string_view<uint8_t> encoded_levels, int num_values) {
    assert(num_values >= 0);
    _num_values = num_values;
    _values_read = 0;
    _decoder = RleDecoder{encoded_levels.data(), static_cast<int>(encoded_levels.size()), _bit_width};
}

template <typename T>
void plain_decoder_trivial<T>::reset(std::basic_string_view<uint8_t> data) {
    _buffer = std::move(data);
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
            throw parquet_exception::corrupted_file("Could not read BYTE_ARRAY length");
        }
        uint32_t len;
        std::memcpy(&len, _buffer.get(), 4);
        _buffer.trim_front(4);
        if (len > _buffer.size()) {
            throw parquet_exception::corrupted_file("Page ended while reading BYTE_ARRAY");
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
            throw parquet_exception::corrupted_file("Page ended while reading FIXED_LEN_BYTE_ARRAY");
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
        throw parquet_exception::corrupted_file("Illegal dictionary-encoded page bit width");
    }
    _rle_decoder.Reset(data.data() + 1, data.size() - 1, bit_width);
}

template <typename T>
size_t dict_decoder<T>::read_batch(size_t n, T out[]) {
    std::array<uint32_t, 100> buf;
    size_t completed = 0;
    while (completed < n) {
        size_t n_to_read = std::min(n - completed, buf.size());
        size_t n_read = _rle_decoder.GetBatch(buf.data(), n_to_read);
        for (size_t i = 0; i < n_read; ++i) {
            if (buf[i] > _dict_size) {
                throw parquet_exception::corrupted_file("Dictionary index bigger than dictionary size");
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
    int32_t block_size;
    if (!_decoder.GetVlqInt(&block_size)) { throw parquet_exception::eof(); }
    if (!_decoder.GetVlqInt(&_num_mini_blocks)) { throw parquet_exception::eof(); }
    if (!_decoder.GetVlqInt(&_values_current_block)) { throw parquet_exception::eof(); }
    if (!_decoder.GetZigZagVlqInt(&_last_value)) { throw parquet_exception::eof(); }

    if (_delta_bit_widths.size() < static_cast<size_t>(_num_mini_blocks)) {
        _delta_bit_widths = buffer(_num_mini_blocks);
    }

    if (!_decoder.GetZigZagVlqInt(&_min_delta)) throw parquet_exception::eof();
    for (int i = 0; i < _num_mini_blocks; ++i) {
        if (!_decoder.GetAligned<uint8_t>(1, _delta_bit_widths.data() + i)) {
            throw parquet_exception::eof();
        }
    }
    _values_per_mini_block = block_size / _num_mini_blocks;
    _mini_block_idx = 0;
    _delta_bit_width = _delta_bit_widths.data()[0];
    _values_current_mini_block = _values_per_mini_block;
}

template <typename T>
size_t delta_binary_packed_decoder<T>::read_batch(size_t n, T out[]) {
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
        if (!_decoder.GetValue(_delta_bit_width, &delta)) { throw parquet_exception::eof(); }
        delta += _min_delta;
        _last_value += static_cast<int32_t>(delta);
        out[i] = _last_value;
        --_values_current_mini_block;
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
void value_decoder<T>::reset(
        std::basic_string_view<uint8_t> buf,
        format::Encoding::type encoding,
        size_t num_values,
        int type_width
) {
    if (type_width < 0) {
        throw parquet_exception::corrupted_file("Negative type_width");
    }
    _remaining_values = num_values;
    switch (encoding) {
    case format::Encoding::PLAIN:
        if constexpr (T == format::Type::BOOLEAN) {
            _decoder = plain_decoder_boolean{};
        } else if constexpr (T == format::Type::BYTE_ARRAY) {
            _decoder = plain_decoder_byte_array{};
        } else if constexpr (T == format::Type::FIXED_LEN_BYTE_ARRAY) {
            _decoder = plain_decoder_fixed_len_byte_array{static_cast<size_t>(type_width)};
        } else {
            _decoder = plain_decoder_trivial<output_type>{};
        }
        break;
    case format::Encoding::RLE_DICTIONARY:
    case format::Encoding::PLAIN_DICTIONARY:
        if (!_dict_set) {
            throw parquet_exception::corrupted_file("Missing dictionary page");
        }
        _decoder = dict_decoder<output_type>{_dict, _dict_size};
        break;
    case format::Encoding::RLE:
        if constexpr (T == format::Type::BOOLEAN) {
            _decoder = rle_decoder_boolean{};
        } else {
            throw parquet_exception::corrupted_file("RLE encoding is valid only for BOOLEAN");
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
        throw parquet_exception::nyi("Unsupported encoding");
    }
    std::visit([&buf] (auto& dec) { dec.reset(buf); }, _decoder);
};

template<format::Type::type T>
size_t value_decoder<T>::read_batch(size_t n, output_type out[]) {
    n = std::min(n, _remaining_values);
    size_t n_read = std::visit([n, out] (auto& d) { return d.read_batch(n, out); }, _decoder);
    _remaining_values -= n_read;
    return n_read;
};

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page(page p) {
    const format::DataPageHeader& header = p.header->data_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    std::basic_string_view<uint8_t> contents = _decompressor(p.contents, p.header->uncompressed_page_size);
    size_t n_read = 0;
    n_read = _rep_decoder.reset_v1(
            contents,
            header.repetition_level_encoding,
            header.num_values);
    contents.remove_prefix(n_read);
    n_read = _def_decoder.reset_v1(
            contents,
            header.definition_level_encoding,
            header.num_values);
    contents.remove_prefix(n_read);
    _val_decoder.reset(
            contents,
            header.encoding,
            header.num_values,
            _schema_node.info.type_length);
}

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page_v2(page p) {
    const format::DataPageHeaderV2& header = p.header->data_page_header_v2;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    if (header.repetition_levels_byte_length < 0 || header.definition_levels_byte_length < 0) {
        throw parquet_exception::corrupted_file("Negative levels byte length");
    }
    std::basic_string_view<uint8_t> contents = p.contents;
    _rep_decoder.reset_v2(contents.substr(0, header.repetition_levels_byte_length), header.num_values);
    contents.remove_prefix(header.repetition_levels_byte_length);
    _def_decoder.reset_v2(contents.substr(0, header.definition_levels_byte_length), header.num_values);
    contents.remove_prefix(header.definition_levels_byte_length);
    size_t n_read = header.repetition_levels_byte_length + header.definition_levels_byte_length;
    size_t uncompressed_values_size = p.header->uncompressed_page_size - n_read;
    std::basic_string_view<uint8_t> values = _decompressor(contents, uncompressed_values_size);
    _val_decoder.reset(
            values,
            header.encoding,
            header.num_values,
            _schema_node.info.type_length);
}

template<format::Type::type T>
void column_chunk_reader<T>::load_dictionary_page(page p) {
    const format::DictionaryPageHeader& header = p.header->dictionary_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    _dict = std::vector<output_type>(header.num_values);
    auto decompressed_values = _decompressor(p.contents, p.header->uncompressed_page_size);
    value_decoder<T> vd;
    vd.reset(
            decompressed_values,
            format::Encoding::PLAIN,
            header.num_values,
            _schema_node.info.type_length);
    size_t n_read = vd.read_batch(_dict->size(), _dict->data());
    if (n_read < _dict->size()) {
        throw parquet_exception::corrupted_file("Unexpected end of dictionary page");
    }
    _val_decoder.reset_dict(_dict->data(), _dict->size());
}

template<format::Type::type T>
seastar::future<> column_chunk_reader<T>::load_next_page() {
    return _source.next_page().then([this] (std::optional<page> p) {
        if (!p) {
            _eof = true;
        } else {
            switch (p->header->type) {
            case format::PageType::DATA_PAGE:
                load_data_page(std::move(*p));
                _initialized = true;
                return;
            case format::PageType::DATA_PAGE_V2:
                load_data_page_v2(std::move(*p));
                _initialized = true;
                return;
            case format::PageType::DICTIONARY_PAGE:
                load_dictionary_page(std::move(*p));
                return;
            default:; // Unknown page types are to be skipped
            }
        }
    });
}

seastar::future<std::unique_ptr<format::FileMetaData>> file_reader::read_file_metadata(seastar::file file) {
    return file.size().then([file] (uint64_t size) mutable {
        if (size < 8) {
            throw parquet_exception::corrupted_file("File too small to be a parquet file");
        }

        // Parquet file structure:
        // ...
        // File Metadata (serialized with thrift compact protocol)
        // 4-byte length in bytes of file metadata (little endian)
        // 4-byte magic number "PAR1"
        // EOF
        return file.dma_read_exactly<uint8_t>(size - 8, 8).then(
        [file, size] (seastar::temporary_buffer<uint8_t> footer) mutable {
            if (std::memcmp(footer.get() + 4, "PAR1", 4) != 0) {
                throw parquet_exception::corrupted_file("Magic bytes not found in footer");
            }

            uint32_t metadata_len;
            std::memcpy(&metadata_len, footer.get(), 4);
            if (metadata_len + 8 > size) {
                throw parquet_exception::corrupted_file("Metadata size reported by footer greater than file size");
            }

            return file.dma_read_exactly<uint8_t>(size - 8 - metadata_len, metadata_len);
        }).then([](seastar::temporary_buffer<uint8_t> serialized_metadata) {
            uint32_t serialized_metadata_len = serialized_metadata.size();
            auto deserialized_metadata = std::make_unique<format::FileMetaData>();
            deserialize_thrift_msg(serialized_metadata.get(), &serialized_metadata_len, *deserialized_metadata);
            return deserialized_metadata;
        });
    });
}

seastar::future<file_reader> file_reader::open(std::string&& path) {
    return seastar::open_file_dma(path, seastar::open_flags::ro).then(
    [path = std::move(path)] (seastar::file file) {
        return read_file_metadata(file).then(
        [path = std::move(path), file] (std::unique_ptr<format::FileMetaData> metadata) {
            file_reader fr;
            fr._path = std::move(path);
            fr._file = file;
            fr._metadata = std::move(metadata);
            fr._schema = std::make_unique<schema::schema>(schema::file_metadata_to_schema(*fr._metadata));
            return fr;
        });
    });
}

namespace {

seastar::future<std::unique_ptr<format::ColumnMetaData>> read_chunk_metadata(seastar::input_stream<char>&& s) {
    using return_type = seastar::future<std::unique_ptr<format::ColumnMetaData>>;
    return seastar::do_with(peekable_stream{std::move(s)}, [] (peekable_stream& stream) -> return_type {
        constexpr size_t max_allowed_column_metadata_size = 16 * 1024 * 1024;
        constexpr size_t expected_size = 1;
        return y_combinator{[&stream] (auto&& retry, size_t peek_size) -> return_type {
            return stream.peek(peek_size).then([peek_size, retry] (std::basic_string_view<uint8_t> peek) {
                try {
                    uint32_t len = peek.size();
                    format::ColumnMetaData column_metadata;
                    deserialize_thrift_msg(peek.data(), &len, column_metadata);
                    return seastar::make_ready_future<std::unique_ptr<format::ColumnMetaData>>(
                            std::make_unique<format::ColumnMetaData>(std::move(column_metadata)));
                } catch (apache::thrift::transport::TTransportException& e) {
                    if (peek_size > max_allowed_column_metadata_size || peek_size > peek.size()) {
                        throw parquet_exception("Maximum allowed ColumnMetaData size exceeded.\n");
                    }
                    return retry(peek_size * 2);
                }
            });
        }}(expected_size);
    });
}

}

/* ColumnMetaData is a structure that has to be read in order to find the beginning of a column chunk.
 * It is written directly after the chunk it describes, and its offset is saved to the FileMetaData.
 * Optionally, the entire ColumnMetaData might be embedded in the FileMetaData.
 * That's what the documentation says. However, Arrow always assumes that ColumnMetaData is always
 * present in the FileMetaData, and doesn't bother reading it from it's required location.
 * One of the tests in parquet-testing also gets this wrong (the offset saved in FileMetaData points to something
 * different than ColumnMetaData), so I'm not sure whether this entire function is needed.
 */
template <format::Type::type T>
seastar::future<column_chunk_reader<T>>
file_reader::open_column_chunk_reader(int row_group, int column) const {
    assert(static_cast<size_t>(column) < schema().leaves.size());
    assert(static_cast<size_t>(row_group) < metadata().row_groups.size());
    assert(schema().leaves[column]->info.type == T);
    if (static_cast<size_t>(column) >= metadata().row_groups[row_group].columns.size()) {
        throw parquet_exception::corrupted_file("Selected column not found not found in row group metadata");
    }
    const format::ColumnChunk& column_chunk = metadata().row_groups[row_group].columns[column];
    const schema::primitive_node& leaf = *schema().leaves[column];
    return [this, &column_chunk] {
        if (!column_chunk.__isset.file_path) {
            return seastar::make_ready_future<seastar::file>(file());
        } else {
            return seastar::open_file_dma(path() + column_chunk.file_path, seastar::open_flags::ro);
        }
    }().then([&column_chunk, &leaf] (seastar::file f) {
        return [&column_chunk, f] {
            if (column_chunk.__isset.meta_data) {
                return seastar::make_ready_future<std::unique_ptr<format::ColumnMetaData>>(
                        std::make_unique<format::ColumnMetaData>(column_chunk.meta_data));
            } else {
                return read_chunk_metadata(seastar::make_file_input_stream(f, column_chunk.file_offset));
            }
        }().then([f, &leaf] (std::unique_ptr<format::ColumnMetaData> column_metadata) {
            size_t file_offset = column_metadata->__isset.dictionary_page_offset
                                 ? column_metadata->dictionary_page_offset
                                 : column_metadata->data_page_offset;
            page_reader page_reader{
                    seastar::make_file_input_stream(f, file_offset, column_metadata->total_compressed_size)};
            return column_chunk_reader<T>(leaf, std::move(page_reader), column_metadata->codec);
        });
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
template seastar::future<column_chunk_reader<format::Type::INT32>>
file_reader::open_column_chunk_reader(int row_group, int column) const;
template seastar::future<column_chunk_reader<format::Type::INT64>>
file_reader::open_column_chunk_reader(int row_group, int column) const;
template seastar::future<column_chunk_reader<format::Type::INT96>>
file_reader::open_column_chunk_reader(int row_group, int column) const;
template seastar::future<column_chunk_reader<format::Type::FLOAT>>
file_reader::open_column_chunk_reader(int row_group, int column) const;
template seastar::future<column_chunk_reader<format::Type::DOUBLE>>
file_reader::open_column_chunk_reader(int row_group, int column) const;
template seastar::future<column_chunk_reader<format::Type::BOOLEAN>>
file_reader::open_column_chunk_reader(int row_group, int column) const;
template seastar::future<column_chunk_reader<format::Type::BYTE_ARRAY>>
file_reader::open_column_chunk_reader(int row_group, int column) const;
template seastar::future<column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>>
file_reader::open_column_chunk_reader(int row_group, int column) const;

} // namespace parquet
