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

////////////////////////////////////////////////////////////////////////////////
// thrift deserialization

namespace {

template <typename DeserializedType>
void deserialize_thrift_msg(
        const uint8_t serialized_msg[],
        uint32_t* serialized_len,
        DeserializedType* deserialized_msg
) {
    using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;
    auto tmem_transport = std::make_shared<ThriftBuffer>(
            const_cast<uint8_t*>(serialized_msg), *serialized_len);
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
            tproto_factory.getProtocol(tmem_transport);
    try {
        deserialized_msg->read(tproto.get());
    } catch (std::exception& e) {
        std::stringstream ss;
        ss << "Couldn't deserialize thrift: " << e.what() << "\n";
        throw parquet_exception(ss.str());
    }
    uint32_t bytes_left = tmem_transport->available_read();
    *serialized_len = *serialized_len - bytes_left;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////
// file_reader

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
            deserialize_thrift_msg(serialized_metadata.get(),
                                   &serialized_metadata_len,
                                   deserialized_metadata.get());
            return deserialized_metadata;
        });
    });
}

seastar::future<file_reader> file_reader::open(std::string path) {
    return seastar::open_file_dma(path, seastar::open_flags::ro).then(
    [path = std::move(path)] (seastar::file file) {
        return read_file_metadata(file).then(
        [path = std::move(path), file] (std::unique_ptr<format::FileMetaData> metadata) {
            file_reader fr;
            fr._path = std::move(path);
            fr._file = file;
            fr._metadata = std::move(metadata);
            fr._schema = std::make_unique<schema::schema>(schema::build_logical_schema(*fr._metadata));
            return fr;
        });
    });
}

namespace {
seastar::future<std::unique_ptr<format::ColumnMetaData>> read_chunk_metadata(seastar::input_stream<char> s) {
    using return_type = seastar::future<std::unique_ptr<format::ColumnMetaData>>;
    return seastar::do_with(peekable_stream(std::move(s)), [] (peekable_stream& stream) -> return_type {
        constexpr size_t max_allowed_column_metadata_size = 16 * 1024 * 1024;
        constexpr size_t expected_size = 256;
        return y_combinator{[&stream] (auto&& retry, size_t peek_size) -> return_type {
            return stream.peek(peek_size).then([peek_size, retry] (std::basic_string_view<uint8_t> peek) {
                try {
                    uint32_t len = peek.size();
                    format::ColumnMetaData column_metadata;
                    deserialize_thrift_msg(peek.data(), &len, &column_metadata);
                    return seastar::make_ready_future<std::unique_ptr<format::ColumnMetaData>>(
                            std::make_unique<format::ColumnMetaData>(std::move(column_metadata)));
                } catch (std::exception& e) {
                    if (peek_size > max_allowed_column_metadata_size || peek_size > peek.size()) {
                        std::stringstream ss;
                        ss << e.what();
                        ss << "Could not deserialize ColumnMetaData.\n";
                        throw parquet_exception(ss.str());
                    }
                    return retry(peek_size * 2);
                }
            });
        }}(expected_size);
    });
}
}

template <format::Type::type T>
seastar::future<column_chunk_reader<T>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const {
    assert(leaf.info.type == T);
    const format::ColumnChunk& column_chunk = metadata().row_groups[row].columns[leaf.column_index];
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
        }().then([f, &leaf] (std::unique_ptr<format::ColumnMetaData> cmd) {
            size_t file_offset = cmd->__isset.dictionary_page_offset
                                 ? cmd->dictionary_page_offset
                                 : cmd->data_page_offset;
            page_reader page_reader{seastar::make_file_input_stream(f, file_offset, cmd->total_compressed_size)};
            page_decompressor pd{std::move(page_reader), cmd->codec};
            return column_chunk_reader<T>(leaf, std::move(pd));
        });
    });

}

////////////////////////////////////////////////////////////////////////////////
// peekable_stream

seastar::future<>
peekable_stream::read_exactly(size_t n) {
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

void peekable_stream::ensure_space(size_t n) {
    if (_buffer.size() - _buffer_end >= n) {
        return;
    } else if (_buffer.size() > n + (_buffer_end - _buffer_start)
            && _buffer_start > _buffer.size() / 2) {
        std::memmove(_buffer.data(), _buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        _buffer_end -= _buffer_start;
        _buffer_start = 0;
    } else {
        buffer b{_buffer_end + n};
        if (_buffer_end - _buffer_start > 0) {
            std::memcpy(b.data(), _buffer.data() + _buffer_start, _buffer_end - _buffer_start);
        }
        _buffer = std::move(b);
        _buffer_end -= _buffer_start;
        _buffer_start = 0;
    }
}

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

////////////////////////////////////////////////////////////////////////////////
// page_reader

seastar::future<std::optional<page>> page_reader::next_page(uint32_t expected_header_size) {
    return _source.peek(expected_header_size).then(
    [this, expected_header_size] (std::basic_string_view<uint8_t> peek) {
        if (peek.size() == 0) {
            return seastar::make_ready_future<std::optional<page>>();
        }
        uint32_t len = peek.size();
        try {
            deserialize_thrift_msg(peek.data(), &len, _latest_header.get());
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
            return _source.advance(_latest_header->compressed_page_size).then(
            [this, contents=std::move(contents)] () mutable {
                page p{_latest_header.get(), std::move(contents)};
                return seastar::make_ready_future<std::optional<page>>(std::move(p));
            });
        });
    });
}

////////////////////////////////////////////////////////////////////////////////
// page_decompressor

seastar::future<std::optional<page>> page_decompressor::next_page() {
    return _source.next_page().then([this] (std::optional<page> p) {
        if (!p) {
            return std::optional<page>{};
        } else if (_codec == format::CompressionCodec::UNCOMPRESSED) {
            return std::move(p);
        } else {
            size_t output_len = p->header->uncompressed_page_size;
            if (output_len == 0) {
                return std::optional<page>{page{p->header, std::basic_string_view<uint8_t>{}}};
            }
            if (output_len > _buffer.size()) {
                _buffer = buffer{output_len};
            }
            switch (_codec) {
            case format::CompressionCodec::SNAPPY:
                compression::snappy_decompress(p->contents.data(), p->contents.size(),
                        _buffer.data(), output_len);
                break;
            case format::CompressionCodec::GZIP:
                compression::zlib_decompress(p->contents.data(), p->contents.size(),
                        _buffer.data(), output_len);
                break;
            // TODO LZO, BROTLI, LZ4, ZSTD
            default:
                throw parquet_exception::nyi("Unsupported compression type");
            }
            std::basic_string_view<uint8_t> v(_buffer.data(), output_len);
            return std::optional<page>{page{p->header, v}};
        }
    });
}

////////////////////////////////////////////////////////////////////////////////
// level_decoder

size_t level_decoder::reset(
        std::basic_string_view<uint8_t> buffer,
        format::Encoding::type encoding,
        int num_values,
        int max_level
) {
    assert(num_values >= 0);
    assert(max_level >= 0);
    _num_values = num_values;
    _values_read = 0;
    if (max_level == 0) {
        _bit_width = 0;
        return 0;
    }
    _bit_width = 32 - __builtin_clz(static_cast<int32_t>(max_level));
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

////////////////////////////////////////////////////////////////////////////////
// plain_decoder_*

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
    std::memcpy(out, _buffer.data(), bytes_to_read);
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
        if (4 > _buffer.size()) {
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

////////////////////////////////////////////////////////////////////////////////
// dict_decoder_*

template <typename T>
void dict_decoder<T>::reset(std::basic_string_view<uint8_t> data) {
    if (data.size() == 0) {
        throw parquet_exception::corrupted_file("Empty dictionary-encoded data page");
    }
    int bit_width = data.data()[0];
    if (bit_width < 0 || bit_width > 32) {
        throw parquet_exception::corrupted_file("Illegal dictionary-encoded page bit width");
    }
    _rle_decoder.Reset(reinterpret_cast<const uint8_t*>(data.data() + 1), data.size() - 1, bit_width);
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

////////////////////////////////////////////////////////////////////////////////
// rle_decoder_boolean

void rle_decoder_boolean::reset(std::basic_string_view<uint8_t> data) {
    _rle_decoder.Reset(data.data(), data.size(), 1);
}

size_t rle_decoder_boolean::read_batch(size_t n, uint8_t out[]) {
    return _rle_decoder.GetBatch(out, n);
}

////////////////////////////////////////////////////////////////////////////////
// value_decoder<T>

template<format::Type::type T>
void value_decoder<T>::reset(
        std::basic_string_view<uint8_t> buf,
        format::Encoding::type encoding,
        int type_width,
        output_type* dictionary,
        size_t dictionary_size
) {
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
        if (!dictionary) {
            throw parquet_exception::corrupted_file("Missing dictionary page");
        }
        _decoder = dict_decoder<output_type>{dictionary, dictionary_size};
        break;
    case format::Encoding::RLE:
        if constexpr (T == format::Type::BOOLEAN) {
            _decoder = rle_decoder_boolean{};
        }
    default:
        throw parquet_exception::nyi("Unsupported encoding");
    }
    std::visit([&buf] (auto& dec) { dec.reset(buf); }, _decoder);
};

template<format::Type::type T>
size_t value_decoder<T>::read_batch(size_t n, output_type out[]) {
    return std::visit([n, out] (auto& d) { return d.read_batch(n, out); }, _decoder);
};

////////////////////////////////////////////////////////////////////////////////
// column_chunk_reader<T>

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page(page p) {
    const format::DataPageHeader& header = p.header->data_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    size_t n_read = 0;
    n_read = _rep_decoder.reset(
            p.contents,
            header.repetition_level_encoding,
            header.num_values,
            _schema_node.rep_level);
    p.contents.remove_prefix(n_read);
    n_read = _def_decoder.reset(
            p.contents,
            header.definition_level_encoding,
            header.num_values,
            _schema_node.def_level);
    p.contents.remove_prefix(n_read);
    if (_dict) {
        _val_decoder.reset(
                p.contents,
                header.encoding,
                _schema_node.info.type_length,
                _dict->data(),
                _dict->size());
    } else {
        _val_decoder.reset(p.contents, header.encoding, _schema_node.info.type_length, nullptr, 0);
    }
}

template<format::Type::type T>
void column_chunk_reader<T>::load_data_page_v2(page p) {
    const format::DataPageHeaderV2& header = p.header->data_page_header_v2;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    size_t n_read = 0;
    n_read = _rep_decoder.reset(
            p.contents,
            format::Encoding::RLE,
            header.num_values,
            _schema_node.rep_level);
    p.contents.remove_prefix(n_read);
    n_read = _def_decoder.reset(
            p.contents,
            format::Encoding::RLE,
            header.num_values,
            _schema_node.def_level);
    p.contents.remove_prefix(n_read);
    if (_dict) {
        _val_decoder.reset(
                p.contents,
                header.encoding,
                _schema_node.info.type_length,
                _dict->data(),
                _dict->size());
    } else {
        _val_decoder.reset(p.contents, header.encoding, _schema_node.info.type_length, nullptr, 0);
    }
}

template<format::Type::type T>
void column_chunk_reader<T>::load_dictionary_page(page p) {
    const format::DictionaryPageHeader& header = p.header->dictionary_page_header;
    if (header.num_values < 0) {
        throw parquet_exception::corrupted_file("Negative num_values");
    }
    _dict = std::vector<output_type>(header.num_values);
    value_decoder<T> vd;
    // TODO throw if schema_node.info.type_length < 0 ?
    vd.reset(p.contents, format::Encoding::PLAIN, _schema_node.info.type_length, nullptr, 0);
    size_t n_read = vd.read_batch(_dict->size(), _dict->data());
    if (n_read < _dict->size()) {
        throw parquet_exception::corrupted_file("Unexpected end of dictionary page");
    }
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

////////////////////////////////////////////////////////////////////////////////

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
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;
template seastar::future<column_chunk_reader<format::Type::INT64>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;
template seastar::future<column_chunk_reader<format::Type::INT96>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;
template seastar::future<column_chunk_reader<format::Type::FLOAT>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;
template seastar::future<column_chunk_reader<format::Type::DOUBLE>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;
template seastar::future<column_chunk_reader<format::Type::BOOLEAN>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;
template seastar::future<column_chunk_reader<format::Type::BYTE_ARRAY>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;
template seastar::future<column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>>
file_reader::open_column_chunk_reader(int row, const schema::primitive_node& leaf) const;

} // namespace parquet
