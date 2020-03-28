#pragma once

#include <seastar/parquet/exception.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/print.hh>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TProtocolException.h>
#include <thrift/transport/TBufferTransports.h>

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

// Deserialize a single thrift structure. Return the number of bytes used.
template <typename DeserializedType>
uint32_t deserialize_thrift_msg(
        const uint8_t serialized_msg[],
        uint32_t serialized_len,
        DeserializedType& deserialized_msg) {
    using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;
    auto tmem_transport = std::make_shared<ThriftBuffer>(const_cast<uint8_t*>(serialized_msg), serialized_len);
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
    deserialized_msg.read(tproto.get());
    uint32_t bytes_left = tmem_transport->available_read();
    return serialized_len - bytes_left;
}

class thrift_serializer {
    using thrift_buffer = apache::thrift::transport::TMemoryBuffer;
    using thrift_protocol = apache::thrift::protocol::TProtocol;
    std::shared_ptr<thrift_buffer> _transport;
    std::shared_ptr<thrift_protocol> _protocol;
public:
    thrift_serializer(size_t starting_size = 1024)
        : _transport{std::make_shared<thrift_buffer>(starting_size)}
        , _protocol{apache::thrift::protocol::TCompactProtocolFactoryT<thrift_buffer>{}.getProtocol(_transport)} {}

    template <typename DeserializedType>
    std::basic_string_view<uint8_t> serialize(const DeserializedType& msg) {
        _transport->resetBuffer();
        msg.write(_protocol.get());
        uint8_t* data;
        uint32_t size;
        _transport->getBuffer(&data, &size);
        return {data, static_cast<size_t>(size)};
    }
};

// Deserialize (and consume from the stream) a single thrift structure.
// Return false if the stream is empty.
template <typename DeserializedType>
seastar::future<bool> read_thrift_from_stream(
        peekable_stream& stream,
        DeserializedType& deserialized_msg,
        size_t expected_size = 1024,
        size_t max_allowed_size = 1024 * 1024 * 16
) {
    if (expected_size > max_allowed_size) {
        return seastar::make_exception_future<bool>(parquet_exception(seastar::format(
                "Could not deserialize thrift: max allowed size of {} exceeded", max_allowed_size)));
    }
    return stream.peek(expected_size).then(
    [&stream, &deserialized_msg, expected_size, max_allowed_size] (std::basic_string_view<uint8_t> peek) {
        uint32_t len = peek.size();
        if (len == 0) {
            return seastar::make_ready_future<bool>(false);
        }
        try {
            len = deserialize_thrift_msg(peek.data(), len, deserialized_msg);
        } catch (const apache::thrift::transport::TTransportException& e) {
            if (e.getType() == apache::thrift::transport::TTransportException::END_OF_FILE) {
                // The serialized structure was bigger than expected. Retry with a bigger expectation.
                if (peek.size() < expected_size) {
                    throw parquet_exception(seastar::format(
                            "Could not deserialize thrift: unexpected end of stream at {}B", peek.size()));
                }
                return read_thrift_from_stream(stream, deserialized_msg, expected_size * 2, max_allowed_size);
            } else {
                throw parquet_exception(seastar::format("Could not deserialize thrift: {}", e.what()));
            }
        } catch (const std::exception& e) {
            throw parquet_exception(seastar::format("Could not deserialize thrift: {}", e.what()));
        }
        return stream.advance(len).then([] {
            return true;
        });
    });
}

} // namespace parquet
