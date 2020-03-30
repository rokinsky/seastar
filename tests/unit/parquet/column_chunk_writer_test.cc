#define BOOST_TEST_MODULE parquet

#include <seastar/parquet/column_chunk_writer.hh>
#include <seastar/parquet/rle_encoding.hh>

#include <boost/test/included/unit_test.hpp>

#include <vector>
#include <array>

BOOST_AUTO_TEST_CASE(dict_encoder_trivial_happy) {
    using namespace parquet;

    dict_encoder<format::Type::INT32> encoder;

    {
        uint8_t out[10000];
        int32_t input_1[] = {2, 1};
        int32_t input_2[] = {2, 3};
        encoder.put_batch(std::data(input_1), std::size(input_1));
        encoder.put_batch(std::data(input_2), std::size(input_2));
        BOOST_REQUIRE(std::size(out) > encoder.max_encoded_size());
        size_t n_written = encoder.flush(std::data(out));

        uint8_t bit_width = out[0];
        BOOST_CHECK_EQUAL(bit_width, 2);

        RleDecoder decoder{std::data(out) + 1, static_cast<int>(n_written - 1), bit_width};
        uint32_t expected[] = {0, 1, 0, 2};
        uint32_t decoded[std::size(expected)];
        size_t n_decoded = decoder.GetBatch(std::data(decoded), std::size(expected));
        BOOST_CHECK_EQUAL(n_decoded, std::size(expected));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(decoded), std::end(decoded), std::begin(expected), std::end(expected));

        auto dict = *encoder.view_dict();
        bytes expected_dict = {
            0x02, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00};
        BOOST_CHECK_EQUAL(std::size(dict), std::size(expected_dict));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(dict), std::end(dict), std::begin(expected_dict), std::end(expected_dict));
    }
    {
        uint8_t out[10000];
        int32_t input[] = {1, 4, 5};
        encoder.put_batch(std::data(input), std::size(input));
        BOOST_REQUIRE(std::size(out) > encoder.max_encoded_size());
        size_t n_written = encoder.flush(std::data(out));

        uint8_t bit_width = out[0];
        BOOST_CHECK_EQUAL(bit_width, 3);

        RleDecoder decoder{std::data(out) + 1, static_cast<int>(n_written - 1), bit_width};
        uint32_t expected[] = {1, 3, 4};
        uint32_t decoded[std::size(expected)];
        size_t n_decoded = decoder.GetBatch(std::data(decoded), std::size(expected));
        BOOST_CHECK_EQUAL(n_decoded, std::size(expected));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(decoded), std::end(decoded), std::begin(expected), std::end(expected));

        auto dict = *encoder.view_dict();
        bytes expected_dict = {
            0x02, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00,
            0x05, 0x00, 0x00, 0x00};
        BOOST_CHECK_EQUAL(std::size(dict), std::size(expected_dict));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(dict), std::end(dict), std::begin(expected_dict), std::end(expected_dict));
    }
}

constexpr parquet::bytes_view operator ""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

BOOST_AUTO_TEST_CASE(dict_encoder_byte_array_happy) {
    using namespace parquet;

    dict_encoder<format::Type::BYTE_ARRAY> encoder;

    {
        uint8_t out[10000];
        bytes_view input_1[] = {"bb"_bv, "aa"_bv};
        bytes_view input_2[] = {"bb"_bv, "cc"_bv};
        encoder.put_batch(std::data(input_1), std::size(input_1));
        encoder.put_batch(std::data(input_2), std::size(input_2));
        BOOST_REQUIRE(std::size(out) > encoder.max_encoded_size());
        size_t n_written = encoder.flush(std::data(out));

        uint8_t bit_width = out[0];
        BOOST_CHECK_EQUAL(bit_width, 2);

        RleDecoder decoder{std::data(out) + 1, static_cast<int>(n_written - 1), bit_width};
        uint32_t expected[] = {0, 1, 0, 2};
        uint32_t decoded[std::size(expected)];
        size_t n_decoded = decoder.GetBatch(std::data(decoded), std::size(expected));
        BOOST_CHECK_EQUAL(n_decoded, std::size(expected));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(decoded), std::end(decoded), std::begin(expected), std::end(expected));

        auto dict = *encoder.view_dict();
        bytes expected_dict = {
            0x02, 0x00, 0x00, 0x00, 'b', 'b',
            0x02, 0x00, 0x00, 0x00, 'a', 'a',
            0x02, 0x00, 0x00, 0x00, 'c', 'c'};
        BOOST_CHECK_EQUAL(std::size(dict), std::size(expected_dict));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(dict), std::end(dict), std::begin(expected_dict), std::end(expected_dict));
    }
    {
        uint8_t out[10000];
        bytes_view input[] = {"aa"_bv, "dd"_bv, "ee"_bv};
        encoder.put_batch(std::data(input), std::size(input));
        BOOST_REQUIRE(std::size(out) > encoder.max_encoded_size());
        size_t n_written = encoder.flush(std::data(out));

        uint8_t bit_width = out[0];
        BOOST_CHECK_EQUAL(bit_width, 3);

        RleDecoder decoder{std::data(out) + 1, static_cast<int>(n_written - 1), bit_width};
        uint32_t expected[] = {1, 3, 4};
        uint32_t decoded[std::size(expected)];
        size_t n_decoded = decoder.GetBatch(std::data(decoded), std::size(expected));
        BOOST_CHECK_EQUAL(n_decoded, std::size(expected));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(decoded), std::end(decoded), std::begin(expected), std::end(expected));

        auto dict = *encoder.view_dict();
        bytes expected_dict = {
            0x02, 0x00, 0x00, 0x00, 'b', 'b',
            0x02, 0x00, 0x00, 0x00, 'a', 'a',
            0x02, 0x00, 0x00, 0x00, 'c', 'c',
            0x02, 0x00, 0x00, 0x00, 'd', 'd',
            0x02, 0x00, 0x00, 0x00, 'e', 'e'};
        BOOST_CHECK_EQUAL(std::size(dict), std::size(expected_dict));
        BOOST_CHECK_EQUAL_COLLECTIONS(std::begin(dict), std::end(dict), std::begin(expected_dict), std::end(expected_dict));
    }
}
