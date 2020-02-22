#include <seastar/parquet/exception.hh>
#include <snappy.h>
#include <zlib.h>
#include "compression.hh"

namespace parquet::compression {

void snappy_decompress(const uint8_t* input, size_t input_len, uint8_t* output, size_t output_len) {
    size_t decompressed_size;
    if (!snappy::GetUncompressedLength(
            reinterpret_cast<const char*>(input),
            input_len,
            &decompressed_size)) {
        throw parquet_exception::corrupted_file("Corrupt snappy-compressed data");
    }
    if (output_len != decompressed_size) {
        throw parquet_exception::corrupted_file("Invalid decompressed page size reported by page header");
    }
    if (!snappy::RawUncompress(
            reinterpret_cast<const char*>(input),
            input_len,
            reinterpret_cast<char*>(output))) {
        throw parquet_exception("Could not decompress snappy.");
    }
}

// FIXME This one is completely untested!
void zlib_decompress(const uint8_t* input, size_t input_len, uint8_t* output, size_t output_len) {
    z_stream stream;
    int err;
    const uInt max = (uInt)-1;
    uLong len, left;
    Byte buf[1];    /* for detection of incomplete stream when output_len == 0 */

    len = input_len;
    if (output_len) {
        left = output_len;
        output_len = 0;
    }
    else {
        left = 1;
        output = buf;
    }

    stream.next_in = (z_const Bytef *)input;
    stream.avail_in = 0;
    stream.zalloc = (alloc_func)0;
    stream.zfree = (free_func)0;
    stream.opaque = (voidpf)0;

    // Determine if this is libz or gzip from header.
    constexpr int DETECT_CODEC = 32;
    // Maximum window size
    constexpr int WINDOW_BITS = 15;

    err = inflateInit2(&stream, DETECT_CODEC | WINDOW_BITS);
    if (err != Z_OK) {
        throw parquet_exception("Could not decompress gzip");
    }

    stream.next_out = output;
    stream.avail_out = 0;

    do {
        if (stream.avail_out == 0) {
            stream.avail_out = left > (uLong)max ? max : (uInt)left;
            left -= stream.avail_out;
        }
        if (stream.avail_in == 0) {
            stream.avail_in = len > (uLong)max ? max : (uInt)len;
            len -= stream.avail_in;
        }
        err = inflate(&stream, Z_NO_FLUSH);
    } while (err == Z_OK);

    input_len -= len + stream.avail_in;
    if (output != buf)
        output_len = stream.total_out;
    else if (stream.total_out && err == Z_BUF_ERROR)
        left = 1;

    inflateEnd(&stream);
    if (err != Z_STREAM_END) {
        throw parquet_exception("Could not decompress gzip");
    }
}

} // namespace parquet::compression
