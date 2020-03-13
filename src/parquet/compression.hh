#pragma once
#include <cstdint>
#include <cstddef>

namespace parquet::compression {

void snappy_decompress(const uint8_t* input, size_t input_len, uint8_t* output, size_t output_len);
void zlib_decompress(const uint8_t* input, size_t input_len, uint8_t* output, size_t output_len);

} // namespace::compression
