#include <seastar/parquet/column_chunk_writer.hh>
#include <seastar/core/bitops.hh>
#include <seastar/parquet/rle_encoding.hh>
#include <type_traits>

namespace parquet {

template class column_chunk_writer<format::Type::FIXED_LEN_BYTE_ARRAY>;

} // namespace parquet
