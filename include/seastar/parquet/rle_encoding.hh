#pragma once

#include <seastar/parquet/bpacking.hh>

#include <cmath>
#include <algorithm>

namespace parquet {

// Returns the ceil of value/divisor
constexpr int64_t CeilDiv(int64_t value, int64_t divisor) {
  return value / divisor + (value % divisor != 0);
}

constexpr int64_t BytesForBits(int64_t bits) { return (bits + 7) >> 3; }

/// Utility class to read bit/byte stream.  This class can read bits or bytes
/// that are either byte aligned or not.  It also has utilities to read multiple
/// bytes in one read (e.g. encoded int).
class BitReader {
 public:
  /// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
  BitReader(const uint8_t* buffer, int buffer_len)
      : buffer_(buffer), max_bytes_(buffer_len), byte_offset_(0), bit_offset_(0) {
    int num_bytes = std::min(8, max_bytes_ - byte_offset_);
    memcpy(&buffered_values_, buffer_ + byte_offset_, num_bytes);
  }

  BitReader()
      : buffer_(NULL),
        max_bytes_(0),
        buffered_values_(0),
        byte_offset_(0),
        bit_offset_(0) {}

  void Reset(const uint8_t* buffer, int buffer_len) {
    buffer_ = buffer;
    max_bytes_ = buffer_len;
    byte_offset_ = 0;
    bit_offset_ = 0;
    int num_bytes = std::min(8, max_bytes_ - byte_offset_);
    memcpy(&buffered_values_, buffer_ + byte_offset_, num_bytes);
  }

  /// Gets the next value from the buffer.  Returns true if 'v' could be read or false if
  /// there are not enough bytes left. num_bits must be <= 32.
  template <typename T>
  bool GetValue(int num_bits, T* v);

  /// Get a number of values from the buffer. Return the number of values actually read.
  template <typename T>
  int GetBatch(int num_bits, T* v, int batch_size);

  /// Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T
  /// needs to be a little-endian native type and big enough to store
  /// 'num_bytes'. The value is assumed to be byte-aligned so the stream will
  /// be advanced to the start of the next byte before 'v' is read. Returns
  /// false if there are not enough bytes left.
  template <typename T>
  bool GetAligned(int num_bytes, T* v);

  /// Reads a vlq encoded int from the stream.  The encoded int must start at
  /// the beginning of a byte. Return false if there were not enough bytes in
  /// the buffer.
  bool GetVlqInt(int32_t* v);

  // Reads a zigzag encoded int `into` v.
  bool GetZigZagVlqInt(int32_t* v);

  /// Returns the number of bytes left in the stream, not including the current
  /// byte (i.e., there may be an additional fraction of a byte).
  int bytes_left() {
    return max_bytes_ -
           (byte_offset_ + static_cast<int>(BytesForBits(bit_offset_)));
  }

  /// Maximum byte length of a vlq encoded int
  static const int MAX_VLQ_BYTE_LEN = 5;

 private:
  const uint8_t* buffer_;
  int max_bytes_;

  /// Bytes are memcpy'd from buffer_ and values are read from this variable. This is
  /// faster than reading values byte by byte directly from buffer_.
  uint64_t buffered_values_;

  int byte_offset_;  // Offset in buffer_
  int bit_offset_;   // Offset in buffered_values_
};

template <typename T>
inline bool BitReader::GetValue(int num_bits, T* v) {
  return GetBatch(num_bits, v, 1) == 1;
}

// Returns the 'num_bits' least-significant bits of 'v'.
static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
  if (__builtin_expect(num_bits == 0, 0)) return 0;
  if (__builtin_expect(num_bits >= 64, 0)) return v;
  int n = 64 - num_bits;
  return (v << n) >> n;
}

namespace detail {

template <typename T>
inline void GetValue_(int num_bits, T* v, int max_bytes, const uint8_t* buffer,
                      int* bit_offset, int* byte_offset, uint64_t* buffered_values) {
  *v = static_cast<T>(TrailingBits(*buffered_values, *bit_offset + num_bits) >>
                      *bit_offset);
  *bit_offset += num_bits;
  if (*bit_offset >= 64) {
    *byte_offset += 8;
    *bit_offset -= 64;

    int bytes_remaining = max_bytes - *byte_offset;
    if (__builtin_expect(bytes_remaining >= 8, 1)) {
      memcpy(buffered_values, buffer + *byte_offset, 8);
    } else {
      memcpy(buffered_values, buffer + *byte_offset, bytes_remaining);
    }
    // Read bits of v that crossed into new buffered_values_
    *v = *v | static_cast<T>(TrailingBits(*buffered_values, *bit_offset)
                             << (num_bits - *bit_offset));
    assert(*bit_offset <= 64);
  }
}

}  // namespace detail

template <typename T>
inline int BitReader::GetBatch(int num_bits, T* v, int batch_size) {
  assert(buffer_ != NULL);
  assert(num_bits <= 32);
  assert(num_bits <= static_cast<int>(sizeof(T) * 8));

  int bit_offset = bit_offset_;
  int byte_offset = byte_offset_;
  uint64_t buffered_values = buffered_values_;
  int max_bytes = max_bytes_;
  const uint8_t* buffer = buffer_;

  uint64_t needed_bits = num_bits * batch_size;
  uint64_t remaining_bits = (max_bytes - byte_offset) * 8 - bit_offset;
  if (remaining_bits < needed_bits) {
    batch_size = static_cast<int>(remaining_bits) / num_bits;
  }

  int i = 0;
  if (__builtin_expect(bit_offset != 0, 0)) {
    for (; i < batch_size && bit_offset != 0; ++i) {
      detail::GetValue_(num_bits, &v[i], max_bytes, buffer, &bit_offset, &byte_offset,
                        &buffered_values);
    }
  }

  if (sizeof(T) == 4) {
    int num_unpacked =
        internal::unpack32(reinterpret_cast<const uint32_t*>(buffer + byte_offset),
                           reinterpret_cast<uint32_t*>(v + i), batch_size - i, num_bits);
    i += num_unpacked;
    byte_offset += num_unpacked * num_bits / 8;
  } else {
    const int buffer_size = 1024;
    uint32_t unpack_buffer[buffer_size];
    while (i < batch_size) {
      int unpack_size = std::min(buffer_size, batch_size - i);
      int num_unpacked =
          internal::unpack32(reinterpret_cast<const uint32_t*>(buffer + byte_offset),
                             unpack_buffer, unpack_size, num_bits);
      if (num_unpacked == 0) {
        break;
      }
      for (int k = 0; k < num_unpacked; ++k) {
        v[i + k] = static_cast<T>(unpack_buffer[k]);
      }
      i += num_unpacked;
      byte_offset += num_unpacked * num_bits / 8;
    }
  }

  int bytes_remaining = max_bytes - byte_offset;
  if (bytes_remaining >= 8) {
    memcpy(&buffered_values, buffer + byte_offset, 8);
  } else {
    memcpy(&buffered_values, buffer + byte_offset, bytes_remaining);
  }

  for (; i < batch_size; ++i) {
    detail::GetValue_(num_bits, &v[i], max_bytes, buffer, &bit_offset, &byte_offset,
                      &buffered_values);
  }

  bit_offset_ = bit_offset;
  byte_offset_ = byte_offset;
  buffered_values_ = buffered_values;

  return batch_size;
}

template <typename T>
inline bool BitReader::GetAligned(int num_bytes, T* v) {
  assert(num_bytes <= static_cast<int>(sizeof(T)));
  int bytes_read = static_cast<int>(BytesForBits(bit_offset_));
  if (__builtin_expect(byte_offset_ + bytes_read + num_bytes > max_bytes_, 0))
    return false;

  // Advance byte_offset to next unread byte and read num_bytes
  byte_offset_ += bytes_read;
  memcpy(v, buffer_ + byte_offset_, num_bytes);
  byte_offset_ += num_bytes;

  // Reset buffered_values_
  bit_offset_ = 0;
  int bytes_remaining = max_bytes_ - byte_offset_;
  if (__builtin_expect(bytes_remaining >= 8, 1)) {
    memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
  } else {
    memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
  }
  return true;
}

inline bool BitReader::GetVlqInt(int32_t* v) {
  *v = 0;
  int shift = 0;
  int num_bytes = 0;
  uint8_t byte = 0;
  do {
    if (!GetAligned<uint8_t>(1, &byte)) return false;
    *v |= (byte & 0x7F) << shift;
    shift += 7;
    assert(++num_bytes <= MAX_VLQ_BYTE_LEN);
  } while ((byte & 0x80) != 0);
  return true;
}

inline bool BitReader::GetZigZagVlqInt(int32_t* v) {
  int32_t u_signed;
  if (!GetVlqInt(&u_signed)) return false;
  uint32_t u = static_cast<uint32_t>(u_signed);
  *reinterpret_cast<uint32_t*>(v) = (u >> 1) ^ -(static_cast<int32_t>(u & 1));
  return true;
}

/// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
/// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
/// (literal encoding).
/// For both types of runs, there is a byte-aligned indicator which encodes the length
/// of the run and the type of the run.
/// This encoding has the benefit that when there aren't any long enough runs, values
/// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
/// the run length are byte aligned. This allows for very efficient decoding
/// implementations.
/// The encoding is:
///    encoded-block := run*
///    run := literal-run | repeated-run
///    literal-run := literal-indicator < literal bytes >
///    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
///    literal-indicator := varint_encode( number_of_groups << 1 | 1)
///    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
/// Each run is preceded by a varint. The varint's least significant bit is
/// used to indicate whether the run is a literal run or a repeated run. The rest
/// of the varint is used to determine the length of the run (eg how many times the
/// value repeats).
//
/// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
/// in groups of 8), so that no matter the bit-width of the value, the sequence will end
/// on a byte boundary without padding.
/// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
/// the actual number of encoded ints. (This means that the total number of encoded values
/// can not be determined from the encoded data, since the number of values in the last
/// group may not be a multiple of 8). For the last group of literal runs, we pad
/// the group to 8 with zeros. This allows for 8 at a time decoding on the read side
/// without the need for additional checks.
//
/// There is a break-even point when it is more storage efficient to do run length
/// encoding.  For 1 bit-width values, that point is 8 values.  They require 2 bytes
/// for both the repeated encoding or the literal encoding.  This value can always
/// be computed based on the bit-width.
/// TODO: think about how to use this for strings.  The bit packing isn't quite the same.
//
/// Examples with bit-width 1 (eg encoding booleans):
/// ----------------------------------------
/// 100 1s followed by 100 0s:
/// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1 byte>
///  - (total 4 bytes)
//
/// alternating 1s and 0s (200 total):
/// 200 ints = 25 groups of 8
/// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
/// (total 26 bytes, 1 byte overhead)
//

/// Decoder class for RLE encoded data.
class RleDecoder {
 public:
  /// Create a decoder object. buffer/buffer_len is the decoded data.
  /// bit_width is the width of each value (before encoding).
  RleDecoder(const uint8_t* buffer, int buffer_len, int bit_width)
      : bit_reader_(buffer, buffer_len),
        bit_width_(bit_width),
        current_value_(0),
        repeat_count_(0),
        literal_count_(0) {
    assert(bit_width_ >= 0);
    assert(bit_width_ <= 64);
  }

  RleDecoder() : bit_width_(-1) {}

  void Reset(const uint8_t* buffer, int buffer_len, int bit_width) {
    assert(bit_width >= 0);
    assert(bit_width <= 64);
    bit_reader_.Reset(buffer, buffer_len);
    bit_width_ = bit_width;
    current_value_ = 0;
    repeat_count_ = 0;
    literal_count_ = 0;
  }

  /// Gets a batch of values.  Returns the number of decoded elements.
  template <typename T>
  int GetBatch(T* values, int batch_size);

 protected:
  BitReader bit_reader_;
  /// Number of bits needed to encode the value. Must be between 0 and 64.
  int bit_width_;
  uint64_t current_value_;
  uint32_t repeat_count_;
  uint32_t literal_count_;

 private:
  /// Fills literal_count_ and repeat_count_ with next values. Returns false if there
  /// are no more.
  template <typename T>
  bool NextCounts();
};

template <typename T>
bool RleDecoder::NextCounts() {
  // Read the next run's indicator int, it could be a literal or repeated run.
  // The int is encoded as a vlq-encoded value.
  int32_t indicator_value = 0;
  bool result = bit_reader_.GetVlqInt(&indicator_value);
  if (!result) return false;

  // lsb indicates if it is a literal run or repeated run
  bool is_literal = indicator_value & 1;
  if (is_literal) {
    literal_count_ = (indicator_value >> 1) * 8;
  } else {
    repeat_count_ = indicator_value >> 1;
    // XXX (ARROW-4018) this is not big-endian compatible
    bool result =
        bit_reader_.GetAligned<T>(static_cast<int>(CeilDiv(bit_width_, 8)),
                                  reinterpret_cast<T*>(&current_value_));
    assert(result);
  }
  return true;
}

template <typename T>
inline int RleDecoder::GetBatch(T* values, int batch_size) {
  assert(bit_width_ >= 0);
  int values_read = 0;

  while (values_read < batch_size) {
    if (repeat_count_ > 0) {
      int repeat_batch =
          std::min(batch_size - values_read, static_cast<int>(repeat_count_));
      std::fill(values + values_read, values + values_read + repeat_batch,
                static_cast<T>(current_value_));
      repeat_count_ -= repeat_batch;
      values_read += repeat_batch;
    } else if (literal_count_ > 0) {
      int literal_batch =
          std::min(batch_size - values_read, static_cast<int>(literal_count_));
      int actual_read =
          bit_reader_.GetBatch(bit_width_, values + values_read, literal_batch);
      assert(actual_read == literal_batch);
      literal_count_ -= literal_batch;
      values_read += literal_batch;
    } else {
      if (!NextCounts<T>()) return values_read;
    }
  }

  return values_read;
}

}  // namespace parquet
