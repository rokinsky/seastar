// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_TABLE_H
#define ARROW_TABLE_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <seastar/parquet/arrow/array.h>
#include <seastar/parquet/arrow/result.h>
#include <seastar/parquet/arrow/status.h>
#include <seastar/parquet/arrow/type.h>
#include <seastar/parquet/arrow/util/macros.h>
#include <seastar/parquet/arrow/util/visibility.h>

namespace arrow {

class MemoryPool;
class Status;

/// \class ChunkedArray
/// \brief A data structure managing a list of primitive Arrow arrays logically
/// as one large array
class ARROW_EXPORT ChunkedArray {
 public:
  /// \brief Construct a chunked array from a vector of arrays
  ///
  /// The vector must be non-empty and all its elements must have the same
  /// data type.
  explicit ChunkedArray(const ArrayVector& chunks);

  /// \brief Construct a chunked array from a single Array
  explicit ChunkedArray(const std::shared_ptr<Array>& chunk)
      : ChunkedArray(ArrayVector({chunk})) {}

  /// \brief Construct a chunked array from a vector of arrays and a data type
  ///
  /// As the data type is passed explicitly, the vector may be empty.
  ChunkedArray(const ArrayVector& chunks, const std::shared_ptr<DataType>& type);

  /// \return the total length of the chunked array; computed on construction
  int64_t length() const { return length_; }

  /// \return the total number of nulls among all chunks
  int64_t null_count() const { return null_count_; }

  int num_chunks() const { return static_cast<int>(chunks_.size()); }

  /// \return chunk a particular chunk from the chunked array
  std::shared_ptr<Array> chunk(int i) const { return chunks_[i]; }

  const ArrayVector& chunks() const { return chunks_; }

  /// \brief Construct a zero-copy slice of the chunked array with the
  /// indicated offset and length
  ///
  /// \param[in] offset the position of the first element in the constructed
  /// slice
  /// \param[in] length the length of the slice. If there are not enough
  /// elements in the chunked array, the length will be adjusted accordingly
  ///
  /// \return a new object wrapped in std::shared_ptr<ChunkedArray>
  std::shared_ptr<ChunkedArray> Slice(int64_t offset, int64_t length) const;

  /// \brief Slice from offset until end of the chunked array
  std::shared_ptr<ChunkedArray> Slice(int64_t offset) const;

  /// \brief Flatten this chunked array as a vector of chunked arrays, one
  /// for each struct field
  ///
  /// \param[in] pool The pool for buffer allocations, if any
  /// \param[out] out The resulting vector of arrays
  Status Flatten(MemoryPool* pool, std::vector<std::shared_ptr<ChunkedArray>>* out) const;

  /// Construct a zero-copy view of this chunked array with the given
  /// type. Calls Array::View on each constituent chunk. Always succeeds if
  /// there are zero chunks
  Status View(const std::shared_ptr<DataType>& type,
              std::shared_ptr<ChunkedArray>* out) const;

  std::shared_ptr<DataType> type() const { return type_; }

  /// \brief Determine if two chunked arrays are equal.
  ///
  /// Two chunked arrays can be equal only if they have equal datatypes.
  /// However, they may be equal even if they have different chunkings.
  bool Equals(const ChunkedArray& other) const;
  /// \brief Determine if two chunked arrays are equal.
  bool Equals(const std::shared_ptr<ChunkedArray>& other) const;

  /// \brief Check that all chunks have the same data type
  Status Validate() const;

 protected:
  ArrayVector chunks_;
  int64_t length_;
  int64_t null_count_;
  std::shared_ptr<DataType> type_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ChunkedArray);
};

}  // namespace arrow

#endif  // ARROW_TABLE_H
