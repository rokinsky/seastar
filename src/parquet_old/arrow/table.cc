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

#include <seastar/parquet/arrow/table.h>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <memory>
#include <sstream>
#include <utility>

#include <seastar/parquet/arrow/array.h>
#include <seastar/parquet/arrow/array/concatenate.h>
#include <seastar/parquet/arrow/status.h>
#include <seastar/parquet/arrow/type.h>
#include <seastar/parquet/arrow/util/checked_cast.h>
#include <seastar/parquet/arrow/util/logging.h>
#include <seastar/parquet/arrow/util/stl.h>

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// ChunkedArray methods

ChunkedArray::ChunkedArray(const ArrayVector& chunks) : chunks_(chunks) {
  length_ = 0;
  null_count_ = 0;

  ARROW_CHECK_GT(chunks.size(), 0)
    << "cannot construct ChunkedArray from empty vector and omitted type";
  type_ = chunks[0]->type();
  for (const std::shared_ptr<Array>& chunk : chunks) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

ChunkedArray::ChunkedArray(const ArrayVector& chunks,
                           const std::shared_ptr<DataType>& type)
  : chunks_(chunks), type_(type) {
  length_ = 0;
  null_count_ = 0;
  for (const std::shared_ptr<Array>& chunk : chunks) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

std::shared_ptr<ChunkedArray> ChunkedArray::Slice(int64_t offset, int64_t length) const {
  ARROW_CHECK_LE(offset, length_) << "Slice offset greater than array length";

  int curr_chunk = 0;
  while (curr_chunk < num_chunks() && offset >= chunk(curr_chunk)->length()) {
    offset -= chunk(curr_chunk)->length();
    curr_chunk++;
  }

  ArrayVector new_chunks;
  while (curr_chunk < num_chunks() && length > 0) {
    new_chunks.push_back(chunk(curr_chunk)->Slice(offset, length));
    length -= chunk(curr_chunk)->length() - offset;
    offset = 0;
    curr_chunk++;
  }

  return std::make_shared<ChunkedArray>(new_chunks, type_);
}

std::shared_ptr<ChunkedArray> ChunkedArray::Slice(int64_t offset) const {
  return Slice(offset, length_);
}

Status ChunkedArray::Flatten(MemoryPool* pool,
                             std::vector<std::shared_ptr<ChunkedArray>>* out) const {
  out->clear();
  if (type()->id() != Type::STRUCT) {
    // Emulate non-existent copy constructor
    *out = {std::make_shared<ChunkedArray>(chunks_, type_)};
    return Status::OK();
  }

  std::vector<ArrayVector> flattened_chunks(type()->num_children());
  for (const auto& chunk : chunks_) {
    ArrayVector res;
    RETURN_NOT_OK(checked_cast<const StructArray&>(*chunk).Flatten(pool, &res));
    DCHECK_EQ(res.size(), flattened_chunks.size());
    for (size_t i = 0; i < res.size(); ++i) {
      flattened_chunks[i].push_back(res[i]);
    }
  }

  out->resize(type()->num_children());
  for (size_t i = 0; i < out->size(); ++i) {
    auto child_type = type()->child(static_cast<int>(i))->type();
    out->at(i) = std::make_shared<ChunkedArray>(flattened_chunks[i], child_type);
  }
  return Status::OK();
}

Status ChunkedArray::View(const std::shared_ptr<DataType>& type,
                          std::shared_ptr<ChunkedArray>* out) const {
  ArrayVector out_chunks(this->num_chunks());
  for (int i = 0; i < this->num_chunks(); ++i) {
    RETURN_NOT_OK(chunks_[i]->View(type, &out_chunks[i]));
  }
  *out = std::make_shared<ChunkedArray>(out_chunks, type);
  return Status::OK();
}

Status ChunkedArray::Validate() const {
  if (chunks_.size() == 0) {
    return Status::OK();
  }

  for (auto chunk : chunks_) {
    // Validate the chunks themselves
    RETURN_NOT_OK(chunk->Validate());
  }

  const auto& type = *chunks_[0]->type();
  // Make sure chunks all have the same type
  for (size_t i = 1; i < chunks_.size(); ++i) {
    const Array& chunk = *chunks_[i];
    if (!chunk.type()->Equals(type)) {
      return Status::Invalid("In chunk ", i, " expected type ", type.ToString(),
                             " but saw ", chunk.type()->ToString());
    }
  }
  return Status::OK();
}

}  // namespace arrow
