#include <seastar/parquet/parquet/cql_reader.h>
#include <seastar/parquet/parquet/column_reader.h>
#include <seastar/util/std-compat.hh>
#include <vector>

using seastar::compat::optional;

namespace parquet::seastarized::cql {

class FieldReader {
 public:
  virtual ~FieldReader() = default;
  explicit FieldReader(const logical_schema::Node& node) : node_(node) {}

  virtual seastar::future<> ReadField(std::ostream& out) = 0;
  virtual seastar::future<> SkipField() = 0;
  virtual seastar::future<int, int> CurrentLevels() = 0;

  static std::unique_ptr<FieldReader> Make(
      RowGroupReader *row_group_reader,
      const logical_schema::Node& node);

  const logical_schema::Node& node_;
};

namespace {

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

using namespace logical_schema;

static constexpr int64_t DEFAULT_BATCH_SIZE = 1024;

class PrimitiveReader : public FieldReader {
 public:
  static std::unique_ptr<PrimitiveReader> Make(
      const Node &node,
      std::shared_ptr<ColumnReader> col_reader,
      int64_t batch_size = DEFAULT_BATCH_SIZE);

  seastar::future<bool> HasNext() {
    if (level_offset_ < levels_buffered_) {
      return seastar::make_ready_future<bool>(true);
    }
    return reader_->HasNext();
  }

 protected:
  explicit PrimitiveReader(
      const Node &node,
      std::shared_ptr<ColumnReader> reader,
      int64_t batch_size = DEFAULT_BATCH_SIZE
  ) : FieldReader(node),
      reader_(reader),
      batch_size_(batch_size) {
    def_levels_.resize(reader_->descr()->max_definition_level() > 0 ? batch_size_ : 0);
    rep_levels_.resize(reader_->descr()->max_repetition_level() > 0 ? batch_size_ : 0);
  }

  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  std::vector<uint8_t> value_buffer_;
  int64_t level_offset_ = 0;
  int64_t levels_buffered_ = 0;
  int value_offset_ = 0;
  int64_t values_buffered_ = 0;

  std::shared_ptr<ColumnReader> reader_;
  int64_t batch_size_ = DEFAULT_BATCH_SIZE;
};

template <typename DType>
class TypedPrimitiveReader : public PrimitiveReader {
 public:
  typedef typename DType::c_type T;

  struct Triplet {
    int16_t def_level;
    int16_t rep_level;
    optional<T> value;
  };

  explicit TypedPrimitiveReader(
      const Node &node,
      std::shared_ptr<ColumnReader> reader,
      int64_t batch_size = DEFAULT_BATCH_SIZE
  ) : PrimitiveReader(node, reader, batch_size) {
    typed_reader_ = static_cast<TypedColumnReader<DType>*>(reader.get());
    int value_byte_size = type_traits<DType::type_num>::value_byte_size;
    value_buffer_.reserve(batch_size_ * value_byte_size);
    values_ = reinterpret_cast<T*>(value_buffer_.data());
  }

  seastar::future<> ReadField(std::ostream& out) override {
    return Next().then([this, &out] (optional<Triplet> triplet) {
      if (!triplet) {
        throw ParquetException("No more values buffered");
      }

      if (triplet->value) {
        FormatValue(*triplet->value, out);
      }
    });
  }

  seastar::future<> SkipField() override {
    return Next().then([this] (optional<Triplet> triplet) {
      if (!triplet) {
        throw ParquetException("No more values buffered");
      }
    });
  }

  seastar::future<int, int> CurrentLevels() override {
    return RefillWhenEmpty().then([this] {
      if (!levels_buffered_) {
        return seastar::make_ready_future<int, int>(-1, -1);
      } else {
        return seastar::make_ready_future<int, int>(CurrentDefLevel(), CurrentRepLevel());
      }
    });
  }

 private:
  int CurrentDefLevel() {
    return reader_->descr()->max_definition_level() > 0 ? def_levels_[level_offset_] : 0;
  }

  int CurrentRepLevel() {
    return reader_->descr()->max_repetition_level() > 0 ? rep_levels_[level_offset_] : 0;
  }

  seastar::future<> RefillWhenEmpty() {
    return HasNext().then([this] (bool has_next) {
      if (!has_next) {
        levels_buffered_ = 0;
        value_offset_ = 0;
        level_offset_ = 0;
        return seastar::make_ready_future<>();
      }

      if (level_offset_ == levels_buffered_) {
        return typed_reader_->ReadBatch(
            static_cast<int>(batch_size_),
            def_levels_.data(),
            rep_levels_.data(),
            values_,
            &values_buffered_
        ).then([this] (int64_t levels_read) {
          levels_buffered_ = levels_read;
          value_offset_ = 0;
          level_offset_ = 0;
        });
      }
      return seastar::make_ready_future<>();
    });
  }

  seastar::future<optional<Triplet>> Next() {
    return RefillWhenEmpty().then([this] {
      if (!levels_buffered_) {
        return optional<Triplet>{};
      }

      int16_t def_level = CurrentDefLevel();
      int16_t rep_level = CurrentRepLevel();
      level_offset_++;

      bool is_null = def_level < reader_->descr()->max_definition_level();
      if (is_null) {
        return optional<Triplet>{Triplet{def_level, rep_level, optional<T>()}};
      }

      if (value_offset_ == values_buffered_) {
        throw ParquetException("Value was non-null, but has not been buffered");
      }

      T val = values_[value_offset_++];
      return optional<Triplet>{Triplet{def_level, rep_level, optional<T>(val)}};
    });
  }

  inline void FormatValue(const T& val, std::ostream &out);

  TypedColumnReader<DType>* typed_reader_;
  T* values_;
};

template <typename DType>
inline void TypedPrimitiveReader<DType>::FormatValue(const T& val, std::ostream &out) {
  out << val;
}

template <>
inline void TypedPrimitiveReader<Int96Type>::FormatValue(const T& val, std::ostream &out) {
  out << Int96ToString(val);
}

template <>
inline void TypedPrimitiveReader<ByteArrayType>::FormatValue(const T& val, std::ostream &out) {
  out << '"' << ByteArrayToString(val) << '"';
}

template <>
inline void TypedPrimitiveReader<FLBAType>::FormatValue(const T& val, std::ostream &out) {
  out << '"' << FixedLenByteArrayToString(val, reader_->descr()->type_length()) << '"';
}

std::unique_ptr<PrimitiveReader>
PrimitiveReader::Make(
    const Node& node,
    std::shared_ptr<ColumnReader> col_reader,
    int64_t batch_size
) {
  switch (col_reader->type()) {
    case Type::BOOLEAN:
      return std::make_unique<TypedPrimitiveReader<BooleanType>>(node, col_reader, batch_size);
    case Type::INT32:
      return std::make_unique<TypedPrimitiveReader<Int32Type>>(node, col_reader, batch_size);
    case Type::INT64:
      return std::make_unique<TypedPrimitiveReader<Int64Type>>(node, col_reader, batch_size);
    case Type::INT96:
      return std::make_unique<TypedPrimitiveReader<Int96Type>>(node, col_reader, batch_size);
    case Type::FLOAT:
      return std::make_unique<TypedPrimitiveReader<FloatType>>(node, col_reader, batch_size);
    case Type::DOUBLE:
      return std::make_unique<TypedPrimitiveReader<DoubleType>>(node, col_reader, batch_size);
    case Type::BYTE_ARRAY:
      return std::make_unique<TypedPrimitiveReader<ByteArrayType>>(node, col_reader, batch_size);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_unique<TypedPrimitiveReader<FLBAType>>(node, col_reader, batch_size);
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::unique_ptr<PrimitiveReader>(nullptr);
}

class StructReader : public FieldReader {
 public:
  explicit StructReader(
      const Node& node,
      int def_level,
      std::vector<std::shared_ptr<FieldReader>> readers)
  : FieldReader(node),
    def_level_(std::visit([](const auto &node) { return node.def_level; }, node)),
    readers_(std::move(readers)) {}

  seastar::future<> ReadField(std::ostream& out) override {
    separator_ = "";
    out << "{";
    return seastar::do_for_each(readers_, [this, &out] (const auto& child) {
      return std::visit(
        [this, &out, &child] (const auto& node) {
          out << separator_;
          separator_ = ", ";
          out << node.name << ": ";
          return child->ReadField(out);
      }, child->node_);
    }).then([&out] {
      out << "}";
    });
  }

  seastar::future<> SkipField() override {
    return seastar::do_for_each(readers_.begin(), readers_.end(), [] (auto reader) {
        return reader->SkipField();
    });
  }

  seastar::future<int, int> CurrentLevels() {
    return readers_.at(0)->CurrentLevels();
  }

 private:
  const char* separator_ = "";
  int def_level_;
  const std::vector<std::shared_ptr<FieldReader>> readers_;
};

class ListReader : public FieldReader {
 public:
  explicit ListReader(
      const Node& node,
      std::shared_ptr<FieldReader> reader)
  : FieldReader(node),
    def_level_(std::visit([](const auto &node) { return node.def_level; }, node)),
    rep_level_(std::visit([](const auto &node) { return node.rep_level; }, node)),
    reader_(std::move(reader)) {}

  seastar::future<> ReadField(std::ostream& out) override {
    out << "[";
    return CurrentLevels().then([this, &out] (int def, int) {
      if (def > def_level_) {
        return reader_->ReadField(out).then([this, &out] {
          return seastar::repeat([this, &out] {
            return CurrentLevels().then([this, &out] (int def, int rep) {
              if (rep > rep_level_) {
                out << ", ";
                return reader_->ReadField(out).then([] {
                    return seastar::stop_iteration::no;
                });
              } else {
                return seastar::make_ready_future<seastar::stop_iteration>(
                    seastar::stop_iteration::yes);
              }
            });
          });
        });
      } else {
        return reader_->SkipField();
      }
    }).then([&out] {
      out << "]";
    });
  }

  seastar::future<> SkipField() override {
    return reader_->SkipField();
  }

  seastar::future<int, int> CurrentLevels() {
    return reader_->CurrentLevels();
  }

 private:
  int def_level_;
  int rep_level_;
  std::shared_ptr<FieldReader> reader_;
};

class OptionalReader : public FieldReader {
 public:
  explicit OptionalReader(
      const Node &node,
      std::shared_ptr<FieldReader> reader)
  : FieldReader(node),
    def_level_(std::visit([](const auto &node) { return node.def_level; }, node)),
    reader_(std::move(reader)) {}

  seastar::future<> ReadField(std::ostream& out) override {
    return CurrentLevels().then([this, &out] (int def, int) {
      if (def > def_level_) {
        return reader_->ReadField(out);
      } else {
        out << "null";
        return reader_->SkipField();
      }
    });
  }

  seastar::future<> SkipField() override {
    return reader_->SkipField();
  }

  seastar::future<int, int> CurrentLevels() {
    return reader_->CurrentLevels();
  }

 private:
  int def_level_;
  std::shared_ptr<FieldReader> reader_;
};

class MapReader : public FieldReader {
 public:
  explicit MapReader(
      const Node& node,
      std::shared_ptr<FieldReader> key_reader,
      std::shared_ptr<FieldReader> value_reader)
  : FieldReader(node),
    def_level_(std::visit([](const auto &node) { return node.def_level; }, node)),
    rep_level_(std::visit([](const auto &node) { return node.rep_level; }, node)),
    key_reader_(std::move(key_reader)),
    value_reader_(std::move(value_reader)) {}

  seastar::future<> ReadField(std::ostream& out) override {
    out << "{";
    return CurrentLevels().then([this, &out] (int def, int) {
      if (def > def_level_) {
        return ReadPair(out).then([this, &out] {
          return seastar::repeat([this, &out] {
            return CurrentLevels().then([this, &out] (int def, int rep) {
              if (rep > rep_level_) {
                out << ", ";
                return ReadPair(out).then([this, &out] {
                  return seastar::stop_iteration::no;
                });
              } else {
                return seastar::make_ready_future<seastar::stop_iteration>(
                    seastar::stop_iteration::yes);
              }
            });
          });
        });
      } else {
        return SkipField();
      }
    }).then([&out] {
      out << "}";
    });
  }

  seastar::future<> SkipField() {
    return seastar::when_all_succeed(
        key_reader_->SkipField(),
        value_reader_->SkipField());
  }

  seastar::future<int, int> CurrentLevels() {
    return key_reader_->CurrentLevels();
  }

 private:
  seastar::future<> ReadPair(std::ostream& out) {
    return key_reader_->ReadField(out).then([this, &out] {
      out << ": ";
      return value_reader_->ReadField(out);
    });
  }

  int def_level_;
  int rep_level_;
  std::shared_ptr<FieldReader> key_reader_;
  std::shared_ptr<FieldReader> value_reader_;;
};

} // namespace

RecordReader::RecordReader(
    std::shared_ptr<const logical_schema::RootNode> schema,
    std::vector<std::unique_ptr<FieldReader>> field_readers)
  : schema_(std::move(schema)), field_readers_(std::move(field_readers)) {}

RecordReader::~RecordReader() = default;

seastar::future<> RecordReader::ReadOne(std::ostream &out) {
  separator_ = "";
  out << "{";
  return seastar::do_for_each(field_readers_, [this, &out] (auto& child) {
    return child->CurrentLevels().then([this, &out, &child] (int def, int) {
      return std::visit(overloaded {
        [this, def, &out, &child] (const OptionalNode& node) {
          if (def > 0) {
            out << separator_;
            separator_ = ", ";
            out << node.name << ": ";
            return child->ReadField(out);
          } else {
            return child->SkipField();
          }
        },
        [this, &out, &child] (const auto& node) {
          out << separator_;
          separator_ = ", ";
          out << node.name << ": ";
          return child->ReadField(out);
        }
      }, child->node_);
    });
  }).then([&out] {
    out << "}\n";
  });
}

seastar::future<> RecordReader::ReadAll(std::ostream &out) {
  return seastar::repeat([this, &out] {
    return CurrentLevels().then([this, &out] (int def, int) {
      if (def < 0) {
        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
      } else {
        return ReadOne(out).then([] { return seastar::stop_iteration::no; });
      }
    });
  });
}

seastar::future<int, int> RecordReader::CurrentLevels() {
  return field_readers_.at(0)->CurrentLevels();
}

std::unique_ptr<FieldReader> FieldReader::Make(
    RowGroupReader *row_group_reader,
    const logical_schema::Node& node_variant) {
  return std::visit(overloaded {
    [&] (const PrimitiveNode& node) -> std::unique_ptr<FieldReader> {
      return PrimitiveReader::Make(node_variant, row_group_reader->Column(node.column_index));
    },
    [&] (const ListNode& node) -> std::unique_ptr<FieldReader>  {
      auto child = FieldReader::Make(row_group_reader, *node.element_node);
      return std::make_unique<ListReader>(node_variant, std::move(child));
    },
    [&] (const OptionalNode& node) -> std::unique_ptr<FieldReader>  {
      auto child = FieldReader::Make(row_group_reader, *node.child_node);
      return std::make_unique<OptionalReader>(node_variant, std::move(child));
    },
    [&] (const MapNode& node) -> std::unique_ptr<FieldReader>  {
      auto key_reader = FieldReader::Make(row_group_reader, *node.key_node);
      auto value_reader = FieldReader::Make(row_group_reader, *node.value_node);
      return std::make_unique<MapReader>(
          node_variant, std::move(key_reader), std::move(value_reader));
    },
    [&] (const StructNode& node) -> std::unique_ptr<FieldReader>  {
      std::vector<std::shared_ptr<FieldReader>> field_readers;
      for (const Node& child : node.field_nodes) {
        field_readers.push_back(FieldReader::Make(row_group_reader, child));
      }
      return std::make_unique<StructReader>(node_variant, node.def_level, std::move(field_readers));
    }
  }, node_variant);
}

RecordReader RecordReader::Make(
    RowGroupReader *row_group_reader,
    std::shared_ptr<const logical_schema::RootNode> schema) {
  std::vector<std::unique_ptr<FieldReader>> field_readers;
  for (const Node& field_node : schema->field_nodes) {
    field_readers.push_back(FieldReader::Make(row_group_reader, field_node));
  }
  return RecordReader{std::move(schema), std::move(field_readers)};
}

} // namespace parquet::seastarized::cql
