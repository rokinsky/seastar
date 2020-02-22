#include <seastar/parquet/cql_reader.hh>
#include <seastar/parquet/file_reader.hh>
#include <seastar/parquet/overloaded.hh>
#include <vector>

namespace parquet::cql {

template <typename format::Type::type T>
seastar::future<> typed_primitive_reader<T>::read_field(std::ostream& out) {
    return next().then([this, &out] (std::optional<triplet> t) {
        if (!t) {
            throw parquet_exception("No more values buffered");
        } else if (t->value) {
            format_value(*t->value, out);
        }
    });
}

template <typename format::Type::type T>
seastar::future<> typed_primitive_reader<T>::skip_field() {
    return next().then([this] (std::optional<triplet> triplet) {
        if (!triplet) {
            throw parquet_exception("No more values buffered");
        }
    });
}

template <typename format::Type::type T>
seastar::future<int, int> typed_primitive_reader<T>::current_levels() {
    return refill_when_empty().then([this] {
        if (!_levels_buffered) {
            return seastar::make_ready_future<int, int>(-1, -1);
        } else {
            return seastar::make_ready_future<int, int>(current_def_level(), current_rep_level());
        }
    });
}

template <typename format::Type::type T>
int typed_primitive_reader<T>::current_def_level() {
    return _node.def_level > 0 ? _def_levels[_levels_offset] : 0;
}

template <typename format::Type::type T>
int typed_primitive_reader<T>::current_rep_level() {
    return _node.rep_level > 0 ? _rep_levels[_levels_offset] : 0;
}

template <typename format::Type::type T>
seastar::future<> typed_primitive_reader<T>::refill_when_empty() {
    if (_levels_offset == _levels_buffered) {
        return _source.read_batch(
            _def_levels.size(),
            _def_levels.data(),
            _rep_levels.data(),
            _values.data()
        ).then([this] (size_t levels_read) {
            _levels_buffered = levels_read;
            _values_buffered = 0;
            for (size_t i = 0; i < levels_read; ++i) {
                if (_def_levels[i] == _node.def_level) {
                    ++_values_buffered;
                }
            }
            _values_offset = 0;
            _levels_offset = 0;
        });
    }
    return seastar::make_ready_future<>();
}

template <typename format::Type::type T>
seastar::future<std::optional<typename typed_primitive_reader<T>::triplet>> typed_primitive_reader<T>::next() {
    return refill_when_empty().then([this] {
        if (!_levels_buffered) {
            return std::optional<triplet>{};
        }

        int16_t def_level = current_def_level();
        int16_t rep_level = current_rep_level();
        _levels_offset++;

        bool is_null = def_level < _node.def_level;
        if (is_null) {
            return std::optional<triplet>{triplet{def_level, rep_level, std::nullopt}};
        }

        if (_values_offset == _values_buffered) {
            throw parquet_exception("Value was non-null, but has not been buffered");
        }

        output_type& val = _values[_values_offset++];
        return std::optional<triplet>{triplet{def_level, rep_level, std::move(val)}};
    });
}

namespace {
    std::ostream& operator<<(std::ostream& out, const seastar::temporary_buffer<uint8_t>& b) {
        return out << '"' << std::string_view{reinterpret_cast<const char*>(b.get()), b.size()} << '"';
    }

    std::ostream& operator<<(std::ostream& out, const std::array<int32_t, 3>& a) {
        return out
                << static_cast<uint32_t>(a[0]) << ' '
                << static_cast<uint32_t>(a[1]) << ' '
                << static_cast<uint32_t>(a[2]);
    }

    std::ostream& operator<<(std::ostream& out, const uint8_t& a) {
        return out << static_cast<int>(a);
    }
}

template <typename format::Type::type T>
inline void typed_primitive_reader<T>::format_value(const output_type& val, std::ostream &out) {
    out << val;
}

seastar::future<> struct_reader::read_field(std::ostream& out) {
    _separator = "";
    out << "{";
    return seastar::do_for_each(_readers, [this, &out] (auto& child) mutable {
        out << _separator;
        _separator = ", ";
        out << *child.name() << ": ";
        return child.read_field(out);
    }).then([&out] {
        out << "}";
    });
}

seastar::future<> struct_reader::skip_field() {
    return seastar::do_for_each(_readers.begin(), _readers.end(), [] (auto& child) {
        return child.skip_field();
    });
}

seastar::future<int, int> struct_reader::current_levels() {
    return _readers[0].current_levels();
}

seastar::future<> list_reader::read_field(std::ostream& out) {
    out << "[";
    return current_levels().then([this, &out] (int def, int) {
        if (def > _node.def_level) {
            return _reader->read_field(out).then([this, &out] {
                return seastar::repeat([this, &out] {
                    return current_levels().then([this, &out] (int def, int rep) {
                        if (rep > _node.rep_level) {
                            out << ", ";
                            return _reader->read_field(out).then([] {
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
            return _reader->skip_field();
        }
    }).then([&out] {
        out << "]";
    });
}

seastar::future<> list_reader::skip_field() {
    return _reader->skip_field();
}

seastar::future<int, int> list_reader::current_levels() {
    return _reader->current_levels();
}

seastar::future<> optional_reader::read_field(std::ostream& out) {
  return current_levels().then([this, &out] (int def, int) {
    if (def > _node.def_level) {
      return _reader->read_field(out);
    } else {
      out << "null";
      return _reader->skip_field();
    }
  });
}

seastar::future<> optional_reader::skip_field() {
  return _reader->skip_field();
}

seastar::future<int, int> optional_reader::current_levels() {
  return _reader->current_levels();
}

seastar::future<> map_reader::read_field(std::ostream& out) {
    out << "{";
    return current_levels().then([this, &out] (int def, int) {
        if (def > _node.def_level) {
            return read_pair(out).then([this, &out] {
                return seastar::repeat([this, &out] {
                    return current_levels().then([this, &out] (int def, int rep) {
                        if (rep > _node.rep_level) {
                            out << ", ";
                            return read_pair(out).then([this, &out] {
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
            return skip_field();
        }
    }).then([&out] {
        out << "}";
    });
}

seastar::future<> map_reader::skip_field() {
    return seastar::when_all_succeed(
            _key_reader->skip_field(),
            _value_reader->skip_field());
}

seastar::future<int, int> map_reader::current_levels() {
    return _key_reader->current_levels();
}

seastar::future<> map_reader::read_pair(std::ostream& out) {
    return _key_reader->read_field(out).then([this, &out] {
        out << ": ";
        return _value_reader->read_field(out);
    });
}

seastar::future<> record_reader::read_one(std::ostream &out) {
    _separator = "";
    out << "{";
    return seastar::do_for_each(_field_readers, [this, &out] (auto& child) {
        return child.current_levels().then([this, &out, &child] (int def, int) {
            return std::visit(overloaded {
                [this, def, &out] (optional_reader& typed_child) {
                    if (def > 0) {
                        out << _separator;
                        _separator = ", ";
                        out << typed_child._node.info.name << ": ";
                        return typed_child.read_field(out);
                    } else {
                        return typed_child.skip_field();
                    }
                },
                [this, &out] (auto& typed_child) {
                    out << _separator;
                    _separator = ", ";
                    out << typed_child._node.info.name << ": ";
                    return typed_child.read_field(out);
                }
            }, child._reader);
        });
    }).then([&out] {
        out << "}\n";
    });
}

seastar::future<> record_reader::read_all(std::ostream &out) {
    return seastar::repeat([this, &out] {
        return current_levels().then([this, &out] (int def, int) {
            if (def < 0) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            } else {
                return read_one(out).then([] { return seastar::stop_iteration::no; });
            }
        });
    });
}

seastar::future<int, int> record_reader::current_levels() {
    return _field_readers[0].current_levels();
}

field_reader field_reader::make(const file_reader& fr, const schema::node& node_variant, int row_group_index) {
    return std::visit(overloaded {
        [&] (const schema::primitive_node& node) {
            const format::ColumnMetaData& column_metadata =
                    fr.metadata().row_groups[row_group_index].columns[node.column_index].meta_data;
            int64_t file_offset = column_metadata.__isset.dictionary_page_offset
                    ? column_metadata.dictionary_page_offset
                    : column_metadata.data_page_offset;
            page_reader page_reader{
                seastar::make_file_input_stream(
                        fr.file(),
                        file_offset,
                        column_metadata.total_compressed_size)};
            page_decompressor pd{std::move(page_reader), column_metadata.codec};
            switch (node.info.type) {
            case format::Type::INT32: {
                column_chunk_reader<format::Type::INT32> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::INT32>{node, std::move(r)}};
            }
            case format::Type::INT64: {
                column_chunk_reader<format::Type::INT64> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::INT64>{node, std::move(r)}};
            }
            case format::Type::INT96: {
                column_chunk_reader<format::Type::INT96> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::INT96>{node, std::move(r)}};
            }
            case format::Type::FLOAT: {
                column_chunk_reader<format::Type::FLOAT> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::FLOAT>{node, std::move(r)}};
            }
            case format::Type::DOUBLE: {
                column_chunk_reader<format::Type::DOUBLE> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::DOUBLE>{node, std::move(r)}};
            }
            case format::Type::BOOLEAN: {
                column_chunk_reader<format::Type::BOOLEAN> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::BOOLEAN>{node, std::move(r)}};
            }
            case format::Type::BYTE_ARRAY: {
                column_chunk_reader<format::Type::BYTE_ARRAY> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::BYTE_ARRAY>{node, std::move(r)}};
            }
            case format::Type::FIXED_LEN_BYTE_ARRAY: {
                column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY> r(node, std::move(pd));
                return field_reader{typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY>{node, std::move(r)}};
            }
            default: throw;
            }
        },
        [&] (const schema::list_node& node) {
            auto child = std::make_unique<field_reader>(
                    field_reader::make(fr, *node.element_node, row_group_index));
            return field_reader{list_reader{node, std::move(child)}};
        },
        [&] (const schema::optional_node& node) {
            auto child = std::make_unique<field_reader>(
                    field_reader::make(fr, *node.child_node, row_group_index));
            return field_reader{optional_reader{node, std::move(child)}};
        },
        [&] (const schema::map_node& node) {
            auto key_reader = std::make_unique<field_reader>(
                    field_reader::make(fr, *node.key_node, row_group_index));
            auto value_reader = std::make_unique<field_reader>(
                    field_reader::make(fr, *node.value_node, row_group_index));
            return field_reader{map_reader{node, std::move(key_reader), std::move(value_reader)}};
        },
        [&] (const schema::struct_node& node) {
            std::vector<field_reader> field_readers;
            for (const schema::node& child : node.field_nodes) {
                field_readers.push_back(field_reader::make(fr, child, row_group_index));
            }
            return field_reader{struct_reader{node, std::move(field_readers)}};
        }
    }, node_variant);
}

record_reader record_reader::make(const file_reader& fr, int row_group_index) {
    std::vector<field_reader> field_readers;
    for (const schema::node& field_node : fr.schema().field_nodes) {
        field_readers.push_back(field_reader::make(fr, field_node, row_group_index));
    }
    return record_reader{fr.schema(), std::move(field_readers)};
}

template class typed_primitive_reader<format::Type::INT32>;
template class typed_primitive_reader<format::Type::INT64>;
template class typed_primitive_reader<format::Type::INT96>;
template class typed_primitive_reader<format::Type::FLOAT>;
template class typed_primitive_reader<format::Type::DOUBLE>;
template class typed_primitive_reader<format::Type::BOOLEAN>;
template class typed_primitive_reader<format::Type::BYTE_ARRAY>;
template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY>;

} // namespace parquet::cql
