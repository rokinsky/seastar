#include <seastar/parquet/cql_reader.hh>
#include <seastar/parquet/file_reader.hh>
#include <seastar/parquet/overloaded.hh>
#include <vector>

namespace parquet::cql {

template <typename format::Type::type T, typename L>
seastar::future<> typed_primitive_reader<T, L>::skip_field() {
    return next().then([this] (std::optional<triplet> triplet) {
        if (!triplet) {
            throw parquet_exception("No more values buffered");
        }
    });
}

template <typename format::Type::type T, typename L>
seastar::future<int, int> typed_primitive_reader<T, L>::current_levels() {
    return refill_when_empty().then([this] {
        if (!_levels_buffered) {
            return seastar::make_ready_future<int, int>(-1, -1);
        } else {
            return seastar::make_ready_future<int, int>(current_def_level(), current_rep_level());
        }
    });
}

template <typename format::Type::type T, typename L>
int typed_primitive_reader<T, L>::current_def_level() {
    return _node.def_level > 0 ? _def_levels[_levels_offset] : 0;
}

template <typename format::Type::type T, typename L>
int typed_primitive_reader<T, L>::current_rep_level() {
    return _node.rep_level > 0 ? _rep_levels[_levels_offset] : 0;
}

template <typename format::Type::type T, typename L>
seastar::future<> typed_primitive_reader<T, L>::refill_when_empty() {
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

template <typename format::Type::type T, typename L>
seastar::future<std::optional<typename typed_primitive_reader<T, L>::triplet>> typed_primitive_reader<T, L>::next() {
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

seastar::future<> struct_reader::skip_field() {
    return seastar::do_for_each(_readers.begin(), _readers.end(), [] (auto& child) {
        return child.skip_field();
    });
}

seastar::future<int, int> struct_reader::current_levels() {
    return _readers[0].current_levels();
}
seastar::future<> list_reader::skip_field() {
    return _reader->skip_field();
}

seastar::future<int, int> list_reader::current_levels() {
    return _reader->current_levels();
}

seastar::future<> optional_reader::skip_field() {
  return _reader->skip_field();
}

seastar::future<int, int> optional_reader::current_levels() {
  return _reader->current_levels();
}

seastar::future<> map_reader::skip_field() {
    return seastar::when_all_succeed(
            _key_reader->skip_field(),
            _value_reader->skip_field());
}

seastar::future<int, int> map_reader::current_levels() {
    return _key_reader->current_levels();
}

seastar::future<int, int> record_reader::current_levels() {
    return _field_readers[0].current_levels();
}

seastar::future<field_reader> field_reader::make(const file_reader& fr, const schema::node& node_variant, int row) {
    using namespace schema::logical_type;
    return std::visit(overloaded {
        [&] (const schema::primitive_node& node) -> seastar::future<field_reader> {
            switch (node.info.type) {
            case format::Type::INT32: {
                return fr.open_column_chunk_reader<format::Type::INT32>(row, node).then(
                [&node] (column_chunk_reader<format::Type::INT32> ccr) {
                    if (std::holds_alternative<INT8>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, INT8>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<INT16>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, INT16>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<INT32>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, INT32>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<UINT8>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, UINT8>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<UINT16>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, UINT16>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<UINT32>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, UINT32>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<DATE>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, DATE>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<TIME>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, TIME>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<DECIMAL>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, DECIMAL>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<UNKNOWN>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT32, UNKNOWN>{node, std::move(ccr)}};
                    } else {
                        return field_reader{typed_primitive_reader<format::Type::INT32, NONE>{node, std::move(ccr)}};
                    }
                });
            }
            case format::Type::INT64: {
                return fr.open_column_chunk_reader<format::Type::INT64>(row, node).then(
                [&node] (column_chunk_reader<format::Type::INT64> ccr) {
                    if (std::holds_alternative<INT64>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT64, INT64>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<UINT64>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT64, UINT64>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<TIME>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT64, TIME>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<TIMESTAMP>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT64, TIMESTAMP>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<DECIMAL>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::INT64, DECIMAL>{node, std::move(ccr)}};
                    } else {
                        return field_reader{typed_primitive_reader<format::Type::INT64, NONE>{node, std::move(ccr)}};
                    }
                });
            }
            case format::Type::INT96: {
                return fr.open_column_chunk_reader<format::Type::INT96>(row, node).then(
                [&node] (column_chunk_reader<format::Type::INT96> ccr) {
                    return field_reader{typed_primitive_reader<format::Type::INT96, NONE>{node, std::move(ccr)}};
                });
            }
            case format::Type::FLOAT: {
                return fr.open_column_chunk_reader<format::Type::FLOAT>(row, node).then(
                [&node] (column_chunk_reader<format::Type::FLOAT> ccr) {
                    return field_reader{typed_primitive_reader<format::Type::FLOAT, NONE>{node, std::move(ccr)}};
                });
            }
            case format::Type::DOUBLE: {
                return fr.open_column_chunk_reader<format::Type::DOUBLE>(row, node).then(
                [&node] (column_chunk_reader<format::Type::DOUBLE> ccr) {
                    return field_reader{typed_primitive_reader<format::Type::DOUBLE, NONE>{node, std::move(ccr)}};
                });
            }
            case format::Type::BOOLEAN: {
                return fr.open_column_chunk_reader<format::Type::BOOLEAN>(row, node).then(
                [&node] (column_chunk_reader<format::Type::BOOLEAN> ccr) {
                    return field_reader{typed_primitive_reader<format::Type::BOOLEAN, NONE>{node, std::move(ccr)}};
                });
            }
            case format::Type::BYTE_ARRAY: {
                return fr.open_column_chunk_reader<format::Type::BYTE_ARRAY>(row, node).then(
                [&node] (column_chunk_reader<format::Type::BYTE_ARRAY> ccr) {
                    if (std::holds_alternative<STRING>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::BYTE_ARRAY, STRING>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<INT16>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::BYTE_ARRAY, ENUM>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<INT32>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::BYTE_ARRAY, DECIMAL>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<UINT8>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::BYTE_ARRAY, JSON>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<UINT16>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::BYTE_ARRAY, BSON>{node, std::move(ccr)}};
                    } else {
                        return field_reader{typed_primitive_reader<format::Type::BYTE_ARRAY, NONE>{node, std::move(ccr)}};
                    }
                });
            }
            case format::Type::FIXED_LEN_BYTE_ARRAY: {
                return fr.open_column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>(row, node).then(
                [&node] (column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY> ccr) {
                    if (std::holds_alternative<STRING>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, STRING>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<INT16>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, UUID>{node, std::move(ccr)}};
                    } else if (std::holds_alternative<INT32>(node.logical_type)) {
                        return field_reader{typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, INTERVAL>{node, std::move(ccr)}};
                    } else {
                        return field_reader{typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, NONE>{node, std::move(ccr)}};
                    }
                });
            }
            default: throw;
            }
        },
        [&] (const schema::list_node& node) {
            return field_reader::make(fr, *node.element, row).then([&node] (field_reader child) {
                return field_reader{list_reader{node, std::make_unique<field_reader>(std::move(child))}};
            });
        },
        [&] (const schema::optional_node& node) {
            return field_reader::make(fr, *node.child, row).then([&node] (field_reader child) {
                return field_reader{optional_reader{node, std::make_unique<field_reader>(std::move(child))}};
            });
        },
        [&] (const schema::map_node& node) {
            return seastar::when_all_succeed(
                    field_reader::make(fr, *node.key, row),
                    field_reader::make(fr, *node.value, row)
            ).then([&node] (field_reader key, field_reader value) {
                return field_reader{map_reader{
                        node,
                        std::make_unique<field_reader>(std::move(key)),
                        std::make_unique<field_reader>(std::move(value))}};
            });
        },
        [&] (const schema::struct_node& node) {
            std::vector<seastar::future<field_reader>> field_readers;
            field_readers.reserve(node.fields.size());
            for (const schema::node& child : node.fields) {
                field_readers.push_back(field_reader::make(fr, child, row));
            }
            return seastar::when_all_succeed(field_readers.begin(), field_readers.end()).then(
            [&node] (std::vector<field_reader> field_readers) {
                return field_reader{struct_reader{node, std::move(field_readers)}};
            });
        }
    }, node_variant);
}

seastar::future<record_reader> record_reader::make(const file_reader& fr, int row_group_index) {
    std::vector<seastar::future<field_reader>> field_readers;
    for (const schema::node& field_node : fr.schema().fields) {
        field_readers.push_back(field_reader::make(fr, field_node, row_group_index));
    }
    return seastar::when_all_succeed(field_readers.begin(), field_readers.end()).then(
    [&fr] (std::vector<field_reader> field_readers) {
        return record_reader{fr.schema(), std::move(field_readers)};
    });
}

template class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT8>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT16>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::INT32>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT8>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT16>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UINT32>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::DATE>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::TIME>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::DECIMAL>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::UNKNOWN>;
template class typed_primitive_reader<format::Type::INT32, schema::logical_type::NONE>;
template class typed_primitive_reader<format::Type::INT64, schema::logical_type::INT64>;
template class typed_primitive_reader<format::Type::INT64, schema::logical_type::UINT64>;
template class typed_primitive_reader<format::Type::INT64, schema::logical_type::TIME>;
template class typed_primitive_reader<format::Type::INT64, schema::logical_type::TIMESTAMP>;
template class typed_primitive_reader<format::Type::INT64, schema::logical_type::DECIMAL>;
template class typed_primitive_reader<format::Type::INT64, schema::logical_type::NONE>;
template class typed_primitive_reader<format::Type::INT96, schema::logical_type::NONE>;
template class typed_primitive_reader<format::Type::FLOAT, schema::logical_type::NONE>;
template class typed_primitive_reader<format::Type::DOUBLE, schema::logical_type::NONE>;
template class typed_primitive_reader<format::Type::BOOLEAN, schema::logical_type::NONE>;
template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::STRING>;
template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::ENUM>;
template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::DECIMAL>;
template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::JSON>;
template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::BSON>;
template class typed_primitive_reader<format::Type::BYTE_ARRAY, schema::logical_type::NONE>;
template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::STRING>;
template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::UUID>;
template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::INTERVAL>;
template class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY, schema::logical_type::NONE>;

} // namespace parquet::cql
