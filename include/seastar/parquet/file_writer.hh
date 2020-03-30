#pragma once

#include <seastar/parquet/write_schema.hh>
#include <seastar/parquet/column_chunk_writer.hh>
#include <seastar/core/seastar.hh>

namespace parquet {

class file_writer {
public:
    using column_chunk_writer_variant = std::variant<
        column_chunk_writer<format::Type::BOOLEAN>,
        column_chunk_writer<format::Type::INT32>,
        column_chunk_writer<format::Type::INT64>,
        column_chunk_writer<format::Type::FLOAT>,
        column_chunk_writer<format::Type::DOUBLE>,
        column_chunk_writer<format::Type::BYTE_ARRAY>,
        column_chunk_writer<format::Type::FIXED_LEN_BYTE_ARRAY>
    >;
private:
    seastar::output_stream<char> _sink;
    std::vector<column_chunk_writer_variant> _writers;
    std::vector<std::vector<std::string>> _schema_paths;
    format::FileMetaData _metadata;
    thrift_serializer _thrift_serializer;
    size_t _file_offset = 0;
private:
    void init_writers(const write_schema::schema &root) {
        using namespace write_schema;
        auto convert = y_combinator{[&](auto&& convert, const node& node_variant, uint32_t def, uint32_t rep) -> void {
            std::visit(overloaded {
                [&] (const list_node& x) { convert(*x.element, def + 1 + !!x.optional, rep + 1); },
                [&] (const map_node& x) {
                    convert(*x.key, def + 1 + !!x.optional, rep + 1);
                    convert(*x.value, def + 1 + !!x.optional, rep + 1);
                },
                [&] (const struct_node& x) {
                    for (const node& child : x.fields) {
                        convert(child, def + !!x.optional, rep);
                    }
                },
                [&] (const primitive_node& x) {
                    std::visit(overloaded {
                        [&] (schema::logical_type::INT96 logical_type) {
                            throw parquet_exception("INT96 is deprecated. Writing INT96 is unsupported.");
                        },
                        [&] (auto logical_type) {
                            _writers.push_back(column_chunk_writer<decltype(logical_type)::physical_type>{def + !!x.optional, rep});
                        }
                    }, x.logical_type);
                }
            }, node_variant);
        }};
        for (const node& field : root.fields) {
            convert(field, 0, 0);
        }
    }

    static void write_logical_type(schema::logical_type::logical_type logical_type, format::SchemaElement& leaf) {
        using namespace schema::logical_type;
        auto int_type = [] (int bit_width, bool is_signed) -> format::LogicalType {
            format::LogicalType logical_type;
            format::IntType int_type;
            int_type.__set_bitWidth(bit_width);
            int_type.__set_isSigned(is_signed);
            logical_type.__set_INTEGER(int_type);
            return logical_type;
        };
        auto decimal_type = [] (int precision, int scale) -> format::LogicalType {
            format::DecimalType decimal_type;
            decimal_type.__set_precision(precision);
            decimal_type.__set_scale(scale);
            format::LogicalType logical_type;
            logical_type.__set_DECIMAL(decimal_type);
            return logical_type;
        };
        return std::visit(overloaded {
            [&] (const STRING&) {
                leaf.__set_converted_type(format::ConvertedType::UTF8);
                format::LogicalType logical_type;
                logical_type.__set_STRING(format::StringType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const ENUM&) {
                leaf.__set_converted_type(format::ConvertedType::ENUM);
                format::LogicalType logical_type;
                logical_type.__set_ENUM(format::EnumType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const UUID&) {
                format::LogicalType logical_type;
                logical_type.__set_UUID(format::UUIDType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const INT8&) {
                leaf.__set_converted_type(format::ConvertedType::INT_8);
                leaf.__set_logicalType(int_type(8, true));
            },
            [&] (const INT16&) {
                leaf.__set_converted_type(format::ConvertedType::INT_16);
                leaf.__set_logicalType(int_type(16, true));
            },
            [&] (const INT32&) {
                leaf.__set_converted_type(format::ConvertedType::INT_32);
                leaf.__set_logicalType(int_type(32, true));
            },
            [&] (const INT64&) {
                leaf.__set_converted_type(format::ConvertedType::INT_64);
                leaf.__set_logicalType(int_type(64, true));
            },
            [&] (const UINT8&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_8);
                leaf.__set_logicalType(int_type(8, false));
            },
            [&] (const UINT16&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_16);
                leaf.__set_logicalType(int_type(16, false));
            },
            [&] (const UINT32&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_32);
                leaf.__set_logicalType(int_type(32, false));
            },
            [&] (const UINT64&) {
                leaf.__set_converted_type(format::ConvertedType::UINT_64);
                leaf.__set_logicalType(int_type(64, false));
            },
            [&] (const DECIMAL_INT32& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DECIMAL_INT64& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DECIMAL_BYTE_ARRAY& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DECIMAL_FIXED_LEN_BYTE_ARRAY& x) {
                leaf.__set_converted_type(format::ConvertedType::DECIMAL);
                leaf.__set_precision(x.precision);
                leaf.__set_scale(x.scale);
                leaf.__set_logicalType(decimal_type(x.precision, x.scale));
            },
            [&] (const DATE&) {
                leaf.__set_converted_type(format::ConvertedType::DATE);
                format::LogicalType logical_type;
                logical_type.__set_DATE(format::DateType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const TIME_INT32& x) {
                leaf.__set_converted_type(format::ConvertedType::TIME_MILLIS);
                format::LogicalType logical_type;
                format::TimeType time_type;
                format::TimeUnit time_unit;
                time_unit.__set_MILLIS(format::MilliSeconds{});
                time_type.__set_isAdjustedToUTC(x.utc_adjustment);
                time_type.__set_unit(time_unit);
                logical_type.__set_TIME(time_type);
                leaf.__set_logicalType(logical_type);
            },
            [&] (const TIME_INT64& x) {
                format::LogicalType logical_type;
                format::TimeType time_type;
                format::TimeUnit time_unit;
                if (x.unit == TIME_INT64::MICROS) {
                    leaf.__set_converted_type(format::ConvertedType::TIME_MICROS);
                    time_unit.__set_MICROS(format::MicroSeconds{});
                } else {
                    time_unit.__set_NANOS(format::NanoSeconds{});
                }
                time_type.__set_isAdjustedToUTC(x.utc_adjustment);
                time_type.__set_unit(time_unit);
                logical_type.__set_TIME(time_type);
                leaf.__set_logicalType(logical_type);
            },
            [&] (const TIMESTAMP& x) {
                format::LogicalType logical_type;
                format::TimestampType timestamp_type;
                format::TimeUnit time_unit;
                if (x.unit == TIMESTAMP::MILLIS) {
                    leaf.__set_converted_type(format::ConvertedType::TIMESTAMP_MILLIS);
                    time_unit.__set_MILLIS(format::MilliSeconds{});
                } else if (x.unit == TIMESTAMP::MICROS) {
                    leaf.__set_converted_type(format::ConvertedType::TIMESTAMP_MICROS);
                    time_unit.__set_MICROS(format::MicroSeconds{});
                } else {
                    time_unit.__set_NANOS(format::NanoSeconds{});
                }
                timestamp_type.__set_isAdjustedToUTC(x.utc_adjustment);
                timestamp_type.__set_unit(time_unit);
                logical_type.__set_TIMESTAMP(timestamp_type);
                leaf.__set_logicalType(logical_type);
            },
            [&] (const INTERVAL&) {
                leaf.__set_converted_type(format::ConvertedType::INTERVAL);
            },
            [&] (const JSON&) {
                leaf.__set_converted_type(format::ConvertedType::JSON);
                format::LogicalType logical_type;
                logical_type.__set_JSON(format::JsonType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const BSON&) {
                leaf.__set_converted_type(format::ConvertedType::BSON);
                format::LogicalType logical_type;
                logical_type.__set_BSON(format::BsonType{});
                leaf.__set_logicalType(logical_type);
            },
            [&] (const FLOAT&) {},
            [&] (const DOUBLE&) {},
            [&] (const BYTE_ARRAY&) {},
            [&] (const FIXED_LEN_BYTE_ARRAY&) {},
            [&] (const INT96&) {},
            [&] (const BOOLEAN&) {},
            [&] (const UNKNOWN&) {
                format::LogicalType logical_type;
                logical_type.__set_UNKNOWN(format::NullType{});
                leaf.__set_logicalType(logical_type);
            }
        }, logical_type);
    }

    void init_schema(const write_schema::schema &root) {
        using namespace write_schema;
        format::FieldRepetitionType::type REQUIRED = format::FieldRepetitionType::REQUIRED;
        format::FieldRepetitionType::type OPTIONAL = format::FieldRepetitionType::OPTIONAL;
        format::FieldRepetitionType::type REPEATED = format::FieldRepetitionType::REPEATED;

        auto convert = y_combinator{[&](auto&& convert, const node& node_variant, std::vector<std::string> path_in_schema) -> void {
            std::visit(overloaded {
                [&] (const list_node& x) {
                    format::SchemaElement group_element;
                    group_element.__set_num_children(1);
                    group_element.__set_name(*path_in_schema.rbegin());
                    group_element.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    group_element.__set_converted_type(format::ConvertedType::LIST);
                    format::LogicalType logical_type;
                    logical_type.__set_LIST({});
                    group_element.__set_logicalType(logical_type);
                    _metadata.schema.push_back(group_element);

                    path_in_schema.push_back("list");
                    format::SchemaElement repeated_element;
                    repeated_element.__set_num_children(1);
                    repeated_element.__set_name(*path_in_schema.rbegin());
                    repeated_element.__set_repetition_type(REPEATED);
                    _metadata.schema.push_back(repeated_element);

                    path_in_schema.push_back("element");
                    convert(*x.element, std::move(path_in_schema));
                },
                [&] (const map_node& x) {
                    format::SchemaElement group_element;
                    group_element.__set_num_children(1);
                    group_element.__set_name(*path_in_schema.rbegin());
                    group_element.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    group_element.__set_converted_type(format::ConvertedType::MAP);
                    format::LogicalType logical_type;
                    logical_type.__set_MAP({});
                    group_element.__set_logicalType(logical_type);
                    _metadata.schema.push_back(group_element);

                    path_in_schema.push_back("key_value");
                    format::SchemaElement repeated_element;
                    repeated_element.__set_num_children(2);
                    repeated_element.__set_name(*path_in_schema.rbegin());
                    repeated_element.__set_repetition_type(REPEATED);
                    _metadata.schema.push_back(repeated_element);

                    bool key_is_optional = std::visit([](auto& k){return k.optional;}, *x.key);
                    if (key_is_optional) {
                        throw parquet_exception("Map key must not be optional");
                    };
                    path_in_schema.push_back("key");
                    convert(*x.key, path_in_schema);

                    path_in_schema.pop_back();
                    path_in_schema.push_back("value");
                    convert(*x.value, std::move(path_in_schema));
                },
                [&] (const struct_node& x) {
                    format::SchemaElement group_element;
                    group_element.__set_num_children(x.fields.size());
                    group_element.__set_name(*path_in_schema.rbegin());
                    group_element.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    _metadata.schema.push_back(group_element);

                    for (const node& child : x.fields) {
                        path_in_schema.push_back(std::visit([] (auto& x) {return x.name;}, child));
                        convert(child, path_in_schema);
                        path_in_schema.pop_back();
                    }
                },
                [&] (const primitive_node& x) {
                    format::SchemaElement leaf;
                    leaf.__set_type(std::visit([] (auto y) {return decltype(y)::physical_type;}, x.logical_type));
                    leaf.__set_name(*path_in_schema.rbegin());
                    if (x.type_length) { leaf.__set_type_length(*x.type_length); }
                    leaf.__set_repetition_type(x.optional ? OPTIONAL : REQUIRED);
                    write_logical_type(x.logical_type, leaf);
                    _metadata.schema.push_back(leaf);
                    _schema_paths.push_back(path_in_schema);
                }
            }, node_variant);
        }};

        format::SchemaElement root_element;
        root_element.__set_num_children(root.fields.size());
        root_element.__set_name("schema");
        _metadata.schema.push_back(root_element);

        for (const node& field : root.fields) {
            std::string name = std::visit([](auto& x){return x.name;}, field);
            convert(field, std::vector<std::string>{name});
        }
    }

public:
    static seastar::future<std::unique_ptr<file_writer>> open(const std::string& path, const write_schema::schema& schema) {
        return seastar::futurize_apply([&schema, path] {
            auto fw = std::unique_ptr<file_writer>(new file_writer{});
            fw->init_schema(schema);
            fw->init_writers(schema);

            seastar::open_flags flags = seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
            return seastar::open_file_dma(path, flags).then(
            [fw = std::move(fw)] (seastar::file file) mutable {
                fw->_sink = seastar::make_file_output_stream(file);
                fw->_file_offset = 4;
                return fw->_sink.write("PAR1", 4).then(
                [fw = std::move(fw)] () mutable {
                    return std::move(fw);
                });
            });
        });
    }

    template <format::Type::type ParquetType>
    column_chunk_writer<ParquetType>& column(int i) {
        return std::get<column_chunk_writer<ParquetType>>(_writers[i]);
    }

    size_t estimated_row_group_size() const {
        size_t size = 0;
        for (const auto& writer : _writers) {
            std::visit([&] (const auto& x) {size += x.estimated_chunk_size();}, writer);
        }
        return size;
    }

    seastar::future<> flush_row_group() {
        using it = boost::counting_iterator<size_t>;

        _metadata.row_groups.push_back(format::RowGroup{});
        size_t rows_written = std::visit([&] (auto& x) {return x.rows_written();}, _writers[0]);
        _metadata.row_groups.rbegin()->num_rows = rows_written;

        return seastar::do_for_each(it(0), it(_writers.size()), [this] (size_t i) {
            return std::visit([&, i] (auto& x) {
                return x.flush_chunk(_sink);
            }, _writers[i]).then([this] (seastar::lw_shared_ptr<format::ColumnMetaData> cmd) {
                cmd->dictionary_page_offset += _file_offset;
                cmd->data_page_offset += _file_offset;
                bytes_view footer = _thrift_serializer.serialize(*cmd);

                _file_offset += cmd->total_compressed_size;
                format::ColumnChunk cc;
                cc.file_offset = _file_offset;
                cc.meta_data = *cmd;
                _metadata.row_groups.rbegin()->columns.push_back(cc);
                _metadata.row_groups.rbegin()->total_byte_size = cmd->total_compressed_size + footer.size();

                _file_offset += footer.size();
                return _sink.write(reinterpret_cast<const char*>(footer.data()), footer.size());
            });
        });
    }

    seastar::future<> close() {
        return flush_row_group().then([this] {
            for (const format::RowGroup& rg : _metadata.row_groups) {
                _metadata.num_rows += rg.num_rows;
            }
            _metadata.version = 1; // Parquet 2.0 == 1
            bytes_view footer = _thrift_serializer.serialize(_metadata);
            return _sink.write(reinterpret_cast<const char*>(footer.data()), footer.size()).then([this, footer] {
                uint32_t footer_size = footer.size();
                return _sink.write(reinterpret_cast<const char*>(&footer_size), 4);
            });
        }).then([this] {
            return _sink.write("PAR1", 4);
        }).then([this] {
            return _sink.flush();
        }).then([this] {
            return _sink.close();
        });
    }
};

} // namespace parquet
