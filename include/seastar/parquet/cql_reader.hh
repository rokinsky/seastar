#pragma once

#include <boost/iterator/counting_iterator.hpp>
#include <seastar/parquet/file_reader.hh>
#include <seastar/parquet/schema.hh>
#include <ostream>

namespace parquet::cql {

struct field_reader;

template <typename format::Type::type T>
class typed_primitive_reader {
public:
    using output_type = typename value_decoder_traits<T>::output_type;
    static constexpr int64_t DEFAULT_BATCH_SIZE = 1024;
    const schema::primitive_node& _node;
private:
    column_chunk_reader<T> _source;
    std::vector<int16_t> _rep_levels;
    std::vector<int16_t> _def_levels;
    std::vector<output_type> _values;
    int _levels_offset = 0;
    int _values_offset = 0;
    int _levels_buffered = 0;
    int _values_buffered = 0;

    struct triplet {
        int16_t def_level;
        int16_t rep_level;
        std::optional<output_type> value;
    };
public:
    explicit typed_primitive_reader(
            const schema::primitive_node& node,
            column_chunk_reader<T> source,
            int64_t batch_size = DEFAULT_BATCH_SIZE)
        : _node{node}
        , _source{std::move(source)}
        , _rep_levels(batch_size)
        , _def_levels(batch_size)
        , _values(batch_size)
        {}

    seastar::future<> read_field(std::ostream& out);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();

private:
    int current_def_level();
    int current_rep_level();
    seastar::future<> refill_when_empty();
    seastar::future<std::optional<triplet>> next();
    inline void format_value(const output_type& val, std::ostream &out);
};

class struct_reader {
    const char* _separator = "";
    std::vector<field_reader> _readers;
public:
    const schema::struct_node& _node;

    explicit struct_reader(
            const schema::struct_node& node,
            std::vector<field_reader> readers)
        : _readers(std::move(readers))
        , _node(node) {
        assert(_readers.size() > 0);
    }

    seastar::future<> read_field(std::ostream& out);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
};

class list_reader {
    std::unique_ptr<field_reader> _reader;
public:
    const schema::list_node& _node;
    explicit list_reader(
            const schema::list_node& node,
            std::unique_ptr<field_reader> reader)
        : _reader(std::move(reader))
        , _node(node) {}

    seastar::future<> read_field(std::ostream& out);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
};

class optional_reader {
    std::unique_ptr<field_reader> _reader;
public:
    const schema::optional_node &_node;
    explicit optional_reader(
            const schema::optional_node &node,
            std::unique_ptr<field_reader> reader)
        : _reader(std::move(reader))
        , _node(node) {}

    seastar::future<> read_field(std::ostream& out);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
};

class map_reader {
    std::unique_ptr<field_reader> _key_reader;
    std::unique_ptr<field_reader> _value_reader;
public:
    const schema::map_node& _node;
    explicit map_reader(
            const schema::map_node& node,
            std::unique_ptr<field_reader> key_reader,
            std::unique_ptr<field_reader> value_reader)
        : _key_reader(std::move(key_reader))
        , _value_reader(std::move(value_reader))
        , _node(node) {}

    seastar::future<> read_field(std::ostream& out);
    seastar::future<> skip_field();
    seastar::future<int, int> current_levels();
private:
    seastar::future<> read_pair(std::ostream& out);
};


struct field_reader {
    std::variant<
        class optional_reader,
        class struct_reader,
        class list_reader,
        class map_reader,
        class typed_primitive_reader<format::Type::INT32>,
        class typed_primitive_reader<format::Type::INT64>,
        class typed_primitive_reader<format::Type::INT96>,
        class typed_primitive_reader<format::Type::FLOAT>,
        class typed_primitive_reader<format::Type::DOUBLE>,
        class typed_primitive_reader<format::Type::BOOLEAN>,
        class typed_primitive_reader<format::Type::BYTE_ARRAY>,
        class typed_primitive_reader<format::Type::FIXED_LEN_BYTE_ARRAY>
    > _reader;

    const std::string* name() {
        return std::visit([](const auto& x) {return &x._node.info.name;}, _reader);
    }
    seastar::future<> read_field(std::ostream& out) {
        return std::visit([&](auto& x) {return x.read_field(out);}, _reader);
    }
    seastar::future<> skip_field() {
        return std::visit([](auto& x) {return x.skip_field();}, _reader);
    }
    seastar::future<int, int> current_levels() {
        return std::visit([](auto& x) {return x.current_levels();}, _reader);
    }
    static field_reader make(const file_reader& file, const schema::node& node_variant, int row_group_index);
};

class record_reader {
    const schema::root_node& _schema;
    std::vector<field_reader> _field_readers;
    const char* _separator = "";
    explicit record_reader(
            const schema::root_node& schema,
            std::vector<field_reader> field_readers)
        : _schema(schema), _field_readers(std::move(field_readers)) {
        assert(_field_readers.size() > 0);
    }
public:
    seastar::future<> read_one(std::ostream &out);
    seastar::future<> read_all(std::ostream &out);
    seastar::future<int, int> current_levels();
    static record_reader make(const file_reader& fr, int row_group_index);
};

} // namespace parquet::cql
