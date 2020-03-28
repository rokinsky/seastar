#define BOOST_TEST_MODULE parquet

#include <seastar/parquet/io.hh>
#include <seastar/parquet/parquet_types.h>
#include <boost/test/included/unit_test.hpp>

BOOST_AUTO_TEST_CASE(thrift_serdes) {
    using namespace parquet;
    thrift_serializer serializer;

    format::SchemaElement se;
    se.__set_type(format::Type::DOUBLE);
    format::FileMetaData fmd;
    fmd.__set_schema({se});
    auto serialized = serializer.serialize(fmd);

    format::FileMetaData fmd2;
    deserialize_thrift_msg(serialized.data(), serialized.size(), fmd2);

    BOOST_CHECK(fmd2.schema[0].type == format::Type::DOUBLE);
}
