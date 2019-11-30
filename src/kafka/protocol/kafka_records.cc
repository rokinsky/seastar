/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#include "kafka_records.hh"

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <iostream>
namespace seastar {

namespace kafka {

void kafka_record_header::serialize(std::ostream &os, int16_t api_version) const {
    kafka_varint_t header_key_length(_header_key.size());
    header_key_length.serialize(os, api_version);
    os.write(_header_key.data(), _header_key.size());

    kafka_varint_t header_value_length(_value.size());
    header_value_length.serialize(os, api_version);
    os.write(_value.data(), _value.size());
}

void kafka_record_header::deserialize(std::istream &is, int16_t api_version) {
    kafka_buffer_t<kafka_varint_t> header_key;
    header_key.deserialize(is, api_version);
    _header_key.swap(*header_key);

    kafka_buffer_t<kafka_varint_t> value;
    value.deserialize(is, api_version);
    _value.swap(*value);
}

void kafka_record::serialize(std::ostream &os, int16_t api_version) const {
    std::vector<char> record_data;
    boost::iostreams::back_insert_device<std::vector<char>> record_data_sink{record_data};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> record_data_stream{record_data_sink};

    kafka_int8_t attributes(0);
    attributes.serialize(record_data_stream, api_version);

    _timestamp_delta.serialize(record_data_stream, api_version);
    _offset_delta.serialize(record_data_stream, api_version);

    kafka_varint_t key_length(_key.size());
    key_length.serialize(record_data_stream, api_version);

    record_data_stream.write(_key.data(), _key.size());

    kafka_varint_t value_length(_value.size());
    value_length.serialize(record_data_stream, api_version);

    record_data_stream.write(_value.data(), _value.size());

    kafka_varint_t header_count(_headers.size());
    header_count.serialize(record_data_stream, api_version);

    for (const auto &header : _headers) {
        header.serialize(record_data_stream, api_version);
    }
    record_data_stream.flush();

    kafka_varint_t length(record_data.size());
    length.serialize(os, api_version);

    os.write(record_data.data(), record_data.size());
}

void kafka_record::deserialize(std::istream &is, int16_t api_version) {
    kafka_varint_t length;
    length.deserialize(is, api_version);
    if (*length < 0) {
        throw parsing_exception("Length of record is invalid");
    }

    auto expected_end_of_record = is.tellg();
    expected_end_of_record += *length;

    kafka_int8_t attributes;
    attributes.deserialize(is, api_version);

    _timestamp_delta.deserialize(is, api_version);
    _offset_delta.deserialize(is, api_version);

    kafka_buffer_t<kafka_varint_t> key;
    key.deserialize(is, api_version);
    _key.swap(*key);

    kafka_buffer_t<kafka_varint_t> value;
    value.deserialize(is, api_version);
    _value.swap(*value);

    kafka_array_t<kafka_record_header, kafka_varint_t> headers;
    headers.deserialize(is, api_version);
    _headers.swap(*headers);

    if (is.tellg() != expected_end_of_record) {
        throw parsing_exception("Stream ended prematurely when reading record");
    }
}

}

}