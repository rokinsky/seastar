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
#include <boost/crc.hpp>

namespace seastar {

namespace kafka {

void kafka_record_header::serialize(kafka::output_stream &os, int16_t api_version) const {
    kafka_varint_t header_key_length(_header_key.size());
    header_key_length.serialize(os, api_version);
    os.write(_header_key.data(), _header_key.size());

    kafka_varint_t header_value_length(_value.size());
    header_value_length.serialize(os, api_version);
    os.write(_value.data(), _value.size());
}

void kafka_record_header::deserialize(kafka::input_stream &is, int16_t api_version) {
    kafka_buffer_t<kafka_varint_t> header_key;
    header_key.deserialize(is, api_version);
    _header_key.swap(*header_key);

    kafka_buffer_t<kafka_varint_t> value;
    value.deserialize(is, api_version);
    _value.swap(*value);
}

void kafka_record::serialize(kafka::output_stream &os, int16_t api_version) const {
    kafka::output_stream record_data_stream = kafka::output_stream::resizable_stream();

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

    kafka_varint_t length(record_data_stream.size());
    length.serialize(os, api_version);

    os.write(record_data_stream.begin(), record_data_stream.size());
}

void kafka_record::deserialize(kafka::input_stream &is, int16_t api_version) {
    kafka_varint_t length;
    length.deserialize(is, api_version);
    if (*length < 0) {
        throw parsing_exception("Length of record is invalid");
    }

    auto expected_end_of_record = is.get_position();
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

    if (is.get_position() != expected_end_of_record) {
        throw parsing_exception("Stream ended prematurely when reading record");
    }
}

void kafka_record_batch::serialize(kafka::output_stream &os, int16_t api_version) const {
    if (*_magic != 2) {
        // TODO: Implement parsing of versions 0, 1.
        throw parsing_exception("Unsupported version of record batch");
    }

    // Payload stores the data after CRC field.
    kafka::output_stream payload_stream = kafka::output_stream::resizable_stream();

    kafka_int16_t attributes(0);
    attributes = *attributes | static_cast<int16_t>(_compression_type);
    attributes = *attributes | (static_cast<int16_t>(_timestamp_type) << 3);
    if (_is_transactional) {
        attributes = *attributes | 0x10;
    }
    if (_is_control_batch) {
        attributes = *attributes | 0x20;
    }

    attributes.serialize(payload_stream, api_version);

    kafka_int32_t last_offset_delta(0);
    if (!_records.empty()) {
        last_offset_delta = *_records.back()._offset_delta;
    }

    last_offset_delta.serialize(payload_stream, api_version);

    _first_timestamp.serialize(payload_stream, api_version);

    int32_t max_timestamp_delta = 0;
    for (const auto &record : _records) {
        max_timestamp_delta = std::max(max_timestamp_delta, *record._timestamp_delta);
    }
    kafka_int64_t max_timestamp(*_first_timestamp + max_timestamp_delta);
    max_timestamp.serialize(payload_stream, api_version);

    _producer_id.serialize(payload_stream, api_version);

    _producer_epoch.serialize(payload_stream, api_version);

    _base_sequence.serialize(payload_stream, api_version);

    kafka::output_stream serialized_records_stream = kafka::output_stream::resizable_stream();

    for (const auto &record : _records) {
        record.serialize(serialized_records_stream, api_version);
    }

    if (_compression_type != kafka_record_compression_type::NO_COMPRESSION) {
        // TODO: Add support for compression.
        throw parsing_exception("Unsupported compression type");
    }

    kafka_int32_t records_count(_records.size());
    records_count.serialize(payload_stream, api_version);

    payload_stream.write(serialized_records_stream.begin(), serialized_records_stream.size());

    _base_offset.serialize(os, api_version);

    kafka_int32_t batch_length(0);
    batch_length = *batch_length + payload_stream.size();
    // fields before the CRC field.
    batch_length = *batch_length + 4 + 4 + 1;
    batch_length.serialize(os, api_version);

    _partition_leader_epoch.serialize(os, api_version);

    _magic.serialize(os, api_version);

    boost::crc_optimal<32, 0x1EDC6F41, 0xFFFFFFFF, 0xFFFFFFFF, true, true> crc_value;
    crc_value.process_bytes(payload_stream.begin(), payload_stream.size());

    kafka_int32_t crc(crc_value.checksum());
    crc.serialize(os, api_version);

    os.write(payload_stream.begin(), payload_stream.size());
}

void kafka_record_batch::deserialize(kafka::input_stream &is, int16_t api_version) {
    // Move to magic byte, read it and return back to start.
    auto start_position = is.get_position();
    is.set_position(8 + 4 + 4 + start_position);
    _magic.deserialize(is, api_version);
    is.set_position(start_position);

    if (*_magic != 2) {
        // TODO: Implement parsing of versions 0, 1.
        throw parsing_exception("Unsupported record batch version");
    }

    _base_offset.deserialize(is, api_version);

    kafka_int32_t batch_length;
    batch_length.deserialize(is, api_version);

    auto expected_end_of_batch = is.get_position();
    expected_end_of_batch += *batch_length;

    _partition_leader_epoch.deserialize(is, api_version);

    _magic.deserialize(is, api_version);

    kafka_int32_t crc;
    crc.deserialize(is, api_version);

    // TODO: Missing validation of returned CRC value.

    kafka_int16_t attributes;
    attributes.deserialize(is, api_version);

    auto compression_type = *attributes & 0x7;
    switch (compression_type) {
        case 0:
            _compression_type = kafka_record_compression_type::NO_COMPRESSION;
            break;
        case 1:
            _compression_type = kafka_record_compression_type::GZIP;
            break;
        case 2:
            _compression_type = kafka_record_compression_type::SNAPPY;
            break;
        case 3:
            _compression_type = kafka_record_compression_type::LZ4;
            break;
        case 4:
            _compression_type = kafka_record_compression_type::ZSTD;
            break;
        default:
            throw parsing_exception("Unsupported compression type");
    }

    if (*attributes & 0x8) {
        _timestamp_type = kafka_record_timestamp_type::LOG_APPEND_TIME;
    } else _timestamp_type = kafka_record_timestamp_type::CREATE_TIME;

    if (*attributes & 0x10) {
        _is_transactional = true;
    } else _is_transactional = false;

    if (*attributes & 0x20) {
        _is_control_batch = true;
    } else _is_control_batch = false;

    kafka_int32_t last_offset_delta;
    last_offset_delta.deserialize(is, api_version);

    _first_timestamp.deserialize(is, api_version);

    kafka_int64_t max_timestamp;
    max_timestamp.deserialize(is, api_version);

    _producer_id.deserialize(is, api_version);

    _producer_epoch.deserialize(is, api_version);

    _base_sequence.deserialize(is, api_version);

    kafka_int32_t records_count;
    records_count.deserialize(is, api_version);

    if (*records_count < 0) {
        throw parsing_exception("Record count in batch is invalid");
    }
    _records.resize(*records_count);

    auto remaining_bytes = expected_end_of_batch - is.get_position();
    std::vector<char> records_payload(remaining_bytes);

    is.read(records_payload.data(), remaining_bytes);
    if (is.gcount() != remaining_bytes) {
        throw parsing_exception("Stream ended prematurely when reading record batch");
    }

    kafka::input_stream records_stream(records_payload.data(), records_payload.size());
    for (auto &record : _records) {
        record.deserialize(records_stream, api_version);
    }

    if (records_stream.get_position() != remaining_bytes) {
        throw parsing_exception("Stream ended prematurely when reading record batch");
    }
}

void kafka_records::serialize(kafka::output_stream &os, int16_t api_version) const {
    kafka::output_stream serialized_batches_stream = kafka::output_stream::resizable_stream();

    for (const auto &batch : _record_batches) {
        batch.serialize(serialized_batches_stream, api_version);
    }

    kafka_int32_t records_length(serialized_batches_stream.size());
    records_length.serialize(os, api_version);

    os.write(serialized_batches_stream.begin(), serialized_batches_stream.size());
}

void kafka_records::deserialize(kafka::input_stream &is, int16_t api_version) {
    kafka_int32_t records_length;
    records_length.deserialize(is, api_version);
    if (*records_length < 0) {
        throw parsing_exception("Records length is invalid");
    }

    auto expected_end_of_records = is.get_position();
    expected_end_of_records += *records_length;

    _record_batches.clear();
    while (is.get_position() < expected_end_of_records) {
        _record_batches.emplace_back();
        _record_batches.back().deserialize(is, api_version);
    }

    if (is.get_position() != expected_end_of_records) {
        throw parsing_exception("Stream ended prematurely when reading records");
    }
}

}

}
