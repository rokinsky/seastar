#pragma once

#include <seastar/core/print.hh>
#include <exception>
#include <sstream>

namespace parquet {

class parquet_exception : public std::exception {
    std::string _msg;
public:
    ~parquet_exception() throw() override {}

    static parquet_exception not_implemented(const std::string& msg) {
        return parquet_exception(seastar::format("Not implemented: {}", msg));
    }

    static parquet_exception corrupted_file(const std::string& msg) {
        return parquet_exception(seastar::format("Invalid or corrupted parquet file: {}", msg));
    }

    explicit parquet_exception(const char* msg) : _msg(msg) {}

    explicit parquet_exception(std::string msg) : _msg(std::move(msg)) {}

    const char* what() const throw() override { return _msg.c_str(); }
};

} // namespace parquet
