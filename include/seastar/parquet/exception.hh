#pragma once

#include <exception>
#include <sstream>

namespace parquet {

class parquet_exception : public std::exception {
    std::string _msg;
public:
    ~parquet_exception() throw() override {}

    static parquet_exception eof_exception(const std::string& msg = "") {
        std::stringstream ss;
        ss << "Unexpected end of stream";
        if (!msg.empty()) {
            ss << ": " << msg << ".";
        }
        return parquet_exception(ss.str());
    }

    static parquet_exception nyi(const std::string& msg = "") {
        std::stringstream ss;
        ss << "Not yet implemented: " << msg << ".";
        return parquet_exception(ss.str());
    }

    static parquet_exception corrupted_file(const std::string& msg = "") {
        std::stringstream ss;
        ss << "Invalid or corrupt parquet file: " << msg << ".";
        return parquet_exception(ss.str());
    }

    explicit parquet_exception(const char* msg) : _msg(msg) {}

    explicit parquet_exception(const std::string& msg) : _msg(msg) {}

    explicit parquet_exception(const char* msg, std::exception&) : _msg(msg) {}

    const char* what() const throw() override { return _msg.c_str(); }
};

} // namespace parquet
