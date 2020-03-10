#pragma once

#include <seastar/core/temporary_buffer.hh>

#include <cctype>
#include <iostream>

namespace seastar::fs {

inline std::ostream& operator<<(std::ostream& os, const temporary_buffer<uint8_t>& data) {
    constexpr size_t MAX_ELEMENTS_PRINTED = 10;
    for (size_t i = 0; i < std::min(MAX_ELEMENTS_PRINTED, data.size()); ++i) {
        if (isprint(data[i]) != 0) {
            os << data[i];
        } else {
            os << "<" << (int)data[i] << ">";
        }
    }
    if (data.size() > MAX_ELEMENTS_PRINTED) {
        os << "...";
    }
    return os;
}

} // seastar::fs
