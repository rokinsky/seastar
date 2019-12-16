#ifndef __STREAMS_HH__
#define __STREAMS_HH__

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <vector>

using std::vector;

namespace seastar {
    namespace kafka {

    struct parsing_exception : public std::runtime_error {
    public:
        parsing_exception(const std::string& message) : runtime_error(message) {}
    };

    class input_stream {
    private:
        const char *_data;
        int32_t _length;
        int32_t _current_position = 0;
        int32_t _gcount = 0;

    public:
        input_stream(const char *data, int32_t length) noexcept
            : _data(data)
            , _length(length)
        {}

        inline void read(char *destination, int32_t length) {
            if(_current_position + length > _length){
                throw kafka::parsing_exception ("Attempted to read more than the remaining length of the input stream.");
            }
            std::copy((_data + _current_position), (_data + _current_position + length), destination);
            _current_position += length;
            _gcount = length;
        }

        inline int32_t gcount() const {
            return _gcount;
        }

        inline const char *get() const {
            if(_current_position == _length) {
                throw kafka::parsing_exception("Input stream is exhausted.");
            }
            return _data + _current_position;
        }

        inline int32_t get_position() const noexcept {
            return _current_position;
        }

        inline void set_position(int32_t new_position) {
            if(new_position >= _length || new_position < 0) {
                throw kafka::parsing_exception("Attempted to set input stream's position outside its data.");
            }
            else {
                _current_position = new_position;
            }
        }

        inline void move_position(int32_t delta) {
            set_position(_current_position + delta);
        }
        
        int32_t size() const noexcept {
            return _length;
        }

        inline const char * begin() const {
            return _data;
        }
    };

    class output_stream {
    private:
        vector<char> _data;

        int32_t _current_position = 0;
        bool _is_resizable;

        output_stream(bool is_resizable, int32_t size) noexcept
                : _data(vector<char>(size, 0))
                , _is_resizable(is_resizable)
        {}

    public:

        static output_stream fixed_size_stream(int32_t size) {
            return output_stream(false, size);
        }

        static output_stream resizable_stream() {
            return output_stream(true, 0);
        }

        inline void write(const char *source, int32_t length) {
            if(length + _current_position > static_cast<int32_t>(_data.size())) {
                if(_is_resizable){
                    _data.resize(length + _current_position);
                }
                else {
                    throw kafka::parsing_exception("This output stream won't fit that many bytes.");
                }
            }
            std::copy(source, source + length, _data.data() + _current_position);
            _current_position += length;
        }

        inline char *get() {
            if(_current_position == static_cast<int32_t>(_data.size())) {
                throw kafka::parsing_exception("Output stream is full.");
            }
            return _data.data() + _current_position;
        }

        inline const char *get() const {
            if(_current_position == static_cast<int32_t>(_data.size())) {
                throw kafka::parsing_exception("Output stream is full.");
            }
            return _data.data() + _current_position;
        }

        inline const char * begin() const {
            return _data.data();
        }

        inline vector<char> &get_vector() noexcept {
            return _data;
        }

        inline const vector<char> &get_vector() const noexcept {
            return _data;
        }

        inline int32_t get_position() const noexcept {
            return _current_position;
        }

        inline void set_position(int32_t new_position) {
            if(new_position < 0) {
                throw kafka::parsing_exception("Cannot set position to negative value.");
            }

            if(_is_resizable) {
                _data.resize(new_position + 1);
            }
            else {
                if(new_position >= static_cast<int32_t>(_data.size())) {
                    kafka::parsing_exception("Attempted to set fixed output stream's position past its data.");
                }
            }

            _current_position = new_position;
        }

        inline void move_position(int32_t delta) {
            set_position(_current_position + delta);
        }

        int32_t size() const noexcept {
            return _data.size();
        }
    };

} // namespace kafka

} // namespace seastar

#endif // __STREAMS_HH__