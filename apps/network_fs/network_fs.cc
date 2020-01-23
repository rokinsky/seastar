#include "seastar/core/do_with.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/future.hh"
#include <array>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/gate.hh>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace seastar;

using std::vector;
using std::cerr;
using std::cout;
using std::endl;

namespace {

constexpr uint32_t alignment = 4096;

template<typename... Cont>
future<> then_seq(Cont&&... conts) {
    auto fut = make_ready_future<>();
    ((fut = std::move(fut).then(std::forward<Cont>(conts))), ...);
    return fut;
}

template<typename T>
future<> read_object(input_stream<char>& input, T& obj) {
    return input.read_exactly(sizeof(T)).then([&obj] (temporary_buffer<char> buff) {
        if (buff.size() != sizeof(T)) {
            return make_exception_future<>(std::runtime_error("Couldn't read object"));
        }
        std::memcpy(&obj, buff.get(), sizeof(T));
        return make_ready_future<>(); // TODO: add htons etc?
    });
}

// sstring format - length:size_t string:char[]
template <>
future<> read_object(input_stream<char>& input, sstring& str) {
    return do_with((size_t)0, [&input, &str] (size_t& size) {
        return read_object(input, size).then([&input, &size] () {
            return input.read_exactly(size);
        }).then([&str] (temporary_buffer<char> buff) {
            str = sstring(buff.begin(), buff.end());
        });
    });
}
template<typename... T>
future<> read_objects(input_stream<char>& input, T&... objects) {
    return then_seq([&input, &objects] {
        return read_object(input, objects);
    }...);
}

template<typename T>
future<> write_object(output_stream<char>& output, const T& obj) {
    return output.write(reinterpret_cast<const char*>(&obj), sizeof(obj));
}

// sstring format - length:size_t string:char[]
template<>
future<> write_object(output_stream<char>& output, const sstring& str) {
    return write_object(output, str.size()).then([&output, &str] {
        return output.write(str);
    });
}

// temporary_buffer format - length:size_t string:char[]
template<>
future<> write_object(output_stream<char>& output, const temporary_buffer<char>& buff) {
    return write_object(output, buff.size()).then([&output, &buff] {
        return output.write(buff.get(), buff.size());
    });
}

// vector format - length:size_t elem1:sstring elem2:sstring ...
template<>
future<> write_object(output_stream<char>& output, const vector<sstring>& vec) {
    return write_object(output, vec.size()).then([&output, &vec] {
        return seastar::do_for_each(vec, [&output] (const sstring& obj) {
            return write_object<sstring>(output, obj);
        });
    });
}

template<typename... T>
future<> write_objects(output_stream<char>& output, T&&... objects) {
    return do_with(std::forward<T>(objects)..., [&output] (auto&... args) {
        return then_seq([&output, &args] {
            return write_object(output, std::move(args));
        }...);
    });
}

// input format  - path:str flags:int
// output format - retopen err:int
future<> handle_open(input_stream<char>& input, output_stream<char>& output, file& file) {
    return now().then([&input, &output, &file] {
        if (file) {
            return make_exception_future(std::runtime_error("file already initialized"));
        }
        return do_with(sstring(), 0, [&input, &output, &file] (sstring& path, int& flags) {
            return read_objects(input, path, flags).then([&file, &path, &flags] {
                return open_file_dma(path, open_flags(flags)).then([&file] (auto new_file) {
                    file = std::move(new_file);
                });
            }).then([&output] {
                return write_objects(output, sstring("retopen"), 0);
            }).then([&output] {
                return output.flush();
            }).then([] {
                cerr << "Sent reply for open operation" << endl;
            });
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        cerr << "An error occurred in handle_open" << endl;
        return write_objects(output, sstring("retopen"), -1).then([&output] {
            return output.flush();
        }).then([e] {
            return make_exception_future(e);
        });
    });
}

// input format  - empty
// output format - retclose err:int
future<> handle_close(output_stream<char>& output, file& file) {
    return now().then([&output, &file] {
        if (!file) {
            return make_exception_future(std::runtime_error("file not initialized"));
        }
        return file.close().then([&output] {
            return write_objects(output, sstring("retclose"), 0);
        }).then([&output] {
            return output.flush();
        }).then([&file] {
            // can't open file after closing it, so we create new file
            file = seastar::file();
            cerr << "Sent reply for close operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        cerr << "An error occurred in handle_close" << endl;
        return write_objects(output, sstring("retclose"), -1).then([&output] {
            return output.flush();
        }).then([e] {
            return make_exception_future(e);
        });
    });
}

// input format  - count:size_t offset:off_t
// output format - retpread err:int [buff:str]
future<> handle_pread(input_stream<char>& input, output_stream<char>& output, file& file) {
    return now().then([&input, &output, &file] {
        if (!file) {
            return make_exception_future(std::runtime_error("file not initialized"));
        }
        return do_with((size_t)0, (off_t)0, [&input, &output, &file] (size_t& count, off_t& offset) {
            return read_objects(input, count, offset).then([&count, &offset, &file] () {
                return file.dma_read<char>(offset, count);
            }).then([&output] (temporary_buffer<char> read_buf) {
                return write_objects(output, sstring("retpread"), 0, std::move(read_buf));
            }).then([&output] {
                return output.flush();
            }).then([] {
                cerr << "Sent reply for pread operation" << endl;
            });
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        cerr << "An error occurred in handle_pread" << endl;
        return write_objects(output, sstring("retpread"), -1).then([&output] {
            return output.flush();
        }).then([e] {
            return make_exception_future(e);
        });
    });
}

// input format  - buff:str count:size_t offset:off_t
// output format - retpwrite err:int [size:size_t]
// TODO: solve alignment problem
future<> handle_pwrite(input_stream<char>& input, output_stream<char>& output, file& file) {
    return now().then([&input, &output, &file] {
        if (!file) {
            return make_exception_future(std::runtime_error("file not initialized"));
        }
        return do_with(sstring(), (size_t)0, (off_t)0,
                [&input, &output, &file] (sstring& buff, size_t& count, off_t& offset) {
            return read_objects(input, buff, count, offset).then([&buff, &count, &offset, &file] () {
                if (count % alignment != 0 || offset % alignment != 0) {
                    return make_exception_future<size_t>(std::runtime_error("count and offset not aligned"));
                }
                auto temp_buf = temporary_buffer<char>::aligned(alignment, buff.size());
                std::memcpy(temp_buf.get_write(), buff.c_str(), buff.size());
                return file.dma_write<char>(offset, temp_buf.get(), count)
                        .then([temp_buf = std::move(temp_buf)] (size_t write_size) {
                    return write_size;
                });
            }).then([&output] (size_t write_size) {
                return write_objects(output, sstring("retpwrite"), 0, write_size);
            }).then([&output] {
                return output.flush();
            }).then([] {
                cerr << "Sent reply for pwrite operation" << endl;
            });
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        cerr << "An error occurred in handle_pwrite" << endl;
        return write_objects(output, sstring("retpwrite"), -1).then([&output] {
            return output.flush();
        }).then([e] {
            return make_exception_future(e);
        });
    });
}

// input format  - path:str
// output format - retreaddir err:int size:int [name:str]{size}
future<> handle_readdir(input_stream<char>& input, output_stream<char>& output) {
    return do_with(sstring(), [&input, &output] (sstring& path) {
        return read_objects(input, path).then([&path] () {
            return open_directory(path);
        }).then([] (file dir) {
            auto vec = make_lw_shared(vector<sstring>());
            auto listing_lmb = dir.list_directory([vec] (directory_entry de) {
                    vec->emplace_back(std::move(de.name));
                    return make_ready_future<>();
                });
            auto listing = make_lw_shared(std::move(listing_lmb));
            return listing->done().then([vec, listing, dir] {
                return std::move(*vec);
            });
        }).then([&output] (vector<sstring> vec) {
            return write_objects(output, sstring("retreaddir"), 0, std::move(vec));
        }).then([&output] {
            return output.flush();
        }).then([] {
            cerr << "Sent reply for readdir operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        cerr << "An error occurred in handle_readdir" << endl;
        return write_objects(output, sstring("retreaddir"), -1).then([&output] {
            return output.flush();
        }).then([e] {
            return make_exception_future(e);
        });
    });
}

struct timespec time_point_to_timespec(const std::chrono::system_clock::time_point &tp) {
    auto secs = std::chrono::time_point_cast<std::chrono::seconds>(tp);
    auto ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(tp) -
            std::chrono::time_point_cast<std::chrono::nanoseconds>(secs);
    return timespec{secs.time_since_epoch().count(), ns.count()};
}

struct stat stat_data_to_stat(const struct stat_data& stat_data) {
    struct stat stat;
    stat.st_dev = stat_data.device_id;
    stat.st_ino = stat_data.inode_number;
    stat.st_mode = stat_data.mode;
    stat.st_nlink = stat_data.number_of_links;
    stat.st_uid = stat_data.uid;
    stat.st_gid = stat_data.gid;
    stat.st_rdev = stat_data.rdev;
    stat.st_size = stat_data.size;
    stat.st_blksize = stat_data.block_size;
    stat.st_blocks = stat_data.allocated_size / 512;
    stat.st_atim = time_point_to_timespec(stat_data.time_accessed);
    stat.st_mtim = time_point_to_timespec(stat_data.time_modified);
    stat.st_ctim = time_point_to_timespec(stat_data.time_changed);
    return stat;
}

// input format  - path:str
// output format - retgetattr err:int [stat:stat]
future<> handle_getattr(input_stream<char>& input, output_stream<char>& output) {
    return do_with(sstring(), [&input, &output] (sstring& path) {
        return read_objects(input, path).then([&path] () {
            return file_stat(path);
        }).then([&output] (struct stat_data stat_data) {
            return write_objects(output, sstring("retgetattr"), 0, stat_data_to_stat(stat_data));
        }).then([&output] {
            return output.flush();
        }).then([] {
            cerr << "Sent reply for getattr operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        cerr << "An error occurred in handle_getattr" << endl;
        return write_objects(output, sstring("retgetattr"), -1).then([&output] {
            return output.flush();
        }).then([e] {
            return make_exception_future(e);
        });
    });
}

future<> handle_single_operation(input_stream<char>& input, output_stream<char>& output, file& file) {
    // read operation name and decide which operation handler to start
    return do_with(sstring(), [&input, &output, &file] (sstring& operation_name) {
        return read_objects(input, operation_name).then([&input, &output, &file, &operation_name] () {
            cerr << "operation: " << operation_name << endl;

            future<> operation = make_ready_future<>();
            if (operation_name == "open") {
                operation = handle_open(input, output, file);
            } else if (operation_name == "close") {
                operation = handle_close(output, file);
            } else if (operation_name == "pread") {
                operation = handle_pread(input, output, file);
            } else if (operation_name == "pwrite") {
                operation = handle_pwrite(input, output, file);
            } else if (operation_name == "readdir") {
                operation = handle_readdir(input, output);
            } else if (operation_name == "getattr") {
                operation = handle_getattr(input, output);
            } else {
                operation = make_exception_future<>(std::runtime_error("Invalid operation name"));
            }

            return operation;
        });
    });
}

future<> handle_connection(connected_socket connection, socket_address remote_address) {
    cerr << "New connection from " << remote_address << endl;
    return do_with(connection.input(), connection.output(), file(),
            [] (input_stream<char>& input, output_stream<char>& output, file& file) {
        return repeat([&input, &output, &file] {
            return handle_single_operation(input, output, file).then([] () {
                return stop_iteration::no;
            });
        }).finally([&output, &file] {
            return output.close().then([&file] {
                if (file)
                    return file.close();
                return make_ready_future<>();
            });
        });
    }).handle_exception([] (std::exception_ptr e) {
        cerr << "An error occurred: " << e << endl;
    }).finally([connection = std::move(connection), remote_address = std::move(remote_address)] {
        cerr << "Closing connection with " << remote_address << endl << endl;
    });
}

future<> start_server(uint16_t port) {
    seastar::listen_options lo;
    lo.reuse_address = true;
    return do_with(engine().listen(make_ipv4_address({port}), lo),
            gate(), [] (server_socket& listener, gate& gate) {
        return keep_doing([&listener, &gate] () {
            return listener.accept().then([&gate] (accept_result connection) {
                auto connection_handler = with_gate(gate, [connection = std::move(connection)] () mutable {
                    return handle_connection(std::move(connection.connection), std::move(connection.remote_address));
                });
            });
        }).finally([&gate] {
            return gate.close();
        });
    });
}

}

// TODO: need to be started with one core (-c 1) else client freezes sometimes
int main(int argc, char** argv) {
    app_template app;
    namespace bpo = boost::program_options;
    app.add_options()
        ("port,p", bpo::value<uint16_t>()->default_value(6969), "port to listen on");

    try {
        app.run(argc, argv, [&app] {
            auto& args = app.configuration();
            return start_server(args["port"].as<uint16_t>())
                    .handle_exception([] (std::exception_ptr e) {
                cerr << "An error occurred: " << e << endl;
            });
        });
    } catch(...) {
        cerr << "Couldn't start application: " << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}
