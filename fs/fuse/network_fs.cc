#include <asm-generic/errno-base.h>
#include <exception>
#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <vector>
#include <string>
#include <iostream>
#include <optional>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sstream>
#include <boost/stacktrace.hpp>

using std::vector;
using std::string;
using std::cerr;
using std::cout;
using std::endl;

template<typename ...Args>
void _debug(const char* func_name, int line, Args&&... args) {
    std::stringstream s;
    s << "\e[1;31mDEBUG \e[1;37m" << func_name << "()\e[0m \e[1;32m" << line << "\e[0m ";
    (s << ... << args) << std::endl;
    std::cerr << s.str();
}

#define DEBUG(args...) _debug(__FUNCTION__, __LINE__, ##args)


/*
 * Command line options
 *
 * We can't set default values for the char* fields here because
 * fuse_opt_parse would attempt to free() them when the user specifies
 * different values on the command line.
 */
// TODO: make options more c++ like (boost?)
static struct options {
    const char *host;
    const char *port;
    int show_help;
} options;

#define OPTION(t, p)                           \
    { t, offsetof(struct options, p), 1 }
static const struct fuse_opt option_spec[] = {
    OPTION("--host=%s", host),
    OPTION("--port=%s", port),
    OPTION("-h", show_help),
    OPTION("--help", show_help),
    FUSE_OPT_END
};

struct fixed_buf_string {
    char* buf;
    size_t size;
};

template<typename F>
class final_action {
private:
    F _clean;
public:
    final_action(F clean) : _clean(std::move(clean)) {}

    final_action(const final_action&) = delete;
    final_action(final_action&&) = delete;
    final_action& operator=(const final_action&) = delete;
    final_action& operator=(final_action&&) = delete;

    ~final_action() {
        try {
            _clean();
        } catch (...) {
        }
    }
};


template<typename F>
final_action<F> finally(F f) {
    return final_action<F>(std::move(f));
}

size_t read_exactly(int fd, size_t num, char* buff) noexcept {
    size_t pos = 0;
    while (num > 0) {
        int rd = read(fd, buff + pos, num);
        if (rd <= 0) {
            return pos;
        }
        num -= rd;
        pos += rd;
    }
    return pos;
}

template<typename T>
int read_object(int fd, T& obj) noexcept {
    T tmp;
    if (read_exactly(fd, sizeof(tmp), reinterpret_cast<char*>(&tmp)) != sizeof(tmp)) {
        return -EIO;
    }

    obj = std::move(tmp);
    return 0;
}

// TODO: try to avoid dynamic length strings because of out of memory errors
template <>
int read_object(int fd, string& str) noexcept {
    try {
        int ret = 0;

        size_t size;
        if ((ret = read_object(fd, size)) != 0) {
            return ret;
        }

        string tmp(size, 0);
        if (read_exactly(fd, size, tmp.data()) != size) {
            return -EIO;
        }

        str = std::move(tmp);
        return 0;
    } catch (...) {
        DEBUG("caught an exception");
        return -EPERM;
    }
}

template <>
int read_object(int fd, fixed_buf_string& str) noexcept {
    try {
        int ret = 0;

        size_t size;
        if ((ret = read_object(fd, size)) != 0) {
            return ret;
        }

        if (read_exactly(fd, size, str.buf) != size) {
            return -EIO;
        }
        str.size = size;

        return 0;
    } catch (...) {
        DEBUG("caught an exception");
        return -EPERM;
    }
}

template <>
int read_object(int fd, vector<string>& vec) noexcept {
    try {
        int ret = 0;

        size_t size;
        if ((ret = read_object(fd, size)) != 0) {
            return ret;
        }

        vector<string> tmp_vec;
        while (size--) {
            string tmp_str(size, 0);
            if ((ret = read_object(fd, tmp_str)) != 0) {
                return ret;
            }
            tmp_vec.emplace_back(std::move(tmp_str));
        }

        vec = std::move(tmp_vec);
        return 0;
    } catch (...) {
        DEBUG("caught an exception");
        return -EPERM;
    }
}

template<typename... T>
int read_objects(int fd, T&... objects) noexcept {
    int ret = 0;
    (void) (((ret = read_object(fd, objects)) == 0) && ...);
    return ret;
}

size_t write_exactly(int fd, const char* buff, size_t num) noexcept {
    size_t pos = 0;
    while (num > 0) {
        int rd = write(fd, buff + pos, num);
        if (rd <= 0) {
            return pos;
        }
        num -= rd;
        pos += rd;
    }
    return pos;
}

template<typename T>
int write_object(int fd, const T& obj) noexcept {
    char buff[sizeof(obj)];
    std::memcpy(buff, &obj, sizeof(obj));
    return write_exactly(fd, buff, sizeof(obj)) == sizeof(obj) ? 0 : -EIO;
}

template<>
int write_object(int fd, const string& str) noexcept {
    int ret = write_object(fd, str.size());
    if (ret) {
        return ret;
    }
    return write_exactly(fd, str.data(), str.size()) == str.size() ? 0 : -EIO;
}

template<typename... T>
int write_objects(int fd, T&&... objects) noexcept {
    int ret = 0;
    (void) (((ret = write_object(fd, objects)) == 0) && ...);
    return ret;
}

int get_connection(const string& host, const string& port) noexcept {
    addrinfo addr_hints;
    addrinfo* addr_result;

    memset(&addr_hints, 0, sizeof(addrinfo));
    addr_hints.ai_family = AF_INET;
    addr_hints.ai_socktype = SOCK_STREAM;
    addr_hints.ai_protocol = IPPROTO_TCP;
    if (getaddrinfo(host.c_str(), port.c_str(), &addr_hints, &addr_result) != 0) {
        return -EINVAL;
    }

    int sock;
    if ((sock = socket(addr_result->ai_family, addr_result->ai_socktype, addr_result->ai_protocol)) < 0) {
        return -errno;
    }

    if (connect(sock, addr_result->ai_addr, addr_result->ai_addrlen) < 0) {
        close(sock);
        return -errno;
    }

    freeaddrinfo(addr_result);

    return sock;
}

int check_ans_prefix(int fd, const string& op) noexcept {
    try {
        string type;
        int err;
        if (read_objects(fd, type, err) || type != "ret" + op) {
            return -EIO;
        }
        if (err < 0) {
            return err;
        }
        return 0;
    } catch (...) {
        DEBUG("caught an exception");
        return -EPERM;
    }
}

static int network_fs_getattr(const char* path, struct stat* stbuf,
        __attribute__((unused)) struct fuse_file_info* fi) noexcept {
    DEBUG("path=", path);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect");
        return ret;
    }
    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });

    if ((ret = write_objects(fd, string("getattr"), "." + string(path))) < 0) {
        DEBUG("error while sending request");
        return ret;
    }

    if ((ret = check_ans_prefix(fd, "getattr")) < 0) {
        DEBUG("initial check error");
        return ret;
    }

    if ((ret = read_objects(fd, *stbuf)) < 0) {
        DEBUG("couldn't receive answer");
        return ret;
    }

    return 0;
}

static int network_fs_readdir(const char* path, void* buf,
        fuse_fill_dir_t filler,
        __attribute__((unused)) off_t offset,
        __attribute__((unused)) struct fuse_file_info* fi,
        __attribute__((unused)) enum fuse_readdir_flags flags) noexcept {
    DEBUG("path=", path);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect");
        return ret;
    }
    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });

    if ((ret = write_objects(fd, string("readdir"), "." + string(path))) < 0) {
        DEBUG("couldn't send request");
        return ret;
    }

    if ((ret = check_ans_prefix(fd, "readdir")) < 0) {
        DEBUG("initial check error");
        return ret;
    }

    std::vector<string> vec;
    if ((ret = read_objects(fd, vec)) < 0) {
        DEBUG("couldn't receive answer");
        return ret;
    }

    filler(buf, ".", NULL, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", NULL, 0, FUSE_FILL_DIR_PLUS);
    for (auto&& p : vec) {
        filler(buf, p.c_str(), NULL, 0, FUSE_FILL_DIR_PLUS);
    }

    return 0;
}

static int network_fs_open_helper(const char* path, int flags) noexcept {
    int ret, fd;

    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect");
        return ret;
    }
    fd = ret;
    auto clean = [&] {
        close(fd);
    };

    if ((ret = write_objects(fd, string("open"), "." + string(path), flags)) < 0) {
        DEBUG("couldn't send request");
        clean();
        return ret;
    }

    if ((ret = check_ans_prefix(fd, "open")) < 0) {
        DEBUG("initial check error");
        clean();
        return ret;
    }

    return fd;
}

static int network_fs_open(const char* path, struct fuse_file_info* fi) noexcept {
    DEBUG("path=", path);
    int ret;

    if ((ret = network_fs_open_helper(path, fi->fh)) > 0) {
        fi->fh = ret;
        return 0;
    }

    return ret;
}

static int network_fs_release_helper(int fd) noexcept {
    auto clean = finally([&] {
        close(fd);
    });

    int ret;
    if ((ret = write_objects(fd, string("close"))) < 0) {
        DEBUG("couldn't send request");
        return ret;
    }

    if ((ret = check_ans_prefix(fd, "close")) < 0) {
        DEBUG("initial check error");
        return ret;
    }

    return 0;
}

static int network_fs_release(const char* path, struct fuse_file_info* fi) noexcept {
    DEBUG("path=", path);
    return network_fs_release_helper(fi->fh);;
}

static int network_fs_read(const char* path, char* buf, size_t size,
        off_t offset, struct fuse_file_info* fi) noexcept {
    DEBUG("path=", path, ", size=", size, ", offset=", offset);

    int fd, ret;
    if (fi == NULL) {
        if ((ret = network_fs_open_helper(path, fi->fh)) < 0) {
            return ret;
        }
        fd = ret;
    } else {
        fd = fi->fh;
    }
    auto clean = finally([&] {
        if (fi == NULL) {
            network_fs_release_helper(fd);
        }
    });

    if ((ret = write_objects(fd, string("pread"), size, offset)) < 0) {
        DEBUG("couldn't send request");
        return ret;
    }

    if ((ret = check_ans_prefix(fd, "pread")) < 0) {
        DEBUG("initial check error");
        return ret;
    }

    fixed_buf_string buf_container{buf, 0};
    if ((ret = read_objects(fd, buf_container)) < 0) {
        DEBUG("couldn't receive answer");
        return ret;
    }

    return buf_container.size;
}

static struct fuse_operations network_fs_oper = {
    .getattr = network_fs_getattr,
    .readlink = nullptr,
    .mknod = nullptr,
    .mkdir = nullptr,
    .unlink = nullptr,
    .rmdir = nullptr,
    .symlink = nullptr,
    .rename = nullptr,
    .link = nullptr,
    .chmod = nullptr,
    .chown = nullptr,
    .truncate = nullptr,
    .open = network_fs_open,
    .read = network_fs_read,
    .write = nullptr,
    .statfs = nullptr,
    .flush = nullptr,
    .release = network_fs_release,
    .fsync = nullptr,
    .setxattr = nullptr,
    .getxattr = nullptr,
    .listxattr = nullptr,
    .removexattr = nullptr,
    .opendir = nullptr,
    .readdir = network_fs_readdir,
    .releasedir = nullptr,
    .fsyncdir = nullptr,
    .init = nullptr,
    .destroy = nullptr,
    .access = nullptr,
    .create = nullptr,
    .lock = nullptr,
    .utimens = nullptr,
    .bmap = nullptr,
    .ioctl = nullptr,
    .poll = nullptr,
    .write_buf = nullptr,
    .read_buf = nullptr,
    .flock = nullptr,
    .fallocate = nullptr,
    .copy_file_range = nullptr,
    .lseek = nullptr,
};

static void show_help(const char* progname) {
    printf("usage: %s [options] <mountpoint>\n\n", progname);
    printf("File-system specific options:\n"
           "    --host=<s>          Host address to connect\n"
           "                        (default: \"localhost\")\n"
           "    --port=<s>          Port to use while connecting\n"
           "                        (default: 6969)\n"
           "\n");
}

int main(int argc, char* argv[]) {
    int ret;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    /* Set defaults -- we have to use strdup so that
       fuse_opt_parse can free the defaults if other
       values are specified */
    options.host = strdup("localhost");
    options.port = strdup("6969");

    /* Parse options */
    if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1)
        return 1;

    /* When --help is specified, first print our own file-system
       specific help text, then signal fuse_main to show
       additional help (by adding `--help` to the options again)
       without usage: line (by setting argv[0] to the empty
       string) */
    if (options.show_help) {
        show_help(argv[0]);
        assert(fuse_opt_add_arg(&args, "--help") == 0);
        args.argv[0][0] = '\0';
    }

    ret = fuse_main(args.argc, args.argv, &network_fs_oper, NULL);
    fuse_opt_free_args(&args);
    return ret;
}
