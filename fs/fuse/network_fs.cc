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

#define DEBUG(format, args...) printf("\e[1;31mDEBUG \e[1;37m%s()\e[0m \e[1;35m%s \e[1;32m%d\e[0m " format "\n", __FUNCTION__, __FILE__, __LINE__, ##args)

/*
 * Command line options
 *
 * We can't set default values for the char* fields here because
 * fuse_opt_parse would attempt to free() them when the user specifies
 * different values on the command line.
 */
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

using std::vector;
using std::string;
using std::cerr;
using std::cout;
using std::endl;

std::optional<std::string> read_exactly(int fd, size_t num) {
    string ret(num, ' ');
    size_t pos = 0;
    while (num > 0) {
        int rd = read(fd, ret.data() + pos, num);
        if (rd <= 0)
            return std::nullopt;
        num -= rd;
        pos += rd;
    }
    return ret;
}

template<typename T>
std::optional<T> read_object(int fd) {
    std::optional<string> ret = read_exactly(fd, sizeof(T));
    if (!ret.has_value())
        return std::nullopt;
    string str = std::move(ret.value());
    T value;
    std::memcpy(&value, str.data(), sizeof(value));
    return value;
}

template <>
std::optional<string> read_object(int fd) {
    std::optional<size_t> ret = read_object<size_t>(fd);
    if (!ret.has_value())
        return std::nullopt;
    size_t size = ret.value();
    return read_exactly(fd, size);
}

template <>
std::optional<vector<string>> read_object(int fd) {
    std::optional<size_t> ret_size = read_object<size_t>(fd);
    if (!ret_size.has_value())
        return std::nullopt;
    size_t size = ret_size.value();

    vector<string> vec;
    while (size--) {
        std::optional<string> ret_str = read_object<string>(fd);
        if (!ret_str.has_value())
            return std::nullopt;
        vec.emplace_back(std::move(ret_str.value()));
    }

    return vec;
}

int read_objects(int) {
    return 0;
}

template<typename T1, typename... T>
int read_objects(int fd, T1 &head, T&... ts) {
    std::optional<T1> ret = read_object<T1>(fd);
    if (!ret.has_value())
        return -EIO;
    head = ret.value();
    return read_objects(fd, ts...);
}

int write_exactly(int fd, char* buff, size_t num) {
    size_t pos = 0;
    while (num > 0) {
        int rd = write(fd, buff + pos, num);
        if (rd <= 0)
            return pos;
        num -= rd;
        pos += rd;
    }
    return pos;
}

template<typename T>
int write_object(int fd, T&& obj) {
    char buff[sizeof(obj)];
    std::memcpy(buff, &obj, sizeof(obj));
    return write_exactly(fd, buff, sizeof(obj)) == sizeof(obj) ? 0 :  -EIO;
}

template<>
int write_object(int fd, string&& str) {
    int ret = write_object(fd, str.size());
    if (ret)
        return ret;
    return write_exactly(fd, str.data(), str.size()) == (ssize_t)str.size() ? 0 : -EIO;
}

int write_objects(int) {
    return 0;
}

template<typename T1, typename... T>
int write_objects(int fd, T1&& head, T&&... ts){
    int ret = write_object(fd, std::forward<T1>(head));
    if (ret)
        return ret;
    return write_objects(fd, std::forward<T>(ts)...);
}

int get_connection(const string& host, const string& port) {
    int sock;
    addrinfo addr_hints;
    addrinfo *addr_result;

    memset(&addr_hints, 0, sizeof(addrinfo));
    addr_hints.ai_family = AF_INET;
    addr_hints.ai_socktype = SOCK_STREAM;
    addr_hints.ai_protocol = IPPROTO_TCP;
    if (getaddrinfo(host.c_str(), port.c_str(), &addr_hints, &addr_result))
        return -errno;

    sock = socket(addr_result->ai_family, addr_result->ai_socktype, addr_result->ai_protocol);
    if (sock < 0)
        return -EINVAL;

    if (connect(sock, addr_result->ai_addr, addr_result->ai_addrlen) < 0)
        return -errno;

    freeaddrinfo(addr_result);

    return sock;
}

int check_ans_prefix(int fd, const string& op) {
    string type;
    int err;
    if (read_objects(fd, type, err) || type != "ret" + op)
        return -EIO;
    if (err < 0)
        return err;
    return 0;
}

static int network_fs_getattr(const char *path, struct stat *stbuf,
        struct fuse_file_info *fi)
{
    int fd, ret;
    DEBUG("path=%s", path);

    ret = get_connection(options.host, options.port);
    if (ret < 0) {
        DEBUG("couldn't connect");
        return ret;
    }
    fd = ret;

    ret = write_objects(fd, string("getattr"), "." + string(path));
    if (ret) {
        DEBUG("error while sending request");
        goto close_fd;
    }

    ret = check_ans_prefix(fd, "getattr");
    if (ret) {
        DEBUG("initial check error");
        goto close_fd;
    }

    if (read_objects(fd, *stbuf)) {
        DEBUG("receive error");
        goto close_fd;
    }

close_fd:
    close(fd);
    return ret;
}

static int network_fs_readdir(const char *path, void *buf,
        fuse_fill_dir_t filler, off_t offset,
        struct fuse_file_info *fi,
        enum fuse_readdir_flags flags)
{
    int fd, ret;
    std::vector<string> vec;
    DEBUG("path=%s", path);

    ret = get_connection(options.host, options.port);
    if (ret < 0) {
        DEBUG("couldn't connect");
        return ret;
    }
    fd = ret;

    ret = write_objects(fd, string("readdir"), "." + string(path));
    if (ret) {
        DEBUG("couldn't send request");
        goto close_fd;
    }

    ret = check_ans_prefix(fd, "readdir");
    if (ret) {
        DEBUG("initial check error");
        goto close_fd;
    }

    ret = read_objects(fd, vec);
    if (ret) {
        DEBUG("couldn't receive answer");
        goto close_fd;
    }

    filler(buf, ".", NULL, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", NULL, 0, FUSE_FILL_DIR_PLUS);
    for (auto&& p : vec)
        filler(buf, p.c_str(), NULL, 0, FUSE_FILL_DIR_PLUS);

close_fd:
    close(fd);
    return ret;
}

static int network_fs_open(const char *path, struct fuse_file_info *fi)
{
    int fd, ret, read_fd;
    DEBUG("path=%s", path);

    ret = get_connection(options.host, options.port);
    if (ret < 0) {
        DEBUG("couldn't connect");
        return ret;
    }
    fd = ret;

    ret = write_objects(fd, string("open"), "." + string(path), fi->flags);
    if (ret) {
        DEBUG("couldn't send request");
        goto close_fd;
    }

    ret = check_ans_prefix(fd, "open");
    if (ret) {
        DEBUG("initial check error");
        goto close_fd;
    }

    ret = read_objects(fd, read_fd);
    if (ret) {
        DEBUG("couldn't receive answer");
        goto close_fd;
    }
    fi->fh = (uint64_t)fd | ((uint64_t)read_fd << 32);

    return 0;

close_fd:
    close(fd);
    return ret;
}

static int network_fs_release(const char *path, struct fuse_file_info *fi)
{
    int fd, ret, read_fd;
    DEBUG("path=%s", path);

    fd = (fi->fh << 32) >> 32;
    read_fd = fi->fh >> 32;
    DEBUG("fd=%d, read_fd=%d", fd, read_fd);

    ret = write_objects(fd, string("close"), read_fd);
    if (ret) {
        DEBUG("couldn't send request");
        goto close_fd;
    }

    ret = check_ans_prefix(fd, "close");
    if (ret) {
        DEBUG("initial check error");
        goto close_fd;
    }

close_fd:
    close(fd);
    return ret;
}

static int network_fs_read(const char *path, char *buf, size_t size,
        off_t offset, struct fuse_file_info *fi)
{
    int fd, ret, read_fd;
    string str;
    DEBUG("path=%s", path);

    if (fi == NULL)
        return -EINVAL;

    fd = (fi->fh << 32) >> 32;
    read_fd = fi->fh >> 32;

    ret = write_objects(fd, string("pread"), read_fd, size, offset);
    if (ret) {
        DEBUG("couldn't send request");
        return ret;
    }

    ret = check_ans_prefix(fd, "pread");
    if (ret) {
        DEBUG("initial check error");
        return ret;
    }

    ret = read_objects(fd, str);
    if (ret) {
        DEBUG("couldn't receive answer");
        return ret;
    }

    std::memcpy(buf, str.c_str(), str.size());

    return str.size();
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

static void show_help(const char *progname)
{
    printf("usage: %s [options] <mountpoint>\n\n", progname);
    printf("File-system specific options:\n"
           "    --host=<s>          Host address to connect\n"
           "                        (default: \"localhost\")\n"
           "    --port=<s>          Port to use while connecting\n"
           "                        (default: 6969)\n"
           "\n");
}

int main(int argc, char *argv[])
{
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
