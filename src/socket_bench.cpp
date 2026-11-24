#include "ring_buffer.hpp"

#include <getopt.h>

#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>

#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

struct SocketFrame {
    zcipc::FrameHeader header;
    uint8_t payload[zcipc::kPayloadBytes];
};

// One reusable frame buffer. BSS, not a hot-path allocation.
SocketFrame g_frame;

void usage(const char* argv0) {
    std::fprintf(stderr,
                 "Usage: %s [--frames N]\n"
                 "\n"
                 "Forks an AF_UNIX SOCK_STREAM producer and consumer and moves %" PRIu64
                 "-byte frames\n"
                 "(48-byte header + 1 MiB payload). Prints receiver transfer latency.\n",
                 argv0, static_cast<uint64_t>(sizeof(SocketFrame)));
}

bool parse_u64(const char* text, uint64_t& out) {
    if (text == nullptr || *text == '\0') {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    unsigned long long value = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return false;
    }
    out = static_cast<uint64_t>(value);
    return true;
}

bool write_full(int fd, const void* data, std::size_t n) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    std::size_t off = 0;
    while (off < n) {
        const ssize_t wrote = ::send(fd, bytes + off, n - off, MSG_NOSIGNAL);
        if (wrote < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (wrote == 0) {
            return false;
        }
        off += static_cast<std::size_t>(wrote);
    }
    return true;
}

bool read_full(int fd, void* data, std::size_t n) {
    auto* bytes = static_cast<uint8_t*>(data);
    std::size_t off = 0;
    while (off < n) {
        const ssize_t got = ::recv(fd, bytes + off, n - off, 0);
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (got == 0) {
            return false;
        }
        off += static_cast<std::size_t>(got);
    }
    return true;
}

void set_buffer(int fd, int opt, int bytes) {
    setsockopt(fd, SOL_SOCKET, opt, &bytes, sizeof(bytes));
}

int run_receiver(int listen_fd, uint64_t frames) {
    sockaddr_un peer{};
    socklen_t peer_len = sizeof(peer);
    const int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (fd < 0) {
        std::fprintf(stderr, "socket accept failed: %s\n", std::strerror(errno));
        return 1;
    }
    set_buffer(fd, SO_RCVBUF, 4 * 1024 * 1024);
    ::close(listen_fd);

    std::unique_ptr<uint64_t[]> transfer(new uint64_t[frames]);
    std::unique_ptr<uint64_t[]> end_to_end(new uint64_t[frames]);

    const uint64_t t0 = zcipc::monotonic_ns();
    uint64_t got = 0;
    uint64_t bad = 0;
    uint64_t inversions = 0;

    while (got < frames) {
        if (!read_full(fd, &g_frame, sizeof(g_frame))) {
            std::fprintf(stderr, "socket short read at frame %" PRIu64 "\n", got);
            break;
        }
        const uint64_t observed = zcipc::monotonic_ns();
        const uint32_t computed = zcipc::fnv1a32(g_frame.payload, zcipc::kPayloadBytes);
        const bool checksum_ok =
            computed == g_frame.header.checksum && g_frame.header.payload_bytes == zcipc::kPayloadBytes;
        if (!checksum_ok) {
            ++bad;
        }
        if (observed >= g_frame.header.timestamp_ns) {
            transfer[got] = observed - g_frame.header.timestamp_ns;
        } else {
            transfer[got] = 0;
            ++inversions;
        }
        if (observed >= g_frame.header.fill_start_ns) {
            end_to_end[got] = observed - g_frame.header.fill_start_ns;
        } else {
            end_to_end[got] = 0;
            ++inversions;
        }
        ++got;
    }
    ::close(fd);

    const uint64_t elapsed_ns = zcipc::monotonic_ns() - t0;
    const double elapsed_s = static_cast<double>(elapsed_ns) / 1e9;
    const zcipc::LatencyStats transfer_stats = zcipc::summarize_latencies(transfer.get(), got);
    const zcipc::LatencyStats e2e_stats = zcipc::summarize_latencies(end_to_end.get(), got);
    const double pass_rate =
        got == 0 ? 0.0 : (100.0 * static_cast<double>(got - bad)) / static_cast<double>(got);
    const double mib_s =
        elapsed_s > 0.0
            ? (static_cast<double>(got) * static_cast<double>(zcipc::kPayloadBytes)) /
                  (1024.0 * 1024.0) / elapsed_s
            : 0.0;

    std::printf("socket_frames_expected=%" PRIu64 "\n", frames);
    std::printf("socket_frames_received=%" PRIu64 "\n", got);
    std::printf("socket_checksum_ok=%" PRIu64 "\n", got - bad);
    std::printf("socket_checksum_bad=%" PRIu64 "\n", bad);
    std::printf("socket_checksum_pass_rate=%.6f\n", pass_rate);
    std::printf("socket_clock_inversions=%" PRIu64 "\n", inversions);
    std::printf("socket_elapsed_s=%.6f\n", elapsed_s);
    std::printf("socket_throughput_MiB_s=%.3f\n", mib_s);
    std::printf("socket_frame_bytes=%zu\n", sizeof(SocketFrame));
    zcipc::print_latency_stats("socket_transfer", transfer_stats);
    zcipc::print_latency_stats("socket_fill_to_observe", e2e_stats);
    std::fflush(stdout);
    return (got == frames && bad == 0) ? 0 : 1;
}

int run_sender(const sockaddr_un& addr, uint64_t frames) {
    const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        std::fprintf(stderr, "socket create failed: %s\n", std::strerror(errno));
        return 1;
    }
    set_buffer(fd, SO_SNDBUF, 4 * 1024 * 1024);

    bool connected = false;
    for (int attempt = 0; attempt < 2000 && !connected; ++attempt) {
        if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0) {
            connected = true;
            break;
        }
        if (errno != ECONNREFUSED && errno != ENOENT) {
            break;
        }
        timespec ts{};
        ts.tv_nsec = 1000000L;
        nanosleep(&ts, nullptr);
    }
    if (!connected) {
        std::fprintf(stderr, "socket connect failed: %s\n", std::strerror(errno));
        ::close(fd);
        return 1;
    }

    const uint64_t t0 = zcipc::monotonic_ns();
    uint64_t sent = 0;
    for (; sent < frames; ++sent) {
        const uint32_t sensor = (sent % 2ull == 0ull)
                                    ? static_cast<uint32_t>(zcipc::SensorType::Camera)
                                    : static_cast<uint32_t>(zcipc::SensorType::Lidar);
        g_frame.header.fill_start_ns = zcipc::monotonic_ns();
        zcipc::fill_synthetic(g_frame.payload, zcipc::kPayloadBytes, sent, sensor);
        g_frame.header.payload_ready_ns = zcipc::monotonic_ns();
        g_frame.header.frame_id = sent;
        g_frame.header.sensor_type = sensor;
        g_frame.header.payload_bytes = static_cast<uint32_t>(zcipc::kPayloadBytes);
        g_frame.header.checksum = zcipc::fnv1a32(g_frame.payload, zcipc::kPayloadBytes);
        g_frame.header.reserved = 0;
        g_frame.header.timestamp_ns = zcipc::monotonic_ns();
        if (!write_full(fd, &g_frame, sizeof(g_frame))) {
            std::fprintf(stderr, "socket short write at frame %" PRIu64 ": %s\n", sent,
                         std::strerror(errno));
            break;
        }
    }
    ::close(fd);
    const double elapsed_s = static_cast<double>(zcipc::monotonic_ns() - t0) / 1e9;
    std::fprintf(stderr, "socket_sender_frames=%" PRIu64 " elapsed_s=%.6f\n", sent, elapsed_s);
    return sent == frames ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t frames = 10000;
    static const option kOptions[] = {
        {"frames", required_argument, nullptr, 'n'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };
    int opt = 0;
    while ((opt = getopt_long(argc, argv, "n:h", kOptions, nullptr)) != -1) {
        switch (opt) {
            case 'n':
                if (!parse_u64(optarg, frames) || frames == 0 || frames > 1000000ull) {
                    std::fprintf(stderr, "invalid --frames\n");
                    return 2;
                }
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                usage(argv[0]);
                return 2;
        }
    }

    std::signal(SIGPIPE, SIG_IGN);

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    const int path_n =
        std::snprintf(addr.sun_path, sizeof(addr.sun_path), "/tmp/zcipc_socket_%d.sock", ::getpid());
    if (path_n <= 0 || static_cast<std::size_t>(path_n) >= sizeof(addr.sun_path)) {
        std::fprintf(stderr, "socket path does not fit\n");
        return 1;
    }
    ::unlink(addr.sun_path);

    const int listen_fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) {
        std::fprintf(stderr, "socket listen create failed: %s\n", std::strerror(errno));
        return 1;
    }
    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::fprintf(stderr, "socket bind failed: %s\n", std::strerror(errno));
        ::close(listen_fd);
        return 1;
    }
    if (::listen(listen_fd, 1) != 0) {
        std::fprintf(stderr, "socket listen failed: %s\n", std::strerror(errno));
        ::close(listen_fd);
        ::unlink(addr.sun_path);
        return 1;
    }

    const pid_t pid = ::fork();
    if (pid < 0) {
        std::fprintf(stderr, "fork failed: %s\n", std::strerror(errno));
        ::close(listen_fd);
        ::unlink(addr.sun_path);
        return 1;
    }
    if (pid == 0) {
        const int rc = run_receiver(listen_fd, frames);
        ::unlink(addr.sun_path);
        return rc;
    }

    ::close(listen_fd);
    const int send_rc = run_sender(addr, frames);
    int status = 0;
    if (::waitpid(pid, &status, 0) < 0) {
        std::fprintf(stderr, "waitpid failed: %s\n", std::strerror(errno));
        ::unlink(addr.sun_path);
        return 1;
    }
    ::unlink(addr.sun_path);
    if (send_rc != 0) {
        return send_rc;
    }
    if (!WIFEXITED(status)) {
        return 1;
    }
    return WEXITSTATUS(status);
}
