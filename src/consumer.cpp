#include "ring_buffer.hpp"
#include "shm_manager.hpp"

#include <getopt.h>

#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>

namespace {

void usage(const char* argv0) {
    std::fprintf(stderr,
                 "Usage: %s [--frames N] [--shm /name] [--attach-timeout-s N]\n"
                 "\n"
                 "Validates FNV-1a checksums of 1 MiB frames from the shared-memory ring and\n"
                 "prints handoff and fill-to-observe latency stats. Default: 10000 frames,\n"
                 "shm name /zcipc_sensor.\n",
                 argv0);
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

void sleep_ms(uint64_t ms) {
    timespec ts{};
    ts.tv_sec = static_cast<time_t>(ms / 1000ull);
    ts.tv_nsec = static_cast<long>((ms % 1000ull) * 1000000ull);
    nanosleep(&ts, nullptr);
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t frames = 10000;
    uint64_t attach_timeout_s = 120;
    const char* shm_name = "/zcipc_sensor";

    static const option kOptions[] = {
        {"frames", required_argument, nullptr, 'n'},
        {"shm", required_argument, nullptr, 'm'},
        {"attach-timeout-s", required_argument, nullptr, 't'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    int opt = 0;
    while ((opt = getopt_long(argc, argv, "n:m:t:h", kOptions, nullptr)) != -1) {
        switch (opt) {
            case 'n':
                if (!parse_u64(optarg, frames) || frames == 0 || frames > 1000000ull) {
                    std::fprintf(stderr, "invalid --frames\n");
                    return 2;
                }
                break;
            case 'm':
                shm_name = optarg;
                break;
            case 't':
                if (!parse_u64(optarg, attach_timeout_s) || attach_timeout_s == 0) {
                    std::fprintf(stderr, "invalid --attach-timeout-s\n");
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

    zcipc::ShmManager::install_signal_handlers();

    zcipc::ShmManager shm;
    const uint64_t attach_deadline =
        zcipc::monotonic_ns() + attach_timeout_s * 1000000000ull;
    while (!shm.open(shm_name, sizeof(zcipc::RingBuffer), false)) {
        if (zcipc::ShmManager::stop_requested != 0) {
            std::fprintf(stderr, "consumer stopped while waiting for %s\n", shm_name);
            return 1;
        }
        if (zcipc::monotonic_ns() >= attach_deadline) {
            std::fprintf(stderr, "consumer timed out waiting for %s: %s\n", shm_name,
                         std::strerror(errno));
            return 1;
        }
        sleep_ms(2);
    }

    zcipc::RingBuffer* ring = zcipc::view_ring(shm.data());
    const uint64_t ready_deadline =
        zcipc::monotonic_ns() + attach_timeout_s * 1000000000ull;
    while (ring->ready.load(std::memory_order_acquire) != 1u) {
        if (zcipc::ShmManager::stop_requested != 0 || zcipc::monotonic_ns() >= ready_deadline) {
            std::fprintf(stderr, "consumer timed out waiting for ring init\n");
            return 1;
        }
        zcipc::cpu_relax();
    }
    if (!zcipc::ring_header_ok(*ring)) {
        std::fprintf(stderr, "consumer ring header mismatch magic=%#" PRIx32 "\n", ring->meta.magic);
        return 1;
    }
    zcipc::prefault_region(shm.data(), shm.size());
    shm.try_mlock();

    // Histogram storage is allocated once, before frame transmission.
    std::unique_ptr<uint64_t[]> handoff(new uint64_t[frames]);
    std::unique_ptr<uint64_t[]> end_to_end(new uint64_t[frames]);

    std::fprintf(stderr, "consumer attached shm=%s frames=%" PRIu64 " session_ns=%" PRIu64 "\n",
                 shm.name(), frames, ring->meta.session_ns);

    const uint64_t t0 = zcipc::monotonic_ns();
    uint64_t got = 0;
    uint64_t bad = 0;
    uint64_t retries = 0;
    uint64_t inversions = 0;
    uint64_t last_id = 0;
    bool have_id = false;
    uint64_t gaps = 0;
    uint64_t last_progress = t0;
    uint32_t spins = 0;
    constexpr uint64_t kStallNs = 120ull * 1000000000ull;
    bool stalled = false;

    while (got < frames) {
        if (zcipc::ShmManager::stop_requested != 0) {
            break;
        }
        zcipc::ConsumedFrame frame{};
        const zcipc::Poll poll = zcipc::poll_frame(*ring, frame);
        if (poll == zcipc::Poll::Frame) {
            uint64_t handoff_ns = 0;
            uint64_t e2e_ns = 0;
            if (frame.observed_ns >= frame.timestamp_ns) {
                handoff_ns = frame.observed_ns - frame.timestamp_ns;
            } else {
                ++inversions;
            }
            if (frame.observed_ns >= frame.fill_start_ns) {
                e2e_ns = frame.observed_ns - frame.fill_start_ns;
            } else {
                ++inversions;
            }
            handoff[got] = handoff_ns;
            end_to_end[got] = e2e_ns;
            if (!frame.checksum_ok) {
                if (bad == 0) {
                    std::fprintf(stderr,
                                 "checksum mismatch frame=%" PRIu64 " expected=%" PRIu32
                                 " computed=%" PRIu32 "\n",
                                 frame.frame_id, frame.checksum, frame.computed);
                }
                ++bad;
            }
            if (have_id && frame.frame_id != last_id + 1) {
                ++gaps;
            }
            last_id = frame.frame_id;
            have_id = true;
            ++got;
            last_progress = frame.observed_ns;
            spins = 0;
            continue;
        }
        if (poll == zcipc::Poll::Empty &&
            ring->producer_done.load(std::memory_order_acquire) == 1u) {
            break;
        }
        if (poll == zcipc::Poll::Retry) {
            ++retries;
        }
        if ((++spins & 4095u) == 0u) {
            if (zcipc::monotonic_ns() - last_progress > kStallNs) {
                stalled = true;
                break;
            }
        }
        zcipc::cpu_relax();
    }

    const uint64_t elapsed_ns = zcipc::monotonic_ns() - t0;
    const double elapsed_s = static_cast<double>(elapsed_ns) / 1e9;
    const zcipc::LatencyStats handoff_stats = zcipc::summarize_latencies(handoff.get(), got);
    const zcipc::LatencyStats e2e_stats = zcipc::summarize_latencies(end_to_end.get(), got);
    const uint64_t dropped_oldest = ring->dropped_oldest.load(std::memory_order_relaxed);
    const uint64_t dropped_newest = ring->dropped_newest.load(std::memory_order_relaxed);
    const double pass_rate =
        got == 0 ? 0.0 : (100.0 * static_cast<double>(got - bad)) / static_cast<double>(got);
    const double mib_s =
        elapsed_s > 0.0
            ? (static_cast<double>(got) * static_cast<double>(zcipc::kPayloadBytes)) /
                  (1024.0 * 1024.0) / elapsed_s
            : 0.0;

    std::printf("consumer_frames_expected=%" PRIu64 "\n", frames);
    std::printf("consumer_frames_received=%" PRIu64 "\n", got);
    std::printf("consumer_checksum_ok=%" PRIu64 "\n", got - bad);
    std::printf("consumer_checksum_bad=%" PRIu64 "\n", bad);
    std::printf("consumer_checksum_pass_rate=%.6f\n", pass_rate);
    std::printf("consumer_frame_id_gaps=%" PRIu64 "\n", gaps);
    std::printf("consumer_clock_inversions=%" PRIu64 "\n", inversions);
    std::printf("consumer_poll_retries=%" PRIu64 "\n", retries);
    std::printf("consumer_dropped_oldest=%" PRIu64 "\n", dropped_oldest);
    std::printf("consumer_dropped_newest=%" PRIu64 "\n", dropped_newest);
    std::printf("consumer_stalled=%d\n", stalled ? 1 : 0);
    std::printf("consumer_elapsed_s=%.6f\n", elapsed_s);
    std::printf("consumer_throughput_MiB_s=%.3f\n", mib_s);
    zcipc::print_latency_stats("consumer_handoff", handoff_stats);
    zcipc::print_latency_stats("consumer_fill_to_observe", e2e_stats);
    std::fflush(stdout);

    if (stalled) {
        std::fprintf(stderr, "consumer stalled with %" PRIu64 "/%" PRIu64 " frames\n", got, frames);
    }
    return (got == frames && bad == 0 && !stalled && zcipc::ShmManager::stop_requested == 0) ? 0
                                                                                            : 1;
}
