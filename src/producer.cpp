#include "ring_buffer.hpp"
#include "shm_manager.hpp"

#include <getopt.h>

#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

namespace {

void usage(const char* argv0) {
    std::fprintf(stderr,
                 "Usage: %s [--frames N] [--fps N] [--policy spin-yield|drop-newest|drop-oldest]\n"
                 "          [--sensor camera|lidar|alternate] [--shm /name]\n"
                 "\n"
                 "Streams synthetic 1 MiB sensor frames into a POSIX shared-memory SPSC ring.\n"
                 "spin-yield (default) waits for a free slot. A consumer must be running or\n"
                 "the producer blocks after %zu slots. Default: 10000 frames at 60 FPS,\n"
                 "shm name /zcipc_sensor.\n",
                 argv0, zcipc::kCapacity);
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

enum class SensorMode { Camera, Lidar, Alternate };

uint32_t sensor_for(uint64_t frame_id, SensorMode mode) {
    if (mode == SensorMode::Camera) {
        return static_cast<uint32_t>(zcipc::SensorType::Camera);
    }
    if (mode == SensorMode::Lidar) {
        return static_cast<uint32_t>(zcipc::SensorType::Lidar);
    }
    const auto type = (frame_id % 2ull == 0ull) ? zcipc::SensorType::Camera : zcipc::SensorType::Lidar;
    return static_cast<uint32_t>(type);
}

bool sleep_until(uint64_t deadline_ns) {
    timespec ts{};
    ts.tv_sec = static_cast<time_t>(deadline_ns / 1000000000ull);
    ts.tv_nsec = static_cast<long>(deadline_ns % 1000000000ull);
    for (;;) {
        const int rc = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr);
        if (rc == 0) {
            return true;
        }
        if (rc == EINTR) {
            if (zcipc::ShmManager::stop_requested != 0) {
                return false;
            }
            continue;
        }
        return false;
    }
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t frames = 10000;
    uint64_t fps = 60;
    zcipc::FullPolicy policy = zcipc::FullPolicy::SpinYield;
    SensorMode sensor_mode = SensorMode::Alternate;
    const char* shm_name = "/zcipc_sensor";

    static const option kOptions[] = {
        {"frames", required_argument, nullptr, 'n'},
        {"fps", required_argument, nullptr, 'r'},
        {"policy", required_argument, nullptr, 'p'},
        {"sensor", required_argument, nullptr, 's'},
        {"shm", required_argument, nullptr, 'm'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    int opt = 0;
    while ((opt = getopt_long(argc, argv, "n:r:p:s:m:h", kOptions, nullptr)) != -1) {
        switch (opt) {
            case 'n':
                if (!parse_u64(optarg, frames) || frames == 0 || frames > 1000000ull) {
                    std::fprintf(stderr, "invalid --frames\n");
                    return 2;
                }
                break;
            case 'r':
                if (!parse_u64(optarg, fps) || fps > 1000000ull) {
                    std::fprintf(stderr, "invalid --fps\n");
                    return 2;
                }
                break;
            case 'p':
                if (std::strcmp(optarg, "spin-yield") == 0) {
                    policy = zcipc::FullPolicy::SpinYield;
                } else if (std::strcmp(optarg, "drop-newest") == 0) {
                    policy = zcipc::FullPolicy::DropNewest;
                } else if (std::strcmp(optarg, "drop-oldest") == 0) {
                    policy = zcipc::FullPolicy::DropOldest;
                } else {
                    std::fprintf(stderr, "invalid --policy\n");
                    return 2;
                }
                break;
            case 's':
                if (std::strcmp(optarg, "camera") == 0) {
                    sensor_mode = SensorMode::Camera;
                } else if (std::strcmp(optarg, "lidar") == 0) {
                    sensor_mode = SensorMode::Lidar;
                } else if (std::strcmp(optarg, "alternate") == 0) {
                    sensor_mode = SensorMode::Alternate;
                } else {
                    std::fprintf(stderr, "invalid --sensor\n");
                    return 2;
                }
                break;
            case 'm':
                shm_name = optarg;
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
    if (!shm.open(shm_name, sizeof(zcipc::RingBuffer), true)) {
        std::fprintf(stderr, "producer shm_open/mmap failed: %s\n", std::strerror(errno));
        return 1;
    }

    zcipc::RingBuffer* ring = zcipc::create_ring(shm.data());
    ring->meta.policy = static_cast<uint32_t>(policy);
    zcipc::prefault_region(shm.data(), shm.size());
    const bool locked = shm.try_mlock();
    ring->ready.store(1, std::memory_order_release);

    std::fprintf(stderr,
                 "producer shm=%s bytes=%zu slots=%zu payload=%zu policy=%s fps=%" PRIu64
                 " frames=%" PRIu64 " mlock=%s write_off=%zu read_off=%zu\n",
                 shm.name(), shm.size(), zcipc::kCapacity, zcipc::kPayloadBytes,
                 zcipc::policy_name(policy), fps, frames, locked ? "yes" : "no",
                 offsetof(zcipc::RingBuffer, write_idx), offsetof(zcipc::RingBuffer, read_idx));

    const uint64_t period_ns = fps == 0 ? 0 : 1000000000ull / fps;
    const uint64_t t0 = zcipc::monotonic_ns();
    uint64_t attempted = 0;

    while (attempted < frames) {
        if (zcipc::ShmManager::stop_requested != 0) {
            break;
        }
        if (period_ns != 0 && attempted > 0) {
            if (!sleep_until(t0 + attempted * period_ns)) {
                break;
            }
            if (zcipc::ShmManager::stop_requested != 0) {
                break;
            }
        }

        const uint64_t frame_id = attempted;
        const uint32_t sensor = sensor_for(frame_id, sensor_mode);
        zcipc::Claim claim =
            zcipc::claim_slot(*ring, policy, &zcipc::ShmManager::stop_requested);
        if (claim.result == zcipc::ClaimResult::Stopped) {
            break;
        }
        if (claim.result == zcipc::ClaimResult::DroppedNewest) {
            ++attempted;
            continue;
        }

        const uint64_t fill_start = zcipc::monotonic_ns();
        zcipc::fill_synthetic(claim.slot->payload, zcipc::kPayloadBytes, frame_id, sensor);
        const uint64_t payload_ready = zcipc::monotonic_ns();
        const uint32_t checksum =
            zcipc::fnv1a32(claim.slot->payload, zcipc::kPayloadBytes);
        zcipc::commit_slot(*ring, *claim.slot, frame_id, sensor, checksum, fill_start,
                           payload_ready);
        ++attempted;
    }

    zcipc::mark_producer_done(*ring);

    const uint64_t elapsed_ns = zcipc::monotonic_ns() - t0;
    const double elapsed_s = static_cast<double>(elapsed_ns) / 1e9;
    const uint64_t published = ring->published.load(std::memory_order_relaxed);
    const uint64_t dropped_oldest = ring->dropped_oldest.load(std::memory_order_relaxed);
    const uint64_t dropped_newest = ring->dropped_newest.load(std::memory_order_relaxed);
    const double mib_s =
        elapsed_s > 0.0
            ? (static_cast<double>(published) * static_cast<double>(zcipc::kPayloadBytes)) /
                  (1024.0 * 1024.0) / elapsed_s
            : 0.0;
    const double achieved_fps = elapsed_s > 0.0 ? static_cast<double>(published) / elapsed_s : 0.0;

    std::printf("producer_frames_requested=%" PRIu64 "\n", frames);
    std::printf("producer_frames_attempted=%" PRIu64 "\n", attempted);
    std::printf("producer_published=%" PRIu64 "\n", published);
    std::printf("producer_dropped_oldest=%" PRIu64 "\n", dropped_oldest);
    std::printf("producer_dropped_newest=%" PRIu64 "\n", dropped_newest);
    std::printf("producer_elapsed_s=%.6f\n", elapsed_s);
    std::printf("producer_fps=%.3f\n", achieved_fps);
    std::printf("producer_throughput_MiB_s=%.3f\n", mib_s);
    std::printf("producer_policy=%s\n", zcipc::policy_name(policy));
    std::printf("producer_target_fps=%" PRIu64 "\n", fps);
    std::fflush(stdout);

    bool ok = false;
    if (zcipc::ShmManager::stop_requested == 0 && attempted == frames) {
        if (policy == zcipc::FullPolicy::DropNewest) {
            ok = published + dropped_newest == frames;
        } else {
            ok = published == frames;
        }
    }
    return ok ? 0 : 1;
}
