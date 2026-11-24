#pragma once

// Lock-free single-producer single-consumer ring resident in POSIX shared memory.
// Producer and consumer cursors are std::atomic<uint64_t> on separate cache lines.
// Publication is release; observation is acquire. Payload bytes are written in place.

#include <algorithm>
#include <atomic>
#include <cmath>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <cinttypes>
#include <new>
#include <sched.h>
#include <type_traits>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace zcipc {

inline constexpr uint32_t kMagic = 0x3143465Au;  // 'ZFC1'
inline constexpr uint32_t kVersion = 1;
inline constexpr std::size_t kPayloadBytes = 1u << 20;  // 1 MiB sensor payload
inline constexpr std::size_t kCapacity = 16;            // power of two
inline constexpr std::size_t kCacheLine = 64;

enum class SensorType : uint32_t { Camera = 1, Lidar = 2 };

enum class FullPolicy : uint32_t {
    SpinYield = 0,
    DropNewest = 1,
    DropOldest = 2,
};

enum class ClaimResult : uint32_t { Ready = 0, DroppedNewest = 1, Stopped = 2 };

enum class Poll : uint32_t { Empty = 0, Retry = 1, Frame = 2 };

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    asm volatile("yield" ::: "memory");
#endif
}

inline uint64_t monotonic_ns() noexcept {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
           static_cast<uint64_t>(ts.tv_nsec);
}

// FNV-1a 32-bit over an in-place payload. No table, no heap.
inline uint32_t fnv1a32(const uint8_t* data, std::size_t n) noexcept {
    uint32_t hash = 2166136261u;
    for (std::size_t i = 0; i < n; ++i) {
        hash ^= data[i];
        hash *= 16777619u;
    }
    return hash;
}

// Deterministic synthetic camera/LiDAR bytes. Writes only into caller storage.
inline void fill_synthetic(uint8_t* dst, std::size_t n, uint64_t frame_id,
                           uint32_t sensor_type) noexcept {
    uint64_t x = 0x9E3779B97F4A7C15ull ^ (frame_id + 1ull) * 0xBF58476D1CE4E5B9ull ^
                 (static_cast<uint64_t>(sensor_type) << 17);
    std::size_t i = 0;
    for (; i + sizeof(uint64_t) <= n; i += sizeof(uint64_t)) {
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9ull;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBull;
        x ^= x >> 31;
        std::memcpy(dst + i, &x, sizeof(x));
    }
    while (i < n) {
        x ^= x >> 12;
        x *= 0x9E3779B185EBCA87ull;
        dst[i++] = static_cast<uint8_t>(x);
    }
}

inline void prefault_region(void* mem, std::size_t n) noexcept {
    volatile uint8_t* bytes = static_cast<volatile uint8_t*>(mem);
    for (std::size_t i = 0; i < n; i += 4096) {
        bytes[i] = bytes[i];
    }
}

struct FrameHeader {
    uint64_t frame_id;
    uint32_t sensor_type;
    uint32_t payload_bytes;
    uint64_t fill_start_ns;
    uint64_t payload_ready_ns;
    uint64_t timestamp_ns;
    uint32_t checksum;
    uint32_t reserved;
};

struct alignas(kCacheLine) SensorFrameSlot {
    std::atomic<uint64_t> sequence;
    FrameHeader header;
    alignas(kCacheLine) uint8_t payload[kPayloadBytes];
};

struct alignas(kCacheLine) RingMeta {
    uint32_t magic;
    uint32_t version;
    uint32_t capacity;
    uint32_t payload_bytes;
    uint32_t policy;
    uint32_t reserved;
    uint64_t session_ns;
};

// Shared-memory image. Do not place this object on the stack: the slot array
// holds kCapacity 1 MiB payloads.
struct RingBuffer {
    alignas(kCacheLine) std::atomic<uint64_t> write_idx;
    alignas(kCacheLine) std::atomic<uint64_t> read_idx;
    alignas(kCacheLine) std::atomic<uint64_t> dropped_oldest;
    alignas(kCacheLine) std::atomic<uint64_t> dropped_newest;
    alignas(kCacheLine) std::atomic<uint64_t> published;
    alignas(kCacheLine) std::atomic<uint32_t> producer_done;
    alignas(kCacheLine) std::atomic<uint32_t> ready;
    alignas(kCacheLine) RingMeta meta;
    SensorFrameSlot slots[kCapacity];

    // User-provided so value-initialization does not zero the payload array.
    RingBuffer() noexcept {}

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
};

static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "uint64_t atomics must be lock-free across processes");
static_assert(std::atomic<uint32_t>::is_always_lock_free,
              "uint32_t atomics must be lock-free across processes");
static_assert((kCapacity & (kCapacity - 1)) == 0, "capacity must be a power of two");
static_assert(sizeof(FrameHeader) == 48, "FrameHeader layout changed");
static_assert(std::is_trivially_copyable<FrameHeader>::value, "header must be trivially copyable");
static_assert(alignof(RingBuffer) >= kCacheLine, "ring alignment");
static_assert(offsetof(RingBuffer, write_idx) % kCacheLine == 0, "write_idx cache line");
static_assert(offsetof(RingBuffer, read_idx) % kCacheLine == 0, "read_idx cache line");
static_assert(offsetof(RingBuffer, read_idx) >= offsetof(RingBuffer, write_idx) + kCacheLine,
              "producer and consumer indices share a cache line");
static_assert(offsetof(RingBuffer, dropped_oldest) >= offsetof(RingBuffer, read_idx) + kCacheLine,
              "drop counter shares the consumer index line");
static_assert(sizeof(RingBuffer) > kCapacity * kPayloadBytes, "payload storage missing");

struct Claim {
    ClaimResult result;
    SensorFrameSlot* slot;
};

struct ConsumedFrame {
    uint64_t frame_id;
    uint32_t sensor_type;
    uint64_t fill_start_ns;
    uint64_t payload_ready_ns;
    uint64_t timestamp_ns;
    uint64_t observed_ns;
    uint32_t checksum;
    uint32_t computed;
    bool checksum_ok;
};

inline RingBuffer* create_ring(void* mem) noexcept {
    auto* ring = new (mem) RingBuffer();
    ring->write_idx.store(0, std::memory_order_relaxed);
    ring->read_idx.store(0, std::memory_order_relaxed);
    ring->dropped_oldest.store(0, std::memory_order_relaxed);
    ring->dropped_newest.store(0, std::memory_order_relaxed);
    ring->published.store(0, std::memory_order_relaxed);
    ring->producer_done.store(0, std::memory_order_relaxed);
    ring->ready.store(0, std::memory_order_relaxed);
    ring->meta.magic = kMagic;
    ring->meta.version = kVersion;
    ring->meta.capacity = static_cast<uint32_t>(kCapacity);
    ring->meta.payload_bytes = static_cast<uint32_t>(kPayloadBytes);
    ring->meta.policy = static_cast<uint32_t>(FullPolicy::SpinYield);
    ring->meta.reserved = 0;
    ring->meta.session_ns = monotonic_ns();
    for (std::size_t i = 0; i < kCapacity; ++i) {
        ring->slots[i].sequence.store(0, std::memory_order_relaxed);
        ring->slots[i].header = FrameHeader{};
    }
    // Caller prefaults, then release-stores ready so the consumer cannot
    // observe a half-initialized segment.
    return ring;
}

inline RingBuffer* view_ring(void* mem) noexcept {
    return static_cast<RingBuffer*>(mem);
}

inline bool ring_header_ok(const RingBuffer& ring) noexcept {
    return ring.meta.magic == kMagic && ring.meta.version == kVersion &&
           ring.meta.capacity == kCapacity && ring.meta.payload_bytes == kPayloadBytes;
}

// Block (spin then yield) until there is a free slot, drop the new frame, or
// drop the oldest committed frame by advancing the consumer cursor.
// `stop` is the signal flag; it is only read.
inline Claim claim_slot(RingBuffer& ring, FullPolicy policy,
                        const volatile std::sig_atomic_t* stop) noexcept {
    const uint64_t w = ring.write_idx.load(std::memory_order_relaxed);
    unsigned pauses = 0;
    for (;;) {
        if (stop != nullptr && *stop != 0) {
            return Claim{ClaimResult::Stopped, nullptr};
        }
        uint64_t r = ring.read_idx.load(std::memory_order_acquire);
        if (w - r < kCapacity) {
            break;
        }
        if (policy == FullPolicy::DropNewest) {
            ring.dropped_newest.fetch_add(1, std::memory_order_relaxed);
            return Claim{ClaimResult::DroppedNewest, nullptr};
        }
        if (policy == FullPolicy::DropOldest) {
            if (ring.read_idx.compare_exchange_weak(r, r + 1, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
                ring.dropped_oldest.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            continue;
        }
        // Lossless replay: each busy burst is bounded, then the producer yields.
        if (pauses < 64) {
            cpu_relax();
            ++pauses;
        } else {
            sched_yield();
            pauses = 0;
        }
    }

    SensorFrameSlot& slot = ring.slots[w & (kCapacity - 1)];
    // Seqlock: readers observe sequence == 0 while this slot is being overwritten.
    slot.sequence.store(0, std::memory_order_release);
    return Claim{ClaimResult::Ready, &slot};
}

inline void commit_slot(RingBuffer& ring, SensorFrameSlot& slot, uint64_t frame_id,
                        uint32_t sensor_type, uint32_t checksum, uint64_t fill_start_ns,
                        uint64_t payload_ready_ns) noexcept {
    const uint64_t w = ring.write_idx.load(std::memory_order_relaxed);
    slot.header.frame_id = frame_id;
    slot.header.sensor_type = sensor_type;
    slot.header.payload_bytes = static_cast<uint32_t>(kPayloadBytes);
    slot.header.fill_start_ns = fill_start_ns;
    slot.header.payload_ready_ns = payload_ready_ns;
    slot.header.checksum = checksum;
    slot.header.reserved = 0;
    // Handoff timestamp: immediately before the release publication.
    slot.header.timestamp_ns = monotonic_ns();
    slot.sequence.store(w + 1, std::memory_order_release);
    ring.write_idx.store(w + 1, std::memory_order_release);
    ring.published.fetch_add(1, std::memory_order_relaxed);
}

inline void mark_producer_done(RingBuffer& ring) noexcept {
    ring.producer_done.store(1, std::memory_order_release);
}

inline Poll poll_frame(RingBuffer& ring, ConsumedFrame& out) noexcept {
    const uint64_t r = ring.read_idx.load(std::memory_order_relaxed);
    const uint64_t w = ring.write_idx.load(std::memory_order_acquire);
    if (r >= w) {
        return Poll::Empty;
    }
    const uint64_t r_now = ring.read_idx.load(std::memory_order_relaxed);
    if (r_now != r) {
        return Poll::Retry;
    }

    SensorFrameSlot& slot = ring.slots[r & (kCapacity - 1)];
    const uint64_t seq1 = slot.sequence.load(std::memory_order_acquire);
    if (seq1 != r + 1) {
        return Poll::Retry;
    }

    const uint64_t observed = monotonic_ns();
    const FrameHeader header = slot.header;
    const uint32_t computed = fnv1a32(slot.payload, kPayloadBytes);
    const uint64_t seq2 = slot.sequence.load(std::memory_order_acquire);
    if (seq2 != seq1) {
        return Poll::Retry;
    }

    uint64_t expected = r;
    if (!ring.read_idx.compare_exchange_strong(expected, r + 1, std::memory_order_release,
                                               std::memory_order_relaxed)) {
        return Poll::Retry;
    }

    out.frame_id = header.frame_id;
    out.sensor_type = header.sensor_type;
    out.fill_start_ns = header.fill_start_ns;
    out.payload_ready_ns = header.payload_ready_ns;
    out.timestamp_ns = header.timestamp_ns;
    out.observed_ns = observed;
    out.checksum = header.checksum;
    out.computed = computed;
    out.checksum_ok = (computed == header.checksum && header.payload_bytes == kPayloadBytes);
    return Poll::Frame;
}

struct LatencyStats {
    uint64_t count = 0;
    double mean_ns = 0;
    uint64_t p50_ns = 0;
    uint64_t p95_ns = 0;
    uint64_t p99_ns = 0;
    uint64_t min_ns = 0;
    uint64_t max_ns = 0;
};

// Nearest-rank percentile: index ceil(p * N) - 1 on the sorted sample.
inline LatencyStats summarize_latencies(uint64_t* samples, uint64_t n) noexcept {
    LatencyStats stats;
    stats.count = n;
    if (n == 0) {
        return stats;
    }
    long double sum = 0;
    uint64_t lo = samples[0];
    uint64_t hi = samples[0];
    for (uint64_t i = 0; i < n; ++i) {
        sum += static_cast<long double>(samples[i]);
        if (samples[i] < lo) {
            lo = samples[i];
        }
        if (samples[i] > hi) {
            hi = samples[i];
        }
    }
    std::sort(samples, samples + n);
    auto rank = [samples, n](double p) noexcept -> uint64_t {
        double scaled = std::ceil(p * static_cast<double>(n));
        uint64_t idx = scaled <= 1.0 ? 0 : static_cast<uint64_t>(scaled) - 1;
        if (idx >= n) {
            idx = n - 1;
        }
        return samples[idx];
    };
    stats.mean_ns = static_cast<double>(sum / static_cast<long double>(n));
    stats.p50_ns = rank(0.50);
    stats.p95_ns = rank(0.95);
    stats.p99_ns = rank(0.99);
    stats.min_ns = lo;
    stats.max_ns = hi;
    return stats;
}

inline const char* policy_name(FullPolicy policy) noexcept {
    switch (policy) {
        case FullPolicy::SpinYield:
            return "spin-yield";
        case FullPolicy::DropNewest:
            return "drop-newest";
        case FullPolicy::DropOldest:
            return "drop-oldest";
    }
    return "unknown";
}

inline const char* sensor_name(uint32_t sensor_type) noexcept {
    if (sensor_type == static_cast<uint32_t>(SensorType::Camera)) {
        return "camera";
    }
    if (sensor_type == static_cast<uint32_t>(SensorType::Lidar)) {
        return "lidar";
    }
    return "unknown";
}

inline void print_latency_stats(const char* name, const LatencyStats& stats) noexcept {
    std::printf("%s_count=%" PRIu64 "\n", name, stats.count);
    std::printf("%s_mean_ns=%.3f\n", name, stats.mean_ns);
    std::printf("%s_p50_ns=%" PRIu64 "\n", name, stats.p50_ns);
    std::printf("%s_p95_ns=%" PRIu64 "\n", name, stats.p95_ns);
    std::printf("%s_p99_ns=%" PRIu64 "\n", name, stats.p99_ns);
    std::printf("%s_min_ns=%" PRIu64 "\n", name, stats.min_ns);
    std::printf("%s_max_ns=%" PRIu64 "\n", name, stats.max_ns);
    std::printf("%s_mean_us=%.3f\n", name, stats.mean_ns / 1000.0);
    std::printf("%s_p50_us=%.3f\n", name, static_cast<double>(stats.p50_ns) / 1000.0);
    std::printf("%s_p95_us=%.3f\n", name, static_cast<double>(stats.p95_ns) / 1000.0);
    std::printf("%s_p99_us=%.3f\n", name, static_cast<double>(stats.p99_ns) / 1000.0);
}

}  // namespace zcipc
