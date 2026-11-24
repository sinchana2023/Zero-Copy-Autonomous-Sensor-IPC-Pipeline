# Zero-Copy Autonomous Sensor IPC Pipeline

C++17 single-producer single-consumer pipeline that publishes 1 MiB camera and LiDAR frames through a POSIX shared-memory ring. The consumer reads each frame in place. A Unix-domain socket benchmark moves the same payload through the kernel so the two paths can be compared.

Measurements below were taken on this VM on 24 November 2026 (Pacific Time). Nothing in the tables was estimated.

## Architecture

```
sensor_producer                         sensor_consumer
  fill 1 MiB in the mapped slot            spin until write_idx advances
  FNV-1a over that slot                    observe CLOCK_MONOTONIC
  release-store write_idx        ------>   FNV-1a the same bytes in place
  (CLOCK_MONOTONIC timestamp)              release-store read_idx
```

The segment is one `mmap` of `/dev/shm/<name>` (`shm_open`, `ftruncate`, `mmap`). Layout:

| Region | Role |
| --- | --- |
| `write_idx` | Producer cursor, `std::atomic<uint64_t>`, `memory_order_release` on publish |
| `read_idx` | Consumer cursor, `memory_order_acquire` on observe, `memory_order_release` after the frame is consumed |
| drop / published counters | Own cache lines, relaxed atomics |
| 16 `SensorFrameSlot`s | `sequence` seqlock, `FrameHeader`, 1,048,576 byte payload |

`write_idx` is at offset 0 and `read_idx` is at offset 64 (`alignas(64)`). A full ring is 16,778,752 bytes. Capacity is fixed at 16, a power of two. Slots are addressed with `index & 15`.

`FrameHeader` holds `frame_id`, `sensor_type` (`camera=1`, `lidar=2`), `payload_bytes`, `fill_start_ns`, `payload_ready_ns`, `timestamp_ns`, and an FNV-1a 32-bit checksum of the payload. The checksum constant is `2166136261` with prime `16777619`.

`timestamp_ns` is taken immediately before the release store. The 1 MiB fill and the producer checksum happen before that timestamp. The consumer samples `CLOCK_MONOTONIC` as soon as the slot sequence matches, and only then checksums. **Handoff latency** is `observed_ns - timestamp_ns`. **Fill-to-observe** is `observed_ns - fill_start_ns` and includes synthetic payload generation plus the producer checksum.

Backpressure (`--policy`):

- `spin-yield` (default): bounded `_mm_pause` burst, then `sched_yield`, until a slot is free. This is the lossless path used for the 10,000-frame run.
- `drop-newest`: leave the ring unchanged and increment `dropped_newest`.
- `drop-oldest`: `compare_exchange` on `read_idx` to retire the oldest committed index, then overwrite that slot. A seqlock rejects a torn read.

`ShmManager` is RAII around `shm_open`, `ftruncate`, `mmap`, `munmap`, and `shm_unlink`. The creator unlinks on destruction and from `SIGINT`/`SIGTERM` (`shm_unlink` is async-signal-safe). The descriptor is closed after `mmap`. Histogram buffers are allocated once, before the transmit loop. The transmit loop does not call `new` or `malloc`.

`socket_bench` forks an `AF_UNIX` `SOCK_STREAM` pair. Each frame is the same 48-byte header plus 1,048,576 payload bytes (1,048,624 bytes on the wire). Transfer latency is the receiver timestamp after a full `recv` minus the timestamp stored immediately before `send`.

Percentiles are nearest-rank on the sorted sample: index `ceil(p * N) - 1`.

## Build

Requirements: CMake 3.16+, g++ with C++17, `librt`, `pthread`. On this image `/usr/bin/c++` is Clang and does not link `libstdc++`, so the project selects `g++` when `CMAKE_CXX_COMPILER` is unset.

```bash
cmake -S . -B build
cmake --build build -j
```

Flags, from the generated compile and link lines:

```text
g++ -std=c++17 -O3 -Wall -Wextra -Werror -pthread
g++ -pthread ... -lrt -lpthread
```

Binaries: `build/sensor_producer`, `build/sensor_consumer`, `build/socket_bench`.

## Run

Start the consumer first. The default shared-memory name is `/zcipc_sensor`. The default policy waits forever once 16 slots are full, so the producer does not finish without a consumer.

```bash
./build/sensor_consumer --frames 10000 --shm /zcipc_func &
./build/sensor_producer --frames 10000 --fps 60 --shm /zcipc_func

./build/socket_bench --frames 10000
```

| Flag | Meaning |
| --- | --- |
| `--frames N` | Frame count (default 10000, max 1000000) |
| `--fps N` | Producer period, `1000000000/N` nanoseconds. `0` runs as fast as the ring allows. Default 60 |
| `--policy spin-yield\|drop-newest\|drop-oldest` | Ring-full policy |
| `--sensor camera\|lidar\|alternate` | Default `alternate` |
| `--shm /name` | POSIX shm name |

`mlock` of the segment failed on this VM (`mlock=no` in the producer log). Pages are still prefaulted before `ready` is published.

## Benchmark

Host: Linux 6.12.113+ x86_64, 4 vCPU, `Intel(R) Xeon(R) Processor` under a hypervisor, g++ 13.3.0, CMake 3.28.3. `mlock` was not permitted.

### 10,000 frames at 60 FPS (shared memory)

`sensor_producer --frames 10000` (default `--fps 60`, `spin-yield`) against `sensor_consumer --frames 10000`.

| Result | Measured |
| --- | --- |
| Frames published / received | 10000 / 10000 |
| Checksum pass rate | 100% (0 bad, 0 frame-id gaps, 0 clock inversions) |
| Drops | oldest 0, newest 0 |
| Elapsed | 166.651215 s (consumer), 166.651621 s (producer) |
| Achieved rate | 60.005 FPS, 60.006 MiB/s |

That throughput is the 60 FPS pace, not the path's maximum. An unpaced shared-memory run of the same 10,000 frames (`--fps 0`) finished in 14.868257 s at 672.574 MiB/s, still with a 100% checksum pass rate and zero drops.

### Latency (10,000 frames each)

Handoff is the IPC publication. Fill-to-observe also includes generating and checksumming the 1 MiB payload. Socket transfer is the kernel copy of the 1,048,624-byte frame and does not include payload generation. Socket fill-to-observe does.

| Path | Mean | p50 | p95 | p99 | Min | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Shared memory handoff, 60 FPS | 1.159 µs | 0.331 µs | 0.649 µs | 0.984 µs | 0.076 µs | 2761.447 µs |
| Unix socket transfer, max rate | 141.487 µs | 109.308 µs | 357.832 µs | 446.548 µs | 94.829 µs | 1932.480 µs |
| Shared memory fill-to-observe, 60 FPS | 1501.066 µs | 1493.448 µs | 1544.155 µs | 1656.891 µs | 1478.597 µs | 4488.339 µs |
| Unix socket fill-to-observe, max rate | 1627.988 µs | 1594.640 µs | 1849.434 µs | 1944.008 µs | 1573.206 µs | 4258.178 µs |

Nanosecond sources: handoff mean `1159.251`, p95 `649`, p99 `984`; socket transfer mean `141487.349`, p95 `357832`, p99 `446548`; fill-to-observe p99 `1656891` (shared memory) and `1944008` (socket).

Socket/shared-memory handoff ratios from those nanosecond values: mean `141487.349/1159.251` = 122.05×, p95 `357832/649` = 551.4×, p99 `446548/984` = 453.8×.

### Latency target

The sub-250 µs p99 target and the 2×–3× reduction are met for **IPC handoff**: p99 is 0.984 µs, against 446.548 µs for the socket transfer of the same 1 MiB frame (453.8×).

They are **not** met for fill-to-observe. That interval's p99 is 1656.891 µs on the shared-memory path and 1944.008 µs on the socket path (1.17×). Both are dominated by writing and FNV-1a-hashing 1 MiB before the publish or `send`.

### Max-rate throughput

| Path | Frames | Elapsed | Payload throughput | Checksum |
| --- | ---: | ---: | ---: | --- |
| Shared memory `--fps 0` | 10000 | 14.868257 s | 672.574 MiB/s | 100% |
| Unix socket | 10000 | 16.082768 s | 621.784 MiB/s | 100% |

`672.574/621.784` = 1.08×. At maximum rate the checksum, not the publication, limits both paths. The unpaced shared-memory handoff p99 on that run was 0.596 µs (mean 1.231 µs, p95 0.476 µs).

### Backpressure

100 frames, `--fps 0`, no consumer:

| Policy | Published | `dropped_oldest` | `dropped_newest` |
| --- | ---: | ---: | ---: |
| `drop-newest` | 16 | 0 | 84 |
| `drop-oldest` | 100 | 84 | 0 |

`SIGTERM` during `spin-yield` unlinked the segment; `/dev/shm` had no `zcipc_*` name afterward.

## Profiling

### Valgrind

Producer: `./sensor_producer --frames 10000 --fps 0 --shm /zcipc_sensor`.

Consumer, matching the checklist command (defaults are 10000 frames and `/zcipc_sensor`):

```text
valgrind --leak-check=full ./sensor_consumer
```

The consumer received 10000 frames with checksum pass rate 100% and exited 0. Valgrind 3.22.0 reported:

```text
HEAP SUMMARY:
    in use at exit: 0 bytes in 0 blocks
  total heap usage: 4 allocs, 4 frees, 237,824 bytes allocated

All heap blocks were freed -- no leaks are possible

ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)
```

The four allocations are the two latency arrays and their startup overhead, all released before exit. Wall time for this valgrind consumer was 39 seconds.

### perf stat

```text
perf stat -e cycles,instructions,cache-misses -- ./sensor_producer --frames 10000
```

The producer ran the same 10,000-frame 60 FPS test (a consumer was attached). perf from `linux-tools` 6.8.0-151 was used because this kernel is `6.12.113+` and Ubuntu has no matching `linux-tools` package. Output:

```text
 Performance counter stats for '/agent/zero-copy-ipc/build/sensor_producer --frames 10000 --shm /zcipc_func':

   <not supported>      cycles:u
   <not supported>      instructions:u
   <not supported>      cache-misses:u

     166.665714578 seconds time elapsed

      14.965659000 seconds user
       0.234586000 seconds sys
```

`/proc/cpuinfo` has no `arch_perfmon` flag, and `/sys/bus/event_source/devices` has no `cpu` PMU (only `breakpoint`, `kprobe`, `msr`, `software`, `tracepoint`, `uprobe`). `perf_event_paranoid` is 2, so the events were requested as userspace (`:u`). The same three events are `<not supported>` under `sudo` without the `:u` suffix. There is no measured cache-miss count on this VM. The index separation is the compile-time layout above (`write_idx` at 0, `read_idx` at 64), not a hardware-counter result.

## Verification checklist

| Item | Result |
| --- | --- |
| 10,000 × 1 MiB at 60 FPS, 100% checksum | Pass. 60.005 FPS, pass rate 100% |
| Mean / p95 / p99 for shared memory and the socket baseline | Pass. Table above |
| `valgrind --leak-check=full` on `sensor_consumer`, 0 errors from 0 contexts | Pass. Also 0 bytes leaked |
| `perf stat -e cycles,instructions,cache-misses` | Fail. Command ran; all three counters are `<not supported>` |
| Sub-250 µs p99 IPC handoff | Pass. 0.984 µs |
| At least 2×–3× lower handoff latency than the Unix socket | Pass. p99 ratio 453.8× |
| Sub-250 µs p99 fill-to-observe | Fail. 1656.891 µs shared memory, 1944.008 µs socket |
