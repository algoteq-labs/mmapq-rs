# mmap-c-rs

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Idiomatic Rust bindings for [mmap-c](https://github.com/algoteq-labs/mmap-c)** — a low-latency,
memory-mapped, shared-memory indexed queue for inter-process messaging on Linux.

## Features

- **Zero-copy** — messages are read directly from mapped memory, no heap allocation on the hot path
- **Lock-free** — multi-producer, multi-consumer (up to 256 concurrent appenders) via atomic CAS on cache-line-spaced headers
- **Cross-process** — share queues between Rust, C, and Java processes on the same machine
- **Thread-safe factory** — `IndexedQueue` is `Send + Sync`, participants are `!Send + !Sync`
- **RAII lifecycle** — `BatchWriter::commit()` consumes `self`, drop rolls back
- **Panic-safe** — poll closure panics are caught and re-raised after the C frame unwinds
- **C-ABI compatible** — identical wire format and protocol as the Java `tools4j/mmap` library

## Quick Start

```rust
use mmap_c_rs::{IndexedQueue, NextMove, PollResult, QueueConfig, Runtime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = Runtime::region_mapper()?;
    let q = IndexedQueue::create(
        QueueConfig::new("/dev/shm", "my_queue")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )?;

    // Writer thread
    let mut appender = q.create_appender()?;
    appender.append(b"hello world")?;

    // Reader thread
    let mut poller = q.create_poller()?;
    poller.poll(|_index, _appender_id, data| {
        println!("Received: {:?}", std::str::from_utf8(data));
        NextMove::Advance
    })?;

    Ok(())
}
```

## Architecture

mmap-c-rs wraps the C library's shared-memory queue. The queue stores messages in
two file types:

- **Header file**: one per queue — stores 8-byte atomically-committed header entries
  spaced 512 bytes apart (vertical walk layout). Each header encodes an appender ID
  and a payload offset in a single 64-bit word.
- **Payload files**: one per appender — stores [length prefix][payload bytes] framing.

The vertical walk layout (index N is at offset N × 512) ensures that consecutive
writes by multiple appenders land on separate cache lines, avoiding false sharing.

### Thread safety

| Type | Send | Sync | Notes |
|---|---|---|---|
| `Runtime` | ✅ | ✅ | Background thread with mutex-guarded registry |
| `IndexedQueue` | ✅ | ✅ | Config holder with re-entrant factory methods |
| `QueueConfig` | ✅ | ✅ | Plain builder data |
| `IndexedAppender` | ❌ | ❌ | One per thread, owns payload file position |
| `IndexedPoller` | ❌ | ❌ | One per thread, owns cursor position |
| `IndexedReader` | ❌ | ❌ | One per thread, owns lazy accessors |
| `IndexedArchiver` | ❌ | ❌ | One per thread, scans metadata |

## Examples

Single-message round-trip latency benchmark:

```bash
# Internal mode (same-process server thread)
cargo run --release --example latency_bench -- 100000 10000 64

# External mode (separate echo server process)
cargo run --release --example echo_server -- /tmp request response &
cargo run --release --example latency_bench -- 100000 10000 64 --external
```

Batch queue equivalent:

```bash
cargo run --release --example batch_latency_bench -- 100000 10000 64
cargo run --release --example batch_echo_server -- /tmp batch_req batch_res &
cargo run --release --example batch_latency_bench -- 100000 10000 64 --external
```

Machine-readable output for regression tracking:

```bash
cargo run --release --example latency_bench -- 100000 10000 64 --sync --bench
# Output: BENCH: iterations=100000 mean=56800.0 p50=54300 p99=109000 min=350 max=1100000
```

CPU pinning (requires isolated cores):

```bash
cargo run --release --example latency_bench -- 2000000 100000 64 5 7 9
#                                              iterations warmup size client server mapper
```

### Cross-process interop with C

The Rust bindings are fully compatible with the C echo server. The cross-process
test verifies this — the C echo server is built automatically:

```bash
cargo test --test cross_process
```

## Performance

The C mmap-c library achieves its published baseline of ~280 ns median round-trip
between two processes on isolated cores. The Rust wrapper adds approximately
**~10 ns overhead** per round-trip (FFI call, closure dispatch, slice construction).

To reproduce the full benchmark on isolated cores:

```bash
# C baseline (from the mmap-c repository)
./latency_bench 20000000 100000 64 5 7 9

# Rust equivalent
cargo run --release --example latency_bench -- 20000000 100000 64 5 7 9
```

Without CPU isolation, expect P50 around 50–60 µs due to scheduling noise and cache
migration. The measured minimum (min) value is the best indicator of raw path latency.

## Acknowledgements

`mmap-c-rs` is a Rust port of [Algoteq's mmap-c](https://github.com/algoteq-labs/mmap-c),
which was originally derived from the [tools4j/mmap](https://github.com/tools4j/mmap) library.

## License

MIT — see [LICENSE](LICENSE).

## Platform

Linux only. Requires naturally aligned lock-free atomics over `MAP_SHARED`
mmap — guaranteed on x86-64 and ARM64.
