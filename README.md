# mmapq

[![License: BSD-2-Clause](https://img.shields.io/badge/License-BSD_2--Clause-blue.svg)](LICENSE)

**Safe, idiomatic Rust bindings for [mmapq](https://github.com/anton-anufriev/mmapq)** — a persistent,
indexed, replayable inter-process queue backed by memory-mapped files. No
broker, no daemon, no garbage collector; the queue is just files.

mmapq passes messages between processes on the same Linux machine with
sub-microsecond latency and allocation-free hot paths. Unlike a transport,
the queue is durable state: every message has a stable index, any process
can read at any index, and a consumer that restarts resumes exactly where
it stopped. These bindings add a thin, zero-overhead safety layer over the
C library — the guarantees below are enforced by Rust's type system, not by
convention.

This library was extracted from production trading infrastructure, where
its Java ancestor carried FX flows for years. The design target is the
storage layer for **event-sourced systems**: sequenced command queues,
replicated event logs, and deterministic replay. For that architecture, see
`doc/event-sourcing-guide.md` in the [mmapq](https://github.com/anton-anufriev/mmapq)
repository; for exact feature coverage across C, Rust, and Python, see
`doc/feature-matrix.md`.

## What the bindings guarantee

The reason to use these bindings rather than calling the C library through
raw FFI is that they make the C contract's sharp edges unrepresentable in
safe code:

- **MmapqRuntime lifetime is enforced.** The C queue stores raw runtime pointers
  without owning them. These bindings make `MmapqIndexedQueue` and every
  participant hold a reference to the `MmapqRuntime`, so a runtime cannot be
  dropped while a queue or poller still uses it. A use-after-free that is
  reachable from safe C is a compile-time impossibility here.
- **Thread confinement is typed.** `MmapqIndexedQueue`, `QueueConfig`, and
  `MmapqRuntime` are `Send + Sync` (create the queue anywhere, share the
  factory). Participants — `MmapqIndexedAppender`, `MmapqIndexedPoller`,
  `MmapqIndexedReader`, `MmapqIndexedArchiver` — are `!Send + !Sync`: each owns a
  position or cursor and must stay on one thread. The compiler enforces
  this; misuse does not build.
- **Batch commit is a transaction boundary in the type system.**
  `BatchWriter::commit(self)` consumes the writer — a committed batch cannot
  be touched again. Dropping a `BatchWriter` without committing rolls the
  batch back. In an event-sourced processor, an early return or `?` on an
  error path *is* a rollback: the partial batch never becomes visible.
- **Reads cannot alias.** `MmapqIndexedReader::read(&mut self)` takes a mutable
  borrow, so the returned message borrow blocks a second read at compile
  time — the "flyweight overwritten under a live reference" bug that a
  `&self` API would allow cannot be written.
- **Poll closures are panic-safe.** A panic inside a poll handler is caught
  at the FFI boundary and re-raised after the C stack frame unwinds cleanly,
  so a panicking closure cannot leave the C library in a torn state or
  unwind across the FFI boundary (which would be undefined behavior).
- **Errors are typed, not sentinels.** C return codes map to `MmapError`
  variants — `InvalidArgument`, `QueueClosed`, `Corrupt`, `SchemaMismatch`,
  `MapTimedOut` — so "the data is damaged" (`Corrupt`) is distinguishable
  from "retry" (`MapTimedOut`) at the call site instead of being one
  overloaded `-1`.

None of this costs anything at runtime: the safety is in the types, and the
generated code is the same FFI calls you would write by hand.

## Quick start

```rust
use mmapq::{MmapqIndexedQueue, MmapqNextMove, QueueConfig, MmapqRuntime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = MmapqRuntime::region_mapper()?;
    let q = MmapqIndexedQueue::create(
        QueueConfig::new("./data", "my_queue")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )?;

    let mut appender = q.create_appender()?;
    appender.append(b"hello world")?;

    let mut poller = q.create_poller()?;
    poller.poll(|_index, _appender_id, data| {
        println!("Received: {:?}", std::str::from_utf8(data));
        MmapqNextMove::Advance
    })?;

    Ok(())
}
```

## Batch queues — the transaction pattern

A batch queue commits N messages atomically: consumers never observe a
partial batch, and a crash mid-batch discards the uncommitted tail. This is
the event-store half of the command/event architecture (one command → one
committed batch). The RAII rollback is the point:

```ignore
let mut batch = appender.start_batch()?;
batch.append(b"event 1")?;
batch.append(b"event 2")?;
// If anything above returned early via `?`, `batch` drops here and rolls
// back — the events never become visible. Only an explicit commit publishes:
batch.commit()?;   // consumes `batch`; the batch is now atomically visible
```

## Cross-language and cross-process

The wire format is identical to the C library and to Python (`mmapq-py`),
and compatible with Algoteq's Java implementation (tools4j-derived; **not**
compatible with upstream tools4j — the formats have diverged). A Rust
process can produce a queue a C or Python process consumes, and vice versa.
The interop test drives a real C echo server:

```bash
cargo test --test cross_process
```

## Thread-safety reference

| Type | `Send` | `Sync` | Notes |
|---|---|---|---|
| `MmapqRuntime` | yes | yes | Background mapper thread; mutex-guarded registry |
| `MmapqIndexedQueue` | yes | yes | Factory; re-entrant participant creation |
| `QueueConfig` | yes | yes | Plain builder data |
| `MmapqIndexedAppender` | no | no | Owns a payload write position |
| `MmapqIndexedPoller` | no | no | Owns a read cursor |
| `MmapqIndexedReader` | no | no | Owns lazy region accessors |
| `MmapqIndexedArchiver` | no | no | Scans metadata |

## Performance

The C library achieves ~280 ns median round-trip between two processes on
isolated cores. The Rust wrapper adds roughly **~10 ns** per round-trip
(one FFI call, closure dispatch, slice construction) — the safety layer is
free in practice.

Quick verification (completes in seconds):

```bash
rm -rf /tmp/request /tmp/response
sleep 3
cargo run --release --example latency_bench -- 20000000 1000 64 --sync --oneline
# BENCH: iterations=20000000 mean=337.9 min=177 p50=247 p90=293 p99=371 p99.9=1871 p99.99=9473 p99.999=4396458
```

Reproduce the full benchmark on isolated cores (`isolcpus=5,7,9`):

```bash
rm -rf /tmp/request /tmp/response
sleep 3
cargo run --release --example latency_bench -- 20000000 1000 64 5 7 9 --oneline
# BENCH: iterations=20000000 mean=278.4 min=173 p50=268 p90=325 p99=380 p99.9=1808 p99.99=6992 p99.999=15036
```

Batch queue benchmark (same options):

```bash
rm -rf /tmp/request /tmp/response
sleep 3
cargo run --release --example batch_latency_bench -- 20000000 1000 64 --sync --oneline
# BENCH: iterations=20000000 mean=387.5 min=213 p50=298 p90=344 p99=429 p99.9=1760 p99.99=9914 p99.999=4295193
```

Reproduce the full benchmark on isolated cores (`isolcpus=5,7,9`):

```bash
rm -rf /tmp/request /tmp/response
sleep 3
cargo run --release --example batch_latency_bench -- 20000000 1000 64 5 7 9 --oneline
# BENCH: iterations=20000000 mean=404.0 min=221 p50=312 p90=353 p99=456 p99.9=1687 p99.99=8802 p99.999=4501173
```

The full suite (`./scripts/bench.sh`, `./scripts/batch_bench.sh`) runs
internal, external-async, and external-sync configurations for both queue
types.

## When to use it — and when not to

**Use mmapq when** you need same-host inter-process messaging where history
matters: event-sourced services, sequenced command/order flow, market-data
or telemetry capture, replay-based testing, or a migration bridge between a
Java system and Rust/C/Python processes sharing one queue.

**Use something else when** you need network transport (mmapq is
same-machine only), pub/sub over many dynamic topics (mmapq queues are
named persistent streams), guaranteed durability through power loss (writes
reach the page cache, not the platter — long-term data belongs in the
export format), or a non-Linux platform.

## Architecture

The bindings wrap the C library's two-file queue layout: one header file per
queue (8-byte atomically-committed entries spaced 512 bytes apart via vertical walk, encoding
appender ID + payload offset in a 64-bit word) and one payload file per
appender (`[length prefix][payload]` framing). The cache-line spacing keeps
concurrent appenders off each other's cache lines. Full derivation is in
`doc/header_layout_design.md` in the mmapq repository.

## Stability

The wire format and the feature set are **frozen**: queue files written
today remain readable by every future version, and the API grows only to
complete parity with the Algoteq Java reference. This is a design position —
the library you audit today is the library you run in five years.

## Acknowledgements

`mmapq-rs` wraps Algoteq's mmapq, originally derived from the
[tools4j/mmap](https://github.com/tools4j/mmap) library. The event-sourcing
architecture follows [tools4j/event-sourcing](https://github.com/tools4j/event-sourcing)
and [elara](https://github.com/tools4j/elara).

## License

BSD 2-Clause — see [LICENSE](LICENSE).

## Platform

Linux only (x86-64, ARM64). Requires naturally aligned lock-free atomics
over `MAP_SHARED` mmap.
