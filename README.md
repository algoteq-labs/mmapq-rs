# mmapq

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](LICENSE)

**Rust bindings for [mmapq](https://github.com/algoteq-labs/mmapq)** — a persistent,
indexed, replayable inter-process queue backed by memory-mapped files. No
broker, no daemon, no garbage collector; the queue is just files.

mmapq passes messages between processes on the same Linux machine with
sub-microsecond latency and allocation-free hot paths. Unlike a transport,
the queue retains file-backed history: every message has a stable index,
and a consumer can resume from an index saved by its application. These
bindings wrap the C library; the zero-copy limitations below require care
in the current release.

The design target is the
storage layer for **event-sourced systems**: sequenced command queues,
replicated event logs, and deterministic replay. For that architecture, see
`doc/guide/event-sourcing.md` in the [mmapq](https://github.com/algoteq-labs/mmapq)
repository; for exact feature coverage across C, Rust, and Python, see
`doc/reference/feature-matrix.md`.

## Ownership and safety

The bindings provide ownership and error handling around the C API:

- **Runtime lifetime is enforced.** The C queue stores raw runtime pointers
  without owning them. These bindings make `IndexedQueue` and every
  participant hold a reference to the `Runtime`, so a runtime cannot be
  dropped while a queue or poller still uses it. A use-after-free that is
  reachable from safe C is a compile-time impossibility here.
- **Thread confinement is typed.** `IndexedQueue`, `QueueConfig`, and
  `Runtime` are `Send + Sync` (create the queue anywhere, share the
  factory). Participants — `IndexedAppender`, `IndexedPoller`,
  `IndexedReader`, `IndexedArchiver` — are `!Send + !Sync`: each owns a
  position or cursor and must stay on one thread. The compiler enforces
  this; misuse does not build.
- **Batch commit is a transaction boundary in the type system.**
  `BatchWriter::commit(self)` consumes the writer — a committed batch cannot
  be touched again. Dropping a `BatchWriter` without committing rolls the
  batch back. In an event-sourced processor, an early return or `?` on an
  error path *is* a rollback: the partial batch never becomes visible.
- **The API enforces zero-copy borrowing.** Reads and
  transfers borrow readers exclusively. A message must be processed before
  another read. Batch cursors use a lending `next()`, not standard
  `Iterator`: process each payload before advancing. No implicit payload
  copies, heap ownership, locks, or runtime borrow checks are introduced.
  This is source-breaking relative to 0.1.0, the previous release; see
  [release notes](RELEASE-NOTES-0.2.0.md).
- **Poll closures are panic-safe.** A panic inside a poll handler is caught
  at the FFI boundary and re-raised after the C stack frame unwinds cleanly,
  so a panicking closure cannot leave the C library in a torn state or
  unwind across the FFI boundary (which would be undefined behavior).
- **Errors are typed, not sentinels.** C return codes map to `MmapError`
  variants — `InvalidArgument`, `QueueClosed`, `Corrupt`, `SchemaMismatch`,
  `MapTimedOut` — so "the data is damaged" (`Corrupt`) is distinguishable
  from "retry" (`MapTimedOut`) at the call site instead of being one
  overloaded `-1`.
- **Read outcomes are typed.** `read`, `read_first` and `read_last` on
  `IndexedReader` and `IndexedBatchReader` return a `#[must_use]`
  `ReadResult`: `Available(message)`, `EndOfStream` (not yet written —
  it may appear later) or `Archived` (below the queue's start index —
  it will not). Failures stay in `Err(MmapError)`.

None of this costs anything at runtime: the safety is in the types, and the
generated code is the same FFI calls you would write by hand.

## Quick start

To build against this repository rather than a published release, clone it
with its C submodule — `git clone --recurse-submodules
https://github.com/algoteq-labs/mmapq-rs` — and depend on the checkout by
path in your application's `Cargo.toml`: `mmapq = { path = "../mmapq-rs" }`
(the crate is named `mmapq`; its other dependencies come from crates.io).

Use a persistent filesystem for `./data`. `/dev/shm` is volatile, and `/tmp`
may be cleared on reboot. Commit makes messages visible; it does not flush
them to stable storage or guarantee power-loss durability. Save consumer
positions in application state and seek explicitly when resuming.

```rust
use mmapq::{IndexedQueue, NextMove, QueueConfig, Runtime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("./data")?; // the base directory must exist
    let rt = Runtime::region_mapper()?;
    let q = IndexedQueue::create(
        QueueConfig::new("./data", "my_queue")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )?;

    let mut appender = q.create_appender()?;
    let index = appender.append(b"hello world")?;

    let mut poller = q.create_poller()?;
    poller.move_to_index(index);
    poller.poll(|_index, _appender_id, data| {
        println!("Received: {:?}", std::str::from_utf8(data));
        NextMove::Advance
    })?;

    Ok(())
}
```

## Re-sequenced commits

A writer can store each payload when it arrives and publish later, in its
own order: `append_uncommitted()` writes the payload invisibly and returns
its offset, and `commit(offset)` assigns the next index to it. Commit in
sequence order and index order is sequence order, whatever order the
payloads arrived in (the C repository's guide, chapter 4).

```rust
use mmapq::{IndexedQueue, QueueConfig, Runtime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("./data")?; // the base directory must exist
    let rt = Runtime::region_mapper()?;
    let q = IndexedQueue::create(
        QueueConfig::new("./data", "my_queue")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )?;
    let mut appender = q.create_appender_with_id(0)?; // this writer's fixed lane

    // Packets arrive out of order: write each payload as it arrives…
    let late = appender.append_uncommitted(b"seq 2")?;
    let early = appender.append_uncommitted(b"seq 1")?;
    // …and commit in sequence order: commit assigns the next index.
    let i1 = appender.commit(early)?;
    let i2 = appender.commit(late)?;
    assert_eq!(i2, i1 + 1);
    Ok(())
}
```

## Batch queues — the transaction pattern

A batch queue commits N messages atomically: consumers never observe a
partial batch. A crash mid-batch leaves the interrupted batch invisible to
consumers; the restarted writer finds it again on recovery and decides what
to do with it (see "Recovery — uncommitted messages and batches" below). This is
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

Consuming a batch queue: `create_poller()` on an `IndexedBatchQueue`
returns the batch poller; its closure gets the batch index and a lending
iterator over the batch's messages.

```rust
use mmapq::{IndexedBatchQueue, NextMove, QueueConfig, Runtime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("./data")?; // the base directory must exist
    let rt = Runtime::region_mapper()?;
    let q = IndexedBatchQueue::create(
        QueueConfig::new("./data", "my_batches")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )?;

    let mut appender = q.create_appender()?;
    let mut batch = appender.start_batch()?;
    batch.append(b"event 1")?;
    batch.append(b"event 2")?;
    batch.commit()?;

    let mut poller = q.create_poller()?;
    poller.poll(|batch_index, mut messages| {
        loop {
            match messages.next() {
                // the payload is borrowed until the next step: copy what you keep
                Ok(Some((data, appender_id))) => {
                    println!("batch {batch_index}, lane {appender_id}: {:?}", data)
                }
                Ok(None) => return NextMove::Advance, // the whole batch was read
                Err(e) => {
                    eprintln!("batch {batch_index}: {e}");
                    return NextMove::Stop; // hold the cursor; poll again to retry
                }
            }
        }
    })?;
    Ok(())
}
```

## Recovery — uncommitted messages and batches

After a restart, messages appended but never committed (and batches staged
or discarded before a crash) are recoverable by the rejoining lane owner.
The iterators snapshot at call time; the `&mut` borrow forbids appending
while iterating — the compiler enforces it. Commit found offsets with
`commit()` / `commit_batch()` — any subset, any order; skipping is fine.
Archiving may remove messages the walk has not yielded, and discarded
batches reappear in recovery — recovery code decides.

After a crash, recover through the same lane. A writer that owns a fixed
lane ID rejoins it with `create_appender_with_id(id)`; `create_appender()`
takes a free lane, and its walk finds nothing of the crashed writer's. A
writer killed without closing leaves its ID taken (`IdConflict`) until an
operator who has established that the owner is dead releases it with the
`mmapq-ids` tool from the [mmapq](https://github.com/algoteq-labs/mmapq)
repository (from its root: `make mmapq-ids`, then
`tools/mmapq-ids/build/release/mmapq-ids <dir> <queue> --unset <id>` — the
tool is not installed on your PATH); `doc/guide/05-crash-and-restart.md`
there gives the procedure. A writer that closed cleanly released its ID.

After a crash the queue keeps every record's framing, not its body: a
record being written when the crash happened keeps its full length, but its
data may be partly written, and the library cannot tell it from a complete
record. Validate each uncommitted record with your own check (a sequence
number or checksum in the payload) before committing it. A record that is
not committed is never removed, so it reappears in every later walk. On a
batch lane, `join_completed_open_batch()` reports — once — the batch the
rejoin completed after a crash; it reappears in later walks unreported.
The contract and both recipes are in `doc/guide/05-crash-and-restart.md`
in the mmapq repository.

```rust
use mmapq::{IndexedQueue, QueueConfig, Runtime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("./data")?;
    let rt = Runtime::region_mapper()?;
    let q = IndexedQueue::create(
        QueueConfig::new("./data", "my_queue")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )?;

    let mut appender = q.create_appender_with_id(0)?; // this writer's fixed lane
    let mut offsets = Vec::new();
    {
        let mut it = appender.uncommitted_iter()?;
        while let Some((offset, data)) = it.next()? {
            println!("recovered {} bytes at {}", data.len(), offset);
            // After a crash a record's data may be partly written, and an
            // uncommitted record reappears in every later walk: commit only
            // what your own check accepts.
            if is_valid(data) {
                offsets.push(offset); // commit below; skipping is also valid
            }
        }
    }
    for offset in offsets {
        appender.commit(offset)?; // publish each recovered message
    }

    // Batch lanes: appender.uncommitted_batches() yields
    // (batch_offset, messages); appender.commit_batch(offset) publishes;
    // appender.join_completed_open_batch() reports a crash-interrupted batch
    // the rejoin completed to staged.
    Ok(())
}

// The application's own integrity check — for example a checksum or a
// sequence number carried in the payload; the library has none.
fn is_valid(data: &[u8]) -> bool {
    !data.is_empty() // replace with your check
}
```

## Cross-language and cross-process

The wire format is identical to the C library and to Python (`mmapq-py`),
and derives from Algoteq's Java implementation (tools4j-derived; **not**
compatible with upstream tools4j — the formats have diverged). Sharing
queues with Java is not supported in 0.2.0: Java-written queues are
refused at open (schema_version 1); planned at schema 2, under the same
protocol contract (see the C library's README). A Rust process can produce
a queue a C or Python process consumes, and vice versa.
The interop test drives a real C echo server:

```bash
cargo test --test cross_process
```

## Thread-safety reference

| Type | `Send` | `Sync` | Notes |
|---|---|---|---|
| `Runtime` | yes | yes | Background mapper thread; mutex-guarded registry |
| `IndexedQueue` | yes | yes | Factory; re-entrant participant creation |
| `QueueConfig` | yes | yes | Plain builder data |
| `IndexedAppender` | no | no | Owns a payload write position |
| `IndexedPoller` | no | no | Owns a read cursor |
| `IndexedReader` | no | no | Owns lazy region accessors |
| `IndexedArchiver` | no | no | Scans metadata |

## Performance

The C library achieves ~280 ns median round-trip between two processes on
isolated cores. In the 0.2.0 measurements the Rust wrapper's single-message
median round-trip is within **1–3 ns** of the C library's in the same mode
(one FFI call, closure dispatch and slice construction per message) — the
safety layer is close to free.

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
or telemetry capture, and replay-based testing.

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
`doc/design/header_layout_design.md` in the mmapq repository.

## Stability

The wire format is frozen within a schema version. 0.2.0 writes
`schema_version=2` and refuses 0.1.0 files (`schema_version=1`) at open
with `MMAPQ_ESCHEMA` (Rust: `MmapError::SchemaMismatch`): 0.2.0 changed
what 0.1.0 files mean and accept (the archive-line field, the geometry
rule, the unified tail rule). To move 0.1.0 data forward, export with a
0.1.0 library and import into a fresh 0.2.0 queue — the export stream
format is unchanged. Each release states the schema versions it reads; a
file of any other version is refused at open. A version change is always
a clean refusal, never a silent misread. The feature set is frozen and
the API grows only for capability already present in the C core —
a design position: the library you audit today is the library you run in
five years.

## Acknowledgements

`mmapq-rs` wraps Algoteq's mmapq, originally derived from the
[tools4j/mmap](https://github.com/tools4j/mmap) library. The event-sourcing
architecture follows [tools4j/event-sourcing](https://github.com/tools4j/event-sourcing)
and [elara](https://github.com/tools4j/elara).

## License

BSD 3-Clause — see [LICENSE](LICENSE).

## Platform

Linux only (x86-64 and ARM64; ARM64 passed a one-off full run for 0.2.0
and is not yet in regular CI). Requires naturally aligned lock-free
atomics over `MAP_SHARED` mmap.
