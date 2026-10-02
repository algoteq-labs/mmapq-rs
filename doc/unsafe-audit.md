# Rust unsafe-boundary audit

This is a documentation audit, **not a declaration that the crate is fully
sound**. The unresolved items below must not be mistaken for established
safety invariants. They were identified by source inspection; no undefined
behavior was deliberately executed to reproduce them in this pass.

## Scope and conventions

Reviewed unsafe operations in `src/`, all four `examples/`, and the allocator
in `tests/zero_copy.rs`, plus the raw export callback in `tests/transfer.rs`. Added local `// SAFETY:` explanations, `# Safety`
contracts for unsafe constructors/callbacks, and explicit unsafe blocks for
operations previously implicit inside unsafe functions. The last change
narrows lexical responsibility; it does not add runtime instructions.

Raw declarations in `src/ffi.rs` share module-level handle, pointer, runtime,
buffer, and callback obligations, with callback-specific contracts. Individual
C function contracts remain in the vendored C headers. Declaring a raw pointer
or an unsafe function-pointer type is not itself an unsafe operation.

`AGENTS.md` now requires these justifications. A comment saying `UNRESOLVED`
records an unmet obligation, not permission to assume it holds. Clippy can
check that a comment exists; it cannot prove that its argument is correct.

## Established wrapper mechanisms

- Reader messages borrow the reader exclusively, excluding subsequent reads,
  repositioning, and close while their payload is used. C validates framing
  and capacity before Rust constructs a slice.
- Batch cursor construction requires an initialized C iterator and a live
  owner/runtime, with no competing cursor access. The `PhantomData` marker
  records an obligation established by callers; it does not validate raw
  pointers. Each yielded slice borrows a cursor step, not the whole batch.
- A batch writer exclusively borrows its appender. Reserved mutable slices
  borrow that writer, preventing another reserve, append, commit, or rollback
  while they remain in use. C bounds reservations to one mapped region.
- Poll callbacks use paired, stack-resident type-erased state synchronously.
  Higher-ranked callback lifetimes prevent borrowed payload/cursor escape.
  Trampolines catch user panics and propagate them after C returns.
- Transfer callbacks keep their state live until synchronous C cleanup has
  completed. `FILE*` ownership stays with the opening Rust method until its
  single `fclose`; C transfer code only borrows the stream.
- Strings returned by archivable-file enumeration are bounded, initialized
  `strdup` results. Rust copies each before calling the matching libc free.
  This does not resolve the archiver runtime issue below.
- Runtime handles use C reference counting, atomic status, and the C runtime's
  synchronization. Appenders/readers/pollers retain runtime handles until
  after their C resources are released. Archivers currently do not.

Mapped slices additionally assume committed backing bytes are not externally
rewritten or truncated while borrowed. Rust lifetimes cannot enforce arbitrary
file modifications by another process. No claim of Miri verification is made
for the C/mmap implementation.

## Unresolved safety obligations

### U1: normal queue Sync and lazy metadata initialization

`src/queue.rs` implements `Sync` for `IndexedQueue`. Its shared metadata
getters call C `mmapq_indexed_queue_region_size`,
`mmapq_indexed_queue_max_file_size`, and `mmapq_indexed_queue_start_index`.
In `c/src/indexed_queue.c`, all three call `ensure_meta_open`, which reads and
writes the ordinary `queue_meta` pointer without synchronization.

The Rust constructor does not eagerly open that metadata. Its old comments
claimed otherwise; those comments have been corrected. Concurrent safe getter
calls can therefore race during initialization. Atomic on-disk metadata fields
do not protect the process-local pointer cache. This is a safety defect, not
merely a missing comment. No concurrent reproducer was run in this audit.

A follow-up must synchronize lazy initialization correctly, or restrict the
Rust API so it cannot occur concurrently. A one-time eager probe is not enough
when metadata does not exist yet and later getter calls retry. Removing Sync
or changing getter borrowing can break callers and needs an explicit API
decision. The batch factory does not expose these metadata getters; its safe
shared methods read configuration and allocate separate participant state.

### U2: normal and batch archivers do not retain their runtime

`IndexedArchiver` and `IndexedBatchArchiver` have no owned runtime field.
Their factories pass the queue's read-runtime pointer into C, where it is
stored for later lazy initialization. Dropping the queue and a custom runtime
can leave that pointer dangling while the Rust archiver remains usable.

The C archivers pass zero regions-to-map-ahead, but that does **not** select
synchronous mapping: `make_ring_accessor` in
`c/src/queue_region_accessor.c` selects asynchronous mapping whenever runtime
is non-null. Async ring creation/close uses that runtime; it does not acquire
an owning runtime clone. Consequently, lazy use and teardown both need the
runtime to remain alive. Singleton references can mask this lifetime gap.

A follow-up should retain the configured read-runtime handle in both Rust
archivers, through C teardown, as other participant wrappers already do.
Regression coverage should use a custom runtime and drop the queue and
original runtime before both first archiver use and archiver destruction.
This pass documents the issue without introducing ownership changes.

### U3: benchmark header references require an unchecked minimum size

Both latency benchmark examples form `&mut LatencyHeader` from a `Vec<u8>`
pointer, during requests and shutdown. The packed struct has alignment one,
so alignment is not the problem. It contains six bytes, but CLI `msg_size`
has no lower bound. A size below six cannot back a valid reference.

A follow-up should reject undersized messages before constructing the probe
and test sizes zero through five. The audit marks both casts in both examples
as unresolved rather than claiming the default size proves their bounds.

## Other existing issue

### R1: singleton runtime reference leak

C singleton getters already return a counted reference. The Rust wrappers
clone it again and, on success, do not release the getter's reference. Existing
comments incorrectly justified the second clone as necessary to protect the
registry's ownership; they now describe the actual behavior. This is a leak,
not evidence that the unsafe clone dereferences a dead pointer. It is not
fixed by the documentation pass.

## Changed-file manifest

- `AGENTS.md`, this report.
- `src/appender.rs`, `src/archiver.rs`, `src/poller.rs`, `src/reader.rs`,
  `src/queue.rs`, `src/runtime.rs`, `src/transfer/mod.rs`, `src/ffi.rs`.
- `src/batch/batch_appender.rs`, `src/batch/batch_archiver.rs`,
  `src/batch/batch_iter.rs`, `src/batch/batch_poller.rs`,
  `src/batch/batch_queue.rs`, `src/batch/batch_reader.rs`.
- `examples/echo_server.rs`, `examples/batch_echo_server.rs`,
  `examples/latency_bench.rs`, `examples/batch_latency_bench.rs`.
- `tests/zero_copy.rs`, `tests/transfer.rs` (allocator/callback documentation
  and explicit unsafe scopes).

No C/Python implementation, signature, allocation, lock, memory ordering,
package version, or runtime safety check changes. No new tests were added;
existing tests exercise the unchanged behavior, not the unresolved cases.
