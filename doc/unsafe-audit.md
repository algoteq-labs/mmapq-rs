# Rust unsafe-boundary audit

Audit record; last reconciled with the source on 2026-11-02 at
ffcd889. Not covered: the unsafe blocks in src/uncommitted.rs (2) and
src/batch/uncommitted.rs (3), added after this audit.

This records the original audit and subsequent fixes, **not a declaration that
the crate is fully sound**. Resolution of the listed findings is limited to
the ownership and bounds obligations described below. The original findings came from source inspection. Subsequent fixes and
executed regression coverage are recorded below.

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
- Runtime handles use C reference counting, atomic status, and the C runtime's
  synchronization. Appenders/readers/pollers/archivers retain runtime handles
  until after their C resources are released.

Mapped slices additionally assume committed backing bytes are not externally
rewritten or truncated while borrowed. Rust lifetimes cannot enforce arbitrary
file modifications by another process. No claim of Miri verification is made
for the C/mmap implementation.

## Resolved safety obligations

### U1: normal queue Sync and lazy metadata initialization — resolved

The normal C factory no longer contains `queue_meta`, opens metadata, or
exposes the `mmapq_indexed_queue_region_size`, `mmapq_indexed_queue_max_file_size`,
and `mmapq_indexed_queue_start_index` getters. Their Rust methods and FFI
bindings are removed. The unused batch-factory metadata field is also removed.
Both factories retain immutable creation preferences and runtime pointers;
participants own their metadata mappings and discover persisted configuration.
Rust keeps Send + Sync: shared factory calls read configuration and construct
independent participants, while Drop excludes overlapping safe calls.

`tests/borrowing.rs` rejects all three removed methods and FFI declarations
and checks that the factory remains Send + Sync. Its new regression was
observed failing before the removal (`factory_region_size unexpectedly compiled`).
`tests/shared_factory.rs` exercises readers/pollers created before metadata
exists, later publication, and concurrent appenders through one shared factory,
with both synchronous and asynchronous mapping. C's `test/test_shared_queue.c`
exercises concurrent participant creation/use; the existing geometry-discovery
regression now inspects the reader's metadata instead of the factory.

### U2: normal and batch archiver runtime ownership — resolved

`IndexedArchiver` and `IndexedBatchArchiver` now retain an optional owned
read-runtime clone, copied from their factory just like readers and pollers.
Their C handles still borrow that runtime: Rust closes each archiver before
its runtime field drops. This covers lazy initialization, continued use, and
asynchronous region teardown after the queue and caller's runtime are gone.
`None` continues to select synchronous mapping. No C or public API changes
were needed, and the clone occurs only during participant creation.

`tests/archiver_runtime.rs` uses custom runtimes (rather than registry-held singletons)
and synchronous writers so another participant cannot mask missing read-runtime
ownership. Six tests cover normal and batch archivers used first after owner
drop, initialized before owner drop and then used again, and dropped unused.
Each also exercises synchronous mapping. Tests run in bounded subprocesses
because a dangling C runtime can abort or hang. Before the fix, both lazy-use
tests returned `InvalidArgument`, both initialized tests timed out, and both
unused-drop tests passed. After the fix all six pass in debug and release.

### U3: benchmark header minimum size — resolved

Both latency examples reject message sizes below six before starting runtimes,
server threads, or queues. Probe construction independently enforces the same
minimum with a release-active assertion. Request and shutdown encoding now use
safe byte-slice writes; the packed header structs and all four pointer casts
are removed. The six-byte big-endian wire format is unchanged.

Each example has tests covering constructor rejection for sizes zero through
five and exact request/shutdown bytes at sizes six and 64, including untouched
trailing payload. The rejection test failed before the fix in each example
(`size 0 was accepted`), without executing the invalid write. Both tests pass
in debug and release. CLI checks additionally cover early rejection and valid
round trips.

### R1: singleton runtime reference leak — resolved

Both Rust singleton getters now adopt the caller-owned reference returned by
C directly. The redundant clone and its error cleanup are removed. Rust Drop
releases that reference once; the registry still retains its independent
reference. Explicit `Runtime::clone()` and participant-owned clones are unchanged.

`tests/runtime_singleton.rs` uses a separate executable with linker wrappers
around the two getters, clone, and destroy. Wrappers delegate to the real C
functions and count caller acquisitions/releases, excluding getter-internal
clones and registry ownership. Each singleton is checked over 32 getter/clone/drop
cycles and overlapping acquisitions. Explicit clones remain usable after the
original handle drops. Before the fix, both tests observed one leaked caller
reference after the first cycle. Both pass with debug and release libraries.

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

The original documentation pass made no C/Python implementation, signature,
allocation, lock, memory ordering, package version, or runtime safety check
changes and added no tests. The subsequent U1, U2, U3, and R1 fixes and regression tests are described
above. All four listed findings are resolved; this is not a fresh exhaustive
audit of the crate or C implementation.
