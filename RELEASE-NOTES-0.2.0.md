# mmapq-rs 0.2.0 release notes

**Breaking, format-level, and protocol-level changes are documented in the
C library's notes (`doc/RELEASE-NOTES-0.2.0.md` in the mmapq repository,
installed with the C library under `share/doc/mmapq/`) — which are
authoritative for the wire format (schema_version 2: 0.1.0
queues are refused at open; recreate or export/import to migrate), the
removed/replaced C functions, the behavior changes under unchanged names,
and the Java-interop position.** This file carries only the
binding-specific entries. The binding changes below break source
compatibility; the queue-format change (schema 2) is described in the
C library's notes.

## Breaking (Rust)

- `ffi::mmapq_runtime_close` now returns `c_int` (breaking for raw-ffi
  callers only); the safe `Runtime` API is unchanged (`Drop` →
  `mmapq_runtime_destroy`). The removed C factory metadata getters and
  the offset-based batch discard were never part of the safe Rust
  surface.
- `IndexedQueue` remains `Send + Sync` and now owns only factory
  configuration and runtime handles. Use a reader or poller for the live
  start index. `QueueConfig` creation preferences are kept separately and
  are not the authoritative geometry of an existing queue; no replacement
  public geometry query is added.
- Declare readers with `let mut reader`. Reads, metadata operations that
  enter C, and transfer source arguments require exclusive borrows.
  Process `message.data()` before reading again; explicit `to_vec()`
  remains available.
- Declare a batch view mutable; `batch.iter()` exclusively borrows it.
  Replace `for` and iterator adapters with a lending loop. Standard
  `Iterator`, `collect`, and simultaneous cursors over one batch are
  intentionally unavailable; `next()` distinguishes exhaustion from
  corruption/timeouts and keeps errors sticky until the batch is re-read.
- Poll handlers still return `NextMove`; handle cursor errors explicitly.
  A corrupt iterator prevents cursor advancement even if the callback
  asks to advance, and already executed application side effects are not
  rolled back.
- `append_remaining_from_iter` copies after the last yielded message; its
  source cursor cannot be advanced while the previous slice is still used.
- `Corrupt` and `Internal` diagnostic payloads are `Cow<'static, str>`
  (use `.into()` for owned diagnostics); unknown C results are
  `UnexpectedCode(i32)`; mapper aborts are `CallbackAborted`.
- Failures may leave a committed destination prefix or an incomplete
  export file. Transfers are not atomic and retries are not automatically
  idempotent.
- `IndexedReader` and `IndexedBatchReader` `read` / `read_first` /
  `read_last` return `Result<ReadResult<T>, MmapError>` instead of
  `Result<Option<T>, MmapError>`. `ReadResult` is `Available(T)`,
  `EndOfStream` or `Archived`: an archived index (below the queue's start
  index) no longer reads as `None` indistinguishable from "not yet
  written". Migrate `.unwrap()` on the option to `.available()` (which
  maps both non-available outcomes to `None`, the 0.1.0 behaviour), or
  match the three variants. Errors are unchanged (`Err(MmapError)`);
  `entry_status` and the pollers are unchanged.

## New (Rust)

- Uncommitted-message iteration on both queue types: `uncommitted_iter`,
  `uncommitted_batches`, `UncommittedIter`, `UncommittedBatches`,
  `UncommittedBatchIterator`.
- Batch lifecycle: `join_completed_open_batch`, `complete_uncommitted`,
  `commit_batch` (checked throughout).
- `mmapq-ids` operator tool (in the C repository's `tools/`).
- `IndexedArchiver::reclaim_files` and `IndexedBatchArchiver::reclaim_files`: delete
  files from an `archivable_files()` listing safely, even against a concurrent undo.

## Fixed (Rust)

- Export errors are preserved across output-close failures.
- The archiver retains its runtimes; shared queue factories are safe.
- `archivable_files` propagates negative `MMAPQ_*` error codes.
- `Runtime::set_affinity` maps the C result through the standard error
  mapping: an out-of-range CPU is `InvalidArgument`, a closed runtime
  `QueueClosed`, allocation failure `OutOfMemory` (was `Internal` for all).
- The crate builds on ARM64 Linux. `IndexedArchiver::print_summary` typed
  its C buffer as `i8` rather than `c_char`, which is unsigned on ARM64
  Linux, so 0.1.0 did not compile there.

## Packaging (Rust)

- The published crate now contains only what builds it: the Rust sources,
  the examples, and the vendored C library sources and headers. The 0.1.0
  crate also shipped the Rust tests and the C library's documentation,
  scripts and examples. The test suite runs from the repository, not from
  the crate.

## Hardening (Rust)

- Exclusive reader borrows with batch payloads lent per cursor step;
  checked C iteration and batch-start APIs throughout; mapper panics are
  caught and unwound only after C cleanup; C error mapping is
  allocation-free. An ffi↔C layout gate pins `ReadingContext` /
  `BatchReadingContext`.
- Transfer mapper panics stop the operation and resume only after C
  cleanup; no silent automatic-ID fallback occurs.
- Reader/cursor thread confinement is unchanged. Independent readers can
  share a runtime but own distinct mapping/accessor state. The library
  adds no runtime borrow checks, payload ownership, reference counting,
  or mapping pins to implement these lifetimes; mapping and producer
  copies already present in the underlying C implementation are not
  eliminated.
