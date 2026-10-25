# mmapq-rs 0.2.0 release notes

**Breaking, format-level, and protocol-level changes are documented in the
C library's notes (`doc/RELEASE-NOTES-0.2.0.md` in the mmapq repository,
installed with the C library under `share/doc/mmapq/`) — which are
authoritative for the wire format (schema_version 2: 0.1.0
queues are refused at open; recreate or export/import to migrate), the
removed/replaced C functions, the behavior changes under unchanged names,
and the Java-interop position.** This file carries only the
binding-specific entries.

## Breaking (Rust)

- `ffi::mmapq_runtime_close` now returns `c_int` (breaking for raw-ffi
  callers only); the safe `Runtime` API is unchanged (`Drop` →
  `mmapq_runtime_destroy`). The removed C factory metadata getters and
  the offset-based batch discard were never part of the safe Rust
  surface.

## New (Rust)

- Uncommitted-message iteration on both queue types: `uncommitted_iter`,
  `uncommitted_batches`, `UncommittedIter`, `UncommittedBatches`,
  `UncommittedBatchIterator`.
- Batch lifecycle: `join_completed_open_batch`, `complete_uncommitted`,
  `commit_batch` (checked throughout).
- `mmapq-ids` operator tool (in the C repository's `tools/`).

## Fixed (Rust)

- Export errors are preserved across output-close failures.
- The archiver retains its runtimes; shared queue factories are safe.
- `archivable_files` propagates negative `MMAPQ_*` error codes.

## Hardening (Rust)

- Exclusive reader borrows with batch payloads lent per cursor step;
  checked C iteration and batch-start APIs throughout; mapper panics are
  caught and unwound only after C cleanup; C error mapping is
  allocation-free. An ffi↔C layout gate pins `ReadingContext` /
  `BatchReadingContext`.
