# Upcoming hardening release: migration from 0.1.0

Package versions are deliberately unchanged until release preparation.
These changes break source compatibility, not the queue or export layout.

- Declare readers with `let mut reader`. Reads, metadata operations that
  enter C, and transfer source arguments require exclusive borrows.
- Process `message.data()` before reading again. Explicit `to_vec()` is
  still available when the application chooses owned storage.
- Declare a batch view mutable; `batch.iter()` exclusively borrows it.
  Replace `for` and iterator adapters with a lending loop:

  ```rust
  fn example(
      reader: &mut mmapq::IndexedBatchReader,
      mut process: impl FnMut(&[u8], u8),
  ) -> Result<(), mmapq::MmapError> {
      if let Some(mut batch) = reader.read(0)? {
          let mut messages = batch.iter();
          while let Some((payload, appender_id)) = messages.next()? {
              process(payload, appender_id);
          }
      }
      Ok(())
  }
  ```

- Standard `Iterator`, `collect`, and simultaneous cursors over one batch
  are intentionally unavailable. `next()` distinguishes exhaustion from
  corruption/timeouts and keeps errors sticky until re-reading the batch.
- Poll handlers still return `NextMove`; handle cursor errors explicitly.
  A corrupt iterator prevents cursor advancement even if the callback asks
  to advance. Already executed application side effects are not rolled back.
- `append_remaining_from_iter` copies after the last yielded message. Its
  source cursor cannot be advanced while the previous slice is still used.
- `Corrupt` and `Internal` diagnostic payloads are `Cow<'static, str>`;
  use `.into()` for owned diagnostics. Unknown C results use
  `UnexpectedCode(i32)`. Mapper aborts have `CallbackAborted`.
- Transfer mapper panics are caught at the C boundary, stop the operation,
  and resume only after C cleanup. No silent automatic-ID fallback occurs.
- Failures may leave a committed destination prefix or incomplete export
  file. Transfers are not atomic and retries are not automatically idempotent.

Reader/cursor thread confinement is unchanged. Independent readers can
share a runtime but own distinct mapping/accessor state. The library adds
no runtime borrow checks, payload ownership, reference counting, or mapping
pins to implement these lifetimes. Mapping and producer copies already
present in the underlying C implementation are not eliminated.
