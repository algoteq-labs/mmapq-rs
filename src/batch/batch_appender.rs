//! Batch appender — writes batches of messages and commits them atomically.
//!
//! The `BatchWriter` RAII guard enforces correct lifecycle at compile time:
//! `commit()` publishes and consumes `self`, `complete_uncommitted()` stages
//! for a later `commit_batch()`, drop without either discards.

use std::marker::PhantomData;

use crate::batch::uncommitted::UncommittedBatches;
use crate::batch::BatchMessageIter;
use crate::error::{checked_len, map_rc, MmapError};
use crate::ffi;

/// Batch appender. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchAppender {
    pub(crate) inner: *mut ffi::IndexedBatchAppender,
    /// Keeps the write runtime alive for this participant's lifetime
    /// independently of the parent queue; None selects synchronous mapping.
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedBatchAppender {
    /// Start a new batch.
    ///
    /// Returns a `BatchWriter` that enforces correct lifecycle:
    /// - `append()` / `append_reserved()` add messages.
    /// - `commit()` publishes the batch and consumes the writer.
    /// - `complete_uncommitted()` stages the batch for a later
    ///   `commit_batch()` and consumes the writer.
    /// - Dropping without either discards via `mmapq_mutable_batch_discard()`.
    #[inline]
    pub fn start_batch(&mut self) -> Result<BatchWriter<'_>, MmapError> {
        let mut batch = std::ptr::null_mut();
        // SAFETY: The live appender is exclusively borrowed; batch is a writable output.
        // Success supplies its active batch, whose guard retains this exclusive borrow.
        let result = unsafe { ffi::mmapq_indexed_batch_appender_start_batch_checked(self.inner, &mut batch) };
        if result != 0 {
            return Err(map_rc(result));
        }
        Ok(BatchWriter {
            batch,
            committed: false,
            _phantom: PhantomData,
        })
    }

    /// The appender's per-lane ID (0–255).
    pub fn id(&self) -> u8 {
        // SAFETY: The owned, thread-confined handle stays live while C reads its scalar ID.
        unsafe { ffi::mmapq_indexed_batch_appender_id(self.inner) }
    }

    /// Commit a staged batch, making it visible to pollers and readers.
    ///
    /// `batch_offset` is the value returned by
    /// `BatchWriter::complete_uncommitted()`, or an offset yielded by
    /// `uncommitted_batches()` — any subset, any order, same session or
    /// after rejoin. Returns the zero-based index of the committed batch,
    /// or `Err(InvalidArgument)` if the offset is not outstanding (never
    /// staged by this incarnation, already committed, a discarded batch,
    /// above the last started batch) or has been overtaken by archiving.
    pub fn commit_batch(&mut self, batch_offset: i64) -> Result<i64, MmapError> {
        // SAFETY: The live appender is exclusively borrowed. C encodes the numeric
        // offset; this operation does not dereference it as a Rust pointer.
        let idx = unsafe { ffi::mmapq_indexed_batch_appender_commit_batch(self.inner, batch_offset) };
        if idx >= 0 {
            Ok(idx)
        } else {
            Err(map_rc(idx as i32))
        }
    }

    /// Iterate this appender's uncommitted (staged or discarded) batches —
    /// snapshot at call time, from the archive line to the frozen end of
    /// stream.
    ///
    /// Yields `(batch_offset, messages)` pairs; commit a found offset with
    /// `commit_batch()`. Discarded batches reappear after a restart —
    /// recovery code decides. The `&mut self` borrow enforces the
    /// no-append-while-iterating rule at compile time. Archiving may remove
    /// batches the walk has not reached, even after an undo. Returns
    /// `Err(Corrupt)` for a pre-0.2.0 queue (zero archive line — recreate
    /// the queue).
    pub fn uncommitted_batches(&mut self) -> Result<UncommittedBatches<'_>, MmapError> {
        let mut it = std::ptr::null_mut();
        // SAFETY: The live, thread-confined appender is exclusively borrowed; it is a
        // writable output local.
        let rc = unsafe { ffi::mmapq_indexed_batch_appender_uncommitted_batches_checked(self.inner, &mut it) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        // The factory returned 0, so `it` is the live appender-owned iterator; the
        // returned guard carries this exclusive borrow for its lifetime. The lane ID
        // labels the yielded messages (all batches walked are this lane's payloads).
        let appender_id = self.id();
        Ok(UncommittedBatches::new(it, appender_id))
    }

    /// Report whether this appender's join completed a trailing open batch
    /// to staged (at the rejoin, or by start_batch's orphan recovery).
    ///
    /// Returns `Some(offset)` once per join-completion event (read-and-clear),
    /// `None` otherwise. The batch was completed by the queue, not the app:
    /// it may lack messages the app intended; the last counted message may
    /// have partial content.
    pub fn join_completed_open_batch(&mut self) -> Option<i64> {
        let mut offset = 0;
        // SAFETY: The live, thread-confined appender is exclusively borrowed; offset is
        // a writable output local. C documents the return as 1 (event) or 0 (none).
        let rc = unsafe { ffi::mmapq_indexed_batch_appender_join_completed_open_batch(self.inner, &mut offset) };
        if rc == 1 {
            Some(offset)
        } else {
            None
        }
    }

    /// Convenience: start a batch, append one message, and publish
    /// in a single call. Avoids three separate FFI crossings.
    ///
    /// Equivalent to:
    ///   let mut b = appender.start_batch()?;
    ///   b.append(data)?;
    ///   b.commit()
    ///
    /// Returns the committed batch index, or an error code on failure.
    pub fn append_and_commit(&mut self, data: &[u8]) -> Result<i64, MmapError> {
        let len = checked_len(data.len())?;
        // SAFETY: The live handle is exclusively borrowed; checked_len bounds
        // data's readable bytes, which C copies before returning.
        let idx = unsafe {
            ffi::mmapq_indexed_batch_appender_append_and_commit(
                self.inner,
                data.as_ptr() as *const _,
                len,
            )
        };
        if idx >= 0 {
            Ok(idx)
        } else {
            Err(map_rc(idx as i32))
        }
    }
}

impl Drop for IndexedBatchAppender {
    fn drop(&mut self) {
        // SAFETY: Drop uniquely owns the handle; no BatchWriter remains, and the runtime
        // field is still alive while C closes the appender.
        unsafe { ffi::mmapq_indexed_batch_appender_close(self.inner) }
    }
}

/// An in-progress batch write session.
///
/// - `append()` / `append_reserved()` add messages.
/// - `commit()` publishes the batch and consumes `self`, preventing
///   double-commit at compile time.
/// - `complete_uncommitted()` stages the batch without publishing and
///   consumes `self`.
/// - `Drop` without either discards via `mmapq_mutable_batch_discard()`
///   (never complete — that publishes now).
pub struct BatchWriter<'a> {
    batch: *mut ffi::MutableBatch,
    /// Set once the C batch handle is consumed (commit or
    /// complete_uncommitted), suppressing Drop's discard.
    committed: bool,
    _phantom: PhantomData<&'a mut IndexedBatchAppender>,
}

impl BatchWriter<'_> {
    /// Append a message to the in-progress batch.
    ///
    /// Returns the zero-based message index within the batch.
    /// Returns `Err(InvalidArgument)` if `data` is empty.
    pub fn append(&mut self, data: &[u8]) -> Result<i32, MmapError> {
        if data.is_empty() {
            return Err(MmapError::InvalidArgument);
        }
        let len = checked_len(data.len())?;
        // SAFETY: The live handle is exclusively borrowed; checked_len bounds
        // data's readable bytes, which C copies before returning.
        let idx = unsafe {
            ffi::mmapq_mutable_batch_append(self.batch, data.as_ptr() as *const _, len)
        };
        if idx >= 0 {
            Ok(idx)
        } else {
            Err(map_rc(idx as i32))
        }
    }

    /// Reserve space to write a message in-place later.
    ///
    /// Returns a mutable byte slice. The slice is valid until the next
    /// `append()` or `append_reserved()` call.
    pub fn append_reserved(&mut self, size: i32) -> Result<&mut [u8], MmapError> {
        let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
        // SAFETY: self exclusively borrows the live active batch. C validates
        // size before reserving mapped bytes; ptr is a writable output local.
        let idx = unsafe {
            ffi::mmapq_mutable_batch_append_reserved(
                self.batch,
                size,
                &mut ptr as *mut *mut std::ffi::c_void,
            )
        };
        if idx < 0 {
            return Err(map_rc(idx));
        }
        // SAFETY: Successful reserve validates positive size within one mapped region.
        // The bytes are initialized file-backed storage; this writer's exclusive borrow
        // prevents append/remap/commit/rollback until the mutable slice is released.
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, size as usize) })
    }

    /// Append all remaining messages from a BatchPayloadIterator into this batch.
    ///
    /// The iterator advances before copying: the last yielded message is not copied again.
    /// Useful for echoing an entire batch: call `mmapq_batch_iterator_advance()` to
    /// inspect the first message, then call `append_remaining_from()` — it
    /// starts from wherever the iterator currently is.
    ///
    /// Returns the number of messages appended, or an error if a message
    /// exceeds the destination queue's region_size (batch is partially
    /// populated — must discard, not commit).
    pub fn append_remaining_from(&mut self, it: &mut ffi::BatchPayloadIterator) -> Result<i32, MmapError> {
        // SAFETY: self owns the active destination batch. The opaque iterator reference
        // must originate from a live C cursor; its exclusive borrow excludes competing
        // steps, and C copies source payloads before advancing that cursor.
        let n = unsafe { ffi::mmapq_mutable_batch_append_remaining_from(self.batch, it) };
        if n >= 0 { Ok(n) } else { Err(map_rc(n)) }
    }

    /// Convenience wrapper: append remaining messages from a `BatchMessageIter`.
    ///
    /// Equivalent to `append_remaining_from(iter.as_raw_iterator())`.
    pub fn append_remaining_from_iter(
        &mut self,
        iter: &mut BatchMessageIter<'_>,
    ) -> Result<i32, MmapError> {
        // SAFETY: The cursor's constructor establishes its live C allocation and owner.
        // Both exclusive borrows exclude outstanding source slices and destination
        // reservations; neither owner can close or reposition during this copy.
        unsafe { self.append_remaining_from(&mut *iter.as_raw_iterator()) }
    }

    /// Commit the batch, making it visible to pollers and readers.
    ///
    /// One call: `mmapq_mutable_batch_complete()` stores the message count
    /// and publishes the batch header. Consumes `self`. Returns the
    /// zero-based index of the committed batch.
    pub fn commit(mut self) -> Result<i64, MmapError> {
        // SAFETY: The guard keeps its appender and active batch alive and exclusively borrowed.
        // Completing consumes no Rust references to reserved bytes.
        let idx = unsafe { ffi::mmapq_mutable_batch_complete(self.batch) };
        if idx >= 0 {
            self.committed = true;
            Ok(idx)
        } else {
            Err(map_rc(idx as i32))
        }
    }

    /// Finalise the batch WITHOUT publishing it (stage): stores the message
    /// count and returns the batch offset for a later
    /// `IndexedBatchAppender::commit_batch()` — same session or after
    /// rejoin. Consumes `self`.
    pub fn complete_uncommitted(mut self) -> Result<i64, MmapError> {
        // SAFETY: The guard keeps its appender and active batch alive and exclusively borrowed.
        // Completing consumes no Rust references to reserved bytes.
        let offset = unsafe { ffi::mmapq_mutable_batch_complete_uncommitted(self.batch) };
        if offset >= 0 {
            self.committed = true;
            Ok(offset)
        } else {
            Err(map_rc(offset as i32))
        }
    }
}

impl Drop for BatchWriter<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: An unconsumed guard still exclusively owns its live batch/appender.
            // No reserved slice can remain borrowed when the guard is dropped.
            // Discard-only, NEVER complete — complete publishes now. The rc is
            // ignored per the owner ruling (drop is best-effort rollback; an
            // already-consumed handle reports EINVAL, which drop cannot act on).
            unsafe { ffi::mmapq_mutable_batch_discard(self.batch) };
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::IndexedBatchQueue;
    use crate::queue::QueueConfig;
    use crate::runtime::Runtime;
    use crate::error::MmapError;

    #[test]
    fn test_batch_append_rejects_oversized_message() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-batch-appender").join("test_batch_append_oversized");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("failed to create test base directory");
        let q = IndexedBatchQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .region_size(32768)
                .ring_size(16) // CAND-38: 32768 is the smallest legal region (one 32 KB header block)
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut a = q.create_appender().unwrap();
        let mut w = a.start_batch().unwrap();

        let ok = w.append(b"small").unwrap();
        assert_eq!(ok, 0);

        let big = vec![0u8; 32768];
        let err = w.append(&big).unwrap_err();
        assert!(matches!(err, MmapError::InvalidArgument));

        drop(w);
        drop(a);
        drop(q);
        drop(rt);
    }
}
