//! Batch appender — writes batches of messages and commits them atomically.
//!
//! The `BatchWriter` RAII guard enforces correct lifecycle at compile time:
//! `commit()` consumes `self`, drop without commit rolls back.

use std::marker::PhantomData;

use crate::batch::BatchMessageIter;
use crate::error::{checked_len, map_rc, MmapError};
use crate::ffi;

/// Batch appender. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchAppender {
    pub(crate) inner: *mut ffi::IndexedBatchAppender,
    /// Keeps the write runtime alive for this participant's lifetime
    /// (batch queues currently always pass NULL runtimes — sync mapping —
    /// so this is None today; the field exists so batch runtime support
    /// cannot reintroduce the UAF fixed in tests/runtime_uaf.rs).
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedBatchAppender {
    /// Start a new batch.
    ///
    /// Returns a `BatchWriter` that enforces correct lifecycle:
    /// - `append()` / `append_reserved()` add messages.
    /// - `commit()` finalises and consumes the writer.
    /// - Dropping without `commit()` auto-rolls back via
    ///   `indexed_batch_appender_discard_batch()`.
    pub fn start_batch(&mut self) -> Result<BatchWriter<'_>, MmapError> {
        let batch = unsafe { ffi::indexed_batch_appender_start_batch(self.inner) };
        if batch.is_null() {
            return Err(MmapError::MapTimedOut);
        }
        Ok(BatchWriter {
            appender: self.inner,
            batch,
            committed: false,
            _phantom: PhantomData,
        })
    }

    /// The appender's per-lane ID (0–255).
    pub fn id(&self) -> u8 {
        unsafe { ffi::indexed_batch_appender_id(self.inner) }
    }

    /// Convenience: start a batch, append one message, complete, and commit
    /// in a single call. Avoids four separate FFI crossings.
    ///
    /// Equivalent to:
    ///   let mut b = appender.start_batch()?;
    ///   b.append(data)?;
    ///   b.commit()
    ///
    /// Returns the committed batch index, or an error code on failure.
    pub fn append_and_commit(&mut self, data: &[u8]) -> Result<i64, MmapError> {
        let len = checked_len(data.len())?;
        let idx = unsafe {
            ffi::indexed_batch_appender_append_and_commit(
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
        unsafe { ffi::indexed_batch_appender_close(self.inner) }
    }
}

/// An in-progress batch write session.
///
/// - `append()` / `append_reserved()` add messages.
/// - `commit()` finalises the batch and consumes `self`, preventing
///   double-commit at compile time.
/// - `Drop` without `commit()` rolls back via `discard_batch()`.
pub struct BatchWriter<'a> {
    appender: *mut ffi::IndexedBatchAppender,
    batch: *mut ffi::MutableBatch,
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
        let idx = unsafe {
            ffi::mutable_batch_append(self.batch, data.as_ptr() as *const _, len)
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
        let idx = unsafe {
            ffi::mutable_batch_append_reserved(
                self.batch,
                size,
                &mut ptr as *mut *mut std::ffi::c_void,
            )
        };
        if idx < 0 {
            return Err(map_rc(idx));
        }
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, size as usize) })
    }

    /// Append all remaining messages from a BatchPayloadIterator into this batch.
    ///
    /// The iterator must be positioned before or at the first un-copied message.
    /// Useful for echoing an entire batch: call `batch_iterator_advance()` to
    /// inspect the first message, then call `append_remaining_from()` — it
    /// starts from wherever the iterator currently is.
    ///
    /// Returns the number of messages appended, or an error if a message
    /// exceeds the destination queue's region_size (batch is partially
    /// populated — must discard, not commit).
    pub fn append_remaining_from(&mut self, it: &mut ffi::BatchPayloadIterator) -> Result<i32, MmapError> {
        let n = unsafe { ffi::mutable_batch_append_remaining_from(self.batch, it) };
        if n >= 0 { Ok(n) } else { Err(map_rc(n)) }
    }

    /// Convenience wrapper: append remaining messages from a `BatchMessageIter`.
    ///
    /// Equivalent to `append_remaining_from(iter.as_raw_iterator())`.
    pub fn append_remaining_from_iter(
        &mut self,
        iter: &mut BatchMessageIter<'_>,
    ) -> Result<i32, MmapError> {
        unsafe { self.append_remaining_from(&mut *iter.as_raw_iterator()) }
    }

    /// Commit the batch, making it visible to pollers and readers.
    ///
    /// Calls `mutable_batch_complete()` then
    /// `indexed_batch_appender_commit_batch()`. Consumes `self`.
    pub fn commit(mut self) -> Result<i64, MmapError> {
        let batch_offset = unsafe { ffi::mutable_batch_complete(self.batch) };
        if batch_offset < 0 {
            return Err(map_rc(batch_offset as i32));
        }
        let idx = unsafe { ffi::indexed_batch_appender_commit_batch(self.appender, batch_offset) };
        if idx >= 0 {
            self.committed = true;
            Ok(idx)
        } else {
            Err(map_rc(idx as i32))
        }
    }
}

impl Drop for BatchWriter<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let batch_offset = unsafe { ffi::mutable_batch_complete(self.batch) };
            if batch_offset >= 0 {
                unsafe { ffi::indexed_batch_appender_discard_batch(self.appender, batch_offset) }
            }
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
                .region_size(4096)
                .ring_size(16) // ring_size=4 at region_size=4096 fails geometry check (mmapq_min_ring_size(4096) = 16)
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut a = q.create_appender().unwrap();
        let mut w = a.start_batch().unwrap();

        let ok = w.append(b"small").unwrap();
        assert_eq!(ok, 0);

        let big = vec![0u8; 4096];
        let err = w.append(&big).unwrap_err();
        assert!(matches!(err, MmapError::InvalidArgument));

        drop(w);
        drop(a);
        drop(q);
        drop(rt);
    }
}
