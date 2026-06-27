//! Batch appender — writes batches of messages and commits them atomically.
//!
//! The `BatchWriter` RAII guard enforces correct lifecycle at compile time:
//! `commit()` consumes `self`, drop without commit rolls back.

use std::marker::PhantomData;

use crate::error::MmapError;
use crate::ffi;

/// Batch appender. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchAppender {
    pub(crate) inner: *mut ffi::IndexedBatchAppender,
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
    pub fn append(&mut self, data: &[u8]) -> Result<i32, MmapError> {
        let idx = unsafe {
            ffi::mutable_batch_append(self.batch, data.as_ptr() as *const _, data.len() as i32)
        };
        if idx >= 0 {
            Ok(idx)
        } else {
            Err(MmapError::MapTimedOut)
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
            return Err(MmapError::MapTimedOut);
        }
        if ptr.is_null() {
            return Err(MmapError::Internal("append_reserved returned null pointer".into()));
        }
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, size as usize) })
    }

    /// Commit the batch, making it visible to pollers and readers.
    ///
    /// Calls `mutable_batch_complete()` then
    /// `indexed_batch_appender_commit_batch()`. Consumes `self`.
    pub fn commit(mut self) -> Result<i64, MmapError> {
        let batch_offset = unsafe { ffi::mutable_batch_complete(self.batch) };
        if batch_offset < 0 {
            return Err(MmapError::MapTimedOut);
        }
        let idx = unsafe { ffi::indexed_batch_appender_commit_batch(self.appender, batch_offset) };
        if idx >= 0 {
            self.committed = true;
            Ok(idx)
        } else {
            Err(MmapError::MapTimedOut)
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
