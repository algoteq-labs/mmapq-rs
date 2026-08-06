//! Batch reader — random-access batch reader.
//!
//! Thread-confined (`!Send + !Sync`). One per reader thread.

use std::marker::PhantomData;

use crate::error::MmapError;
use crate::ffi;
use crate::types::BatchEntryStatus;

/// Random-access batch reader. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchReader {
    pub(crate) inner: *mut ffi::IndexedBatchReader,
    /// Keeps the read runtime alive for this participant's lifetime
    /// (batch queues currently always pass NULL runtimes — sync mapping —
    /// so this is None today; the field exists so batch runtime support
    /// cannot reintroduce the UAF fixed in tests/runtime_uaf.rs).
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) ctx: std::cell::UnsafeCell<ffi::BatchReadingContext>,
}

impl IndexedBatchReader {
    /// Return the raw C pointer (for transfer module use).
    pub(crate) fn as_ptr(&self) -> *mut ffi::IndexedBatchReader {
        self.inner
    }

    /// Return the per-lane appender ID that wrote the last batch read.
    ///
    /// Each appender in a queue has a unique lane ID (0–255). Payload files
    /// are per-appender (`payload_<id>`). This value is populated by the most
    /// recent `read()` / `read_first()` / `read_last()` call and is valid
    /// only when the returned status was `BATCH_ENTRY_AVAILABLE`.
    ///
    /// # Panics
    ///
    /// Panics if the reader has already been closed.
    pub fn appender_id(&self) -> u8 {
        unsafe { (*self.ctx.get()).appender_id }
    }

    /// Read the batch at the given index.
    pub fn read(&self, index: i64) -> Result<Option<BatchMessage<'_>>, MmapError> {
        let ctx = self.ctx.get();
        unsafe { ffi::indexed_batch_reader_read(self.inner, index, ctx) };
        match unsafe { (*ctx).status } {
            ffi::BATCH_ENTRY_AVAILABLE => Ok(Some(BatchMessage {
                ctx: unsafe { &*ctx },
                _phantom: PhantomData,
            })),
            ffi::BATCH_ENTRY_END_OF_STREAM | ffi::BATCH_ENTRY_ARCHIVED => Ok(None),
            ffi::BATCH_ENTRY_ERROR => Err(MmapError::MapTimedOut),
            _ => Err(MmapError::Corrupt("unknown batch entry status".into())),
        }
    }

    /// Read the first unarchived batch.
    pub fn read_first(&self) -> Result<Option<BatchMessage<'_>>, MmapError> {
        let ctx = self.ctx.get();
        unsafe { ffi::indexed_batch_reader_read_first(self.inner, ctx) };
        match unsafe { (*ctx).status } {
            ffi::BATCH_ENTRY_AVAILABLE => Ok(Some(BatchMessage {
                ctx: unsafe { &*ctx },
                _phantom: PhantomData,
            })),
            ffi::BATCH_ENTRY_END_OF_STREAM | ffi::BATCH_ENTRY_ARCHIVED => Ok(None),
            ffi::BATCH_ENTRY_ERROR => Err(MmapError::MapTimedOut),
            _ => Err(MmapError::Corrupt("unknown batch entry status".into())),
        }
    }

    /// Read the last committed batch.
    pub fn read_last(&self) -> Result<Option<BatchMessage<'_>>, MmapError> {
        let ctx = self.ctx.get();
        unsafe { ffi::indexed_batch_reader_read_last(self.inner, ctx) };
        match unsafe { (*ctx).status } {
            ffi::BATCH_ENTRY_AVAILABLE => Ok(Some(BatchMessage {
                ctx: unsafe { &*ctx },
                _phantom: PhantomData,
            })),
            ffi::BATCH_ENTRY_END_OF_STREAM | ffi::BATCH_ENTRY_ARCHIVED => Ok(None),
            ffi::BATCH_ENTRY_ERROR => Err(MmapError::MapTimedOut),
            _ => Err(MmapError::Corrupt("unknown batch entry status".into())),
        }
    }

    /// Check availability of the batch at `index` without reading it.
    pub fn entry_status(&self, index: i64) -> BatchEntryStatus {
        let s = unsafe { ffi::indexed_batch_reader_entry_status(self.inner, index) };
        match s {
            ffi::BATCH_ENTRY_AVAILABLE => BatchEntryStatus::Available,
            ffi::BATCH_ENTRY_END_OF_STREAM => BatchEntryStatus::EndOfStream,
            ffi::BATCH_ENTRY_ARCHIVED => BatchEntryStatus::Archived,
            ffi::BATCH_ENTRY_ERROR => BatchEntryStatus::Error,
            _ => BatchEntryStatus::Error,
        }
    }

    /// The highest committed batch index.
    pub fn last_index(&self) -> i64 {
        unsafe { ffi::indexed_batch_reader_last_index(self.inner) }
    }

    /// The lowest readable batch index.
    pub fn start_index(&self) -> i64 {
        unsafe { ffi::indexed_batch_reader_start_index(self.inner) }
    }
}

impl Drop for IndexedBatchReader {
    fn drop(&mut self) {
        unsafe { ffi::indexed_batch_reader_close(self.inner) }
    }
}

/// A zero-copy view of a single batch.
pub struct BatchMessage<'a> {
    ctx: &'a ffi::BatchReadingContext,
    _phantom: PhantomData<&'a IndexedBatchReader>,
}

impl BatchMessage<'_> {
    /// The payload file offset of the batch's message count field.
    pub fn batch_offset(&self) -> i64 {
        self.ctx.batch_offset
    }

    /// The number of messages in this batch.
    pub fn message_count(&self) -> i32 {
        self.ctx.message_count
    }

    /// The batch index.
    pub fn index(&self) -> i64 {
        self.ctx.index
    }
}

impl std::fmt::Debug for BatchMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchMessage")
            .field("index", &self.index())
            .field("message_count", &self.message_count())
            .finish()
    }
}
