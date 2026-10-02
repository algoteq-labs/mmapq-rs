//! Batch reader — random-access batch reader.
//!
//! Thread-confined (`!Send + !Sync`). One per reader thread.

use std::marker::PhantomData;

use crate::batch::BatchMessageIter;
use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::types::BatchEntryStatus;

/// Random-access batch reader. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchReader {
    pub(crate) inner: *mut ffi::IndexedBatchReader,
    /// Keeps the read runtime alive for this participant's lifetime.
    /// Forwarded from the parent queue at creation; None = sync mapping.
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) ctx: ffi::BatchReadingContext,
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
    /// only when the returned status was `MMAPQ_BATCH_ENTRY_AVAILABLE`.
    ///
    /// # Panics
    ///
    /// Panics if the reader has already been closed.
    pub fn appender_id(&self) -> u8 {
        self.ctx.appender_id
    }

    /// Read the batch at the given index.
    pub fn read(&mut self, index: i64) -> Result<Option<BatchMessage<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The live reader retains its runtime and ctx is a writable owned field.
        // The exclusive reader borrow prevents remapping while a BatchMessage is used.
        unsafe { ffi::mmapq_indexed_batch_reader_read(self.inner, index, ctx) };
        match ctx.status {
            ffi::MMAPQ_BATCH_ENTRY_AVAILABLE => Ok(Some(BatchMessage {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_BATCH_ENTRY_END_OF_STREAM | ffi::MMAPQ_BATCH_ENTRY_ARCHIVED => Ok(None),
            // SAFETY: The reader remains live after the failed read; this only fetches its error code.
            ffi::MMAPQ_BATCH_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_batch_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown batch entry status".into())),
        }
    }

    /// Read the first unarchived batch.
    pub fn read_first(&mut self) -> Result<Option<BatchMessage<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The live reader retains its runtime and ctx is a writable owned field.
        // The exclusive reader borrow prevents remapping while a BatchMessage is used.
        unsafe { ffi::mmapq_indexed_batch_reader_read_first(self.inner, ctx) };
        match ctx.status {
            ffi::MMAPQ_BATCH_ENTRY_AVAILABLE => Ok(Some(BatchMessage {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_BATCH_ENTRY_END_OF_STREAM | ffi::MMAPQ_BATCH_ENTRY_ARCHIVED => Ok(None),
            // SAFETY: The reader remains live after the failed read; this only fetches its error code.
            ffi::MMAPQ_BATCH_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_batch_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown batch entry status".into())),
        }
    }

    /// Read the last committed batch.
    pub fn read_last(&mut self) -> Result<Option<BatchMessage<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The live reader retains its runtime and ctx is a writable owned field.
        // The exclusive reader borrow prevents remapping while a BatchMessage is used.
        unsafe { ffi::mmapq_indexed_batch_reader_read_last(self.inner, ctx) };
        match ctx.status {
            ffi::MMAPQ_BATCH_ENTRY_AVAILABLE => Ok(Some(BatchMessage {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_BATCH_ENTRY_END_OF_STREAM | ffi::MMAPQ_BATCH_ENTRY_ARCHIVED => Ok(None),
            // SAFETY: The reader remains live after the failed read; this only fetches its error code.
            ffi::MMAPQ_BATCH_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_batch_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown batch entry status".into())),
        }
    }

    /// Check availability of the batch at `index` without reading it.
    pub fn entry_status(&mut self, index: i64) -> BatchEntryStatus {
        // SAFETY: Exclusive access to the live reader excludes batch views while C performs lookup.
        let s = unsafe { ffi::mmapq_indexed_batch_reader_entry_status(self.inner, index) };
        match s {
            ffi::MMAPQ_BATCH_ENTRY_AVAILABLE => BatchEntryStatus::Available,
            ffi::MMAPQ_BATCH_ENTRY_END_OF_STREAM => BatchEntryStatus::EndOfStream,
            ffi::MMAPQ_BATCH_ENTRY_ARCHIVED => BatchEntryStatus::Archived,
            ffi::MMAPQ_BATCH_ENTRY_ERROR => BatchEntryStatus::Error,
            _ => BatchEntryStatus::Error,
        }
    }

    /// The highest committed batch index.
    pub fn last_index(&mut self) -> i64 {
        // SAFETY: The exclusive reader borrow excludes batch views while C searches header mappings.
        unsafe { ffi::mmapq_indexed_batch_reader_last_index(self.inner) }
    }

    /// The lowest readable batch index.
    pub fn start_index(&mut self) -> i64 {
        // SAFETY: The live reader is exclusively borrowed while C may initialize metadata.
        unsafe { ffi::mmapq_indexed_batch_reader_start_index(self.inner) }
    }
}

impl Drop for IndexedBatchReader {
    fn drop(&mut self) {
        // SAFETY: Drop owns this handle uniquely; borrowed batches are gone and the
        // runtime field is still alive while C releases its mappings.
        unsafe { ffi::mmapq_indexed_batch_reader_close(self.inner) }
    }
}

/// A zero-copy view of a single batch.
///
/// Obtained from `IndexedBatchReader::read()` and friends.
/// Exclusively borrows the reader, preventing another read while this view is used.
pub struct BatchMessage<'a> {
    ctx: &'a mut ffi::BatchReadingContext,
    _phantom: PhantomData<&'a mut IndexedBatchReader>,
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

impl BatchMessage<'_> {
    /// Iterate over messages in this batch.
    ///
    /// Returns a `BatchMessageIter` yielding `(&[u8], u8)` pairs —
    /// the message payload (zero-copy, lifetime-bound to this batch)
    /// and the appender ID that wrote the batch.
    ///
    /// Each call resets the underlying C iterator, making repeated
    /// iterations safe.
    ///
    /// A mutable borrow prevents overlapping cursors and reader repositioning.
    pub fn iter(&mut self) -> BatchMessageIter<'_> {
        // SAFETY: BatchMessage exists only after an AVAILABLE read and exclusively borrows
        // its reader. This reborrow excludes other cursors and live payload steps;
        // reset uses that context's validated batch metadata, and from_raw's lifetime
        // is limited to this borrow of self, not an unconstrained caller lifetime.
        unsafe {
            let ctx_ptr = self.ctx as *mut ffi::BatchReadingContext;
            ffi::mmapq_batch_iterator_init(
                (*ctx_ptr).iterator,
                (*ctx_ptr).batch_offset,
                (*ctx_ptr).message_count,
                0,
            );
            BatchMessageIter::from_raw(ctx_ptr)
        }
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
