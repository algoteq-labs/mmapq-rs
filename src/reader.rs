//! Random-access reader — read any message by index without advancing a cursor.
//!
//! `IndexedReader` provides zero-copy access to committed messages. Each
//! `read()` returns a `Message<'_>` whose data pointer borrows from the
//! reader's mapped memory. The borrow prevents another read until the message's last use.
//!
//! For long-lived access, call `Message::to_vec()` to produce an owned copy.
//!
//! Thread-confined (`!Send + !Sync`). One per reader thread.

use std::marker::PhantomData;

use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::types::EntryStatus;

/// Random-access reader. Thread-confined (`!Send + !Sync`).
pub struct IndexedReader {
    /* field order: `inner` first so close runs while the runtime is alive */
    pub(crate) inner: *mut ffi::IndexedReader,
    /// Keeps the read runtime alive for this participant's lifetime,
    /// independently of the queue (UAF regression: tests/runtime_uaf.rs).
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) ctx: ffi::ReadingContext,
}

impl IndexedReader {
    /// Return the raw C pointer (for transfer module use).
    pub fn as_ptr(&self) -> *mut ffi::IndexedReader {
        self.inner
    }

    /// Read the entry at the given index.
    ///
    /// Returns `Ok(Some(Message))` if the entry exists,
    /// `Ok(None)` if not yet written or archived,
    /// `Err(MapTimedOut)` on mapping timeout.
    pub fn read(&mut self, index: i64) -> Result<Option<Message<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The owned reader and retained runtime are live. The exclusive reader borrow
        // prevents another read/close while C fills ctx or while the returned message is used.
        unsafe {
            ffi::mmapq_indexed_reader_read(self.inner, index, ctx);
        }
        match ctx.status {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(Some(Message {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM | ffi::MMAPQ_ENTRY_ARCHIVED => Ok(None),
            // SAFETY: The preceding read left a live handle; last_error only returns its error code.
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Read the first unarchived entry (at start_index).
    pub fn read_first(&mut self) -> Result<Option<Message<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The owned reader and retained runtime are live. The exclusive reader borrow
        // prevents another read/close while C fills ctx or while the returned message is used.
        unsafe {
            ffi::mmapq_indexed_reader_read_first(self.inner, ctx);
        }
        match ctx.status {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(Some(Message {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM | ffi::MMAPQ_ENTRY_ARCHIVED => Ok(None),
            // SAFETY: The preceding read left a live handle; last_error only returns its error code.
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Read the last committed entry.
    pub fn read_last(&mut self) -> Result<Option<Message<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The owned reader and retained runtime are live. The exclusive reader borrow
        // prevents another read/close while C fills ctx or while the returned message is used.
        unsafe {
            ffi::mmapq_indexed_reader_read_last(self.inner, ctx);
        }
        match ctx.status {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(Some(Message {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM | ffi::MMAPQ_ENTRY_ARCHIVED => Ok(None),
            // SAFETY: The preceding read left a live handle; last_error only returns its error code.
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Check availability of the entry at `index` without reading it.
    pub fn entry_status(&mut self, index: i64) -> EntryStatus {
        // SAFETY: The exclusive reader borrow prevents a live Message while this C lookup can remap.
        let s = unsafe { ffi::mmapq_indexed_reader_entry_status(self.inner, index) };
        match s {
            ffi::MMAPQ_ENTRY_AVAILABLE => EntryStatus::Available,
            ffi::MMAPQ_ENTRY_END_OF_STREAM => EntryStatus::EndOfStream,
            ffi::MMAPQ_ENTRY_ARCHIVED => EntryStatus::Archived,
            ffi::MMAPQ_ENTRY_ERROR => EntryStatus::Error,
            other => { debug_assert!(false, "mmapq_indexed_reader_entry_status: unknown status {}", other); EntryStatus::Error },
        }
    }

    /// The highest committed index, or -1 if the queue is empty.
    pub fn last_index(&mut self) -> i64 {
        // SAFETY: The exclusive borrow prevents outstanding messages while C searches header mappings.
        unsafe { ffi::mmapq_indexed_reader_last_index(self.inner) }
    }

    /// The lowest readable index (0 for non-archivable queues).
    pub fn start_index(&mut self) -> i64 {
        // SAFETY: The live reader is exclusively borrowed while C may initialize its metadata.
        unsafe { ffi::mmapq_indexed_reader_start_index(self.inner) }
    }
}

impl Drop for IndexedReader {
    fn drop(&mut self) {
        // SAFETY: Drop uniquely owns the reader; no Message borrow remains, and
        // its runtime field stays alive until C has released the mappings.
        unsafe { ffi::mmapq_indexed_reader_close(self.inner) }
    }
}

/// A zero-copy view of a single queue message.
///
/// The data pointer borrows from the reader's mapped memory and is valid
/// while the message exclusively borrows the reader. For long-lived
/// access, call `.to_vec()`.
pub struct Message<'a> {
    ctx: &'a ffi::ReadingContext,
    _phantom: PhantomData<&'a mut IndexedReader>,
}

impl Message<'_> {
    /// The message payload bytes — zero-copy borrow into mapped memory.
    pub fn data(&self) -> &[u8] {
        if self.ctx.data.is_null() {
            return &[];
        }
        // SAFETY: An AVAILABLE read validated positive length within one live mapping.
        // Message retains the reader's exclusive borrow, preventing remap/close.
        // Committed payload bytes must not be externally rewritten or truncated.
        unsafe { std::slice::from_raw_parts(self.ctx.data as *const u8, self.ctx.length as usize) }
    }

    /// Produce an owned copy of the payload.
    pub fn to_vec(&self) -> Vec<u8> {
        self.data().to_vec()
    }

    /// The queue index of this message.
    pub fn index(&self) -> i64 {
        self.ctx.index
    }

    /// The appender ID that wrote this message.
    pub fn appender_id(&self) -> u8 {
        self.ctx.appender_id
    }

    /// The payload length in bytes.
    pub fn length(&self) -> i32 {
        self.ctx.length
    }
}

impl std::fmt::Debug for Message<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("index", &self.index())
            .field("appender_id", &self.appender_id())
            .field("length", &self.length())
            .finish()
    }
}
