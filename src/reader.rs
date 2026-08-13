//! Random-access reader — read any message by index without advancing a cursor.
//!
//! `IndexedReader` provides zero-copy access to committed messages. Each
//! `read()` returns a `Message<'_>` whose data pointer borrows from the
//! reader's mapped memory. The borrow is valid until the next `read()` call.
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
    pub(crate) ctx: std::cell::UnsafeCell<ffi::ReadingContext>,
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
    pub fn read(&self, index: i64) -> Result<Option<Message<'_>>, MmapError> {
        let ctx = self.ctx.get();
        unsafe {
            ffi::mmapq_indexed_reader_read(self.inner, index, ctx);
        }
        match unsafe { (*ctx).status } {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(Some(Message {
                ctx: unsafe { &*ctx },
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM | ffi::MMAPQ_ENTRY_ARCHIVED => Ok(None),
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Read the first unarchived entry (at start_index).
    pub fn read_first(&self) -> Result<Option<Message<'_>>, MmapError> {
        let ctx = self.ctx.get();
        unsafe {
            ffi::mmapq_indexed_reader_read_first(self.inner, ctx);
        }
        match unsafe { (*ctx).status } {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(Some(Message {
                ctx: unsafe { &*ctx },
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM | ffi::MMAPQ_ENTRY_ARCHIVED => Ok(None),
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Read the last committed entry.
    pub fn read_last(&self) -> Result<Option<Message<'_>>, MmapError> {
        let ctx = self.ctx.get();
        unsafe {
            ffi::mmapq_indexed_reader_read_last(self.inner, ctx);
        }
        match unsafe { (*ctx).status } {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(Some(Message {
                ctx: unsafe { &*ctx },
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM | ffi::MMAPQ_ENTRY_ARCHIVED => Ok(None),
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Check availability of the entry at `index` without reading it.
    pub fn entry_status(&self, index: i64) -> EntryStatus {
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
    pub fn last_index(&self) -> i64 {
        unsafe { ffi::mmapq_indexed_reader_last_index(self.inner) }
    }

    /// The lowest readable index (0 for non-archivable queues).
    pub fn start_index(&self) -> i64 {
        unsafe { ffi::mmapq_indexed_reader_start_index(self.inner) }
    }
}

impl Drop for IndexedReader {
    fn drop(&mut self) {
        unsafe { ffi::mmapq_indexed_reader_close(self.inner) }
    }
}

/// A zero-copy view of a single queue message.
///
/// The data pointer borrows from the reader's mapped memory and is valid
/// only as long as no read operation advances the ring. For long-lived
/// access, call `.to_vec()`.
pub struct Message<'a> {
    ctx: &'a ffi::ReadingContext,
    _phantom: PhantomData<&'a IndexedReader>,
}

impl Message<'_> {
    /// The message payload bytes — zero-copy borrow into mapped memory.
    pub fn data(&self) -> &[u8] {
        if self.ctx.data.is_null() {
            return &[];
        }
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
