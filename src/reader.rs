//! Random-access reader — read any message by index without advancing a cursor.
//!
//! `IndexedReader` provides zero-copy access to committed messages. Each
//! `read()` returns a [`ReadResult`]; an available entry is a `Message<'_>`
//! whose data pointer borrows from the reader's mapped memory. The borrow prevents another read until the message's last use.
//!
//! For long-lived access, call `Message::to_vec()` to produce an owned copy.
//!
//! Thread-confined (`!Send + !Sync`). One per reader thread.

use std::marker::PhantomData;

use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::types::EntryStatus;

/// Outcome of a random-access read (`IndexedReader` and `IndexedBatchReader`).
///
/// Mirrors the C entry statuses AVAILABLE / END_OF_STREAM / ARCHIVED; the
/// fourth, ERROR, is returned as `Err(MmapError)`. `EndOfStream` and
/// `Archived` call for different actions: an index that is not yet written
/// may become readable later, while an archived index lies below the
/// queue's start index and stays unreadable unless an archiver undo lowers
/// that index again.
#[must_use = "a read can report EndOfStream or Archived instead of an entry"]
#[derive(Debug)]
pub enum ReadResult<T> {
    /// The entry exists.
    Available(T),
    /// The index is not yet written.
    EndOfStream,
    /// The index is below the queue's start index.
    Archived,
}

impl<T> ReadResult<T> {
    /// The entry if available; `None` for `EndOfStream` and `Archived`.
    pub fn available(self) -> Option<T> {
        match self {
            ReadResult::Available(entry) => Some(entry),
            ReadResult::EndOfStream | ReadResult::Archived => None,
        }
    }

    /// Whether the read reported `Archived`.
    pub fn is_archived(&self) -> bool {
        matches!(self, ReadResult::Archived)
    }
}

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
    /// Returns `Ok(ReadResult::Available(Message))` if the entry exists,
    /// `Ok(ReadResult::EndOfStream)` if it is not yet written,
    /// `Ok(ReadResult::Archived)` if it is below the queue's start index,
    /// `Err(MapTimedOut)` on mapping timeout.
    pub fn read(&mut self, index: i64) -> Result<ReadResult<Message<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The owned reader and retained runtime are live. The exclusive reader borrow
        // prevents another read/close while C fills ctx or while the returned message is used.
        unsafe {
            ffi::mmapq_indexed_reader_read(self.inner, index, ctx);
        }
        match ctx.status {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(ReadResult::Available(Message {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM => Ok(ReadResult::EndOfStream),
            ffi::MMAPQ_ENTRY_ARCHIVED => Ok(ReadResult::Archived),
            // SAFETY: The preceding read left a live handle; last_error only returns its error code.
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Read the first unarchived entry (at start_index). Outcomes as for [`read`](Self::read).
    pub fn read_first(&mut self) -> Result<ReadResult<Message<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The owned reader and retained runtime are live. The exclusive reader borrow
        // prevents another read/close while C fills ctx or while the returned message is used.
        unsafe {
            ffi::mmapq_indexed_reader_read_first(self.inner, ctx);
        }
        match ctx.status {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(ReadResult::Available(Message {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM => Ok(ReadResult::EndOfStream),
            ffi::MMAPQ_ENTRY_ARCHIVED => Ok(ReadResult::Archived),
            // SAFETY: The preceding read left a live handle; last_error only returns its error code.
            ffi::MMAPQ_ENTRY_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_reader_last_error(self.inner) })),
            _ => Err(MmapError::Corrupt("unknown entry status".into())),
        }
    }

    /// Read the last committed entry. Outcomes as for [`read`](Self::read).
    pub fn read_last(&mut self) -> Result<ReadResult<Message<'_>>, MmapError> {
        let ctx = &mut self.ctx;
        // SAFETY: The owned reader and retained runtime are live. The exclusive reader borrow
        // prevents another read/close while C fills ctx or while the returned message is used.
        unsafe {
            ffi::mmapq_indexed_reader_read_last(self.inner, ctx);
        }
        match ctx.status {
            ffi::MMAPQ_ENTRY_AVAILABLE => Ok(ReadResult::Available(Message {
                ctx,
                _phantom: PhantomData,
            })),
            ffi::MMAPQ_ENTRY_END_OF_STREAM => Ok(ReadResult::EndOfStream),
            ffi::MMAPQ_ENTRY_ARCHIVED => Ok(ReadResult::Archived),
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

#[cfg(test)]
mod tests {
    use crate::queue::QueueConfig;
    use crate::runtime::Runtime;
    use crate::types::EntryStatus;
    use crate::{IndexedQueue, ReadResult};

    /// POLL-ARCH-01 (outcome level), E3: an index below a moved start_index
    /// reads as `ReadResult::Archived` — distinct from `EndOfStream` for an
    /// index not yet written — and `entry_status` reports
    /// `EntryStatus::Archived`. The mid-call race is pinned in C
    /// (`test/test_poll_arch_pins.c`); the
    /// Rust crate builds the C mirror without test hooks by owner ruling,
    /// so this is the outcome-level pin: archive past the index, then
    /// read/entry_status it.
    // covers: IndexedQueue::create
    // covers: IndexedQueue::create_reader
    // covers: IndexedQueue::create_archiver
    // covers: IndexedArchiver::archive_up_to_index
    // covers: IndexedReader::read
    // covers: IndexedReader::read_first
    // covers: IndexedReader::entry_status
    // covers: ReadResult::is_archived
    // covers: Message::index
    // covers: Message::data
    #[test]
    fn test_read_archived_below_moved_start_index() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-reader").join("read_archived");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt)
                .archivable(true),
        )
        .unwrap();
        {
            let mut a = q.create_appender().unwrap();
            a.append(b"m0").unwrap();
            a.append(b"m1").unwrap();
        }
        let mut r = q.create_reader().unwrap();
        assert_eq!(r.read(0).unwrap().available().unwrap().data(), b"m0");
        assert!(matches!(r.read(2).unwrap(), ReadResult::EndOfStream)); // not yet written
        let mut arch = q.create_archiver().unwrap();
        arch.archive_up_to_index(1).unwrap();
        drop(arch);
        assert_eq!(r.entry_status(0), EntryStatus::Archived);
        let below = r.read(0).unwrap();
        assert!(below.is_archived());
        assert!(matches!(below, ReadResult::Archived));
        assert!(matches!(r.read_first().unwrap(), ReadResult::Available(m) if m.index() == 1));
        assert_eq!(r.read(1).unwrap().available().unwrap().data(), b"m1"); // above the line, still readable
        assert!(matches!(r.read(2).unwrap(), ReadResult::EndOfStream)); // still not written, not archived
    }

    /// E3: `available()` keeps only the entry; `is_archived()` is true only
    /// for `Archived`.
    // covers: ReadResult::available
    #[test]
    fn test_read_result_helpers() {
        assert_eq!(ReadResult::Available(7).available(), Some(7));
        assert_eq!(ReadResult::<i32>::EndOfStream.available(), None);
        assert_eq!(ReadResult::<i32>::Archived.available(), None);
        assert!(ReadResult::<i32>::Archived.is_archived());
        assert!(!ReadResult::<i32>::EndOfStream.is_archived());
        assert!(!ReadResult::Available(7).is_archived());
    }

    /// read_last delivers the newest entry; Message accessors report the
    /// exact payload. as_ptr exposes the raw handle (non-null while open).
    // covers: IndexedReader::read_last
    // covers: IndexedReader::as_ptr
    // covers: Message::length
    // covers: Message::to_vec
    #[test]
    fn test_read_last_and_message_accessors() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-reader").join("read_last");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        {
            let mut a = q.create_appender().unwrap();
            for i in 0..5 {
                let msg = format!("msg{i}");
                assert_eq!(a.append(msg.as_bytes()).unwrap(), i);
            }
        }
        let mut r = q.create_reader().unwrap();
        assert!(!r.as_ptr().is_null(), "open reader has a live handle");
        let msg = r.read_last().unwrap().available().expect("last entry exists");
        assert_eq!(msg.index(), 4);
        assert_eq!(msg.length(), 4);
        assert_eq!(msg.data(), b"msg4");
        assert_eq!(msg.to_vec(), b"msg4".to_vec());
    }
}
