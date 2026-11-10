//! Message appender — writes and commits messages to a queue.
//!
//! Each `IndexedAppender` owns a per-thread payload file position and
//! appender ID. It is thread-confined (`!Send + !Sync`).
//!
//! The append lifecycle:
//! 1. `append(data)` — writes `[length_prefix][payload]` to the payload
//!    file and atomically commits a header entry, making the message visible.
//! 2. `append_uncommitted(data)` — writes the payload but does NOT commit.
//!    The message is invisible until `commit(payload_offset)` is called.
//! 3. `commit(payload_offset)` — commits a previously uncommitted payload.

use std::marker::PhantomData;

use crate::error::{checked_len, map_rc, MmapError};
use crate::ffi;
use crate::uncommitted::UncommittedIter;

/// A queue appender — write and commit messages.
///
/// Thread-confined (`!Send + !Sync`). One per writer thread.
pub struct IndexedAppender {
    /* field order: `inner` first so close runs while the runtime is alive */
    pub(crate) inner: *mut ffi::IndexedAppender,
    /// Keeps the write runtime alive for this participant's lifetime,
    /// independently of the queue (UAF regression: tests/runtime_uaf.rs).
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedAppender {
    /// Append a message and commit it atomically.
    ///
    /// Returns the queue index at which the message was committed,
    /// or `Err(MapTimedOut)` on mapping timeout.
    /// Returns `Err(InvalidArgument)` if `data` is empty.
    pub fn append(&mut self, data: &[u8]) -> Result<i64, MmapError> {
        if data.is_empty() {
            return Err(MmapError::InvalidArgument);
        }
        let len = checked_len(data.len())?;
        // SAFETY: The live, thread-confined appender retains its runtime; checked_len
        // bounds the input slice, which C copies before this call returns.
        let n = unsafe {
            ffi::mmapq_indexed_appender_append(self.inner, data.as_ptr() as *const _, len)
        };
        if n >= 0 {
            Ok(n)
        } else {
            Err(map_rc(n as i32))
        }
    }

    /// Write the payload without committing the header.
    ///
    /// The message is invisible to pollers and readers until `commit()`
    /// is called with the returned payload offset.
    ///
    /// Returns the payload offset, or `Err(MapTimedOut)` on mapping timeout.
    /// Returns `Err(InvalidArgument)` if `data` is empty.
    pub fn append_uncommitted(&mut self, data: &[u8]) -> Result<i64, MmapError> {
        if data.is_empty() {
            return Err(MmapError::InvalidArgument);
        }
        let len = checked_len(data.len())?;
        // SAFETY: The live, thread-confined appender retains its runtime; checked_len
        // bounds the input slice, which C copies before this call returns.
        let n = unsafe {
            ffi::mmapq_indexed_appender_append_uncommitted(
                self.inner,
                data.as_ptr() as *const _,
                len,
            )
        };
        if n >= 0 {
            Ok(n)
        } else {
            Err(map_rc(n as i32))
        }
    }

    /// Commit a previously uncommitted payload.
    ///
    /// `payload_offset` is the value returned by `append_uncommitted()`.
    /// Returns the queue index assigned to this message,
    /// or an `MMAPQ_*` error code on failure.
    pub fn commit(&mut self, payload_offset: i64) -> Result<i64, MmapError> {
        // SAFETY: The live appender is exclusively borrowed. C encodes the numeric offset;
        // this operation does not dereference it as a Rust pointer.
        let n = unsafe { ffi::mmapq_indexed_appender_commit(self.inner, payload_offset) };
        if n >= 0 {
            Ok(n)
        } else {
            Err(map_rc(n as i32))
        }
    }

    /// Iterate this appender's uncommitted payloads (snapshot at call time —
    /// appends/commits after the call are invisible).
    ///
    /// Yields `(offset, payload)` pairs from the archive line onward; commit
    /// a found offset with `commit()` — any subset, any order, same session
    /// or after rejoin. Not committing = skip; an offset archiving has
    /// overtaken returns `Err(InvalidArgument)`. The `&mut self` borrow
    /// enforces the no-append-while-iterating rule at compile time.
    /// Archiving may remove messages the walk has not reached, even after
    /// an undo. Returns `Err(Corrupt)` on a zero archive line — a corrupt
    /// appender meta (pre-0.2.0 queues are refused at open with
    /// `SchemaMismatch`). After a process crash at most the record that was
    /// being written — the lane's highest-offset uncommitted record at that
    /// crash — can be torn (single writer, sequential writes, the length
    /// prefix is written before the data); every record written before it is
    /// byte-intact. A record that is not committed is never removed: one
    /// torn at an earlier crash reappears in every later walk at an offset
    /// that is no longer the highest. Discard the highest-offset record
    /// and re-request or re-append it; validate the others before
    /// committing (the library has no payload integrity check) or remember
    /// the offsets discarded earlier.
    pub fn uncommitted_iter(&mut self) -> Result<UncommittedIter<'_>, MmapError> {
        let mut it = std::ptr::null_mut();
        // SAFETY: The live, thread-confined appender is exclusively borrowed; it is a
        // writable output local. MMAPQ_PAYLOAD_ITERATOR_NOT_SET for both bounds selects
        // the full archive-line-to-end walk.
        let rc = unsafe {
            ffi::mmapq_indexed_appender_uncommitted_iterator_checked(
                self.inner,
                ffi::MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
                ffi::MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
                &mut it,
            )
        };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        // The factory returned 0, so `it` is the live appender-owned iterator;
        // the returned guard carries this exclusive borrow for its lifetime.
        Ok(UncommittedIter::new(it))
    }

    /// The appender's per-lane ID (0–255).
    pub fn id(&self) -> u8 {
        // SAFETY: The owned handle stays live under this borrow; C only reads the appender ID.
        unsafe { ffi::mmapq_indexed_appender_id(self.inner) }
    }

    /// The current (last committed) index.
    pub fn current_index(&self) -> i64 {
        // SAFETY: The thread-confined handle stays live; this query returns a scalar, not a borrow.
        unsafe { ffi::mmapq_indexed_appender_current_index(self.inner) }
    }

    /// The appender's payload boundary (`start_offset`): payload files
    /// wholly below it may be reclaimed. It is a deletion boundary, not a
    /// write position; a joining appender with no live entry anchors at
    /// `max(start_offset, archive_offset)` — equal to the archive line when
    /// `start_offset` is not ahead of it (CAND-54 L(ii)). A payload appended
    /// uncommitted below the commit floor `max(start_offset, archive_offset)`
    /// can no longer be committed (`commit` returns
    /// `MmapError::InvalidArgument`; CAND-54 ruling 1).
    pub fn start_offset(&self) -> i64 {
        // SAFETY: The thread-confined handle stays live; this query returns a scalar, not a borrow.
        unsafe { ffi::mmapq_indexed_appender_start_offset(self.inner) }
    }
}

impl Drop for IndexedAppender {
    fn drop(&mut self) {
        // SAFETY: Drop uniquely owns the handle; it is closed once before the runtime field drops.
        unsafe { ffi::mmapq_indexed_appender_close(self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::MmapError;
    use crate::runtime::Runtime;
    use crate::queue::QueueConfig;
    use crate::IndexedQueue;

    fn setup_queue(name: &str) -> (IndexedQueue, Runtime) {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-appender").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("failed to create test base directory");
        let q = IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        (q, rt)
    }

    // covers: IndexedQueue::create_appender
    // covers: IndexedAppender::append
    #[test]
    fn test_append_and_read_back() {
        let (q, _rt) = setup_queue("test_app_single");
        let mut a = q.create_appender().unwrap();
        let idx = a.append(b"hello").unwrap();
        assert!(idx >= 0, "append should return a valid index");
    }

    // covers: IndexedAppender::append_uncommitted
    // covers: IndexedAppender::commit
    #[test]
    fn test_append_uncommitted_and_commit() {
        let (q, _rt) = setup_queue("test_app_uncommitted");
        let mut a = q.create_appender().unwrap();
        let offset = a.append_uncommitted(b"world").unwrap();
        assert!(offset >= 0, "uncommitted append should return payload offset");
        let idx = a.commit(offset).unwrap();
        assert!(idx >= 0, "commit should return a valid index");
    }

    #[test]
    fn test_append_rejects_oversized_message() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-appender").join("test_app_oversized");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("failed to create test base directory");
        let q = IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .region_size(32768)
                .ring_size(16) // CAND-38: 32768 is the smallest legal region (one 32 KB header block)
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut a = q.create_appender().unwrap();

        let ok = a.append(b"small").unwrap();
        assert_eq!(ok, 0);

        let big = vec![0u8; 32768];
        let err = a.append(&big).unwrap_err();
        assert!(matches!(err, MmapError::InvalidArgument));

        let next = a.append(b"after").unwrap();
        assert_eq!(next, 1);

        drop(a);
        drop(q);
        drop(rt);
    }

    #[test]
    fn test_append_uncommitted_rejects_oversized_message() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-appender").join("test_app_uncommitted_oversized");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("failed to create test base directory");
        let q = IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .region_size(32768)
                .ring_size(16) // CAND-38: 32768 is the smallest legal region (one 32 KB header block)
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut a = q.create_appender().unwrap();

        let big = vec![0u8; 32768];
        let err = a.append_uncommitted(&big).unwrap_err();
        assert!(matches!(err, MmapError::InvalidArgument));

        drop(a);
        drop(q);
        drop(rt);
    }

    /// A fresh lane's start_offset is the payload start (64) and appends do
    /// not move it — only archiving does (INITIAL_PAYLOAD_POSITION).
    // covers: IndexedAppender::start_offset
    #[test]
    fn test_appender_start_offset_is_payload_start() {
        let (q, _rt) = setup_queue("test_app_start_offset");
        let mut a = q.create_appender().unwrap();
        assert_eq!(a.start_offset(), 64);
        a.append(b"hello").unwrap();
        a.append_uncommitted(b"uncommitted").unwrap();
        assert_eq!(a.start_offset(), 64, "writes do not move the boundary");
    }
}
