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

use crate::error::{map_rc, MmapError};
use crate::ffi;

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
        let n = unsafe {
            ffi::indexed_appender_append(self.inner, data.as_ptr() as *const _, data.len() as i32)
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
        let n = unsafe {
            ffi::indexed_appender_append_uncommitted(
                self.inner,
                data.as_ptr() as *const _,
                data.len() as i32,
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
        let n = unsafe { ffi::indexed_appender_commit(self.inner, payload_offset) };
        if n >= 0 {
            Ok(n)
        } else {
            Err(map_rc(n as i32))
        }
    }

    /// The appender's per-lane ID (0–255).
    pub fn id(&self) -> u8 {
        unsafe { ffi::indexed_appender_id(self.inner) }
    }

    /// The current (last committed) index.
    pub fn current_index(&self) -> i64 {
        unsafe { ffi::indexed_appender_current_index(self.inner) }
    }

    /// The start offset of the appender's payload file.
    pub fn start_offset(&self) -> i64 {
        unsafe { ffi::indexed_appender_start_offset(self.inner) }
    }
}

impl Drop for IndexedAppender {
    fn drop(&mut self) {
        unsafe { ffi::indexed_appender_close(self.inner) }
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

    #[test]
    fn test_append_and_read_back() {
        let (q, _rt) = setup_queue("test_app_single");
        let mut a = q.create_appender().unwrap();
        let idx = a.append(b"hello").unwrap();
        assert!(idx >= 0, "append should return a valid index");
    }

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
                .region_size(4096)
                .ring_size(4)
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut a = q.create_appender().unwrap();

        let ok = a.append(b"small").unwrap();
        assert_eq!(ok, 0);

        let big = vec![0u8; 4096];
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
                .region_size(4096)
                .ring_size(4)
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut a = q.create_appender().unwrap();

        let big = vec![0u8; 4096];
        let err = a.append_uncommitted(&big).unwrap_err();
        assert!(matches!(err, MmapError::InvalidArgument));

        drop(a);
        drop(q);
        drop(rt);
    }
}
