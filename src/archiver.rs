//! Queue archiver — advances the start index and manages file lifecycle.
//!
//! An `IndexedArchiver` is created from an archivable queue. It can:
//! - **Forward archiving** (`index > current start_index`): walk headers
//!   backwards from `index - 1` to the current start index, record each
//!   appender's last committed payload position, advance past the committed
//!   message to find the next write offset, write that offset to
//!   appender_meta, and commit the new start_index atomically in queue meta.
//! - **Backward / undo archiving** (`index < current start_index`): walk
//!   headers forward from `index` to the current start index. Validate that
//!   all header and payload files covering that range still exist before
//!   modifying anything. Record the first payload position per appender,
//!   write to appender_meta, commit the lower start_index.
//! - **No-op** when `index == current start_index`.
//!
//! The archiver does **not** delete files — `archivable_files()` returns the
//! list of files fully covered by the archive boundary (safe to delete, move,
//! or compress), but the caller decides what to do with them.
//!
//! Thread-confined (`!Send + !Sync`). One per archiver thread.

use std::ffi::CStr;
use std::marker::PhantomData;

use crate::error::{map_rc, MmapError};
use crate::ffi;

/// Archiver for managing the queue's start index and archivable files.
///
/// Thread-confined (`!Send + !Sync`). One per archiver thread.
pub struct IndexedArchiver {
    pub(crate) inner: *mut ffi::IndexedArchiver,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedArchiver {
    /// Advance the start index past the given index.
    ///
    /// **Forward** (`index > current start_index`): walks headers backwards,
    /// records the last committed payload position per appender, advances
    /// past the committed message to find the next write offset, and commits
    /// the new start_index atomically. After this call, entries at or below
    /// `index` are no longer accessible via poller or reader.
    ///
    /// **Backward / undo** (`index < current start_index`): validates that
    /// all header and payload files covering the range still exist, then
    /// commits the lower start_index. Only possible if the backing files
    /// have not been deleted.
    ///
    /// **No-op** when `index == current start_index`.
    ///
    /// The backing files can be listed via `archivable_files()`.
    pub fn archive_up_to_index(&mut self, index: i64) -> Result<(), MmapError> {
        let rc = unsafe {
            ffi::indexed_archiver_archive_up_to_index(self.inner, index)
        };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(())
    }

    /// List files whose entire content is covered by the archive boundary
    /// and are safe to delete, move, or compress.
    ///
    /// Only returns paths to files that actually exist on disk. The caller
    /// is responsible for any file management (delete, compress, etc.).
    pub fn archivable_files(&self) -> Result<Vec<String>, MmapError> {
        const MAX: usize = 1024;
        let mut paths: [*mut libc::c_char; MAX] = [std::ptr::null_mut(); MAX];
        let count = unsafe {
            ffi::indexed_archiver_archivable_files(self.inner, paths.as_mut_ptr(), MAX as i32)
        };
        if count <= 0 {
            return Ok(Vec::new());
        }
        let mut result = Vec::with_capacity(count as usize);
        for i in 0..count as usize {
            let s = unsafe { CStr::from_ptr(paths[i]).to_string_lossy().into_owned() };
            unsafe { libc::free(paths[i] as *mut libc::c_void) }
            result.push(s);
        }
        Ok(result)
    }

    /// Return a human-readable summary of the queue's file layout.
    pub fn print_summary(&self) -> String {
        let mut buf = [0i8; 4096];
        let n = unsafe { ffi::indexed_archiver_print_summary(self.inner, buf.as_mut_ptr(), 4096) };
        if n > 0 {
            unsafe { CStr::from_ptr(buf.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        } else {
            String::new()
        }
    }

    /// Return the start offset for a given appender, or `None` if the
    /// appender has not written any messages.
    pub fn start_offset(&self, appender_id: u8) -> Result<Option<i64>, MmapError> {
        let mut offset: i64 = 0;
        let ok = unsafe {
            ffi::indexed_archiver_start_offset(self.inner, appender_id, &mut offset as *mut i64)
        };
        if ok != 0 {
            Ok(Some(offset))
        } else {
            Ok(None)
        }
    }
}

impl Drop for IndexedArchiver {
    fn drop(&mut self) {
        unsafe { ffi::indexed_archiver_close(self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use crate::queue::QueueConfig;
    use crate::runtime::Runtime;

    fn setup(name: &str) -> (crate::IndexedQueue, crate::IndexedAppender, crate::IndexedPoller, Runtime) {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-archiver").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");

        // Create an archivable queue — we need to call indexed_queue_create
        // directly with an archivable meta file. For now use the public API
        // (non-archivable) for a basic smoke test.
        let q = crate::IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let a = q.create_appender().unwrap();
        let p = q.create_poller().unwrap();
        (q, a, p, rt)
    }

    #[test]
    fn test_archiver_create_and_close() {
        let (q, _a, _p, _rt) = setup("test_arch_create");
        let arch = q.create_archiver().unwrap();
        // Smoke test: create and close without crash.
        drop(arch);
        drop(q);
    }

    #[test]
    fn test_archiver_print_summary() {
        let (q, _a, _p, _rt) = setup("test_arch_summary");
        let arch = q.create_archiver().unwrap();
        let s = arch.print_summary();
        assert!(!s.is_empty(), "archiver summary should not be empty");
        drop(arch);
        drop(q);
    }

    // archivable_files test removed: requires archivable=1 meta which is not
    // exposed through the public Rust API. Tested in C (test_indexed_archiver.c).
}
