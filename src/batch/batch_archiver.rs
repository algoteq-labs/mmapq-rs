//! Batch archiver — manages start index and archivable files for batch queues.
//!
//! Functionally identical to `IndexedArchiver` but operates on a batch queue
//! where each header entry points to a batch payload rather than a single
//! message. The key difference in the forward archiving path is how the
//! "next write offset" is calculated: for batches, the iterator must exhaust
//! the entire batch (all messages) via `mmapq_batch_iterator_exhaust()` to find
//! `nextPayloadPosition`, whereas the normal archiver reads a single length
//! field after the committed position.
//!
//! **Forward archiving** (`index > current start_index`): walks headers
//! backwards, records the last committed batch payload position per appender,
//! exhausts the batch iterator to find the next write offset, writes that
//! offset to appender_meta, and commits the new start_index atomically.
//!
//! **Backward / undo archiving** (`index < current start_index`): validates
//! all header and payload files covering the range still exist, then commits
//! the lower start_index.
//!
//! **No-op** when `index == current start_index`.
//!
//! The archiver does **not** delete files — `archivable_files()` returns the
//! list, the caller decides what to do with them.
//!
//! Thread-confined (`!Send + !Sync`). One per archiver thread.

use std::ffi::CStr;
use std::marker::PhantomData;

use crate::error::{map_rc, MmapError};
use crate::ffi;

/// Batch queue archiver. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchArchiver {
    pub(crate) inner: *mut ffi::IndexedBatchArchiver,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedBatchArchiver {
    /// Advance the start index past the given batch index.
    ///
    /// **Forward** (`index > current start_index`): walks headers backwards,
    /// records the last committed batch per appender, exhausts the batch
    /// iterator to find `nextPayloadPosition`, and commits the new
    /// start_index atomically.
    ///
    /// **Backward / undo** (`index < current start_index`): validates that
    /// all header and payload files still exist, then commits the lower
    /// start_index.
    ///
    /// **No-op** when `index == current start_index`.
    pub fn archive_up_to_index(&mut self, index: i64) -> Result<(), MmapError> {
        // SAFETY: UNRESOLVED: this archiver handle is owned and thread-confined, but its
        // C runtime pointer is borrowed from the factory and not retained here.
        // Mapping operations require that runtime to remain alive; see U2 in doc/unsafe-audit.md.
        let rc = unsafe {
            ffi::mmapq_indexed_batch_archiver_archive_up_to_index(self.inner, index)
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
    /// is responsible for any file management.
    pub fn archivable_files(&self) -> Result<Vec<String>, MmapError> {
        const MAX: usize = 1024;
        let mut paths: [*mut libc::c_char; MAX] = [std::ptr::null_mut(); MAX];
        // SAFETY: paths has MAX writable slots; C returns at most MAX owned strdup strings.
        // UNRESOLVED: lazy initialization also needs the unretained runtime (U2).
        let count = unsafe {
            ffi::mmapq_indexed_batch_archiver_archivable_files(self.inner, paths.as_mut_ptr(), MAX as i32)
        };
        if count <= 0 {
            return Ok(Vec::new());
        }
        let mut result = Vec::with_capacity(count as usize);
        for i in 0..count as usize {
            // SAFETY: A positive C count denotes initialized, non-null, NUL-terminated
            // strdup results in paths; conversion copies the string before it is freed.
            let s = unsafe { CStr::from_ptr(paths[i]).to_string_lossy().into_owned() };
            // SAFETY: This pointer came from C strdup, has been copied into Rust ownership,
            // and is freed exactly once with the matching libc allocator.
            unsafe { libc::free(paths[i] as *mut libc::c_void) }
            result.push(s);
        }
        Ok(result)
    }

    // print_summary is not available for batch archivers — the C function
    // works on an IndexedArchiver type that is not layout-compatible with
    // IndexedBatchArchiver. Use the single-message archiver for summaries.
}

impl Drop for IndexedBatchArchiver {
    fn drop(&mut self) {
        // SAFETY: Drop owns the handle and calls its matching destructor once.
        // UNRESOLVED: closing async regions also requires the unretained runtime (U2).
        unsafe { ffi::mmapq_indexed_batch_archiver_close(self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::IndexedBatchQueue;
    use crate::queue::QueueConfig;
    use crate::runtime::Runtime;

    fn setup(name: &str) -> (IndexedBatchQueue, super::IndexedBatchArchiver, Runtime) {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-batch-archiver").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = IndexedBatchQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let arch = q.create_archiver().unwrap();
        (q, arch, rt)
    }

    #[test]
    fn test_batch_archiver_create_and_close() {
        let (q, arch, _rt) = setup("test_batch_arch_create");
        drop(arch);
        drop(q);
    }

    // archivable_files test removed: requires archivable=1 meta which is not
    // exposed through the public Rust API. Tested in C (test_indexed_batch_archiver.c).
}
