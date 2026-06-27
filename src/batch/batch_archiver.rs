//! Batch archiver — manages start index and archivable files for batch queues.

use std::ffi::CStr;
use std::marker::PhantomData;

use crate::error::MmapError;
use crate::ffi;

/// Batch queue archiver. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchArchiver {
    pub(crate) inner: *mut ffi::IndexedBatchArchiver,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedBatchArchiver {
    /// Advance the start index past the given batch index.
    pub fn archive_up_to_index(&mut self, index: i64) -> Result<(), MmapError> {
        unsafe { ffi::indexed_batch_archiver_archive_up_to_index(self.inner, index) }
        Ok(())
    }

    /// List archivable files that can be safely deleted.
    pub fn archivable_files(&self) -> Result<Vec<String>, MmapError> {
        const MAX: usize = 1024;
        let mut paths: [*mut libc::c_char; MAX] = [std::ptr::null_mut(); MAX];
        let count = unsafe {
            ffi::indexed_batch_archiver_archivable_files(self.inner, paths.as_mut_ptr(), MAX as i32)
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
        let n = unsafe {
            ffi::indexed_batch_archiver_print_summary(self.inner, buf.as_mut_ptr(), 4096)
        };
        if n > 0 {
            unsafe { CStr::from_ptr(buf.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        } else {
            String::new()
        }
    }
}

impl Drop for IndexedBatchArchiver {
    fn drop(&mut self) {
        unsafe { ffi::indexed_batch_archiver_close(self.inner) }
    }
}
