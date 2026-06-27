//! Queue archiver — advances the start index and manages file lifecycle.
//!
//! An `IndexedArchiver` is created from an archivable queue. It can:
//! - Advance the start index past old entries (`archive_up_to_index()`).
//! - List files that are safe to delete (`archivable_files()`).
//! - Print a human-readable summary (`print_summary()`).

use std::ffi::CStr;
use std::marker::PhantomData;

use crate::error::MmapError;
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
    /// After this call, entries at or below `index` are no longer accessible
    /// via poller or reader. The backing files can be listed via
    /// `archivable_files()` and deleted.
    pub fn archive_up_to_index(&mut self, index: i64) -> Result<(), MmapError> {
        unsafe { ffi::indexed_archiver_archive_up_to_index(self.inner, index) }
        Ok(())
    }

    /// List files whose entire content has been archived and can be deleted.
    ///
    /// The caller is responsible for deleting the returned files.
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
