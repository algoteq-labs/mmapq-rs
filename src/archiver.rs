//! Queue archiver — advances the start index and manages file lifecycle.
//!
//! An `IndexedArchiver` is created from an archivable queue. It can:
//! - **Forward archiving** (`index > current start_index`): read and validate
//!   every header in the archived range, compute each appender's boundary as
//!   the end of the archived entry with the maximum payload position (commit
//!   order is not write order under resequencing), publish the boundary,
//!   commit the new start_index atomically in queue meta, then make one pass
//!   over the live headers, lowering the boundary below any live payload
//!   that sits under it.
//! - **Backward / undo archiving** (`index < current start_index`): walk
//!   headers forward from `index` to the current start index. Validate that
//!   all header and payload files covering that range still exist before
//!   modifying anything. Lower each appender's boundary to the lowest
//!   restored payload position — undo never raises it.
//! - **No-op** when `index == current start_index`.
//!
//! Archiving is serialised per queue on `archive.lock` (flock); a listing
//! never observes an archive in progress.
//!
//! The archiver does **not** delete files — `archivable_files()` returns the
//! list of files fully covered by the archive boundary at the time of the
//! call: files no live entry can need at that moment. Delete them with
//! `reclaim_files()` (always safe). Hand-deleted, moved or compressed, they
//! are safe provided no undo (`archive_up_to_index` below the current start
//! index) runs between the listing call and the last unlink; an undo that
//! ran before the call is harmless, because the listing already reflects it.
//! There is no deletion wait (the commit-intent protocol retired it —
//! see `doc/design/archiving-design.md`).
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
    /// Keeps the read runtime alive through C teardown, independently of the
    /// parent queue; None selects synchronous mapping.
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedArchiver {
    /// Report whether the queue was created archivable.
    ///
    /// The queue's meta is read on first use and cached on this archiver.
    /// Until the queue's first appender has written its files the answer is
    /// not determinable yet and the call returns `Err(FileNotFound)`,
    /// `Err(NotReady)` or `Err(MapTimedOut)` — retry on this same archiver.
    /// Any other error is terminal and repeats on every later call.
    pub fn archivable(&self) -> Result<bool, MmapError> {
        let mut out: libc::c_int = 0;
        // SAFETY: out is a writable local; the owned handle is thread-confined and
        // retains its read runtime for the lazy meta open.
        let rc = unsafe { ffi::mmapq_indexed_archiver_archivable_checked(self.inner, &mut out) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(out != 0)
    }

    /// Archive every entry below `index`: `index` becomes the new start index
    /// (the first unarchived entry).
    ///
    /// **Forward** (`index > current start_index`): walks headers backwards,
    /// records the last committed payload position per appender, advances
    /// past the committed message to find the next write offset, and commits
    /// the new start_index atomically. After this call, entries below `index` are no longer
    /// accessible via poller or reader; `index` itself is.
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
        // SAFETY: The owned, thread-confined handle is exclusively borrowed.
        // Its retained read runtime stays alive during lazy initialization and mapping.
        let rc = unsafe {
            ffi::mmapq_indexed_archiver_archive_up_to_index(self.inner, index)
        };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(())
    }

    /// List files whose entire content is covered by the archive boundary:
    /// files no live entry can need at the time of the call. Delete them
    /// with `reclaim_files()` (always safe). If you delete, move or
    /// compress them yourself, they are safe provided no undo
    /// (`archive_up_to_index` below the current start index) runs between
    /// this call and your last unlink; an undo that ran before this call
    /// is harmless, because the listing already reflects it.
    ///
    /// Only returns paths to files that actually exist on disk. The listing
    /// can fail with `MapTimedOut` if a live-header read times out during
    /// the boundary cap scan.
    pub fn archivable_files(&self) -> Result<Vec<String>, MmapError> {
        const MAX: usize = 1024;
        let mut paths: [*mut libc::c_char; MAX] = [std::ptr::null_mut(); MAX];
        // SAFETY: paths has MAX writable slots; C returns at most MAX owned strdup strings.
        // The owned handle is thread-confined and retains its read runtime for lazy initialization.
        let count = unsafe {
            ffi::mmapq_indexed_archiver_archivable_files(self.inner, paths.as_mut_ptr(), MAX as i32)
        };
        if count < 0 {
            return Err(map_rc(count));
        }
        if count == 0 {
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

    /// Delete the files in `marked` that a fresh listing still reports as
    /// archivable, and return how many this call deleted.
    ///
    /// Pass paths exactly as `archivable_files()` returned them; they are
    /// matched by exact string comparison. The re-list and the deletion happen
    /// in one shared hold of the queue's `archive.lock`, so an undo cannot run
    /// in between: a mark that a fresh listing no longer reports — live again
    /// after an undo, or already deleted — is skipped, never deleted. Safe to
    /// call at any time; there is no wait.
    ///
    /// Errors: `InvalidArgument` if a path contains a NUL byte or there are more
    /// than `i32::MAX` marks (nothing is deleted); `MapTimedOut` if a live-header
    /// read timed out during the re-list; `Eio` if locking or deleting failed —
    /// files deleted so far stay deleted, and a retry re-lists and skips them.
    pub fn reclaim_files<S: AsRef<str>>(&self, marked: &[S]) -> Result<usize, MmapError> {
        let cstrings: Vec<std::ffi::CString> = marked
            .iter()
            .map(|s| std::ffi::CString::new(s.as_ref()))
            .collect::<Result<_, _>>()
            .map_err(|_| MmapError::InvalidArgument)?;
        let n = i32::try_from(marked.len()).map_err(|_| MmapError::InvalidArgument)?;
        let mut ptrs: Vec<*mut libc::c_char> = cstrings
            .iter()
            .map(|c| c.as_ptr() as *mut libc::c_char)
            .collect();
        // SAFETY: the header documents that the marked array and its strings
        // are only read for the duration of the call, and the CStrings (and
        // the pointer vector backing ptrs) outlive it. The owned handle is
        // thread-confined and retains its read runtime for lazy initialization.
        let rc = unsafe {
            ffi::mmapq_indexed_archiver_reclaim_files(self.inner, ptrs.as_mut_ptr(), n)
        };
        if rc < 0 {
            return Err(map_rc(rc));
        }
        Ok(rc as usize)
    }

    /// Return a human-readable summary of the queue's file layout.
    pub fn print_summary(&self) -> String {
        let mut buf = [0 as libc::c_char; 4096];
        // SAFETY: buf supplies the full advertised writable capacity to snprintf-based C output.
        // The owned handle is thread-confined and retains its read runtime for lazy initialization.
        let n = unsafe { ffi::mmapq_indexed_archiver_print_summary(self.inner, buf.as_mut_ptr(), 4096) };
        if n > 0 {
            // SAFETY: C writes at most buf.len() bytes and NUL-terminates via snprintf.
            // The initialized local buffer remains alive until conversion finishes.
            unsafe { CStr::from_ptr(buf.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        } else {
            String::new()
        }
    }

    /// Return the payload boundary for a given appender, or `None` if the
    /// appender has not written any messages.
    ///
    /// `start_offset` is the per-appender deletion boundary: payload files
    /// wholly below it may be reclaimed. A joining appender with no live
    /// entry walks forward from the archive line — exactly
    /// `max(start_offset, archive_offset)` (the line never
    /// moves back) — never from a write position.
    pub fn start_offset(&self, appender_id: u8) -> Result<Option<i64>, MmapError> {
        let mut offset: i64 = 0;
        // SAFETY: offset is a writable local and the archiver is thread-confined.
        // The owned handle retains its read runtime during C lazy initialization.
        let ok = unsafe {
            ffi::mmapq_indexed_archiver_start_offset(self.inner, appender_id, &mut offset as *mut i64)
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
        // SAFETY: Drop owns the handle and calls its matching destructor once.
        // The runtime field drops only after this method returns, keeping it alive
        // while C closes the archiver and its asynchronous regions.
        unsafe { ffi::mmapq_indexed_archiver_close(self.inner) }
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

        // Create an archivable queue — we need to call mmapq_indexed_queue_create
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

    // covers: IndexedArchiver::print_summary
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

    /// start_offset reports the lane's deletion boundary: an unused lane is
    /// None; after archiving five 2-byte entries (6 framed bytes each) the
    /// used lane's boundary is 64 + 5*6 = 94. Nothing is wholly below it at
    /// the default geometry, so archivable_files is empty.
    // covers: IndexedArchiver::start_offset
    // covers: IndexedArchiver::archivable_files
    #[test]
    fn test_archiver_start_offset_and_archivable_files() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-archiver").join("start_offset");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = crate::IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .archivable(true)
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let used;
        {
            let mut a = q.create_appender().unwrap();
            used = a.id();
            for i in 0..10 {
                let msg = format!("m{i}");
                assert_eq!(a.append(msg.as_bytes()).unwrap(), i);
            }
        }
        let mut arch = q.create_archiver().unwrap();
        arch.archive_up_to_index(5).unwrap();

        let unused: u8 = if used == 255 { 0 } else { used + 1 };
        assert_eq!(arch.start_offset(unused).unwrap(), None, "unused lane");
        assert_eq!(arch.start_offset(used).unwrap(), Some(94), "boundary past five archived entries");
        assert_eq!(arch.archivable_files().unwrap(), Vec::<String>::new());
    }
}
