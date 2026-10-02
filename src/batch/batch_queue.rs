//! Batch queue factory — creates batch-aware participants.
//!
//! The factory is Send + Sync; the participants it creates are thread-confined.

use std::ffi::CString;
use std::marker::PhantomData;

use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::queue::QueueConfig;

/// Shareable batch queue factory; participant handles remain thread-confined.
pub struct IndexedBatchQueue {
    /* field order matters: `inner` first so Drop closes the queue while
       the runtime fields are still alive. */
    inner: *mut ffi::IndexedBatchQueue,
    /// Keep the C write runtime alive for the queue's lifetime.
    /// Forwarded from QueueConfig at creation; None = sync mapping.
    _write_rt: Option<crate::runtime::Runtime>,
    /// Keep the C read runtime alive for the queue's lifetime.
    /// Forwarded from QueueConfig at creation; None = sync mapping.
    _read_rt: Option<crate::runtime::Runtime>,
}

// SAFETY: Moving the factory transfers sole ownership of its C handle. Participant
// handles own their own state, and retained Runtime handles are Send + Sync.
unsafe impl Send for IndexedBatchQueue {}
// SAFETY: Only immutable factory configuration is accessed by shared methods;
// participants allocate independent state. The non-atomic C closed field is
// written only by Drop, which cannot overlap a live Rust borrow.
unsafe impl Sync for IndexedBatchQueue {}

impl IndexedBatchQueue {
    /// Create or open a batch queue with the given configuration.
    pub fn create(mut config: QueueConfig) -> Result<Self, MmapError> {
        let dir = CString::new(config.directory())
            .map_err(|_| MmapError::Internal("directory contains null byte".into()))?;
        let name = CString::new(config.queue_name())
            .map_err(|_| MmapError::Internal("queue name contains null byte".into()))?;

        let write_rt = config.take_write_runtime();
        let read_rt = config.take_read_runtime();
        let write_rt_ptr = write_rt.as_ref().map_or(std::ptr::null_mut(), |r| r.as_ptr());
        let read_rt_ptr = read_rt.as_ref().map_or(std::ptr::null_mut(), |r| r.as_ptr());

        let mut ptr: *mut ffi::IndexedBatchQueue = std::ptr::null_mut();
        let archivable = if config.get_archivable() { 1 } else { 0 };
        // SAFETY: CString inputs and runtime handles outlive this synchronous call; out
        // points to a writable local. C copies names, validates configuration, and
        // returns an independently owned handle; Rust retains runtimes on success.
        let rc = unsafe {
            ffi::mmapq_indexed_batch_queue_create(
                dir.as_ptr(),
                name.as_ptr(),
                config.get_region_size(),
                config.get_ring_size(),
                config.get_regions_to_map_ahead(),
                config.get_max_file_size(),
                config.get_start_index(),
                config.get_timeout(),
                archivable,
                write_rt_ptr,
                read_rt_ptr,
                &mut ptr,
            )
        };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(IndexedBatchQueue {
            inner: ptr,
            _write_rt: write_rt,
            _read_rt: read_rt,
                    })
    }

    /// Create a batch appender with a fresh ID from the pool.
    pub fn create_appender(&self) -> Result<super::batch_appender::IndexedBatchAppender, MmapError> {
        let mut ptr: *mut ffi::IndexedBatchAppender = std::ptr::null_mut();
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_batch_queue_create_appender(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::batch_appender::IndexedBatchAppender {
            inner: ptr,
            _runtime: self._write_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create a batch appender with a specific lane ID.
    pub fn create_appender_with_id(
        &self,
        appender_id: u8,
    ) -> Result<super::batch_appender::IndexedBatchAppender, MmapError> {
        let mut ptr: *mut ffi::IndexedBatchAppender = std::ptr::null_mut();
        // SAFETY: The live factory is shared only for immutable configuration reads.
        // ptr is a writable output; C allocates an independent appender for this
        // u8 lane ID, and the returned wrapper retains the write runtime.
        let rc = unsafe {
            ffi::mmapq_indexed_batch_queue_create_appender_with_id(self.inner, appender_id, &mut ptr)
        };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::batch_appender::IndexedBatchAppender {
            inner: ptr,
            _runtime: self._write_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create a batch poller for sequential consumption.
    pub fn create_poller(&self) -> Result<super::batch_poller::IndexedBatchPoller, MmapError> {
        let mut ptr: *mut ffi::IndexedBatchPoller = std::ptr::null_mut();
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_batch_queue_create_poller(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::batch_poller::IndexedBatchPoller {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create a batch reader for random-access reads.
    pub fn create_reader(&self) -> Result<super::batch_reader::IndexedBatchReader, MmapError> {
        let mut ptr: *mut ffi::IndexedBatchReader = std::ptr::null_mut();
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_batch_queue_create_reader(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::batch_reader::IndexedBatchReader {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            // SAFETY: BatchReadingContext contains only integers, byte padding, and raw pointers;
            // zero is a valid Rust representation. C initializes it before any iterator use.
            ctx: unsafe { std::mem::zeroed() },
        })
    }

    /// Create a batch archiver.
    pub fn create_archiver(&self) -> Result<super::batch_archiver::IndexedBatchArchiver, MmapError> {
        let mut ptr: *mut ffi::IndexedBatchArchiver = std::ptr::null_mut();
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_batch_queue_create_archiver(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::batch_archiver::IndexedBatchArchiver {
            inner: ptr,
            _not_send: PhantomData,
        })
    }

}

impl Drop for IndexedBatchQueue {
    fn drop(&mut self) {
        // SAFETY: Drop uniquely owns the factory, so no Rust factory calls can overlap close.
        // Participant state is independently owned; runtime fields drop after close.
        unsafe { ffi::mmapq_indexed_batch_queue_close(self.inner) }
    }
}
