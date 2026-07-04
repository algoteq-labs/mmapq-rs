//! Batch queue factory — creates batch-aware participants.
//!
//! Thread-confined (`!Send + !Sync`). One per thread.

use std::ffi::CString;
use std::marker::PhantomData;

use crate::error::MmapError;
use crate::ffi;
use crate::queue::QueueConfig;

/// Batch queue factory. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchQueue {
    /* field order matters: `inner` first so Drop closes the queue while
       the runtime fields (future batch runtime support) are still alive. */
    inner: *mut ffi::IndexedBatchQueue,
    /// Batch queues currently always use sync mapping (NULL runtimes);
    /// these fields exist so future batch runtime support cannot
    /// reintroduce the UAF fixed in tests/runtime_uaf.rs.
    _write_rt: Option<crate::runtime::Runtime>,
    _read_rt: Option<crate::runtime::Runtime>,
    _not_send: PhantomData<*mut ()>,
}

unsafe impl Send for IndexedBatchQueue {}
unsafe impl Sync for IndexedBatchQueue {}

impl IndexedBatchQueue {
    /// Create or open a batch queue with the given configuration.
    pub fn create(config: QueueConfig) -> Result<Self, MmapError> {
        let dir = CString::new(config.directory())
            .map_err(|_| MmapError::Internal("directory contains null byte".into()))?;
        let name = CString::new(config.queue_name())
            .map_err(|_| MmapError::Internal("queue name contains null byte".into()))?;

        let ptr = unsafe {
            ffi::indexed_batch_queue_create(
                dir.as_ptr(),
                name.as_ptr(),
                config.get_region_size(),
                config.get_ring_size(),
                config.get_regions_to_map_ahead(),
                config.get_max_file_size(),
                config.get_start_index(),
                config.get_timeout(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if ptr.is_null() {
            return Err(MmapError::Internal("indexed_batch_queue_create returned NULL".into()));
        }
        Ok(IndexedBatchQueue {
            inner: ptr,
            _write_rt: None,
            _read_rt: None,
            _not_send: PhantomData,
        })
    }

    /// Create a batch appender with a fresh ID from the pool.
    pub fn create_appender(&self) -> Result<super::batch_appender::IndexedBatchAppender, MmapError> {
        let ptr = unsafe { ffi::indexed_batch_queue_create_appender(self.inner) };
        if ptr.is_null() {
            return Err(MmapError::PoolExhausted);
        }
        Ok(super::batch_appender::IndexedBatchAppender {
            inner: ptr,
            _runtime: self._write_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create a batch poller for sequential consumption.
    pub fn create_poller(&self) -> Result<super::batch_poller::IndexedBatchPoller, MmapError> {
        let ptr = unsafe { ffi::indexed_batch_queue_create_poller(self.inner) };
        if ptr.is_null() {
            return Err(MmapError::Internal("create_poller returned NULL".into()));
        }
        Ok(super::batch_poller::IndexedBatchPoller {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create a batch reader for random-access reads.
    pub fn create_reader(&self) -> Result<super::batch_reader::IndexedBatchReader, MmapError> {
        let ptr = unsafe { ffi::indexed_batch_queue_create_reader(self.inner) };
        if ptr.is_null() {
            return Err(MmapError::Internal("create_reader returned NULL".into()));
        }
        Ok(super::batch_reader::IndexedBatchReader {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            ctx: std::cell::UnsafeCell::new(unsafe { std::mem::zeroed() }),
        })
    }

    /// Create a batch archiver.
    pub fn create_archiver(&self) -> Result<super::batch_archiver::IndexedBatchArchiver, MmapError> {
        let ptr = unsafe { ffi::indexed_batch_queue_create_archiver(self.inner) };
        if ptr.is_null() {
            return Err(MmapError::Internal("create_archiver returned NULL".into()));
        }
        Ok(super::batch_archiver::IndexedBatchArchiver {
            inner: ptr,
            _not_send: PhantomData,
        })
    }

    /// The configured region size in bytes.
    pub fn region_size(&self) -> i32 {
        unsafe { ffi::indexed_batch_queue_region_size(self.inner) }
    }

    /// The configured maximum file size in bytes.
    pub fn max_file_size(&self) -> i64 {
        unsafe { ffi::indexed_batch_queue_max_file_size(self.inner) }
    }

    /// The current start index (0 for non-archivable queues).
    pub fn start_index(&self) -> i64 {
        unsafe { ffi::indexed_batch_queue_start_index(self.inner) }
    }
}

impl Drop for IndexedBatchQueue {
    fn drop(&mut self) {
        unsafe { ffi::indexed_batch_queue_close(self.inner) }
    }
}
