//! Queue factory — creates appenders, pollers, readers, and archivers.
//!
//! `IndexedQueue` is a thin config holder that delegates participant creation
//! to the C library. All factory methods are re-entrant, making the queue
//! safe to share across threads (`Send + Sync`). Only `close()` (called via
//! `Drop`) requires exclusive access.
//!
//! Metadata is opened eagerly during `create()` to eliminate any lazy-init
//! data race present in the raw C API.

use std::ffi::CString;
use std::marker::PhantomData;

use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::runtime::Runtime;

/// A factory for creating queue participants (appenders, pollers, readers).
///
/// `IndexedQueue` is `Send + Sync` — it holds no mutable per-thread state.
/// Participant creation is re-entrant. Only `close()` (called via `Drop`)
/// requires exclusive access, which Rust's borrow checker enforces via
/// `&mut self`.
///
/// The queue's metadata is opened eagerly during `create()` to eliminate
/// any lazy-init data race.
pub struct IndexedQueue {
    /* field order matters: `inner` is declared first so Drop closes the
       queue while `_write_rt`/`_read_rt` still hold the C runtimes alive. */
    inner: *mut ffi::IndexedQueue,
    /// Owned clones keeping the runtimes alive for the queue's lifetime.
    /// The C queue stores raw runtime pointers without cloning; these
    /// fields are the ownership (UAF regression: tests/runtime_uaf.rs).
    _write_rt: Option<Runtime>,
    _read_rt: Option<Runtime>,
    _not_send: PhantomData<*mut ()>,
}

unsafe impl Send for IndexedQueue {}
unsafe impl Sync for IndexedQueue {}

impl IndexedQueue {
    /// Create or open the queue with the given configuration.
    ///
    /// The queue directory and metadata are created lazily (on first
    /// participant creation) if they do not exist — matching C semantics.
    /// However, metadata is opened read-only immediately to pre-resolve
    /// parameters, eliminating the lazy-init race present in the raw C API.
    pub fn create(mut config: QueueConfig) -> Result<Self, MmapError> {
        let dir = CString::new(config.directory.as_str())
            .map_err(|_| MmapError::Internal("directory contains null byte".into()))?;
        let name = CString::new(config.queue_name.as_str())
            .map_err(|_| MmapError::Internal("queue name contains null byte".into()))?;

        let write_rt = config
            .write_runtime
            .as_ref()
            .map_or(std::ptr::null_mut(), |r| r.as_ptr());
        let read_rt = config
            .read_runtime
            .as_ref()
            .map_or(std::ptr::null_mut(), |r| r.as_ptr());

        let mut ptr: *mut ffi::IndexedQueue = std::ptr::null_mut();
        let rc = unsafe {
            ffi::indexed_queue_create(
                dir.as_ptr(),
                name.as_ptr(),
                config.region_size,
                config.ring_size,
                config.regions_to_map_ahead,
                config.max_file_size,
                config.start_index,
                config.timeout_ns,
                config.archivable as i32,
                write_rt,
                read_rt,
                &mut ptr,
            )
        };
        if rc != 0 {
            return Err(map_rc(rc));
        }

        /* Move the runtime handles out of the config into the queue: the
           raw pointers were extracted above, so `take()` here transfers
           ownership without touching the refcount. */
        let write_owned = config.write_runtime.take();
        let read_owned = config.read_runtime.take();

        Ok(IndexedQueue {
            inner: ptr,
            _write_rt: write_owned,
            _read_rt: read_owned,
            _not_send: PhantomData,
        })
    }

    /// Create a new appender with a fresh ID from the pool.
    /// Returns `Err(IdConflict)` if all 256 IDs are in use.
    pub fn create_appender(&self) -> Result<super::appender::IndexedAppender, MmapError> {
        let mut ptr: *mut ffi::IndexedAppender = std::ptr::null_mut();
        let rc = unsafe { ffi::indexed_queue_create_appender(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::appender::IndexedAppender {
            inner: ptr,
            _runtime: self._write_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create an appender with a specific ID.
    /// Returns `Err(IdConflict)` if the ID is already in use.
    pub fn create_appender_with_id(
        &self,
        appender_id: u8,
    ) -> Result<super::appender::IndexedAppender, MmapError> {
        let mut ptr: *mut ffi::IndexedAppender = std::ptr::null_mut();
        let rc =
            unsafe { ffi::indexed_queue_create_appender_with_id(self.inner, appender_id, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::appender::IndexedAppender {
            inner: ptr,
            _runtime: self._write_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create a sequential cursor-based poller.
    /// Starts at the queue's start index and advances on each successful poll.
    pub fn create_poller(&self) -> Result<super::poller::IndexedPoller, MmapError> {
        let mut ptr: *mut ffi::IndexedPoller = std::ptr::null_mut();
        let rc = unsafe { ffi::indexed_queue_create_poller(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::poller::IndexedPoller {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            _not_send: PhantomData,
        })
    }

    /// Create a random-access reader.
    /// Call `read()` with any index to inspect a message without advancing a cursor.
    pub fn create_reader(&self) -> Result<super::reader::IndexedReader, MmapError> {
        let mut ptr: *mut ffi::IndexedReader = std::ptr::null_mut();
        let rc = unsafe { ffi::indexed_queue_create_reader(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::reader::IndexedReader {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            ctx: std::cell::UnsafeCell::new(ffi::ReadingContext {
                index: 0,
                appender_id: 0,
                msg_offset: 0,
                data: std::ptr::null(),
                length: 0,
                status: 0,
            }),
        })
    }

    /// Create an archiver for advancing the queue's start index and listing
    /// files that can be safely deleted.
    pub fn create_archiver(&self) -> Result<super::archiver::IndexedArchiver, MmapError> {
        let mut ptr: *mut ffi::IndexedArchiver = std::ptr::null_mut();
        let rc = unsafe { ffi::indexed_queue_create_archiver(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::archiver::IndexedArchiver {
            inner: ptr,
            _not_send: PhantomData,
        })
    }

    /// The configured region size in bytes (typically 4 MB).
    pub fn region_size(&self) -> i32 {
        unsafe { ffi::indexed_queue_region_size(self.inner) }
    }

    /// The maximum payload file size in bytes (typically 1 GB).
    pub fn max_file_size(&self) -> i64 {
        unsafe { ffi::indexed_queue_max_file_size(self.inner) }
    }

    /// The current start index (0 for non-archivable queues).
    pub fn start_index(&self) -> i64 {
        unsafe { ffi::indexed_queue_start_index(self.inner) }
    }

    #[allow(dead_code)]
    pub(crate) fn as_ptr(&self) -> *mut ffi::IndexedQueue {
        self.inner
    }
}

impl Drop for IndexedQueue {
    fn drop(&mut self) {
        unsafe {
            ffi::indexed_queue_close(self.inner);
        }
    }
}

impl std::fmt::Debug for IndexedQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedQueue").finish()
    }
}

/* ── QueueConfig ──────────────────────────────────────────────────────────── */

/// Builder for `IndexedQueue` creation parameters.
///
/// `QueueConfig` stores cloned `Runtime` handles so that the queue's
/// lifetime is independent of the `Runtime` that was passed in during
/// configuration.
#[derive(Clone)]
pub struct QueueConfig {
    directory: String,
    queue_name: String,
    region_size: i32,
    ring_size: i32,
    regions_to_map_ahead: i32,
    max_file_size: i64,
    start_index: i64,
    timeout_ns: u64,
    write_runtime: Option<Runtime>,
    read_runtime: Option<Runtime>,
    archivable: bool,
}

impl QueueConfig {
    /// Creates a default queue configuration.
    ///
    /// Defaults: region_size=4MB, ring_size=4, max_file_size=1GB,
    /// timeout=10s, no runtimes.
    pub fn new(directory: &str, queue_name: &str) -> Self {
        QueueConfig {
            directory: directory.to_string(),
            queue_name: queue_name.to_string(),
            region_size: 4096 * 1024,
            ring_size: 4,
            regions_to_map_ahead: 1,
            max_file_size: (4096 * 1024) as i64 * 256,
            start_index: 0,
            timeout_ns: 10_000_000_000,
            write_runtime: None,
            read_runtime: None,
            archivable: false,
        }
    }

    /// Create the queue archivable (archiver may advance start_index).
    /// Applies at creation only; existing queues keep their on-disk value.
    /// Non-archivable (default) is the performant mode.
    pub fn archivable(mut self, archivable: bool) -> Self {
        self.archivable = archivable;
        self
    }

    /// Base directory for queue files.
    pub fn directory(&self) -> &str {
        &self.directory
    }

    /// Queue subdirectory name.
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Size in bytes of each mapped region (must be a power of two).
    pub fn region_size(mut self, bytes: i32) -> Self {
        self.region_size = bytes;
        self
    }

    /// Returns the configured region size.
    pub fn get_region_size(&self) -> i32 {
        self.region_size
    }

    /// Number of regions in the ring (must be a power of two, typically 4).
    pub fn ring_size(mut self, size: i32) -> Self {
        self.ring_size = size;
        self
    }

    /// Returns the configured ring size.
    pub fn get_ring_size(&self) -> i32 {
        self.ring_size
    }

    /// Number of regions to pre-map ahead of the current position.
    pub fn regions_to_map_ahead(mut self, n: i32) -> Self {
        self.regions_to_map_ahead = n;
        self
    }

    /// Returns the configured regions-to-map-ahead count.
    pub fn get_regions_to_map_ahead(&self) -> i32 {
        self.regions_to_map_ahead
    }

    /// Maximum payload file size in bytes (must be a multiple of region_size).
    pub fn max_file_size(mut self, bytes: i64) -> Self {
        self.max_file_size = bytes;
        self
    }

    /// Returns the configured max file size.
    pub fn get_max_file_size(&self) -> i64 {
        self.max_file_size
    }

    /// Initial start index. Only used when creating a new queue.
    pub fn start_index(mut self, index: i64) -> Self {
        self.start_index = index;
        self
    }

    /// Returns the configured start index.
    pub fn get_start_index(&self) -> i64 {
        self.start_index
    }

    /// Region-mapping timeout in nanoseconds.
    pub fn timeout(mut self, nanos: u64) -> Self {
        self.timeout_ns = nanos;
        self
    }

    /// Returns the configured timeout.
    pub fn get_timeout(&self) -> u64 {
        self.timeout_ns
    }

    /// Set the write runtime. Clones the runtime handle (refcount increment).
    pub fn write_runtime(mut self, rt: &Runtime) -> Self {
        self.write_runtime = Some(rt.clone());
        self
    }

    /// Set the read runtime. Clones the runtime handle (refcount increment).
    pub fn read_runtime(mut self, rt: &Runtime) -> Self {
        self.read_runtime = Some(rt.clone());
        self
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig::new("/dev/shm", "default")
    }
}
