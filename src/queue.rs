//! Queue factory — creates appenders, pollers, readers, and archivers.
//!
//! `IndexedQueue` holds immutable creation preferences and runtime handles.
//! Shared factory calls create independent participants, each owning its metadata
//! and mapping state. Only Drop requires exclusive access.

use std::ffi::CString;
use std::marker::PhantomData;

use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::runtime::Runtime;

/// A factory for creating queue participants (appenders, pollers, readers).
///
/// `IndexedQueue` is Send + Sync. Shared methods read immutable configuration
/// and create thread-confined participants. The factory holds no metadata mapping.
/// Drop has exclusive ownership, so closing cannot overlap a safe factory call.
pub struct IndexedQueue {
    /* field order matters: `inner` is declared first so Drop closes the
       queue while `_write_rt`/`_read_rt` still hold the C runtimes alive. */
    inner: *mut ffi::IndexedQueue,
    /// Owned clones keeping the runtimes alive for the queue's lifetime.
    /// The C queue stores raw runtime pointers without cloning; these
    /// fields are the ownership (UAF regression: tests/runtime_uaf.rs).
    _write_rt: Option<Runtime>,
    _read_rt: Option<Runtime>,
}

// SAFETY: Moving the factory transfers sole ownership of its C handle. Participant
// handles own their own state, and retained Runtime handles are Send + Sync.
unsafe impl Send for IndexedQueue {}
// SAFETY: Shared methods read immutable C factory configuration and allocate
// independent participants. The factory has no lazy metadata cache; its closed
// flag is changed only by Drop, which cannot overlap a live Rust borrow.
unsafe impl Sync for IndexedQueue {}

impl IndexedQueue {
    /// Create or open the queue with the given configuration.
    ///
    /// Nothing is written until the first appender is created; that
    /// appender's queue handle's configuration is what goes to disk. Of it,
    /// `archivable`, `region_size`, `max_file_size` and `start_index` are
    /// stored and fixed: an existing queue keeps its stored values and this
    /// config's are ignored. `ring_size` and `timeout` apply to this handle.
    /// Metadata is not pre-opened
    /// here; the metadata getters that raced with lazy initialization were
    /// removed (doc/unsafe-audit.md U1).
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
        // SAFETY: CString inputs and runtime handles outlive this synchronous call; out
        // points to a writable local. C copies names, validates configuration, and
        // returns an independently owned handle; Rust retains runtimes on success.
        let rc = unsafe {
            ffi::mmapq_indexed_queue_create(
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
                    })
    }

    /// Create a new appender with a fresh ID from the pool.
    /// Returns `Err(IdConflict)` if all 256 IDs are in use.
    pub fn create_appender(&self) -> Result<super::appender::IndexedAppender, MmapError> {
        let mut ptr: *mut ffi::IndexedAppender = std::ptr::null_mut();
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_queue_create_appender(self.inner, &mut ptr) };
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
    /// A writer killed without closing leaves its ID taken until an operator
    /// releases it (the `mmapq-ids` tool in the C repository); one that
    /// closed released it.
    pub fn create_appender_with_id(
        &self,
        appender_id: u8,
    ) -> Result<super::appender::IndexedAppender, MmapError> {
        let mut ptr: *mut ffi::IndexedAppender = std::ptr::null_mut();
        let rc =
            // SAFETY: The shared factory is live; this factory reads immutable config, and ptr
            // is a writable output for a new appender with the validated u8 lane ID.
            unsafe { ffi::mmapq_indexed_queue_create_appender_with_id(self.inner, appender_id, &mut ptr) };
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
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_queue_create_poller(self.inner, &mut ptr) };
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
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_queue_create_reader(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::reader::IndexedReader {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            ctx: ffi::ReadingContext {
                index: 0,
                appender_id: 0,
                msg_offset: 0,
                data: std::ptr::null(),
                length: 0,
                status: 0,
            },
        })
    }

    /// Create an archiver for advancing the queue's start index and listing
    /// files that can be safely deleted.
    pub fn create_archiver(&self) -> Result<super::archiver::IndexedArchiver, MmapError> {
        let mut ptr: *mut ffi::IndexedArchiver = std::ptr::null_mut();
        // SAFETY: The factory stays live under &self and out points to a writable local.
        // C creates independent participant state; errors are checked before wrapping.
        let rc = unsafe { ffi::mmapq_indexed_queue_create_archiver(self.inner, &mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(super::archiver::IndexedArchiver {
            inner: ptr,
            _runtime: self._read_rt.clone(),
            _not_send: PhantomData,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn as_ptr(&self) -> *mut ffi::IndexedQueue {
        self.inner
    }
}

impl Drop for IndexedQueue {
    fn drop(&mut self) {
        // SAFETY: Drop has exclusive ownership of the factory and keeps both runtime fields
        // alive until C close returns. Participant handles own independent C state.
        unsafe {
            ffi::mmapq_indexed_queue_close(self.inner);
        }
    }
}

impl std::fmt::Debug for IndexedQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedQueue")
            .field("inner", &self.inner)
            .finish()
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

    /// Size in bytes of each mapped region. Must be a power of two and a
    /// multiple of 32 KB (one transposed header block), and a multiple of
    /// the OS page size — header-file reclaim relies on whole blocks per
    /// file, and region mmap offsets must stay page-aligned (CAND-38 rule).
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

    /// Maximum payload file size in bytes (must be a multiple of
    /// region_size, and therefore of the 32 KB header block).
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

    /// Returns the configured write runtime (clones handle).
    pub fn get_write_runtime(&self) -> Option<Runtime> {
        self.write_runtime.clone()
    }

    /// Take ownership of the write runtime (avoids cloning).
    pub fn take_write_runtime(&mut self) -> Option<Runtime> {
        self.write_runtime.take()
    }

    /// Set the read runtime. Clones the runtime handle (refcount increment).
    pub fn read_runtime(mut self, rt: &Runtime) -> Self {
        self.read_runtime = Some(rt.clone());
        self
    }

    /// Set both write and read runtimes to the same runtime.
    /// Convenience for the common case where one background mapper thread
    /// handles both sides. Equivalent to `.write_runtime(&rt).read_runtime(&rt)`.
    pub fn both_runtimes(mut self, rt: &Runtime) -> Self {
        let clone = rt.clone();
        self.write_runtime = Some(rt.clone());
        self.read_runtime = Some(clone);
        self
    }

    /// Returns the configured read runtime (clones handle).
    pub fn get_read_runtime(&self) -> Option<Runtime> {
        self.read_runtime.clone()
    }

    /// Take ownership of the read runtime (avoids cloning).
    pub fn take_read_runtime(&mut self) -> Option<Runtime> {
        self.read_runtime.take()
    }

    /// Whether the queue should be created as archivable.
    pub fn get_archivable(&self) -> bool {
        self.archivable
    }

}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig::new(std::env::temp_dir().to_str().unwrap(), "default")
    }
}

#[cfg(test)]
mod tests {
    use super::QueueConfig;
    use crate::runtime::Runtime;

    // covers: QueueConfig::directory
    // covers: QueueConfig::queue_name
    // covers: QueueConfig::timeout
    // covers: QueueConfig::regions_to_map_ahead
    // covers: QueueConfig::both_runtimes
    // covers: QueueConfig::get_region_size
    // covers: QueueConfig::get_ring_size
    // covers: QueueConfig::get_regions_to_map_ahead
    // covers: QueueConfig::get_max_file_size
    // covers: QueueConfig::get_start_index
    // covers: QueueConfig::get_timeout
    // covers: QueueConfig::get_archivable
    // covers: QueueConfig::get_write_runtime
    // covers: QueueConfig::take_write_runtime
    // covers: QueueConfig::get_read_runtime
    // covers: QueueConfig::take_read_runtime
    #[test]
    fn config_setters_getters_and_runtime_slots_round_trip() {
        let rt = Runtime::region_mapper().unwrap();
        let mut cfg = QueueConfig::new("/tmp/mmap-rs-cfg-dir", "cfg-queue");
        assert_eq!(cfg.directory(), "/tmp/mmap-rs-cfg-dir");
        assert_eq!(cfg.queue_name(), "cfg-queue");

        // Documented defaults, asserted exactly.
        assert_eq!(cfg.get_region_size(), 4096 * 1024);
        assert_eq!(cfg.get_ring_size(), 4);
        assert_eq!(cfg.get_regions_to_map_ahead(), 1);
        assert_eq!(cfg.get_max_file_size(), (4096 * 1024) as i64 * 256);
        assert_eq!(cfg.get_start_index(), 0);
        assert_eq!(cfg.get_timeout(), 10_000_000_000);
        assert!(!cfg.get_archivable());

        // Every setter round-trips through its getter.
        cfg = cfg.region_size(8192);
        assert_eq!(cfg.get_region_size(), 8192);
        cfg = cfg.ring_size(6);
        assert_eq!(cfg.get_ring_size(), 6);
        cfg = cfg.regions_to_map_ahead(3);
        assert_eq!(cfg.get_regions_to_map_ahead(), 3);
        cfg = cfg.max_file_size(1 << 20);
        assert_eq!(cfg.get_max_file_size(), 1 << 20);
        cfg = cfg.start_index(7);
        assert_eq!(cfg.get_start_index(), 7);
        cfg = cfg.timeout(123_456);
        assert_eq!(cfg.get_timeout(), 123_456);
        cfg = cfg.archivable(true);
        assert!(cfg.get_archivable());

        // Runtime slots: set, read back, take. Taking empties the slot.
        cfg = cfg.write_runtime(&rt);
        assert!(cfg.get_write_runtime().is_some());
        assert!(cfg.take_write_runtime().is_some());
        assert!(cfg.get_write_runtime().is_none());
        assert!(cfg.take_write_runtime().is_none());

        cfg = cfg.read_runtime(&rt);
        assert!(cfg.get_read_runtime().is_some());
        assert!(cfg.take_read_runtime().is_some());
        assert!(cfg.get_read_runtime().is_none());
        assert!(cfg.take_read_runtime().is_none());

        cfg = cfg.both_runtimes(&rt);
        assert!(cfg.get_write_runtime().is_some());
        assert!(cfg.get_read_runtime().is_some());
    }
}
