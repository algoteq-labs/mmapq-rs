//! Background thread for asynchronous region mapping.
//!
//! A `Runtime` owns a dedicated `pthread` that polls registered executables
//! (region mappers, metric collectors, etc.) in a tight loop. When a queue
//! participant needs a file region mapped, it delegates the `mmap` call to
//! the runtime thread so the hot-path thread is never blocked by page faults.
//!
//! Runtimes are reference-counted at the C level via `runtime_clone()` /
//! `runtime_destroy()`. Cloning is cheap (one atomic increment). The
//! underlying C runtime is freed when the last handle is dropped.
//!
//! `Runtime` is `Send + Sync` — its internal registration is mutex-guarded.

use std::ffi::CString;

use crate::error::{map_rc, MmapError};
use crate::ffi;

/// A dedicated background thread that polls registered Executables.
///
/// `Runtime` is reference-counted at the C level via `runtime_clone()` /
/// `runtime_destroy()`. Cloning is cheap (one atomic increment). The
/// underlying C runtime is freed when the last handle is dropped.
///
/// `Runtime` is `Send + Sync` — its internal registration is mutex-guarded.
pub struct Runtime {
    inner: *mut ffi::Runtime,
}

unsafe impl Send for Runtime {}
unsafe impl Sync for Runtime {}

impl Runtime {
    /// Create a new Runtime with the given configuration.
    pub fn new(config: RuntimeConfig) -> Result<Self, MmapError> {
        let name = CString::new(config.name)
            .map_err(|_| MmapError::Internal("runtime name contains null byte".into()))?;

        let idle = config.idle_strategy.into_c_ptr()?;

        let mut ptr: *mut ffi::Runtime = std::ptr::null_mut();
        let rc = unsafe {
            ffi::runtime_create(
                name.as_ptr(),
                idle,
                config.close_timeout_ns,
                None,
                std::ptr::null_mut(),
                &mut ptr,
            )
        };
        if rc != 0 {
            unsafe { ffi::idle_strategy_destroy(idle) };
            return Err(map_rc(rc));
        }
        Ok(Runtime { inner: ptr })
    }

    /// Get or create the singleton region-mapper Runtime.
    ///
    /// The region mapper uses a busy-spin idle strategy and is intended
    /// for latency-sensitive background mapping tasks.
    ///
    /// Each call to `region_mapper()` increments the C-level reference count.
    /// Dropping the returned handle decrements it; the singleton itself
    /// remains alive.
    pub fn region_mapper() -> Result<Self, MmapError> {
        let mut ptr: *mut ffi::Runtime = std::ptr::null_mut();
        let rc = unsafe { ffi::runtime_region_mapper(&mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        // SAFETY: Must clone — the C singleton was created with refcount 1.
        // Our handle adds a second reference so that dropping this handle
        // does not free the singleton.
        let mut cloned: *mut ffi::Runtime = std::ptr::null_mut();
        let rc = unsafe { ffi::runtime_clone(ptr, &mut cloned) };
        if rc != 0 {
            unsafe { ffi::runtime_destroy(ptr) };
            return Err(map_rc(rc));
        }
        Ok(Runtime { inner: cloned })
    }

    /// Get or create the singleton metrics-processor Runtime.
    ///
    /// The metrics processor uses a backoff idle strategy and is intended
    /// for lower-priority periodic tasks.
    pub fn metrics_processor() -> Result<Self, MmapError> {
        let mut ptr: *mut ffi::Runtime = std::ptr::null_mut();
        let rc = unsafe { ffi::runtime_metrics_processor(&mut ptr) };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        let mut cloned: *mut ffi::Runtime = std::ptr::null_mut();
        let rc = unsafe { ffi::runtime_clone(ptr, &mut cloned) };
        if rc != 0 {
            unsafe { ffi::runtime_destroy(ptr) };
            return Err(map_rc(rc));
        }
        Ok(Runtime { inner: cloned })
    }

    /// Pin the background thread of this Runtime to the given CPU core.
    ///
    /// Returns `Ok(())` on success, or an error code from the C library.
    pub fn set_affinity(&self, cpu_core: u32) -> Result<(), MmapError> {
        let ret = unsafe { ffi::runtime_set_affinity(self.inner, cpu_core as i32) };
        if ret == 0 {
            Ok(())
        } else {
            Err(MmapError::Internal(format!(
                "runtime_set_affinity failed with code {}",
                ret
            )))
        }
    }

    /// Returns `true` if the underlying C Runtime has been closed.
    pub fn is_closed(&self) -> bool {
        unsafe { ffi::runtime_is_closed(self.inner) != 0 }
    }

    /// Access the raw C pointer (for use by other wrapper types).
    pub(crate) fn as_ptr(&self) -> *mut ffi::Runtime {
        self.inner
    }
}

impl Clone for Runtime {
    fn clone(&self) -> Self {
        let mut cloned: *mut ffi::Runtime = std::ptr::null_mut();
        let rc = unsafe { ffi::runtime_clone(self.inner, &mut cloned) };
        assert_eq!(rc, 0, "runtime_clone failed with code {}", rc);
        Runtime { inner: cloned }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        unsafe {
            ffi::runtime_destroy(self.inner);
        }
    }
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime").finish()
    }
}

/* ── RuntimeConfig ────────────────────────────────────────────────────────── */

/// Builder for `Runtime` creation parameters.
#[derive(Clone)]
pub struct RuntimeConfig {
    name: String,
    idle_strategy: IdleStrategy,
    close_timeout_ns: u64,
}

impl RuntimeConfig {
    /// Creates a default `RuntimeConfig` with a busy-spin idle strategy
    /// and a 10-second close timeout.
    pub fn new() -> Self {
        RuntimeConfig {
            name: "mmap-rs-runtime".to_string(),
            idle_strategy: IdleStrategy::BusySpin,
            close_timeout_ns: 10_000_000_000, // 10 seconds
        }
    }

    /// Sets the thread name (visible in `ps`/`top`, max 15 characters).
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Sets the idle behaviour when no work is available.
    pub fn idle_strategy(mut self, strategy: IdleStrategy) -> Self {
        self.idle_strategy = strategy;
        self
    }

    /// Sets how long `close()` waits for the background thread to drain
    /// before forcibly shutting down.
    pub fn close_timeout_ns(mut self, nanos: u64) -> Self {
        self.close_timeout_ns = nanos;
        self
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig::new()
    }
}

/* ── IdleStrategy ─────────────────────────────────────────────────────────── */

/// Idle behaviour applied by the Runtime thread when no work is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleStrategy {
    /// Never yields — spins on `spin_hint()`. Lowest latency, 100% CPU.
    BusySpin,

    /// Calls `sched_yield()` when idle. Lower CPU, slightly higher latency.
    Yield,

    /// Calls `nanosleep()` with increasing duration.
    Sleep {
        /// Initial sleep duration in nanoseconds.
        min_ns: u64,
        /// Maximum sleep duration in nanoseconds (doubled each idle cycle).
        max_ns: u64,
    },

    /// Progressive backoff: spin → yield → sleep.
    Backoff {
        /// Number of spin iterations before switching to yield.
        spin_count: i32,
        /// Number of yield iterations before switching to sleep.
        yield_count: i32,
        /// Maximum sleep duration in nanoseconds.
        max_sleep_ns: u64,
    },
}

impl IdleStrategy {
    fn into_c_ptr(self) -> Result<*mut ffi::IdleStrategy, MmapError> {
        let mut out: *mut ffi::IdleStrategy = std::ptr::null_mut();
        let rc = unsafe {
            match self {
                IdleStrategy::BusySpin => {
                    ffi::idle_strategy_busy_spin_create(&mut out)
                }
                IdleStrategy::Yield => {
                    ffi::idle_strategy_yield_create(&mut out)
                }
                IdleStrategy::Sleep { min_ns, max_ns } => {
                    ffi::idle_strategy_sleep_create(min_ns, max_ns, &mut out)
                }
                IdleStrategy::Backoff {
                    spin_count,
                    yield_count,
                    max_sleep_ns,
                } => {
                    ffi::idle_strategy_backoff_create(spin_count, yield_count, max_sleep_ns, &mut out)
                }
            }
        };
        if rc != 0 {
            return Err(map_rc(rc));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_mapper_non_null() {
        let rt = Runtime::region_mapper().expect("region_mapper should succeed");
        assert!(!rt.as_ptr().is_null(), "runtime pointer should be non-null");
    }

    #[test]
    fn test_metrics_processor_non_null() {
        let rt = Runtime::metrics_processor().expect("metrics_processor should succeed");
        assert!(!rt.as_ptr().is_null(), "runtime pointer should be non-null");
    }

    #[test]
    fn test_runtime_clone() {
        let rt = Runtime::region_mapper().unwrap();
        let cloned = rt.clone();
        // Both should be alive — dropping rt does not invalidate cloned
        drop(rt);
        assert!(!cloned.is_closed(), "cloned runtime should still be open");
    }

    #[test]
    fn test_runtime_create_and_drop() {
        let config = RuntimeConfig::new()
            .name("test-rt")
            .idle_strategy(IdleStrategy::Yield);
        let rt = Runtime::new(config).expect("runtime creation should succeed");
        assert!(!rt.is_closed(), "runtime should be open after creation");
        drop(rt);
        // No crash on drop — verifies runtime_destroy works
    }

    #[test]
    fn test_runtime_is_closed() {
        let rt = Runtime::region_mapper().unwrap();
        assert!(!rt.is_closed(), "fresh runtime should not be closed");
    }
}
