// Compiled directly by tests/runtime_singleton.rs with --wrap linker flags;
// intentionally not a module in the integration-test crate.
//noinspection RsDetachedFile
//! Linker instrumentation of caller-owned references; all operations still
//! delegate to the real C runtime. No private C struct layout is assumed.

use mmapq::{ffi, Runtime};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

static REFERENCES: AtomicIsize = AtomicIsize::new(0);
static IN_GETTER: AtomicBool = AtomicBool::new(false);

extern "C" {
    fn __real_mmapq_runtime_region_mapper(out: *mut *mut ffi::Runtime) -> i32;
    fn __real_mmapq_runtime_metrics_processor(out: *mut *mut ffi::Runtime) -> i32;
    fn __real_mmapq_runtime_clone(rt: *mut ffi::Runtime, out: *mut *mut ffi::Runtime) -> i32;
    fn __real_mmapq_runtime_destroy(rt: *mut ffi::Runtime);
}

macro_rules! getter {
    ($wrapper:ident, $real:ident) => {
        /// # Safety
        /// The output must be writable for one runtime pointer, as required by
        /// the original C getter. Only the probe's main thread may call this.
        #[no_mangle]
        unsafe extern "C" fn $wrapper(out: *mut *mut ffi::Runtime) -> i32 {
            IN_GETTER.store(true, Ordering::Relaxed);
            // SAFETY: The original getter receives the same valid output pointer.
            let rc = unsafe { $real(out) };
            IN_GETTER.store(false, Ordering::Relaxed);
            if rc == 0 {
                REFERENCES.fetch_add(1, Ordering::Relaxed);
            }
            rc
        }
    };
}

getter!(__wrap_mmapq_runtime_region_mapper, __real_mmapq_runtime_region_mapper);
getter!(__wrap_mmapq_runtime_metrics_processor, __real_mmapq_runtime_metrics_processor);

/// # Safety
/// The runtime must be kept alive by a counted reference, and out must be
/// writable for one pointer. This preserves the original C clone contract.
#[no_mangle]
unsafe extern "C" fn __wrap_mmapq_runtime_clone(
    rt: *mut ffi::Runtime, out: *mut *mut ffi::Runtime,
) -> i32 {
    // SAFETY: Forward the original caller's live handle and output unchanged.
    let rc = unsafe { __real_mmapq_runtime_clone(rt, out) };
    // Getter-internal clones implement the one reference already counted by
    // the getter wrapper. Only additional caller clones count here.
    if rc == 0 && !IN_GETTER.load(Ordering::Relaxed) {
        REFERENCES.fetch_add(1, Ordering::Relaxed);
    }
    rc
}

/// # Safety
/// The caller transfers exactly one owned runtime reference, or passes null.
/// The real destructor receives it exactly once.
#[no_mangle]
unsafe extern "C" fn __wrap_mmapq_runtime_destroy(rt: *mut ffi::Runtime) {
    if !rt.is_null() {
        REFERENCES.fetch_sub(1, Ordering::Relaxed);
    }
    // SAFETY: Forward this caller-owned reference (or allowed null) unchanged.
    unsafe { __real_mmapq_runtime_destroy(rt) };
}

fn main() {
    let get = match std::env::args().nth(1).as_deref() {
        Some("region_mapper") => Runtime::region_mapper,
        Some("metrics_processor") => Runtime::metrics_processor,
        _ => panic!("expected singleton name"),
    };
    for _ in 0..32 {
        let original = get().unwrap();
        let cloned = original.clone();
        drop(original);
        assert!(!cloned.is_closed());
        drop(cloned);
        assert_eq!(REFERENCES.load(Ordering::Relaxed), 0,
                   "singleton getter/clone/drop leaked a caller reference");
    }
    // Check overlapping acquisitions and that releasing callers leaves the
    // registry-owned singleton available for later acquisition.
    let first = get().unwrap();
    let second = get().unwrap();
    assert_eq!(REFERENCES.load(Ordering::Relaxed), 2);
    drop(first);
    assert!(!second.is_closed());
    drop(second);
    assert_eq!(REFERENCES.load(Ordering::Relaxed), 0);
}
