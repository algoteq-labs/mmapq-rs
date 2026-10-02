//! Sequential cursor-based message consumer.
//!
//! An `IndexedPoller` advances through messages in index order. Each call
//! to `poll()` reads the message at the current cursor position, passes
//! it to a user-supplied closure, and advances the cursor according to
//! the returned `NextMove` directive.
//!
//! Thread-confined (`!Send + !Sync`). One per reader thread.
//!
//! ## Panic safety
//!
//! The poll closure is called from a C trampoline. Panics are caught by
//! `catch_unwind`, stored in a thread-local, and re-raised after the C
//! frame unwinds. This prevents undefined behaviour from unwinding
//! through FFI.

use std::any::Any;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::types::{NextMove, PollResult};

thread_local! {
    static POLL_PANIC: UnsafeCell<Option<Box<dyn Any + Send>>> =
        const { UnsafeCell::new(None) };
}

/// Type-erased callback that the C library invokes.
struct Callback {
    func: fn(*mut std::ffi::c_void, PollEvent) -> i32,
    data: *mut std::ffi::c_void,
}

/// Event data passed from the C trampoline to the Rust closure.
struct PollEvent<'a> {
    index: i64,
    appender_id: u8,
    data: &'a [u8],
}

/// Monomorphized per closure type `H` — produces a concrete function pointer.
fn call_handler<H: FnMut(i64, u8, &[u8]) -> NextMove>(
    data: *mut std::ffi::c_void,
    event: PollEvent,
) -> i32 {
    // SAFETY: poll pairs this monomorphized function with &mut handler of exactly H.
    // C invokes it synchronously; no other access to that handler overlaps this call.
    let handler: &mut H = unsafe { &mut *(data as *mut H) };
    handler(event.index, event.appender_id, event.data) as i32
}

/// Single non-generic `extern "C"` trampoline that C calls.
///
/// Catches panics from the user closure and stores them in a thread-local
/// for re-raising after `mmapq_indexed_poller_poll` returns.
///
/// # Safety
///
/// `ctx` must point to a valid `Callback` whose `data` field points to a
/// live `H` closure on the call stack of `poll()`. Both `Callback` and `H`
/// live for the same stack frame and are valid because the C call is
/// synchronous — the trampoline is only called before `poll()` returns.
/// The payload must be non-null, readable for length bytes in one live mapping,
/// and immutable during the callback. Neither pointer may be retained.
///
/// `data` is a pointer to mapped payload memory returned by
/// `indexed_poller.c::mmapq_indexed_poller_poll`. The C library guarantees
/// `length > 0` (zero-length is the end-of-stream sentinel and is never
/// delivered to the handler). The `debug_assert` here catches a mismatch if that invariant
/// regresses.
unsafe extern "C" fn c_trampoline(
    index: i64,
    appender_id: u8,
    data: *const std::ffi::c_void,
    length: i32,
    ctx: *mut std::ffi::c_void,
) -> i32 {
    debug_assert!(length > 0, "indexed_poller.c never delivers zero-length to handler");
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: poll keeps this aligned stack Callback alive and exclusively
        // accessible through this synchronous trampoline invocation.
        let cb: &mut Callback = unsafe { &mut *(ctx as *mut Callback) };
        let event = PollEvent {
            index,
            appender_id,
            // SAFETY: C validates positive length within the live current mapping;
            // the callback cannot retain this slice or reposition the borrowed poller.
            data: unsafe { std::slice::from_raw_parts(data as *const u8, length as usize) },
        };
        (cb.func)(cb.data, event)
    }));
    match result {
        Ok(m) => m,
        Err(e) => {
            // SAFETY: The cell belongs to this thread; the raw write does not expose a reference
            // to the slot. The panic is saved only after the handler has unwound.
            POLL_PANIC.with(|p| unsafe { *p.get() = Some(e) });
            NextMove::Stop as i32
        }
    }
}

/// Sequential cursor-based poller.
///
/// Thread-confined (`!Send + !Sync`). One per reader thread.
pub struct IndexedPoller {
    /* field order: `inner` first so close runs while the runtime is alive */
    pub(crate) inner: *mut ffi::IndexedPoller,
    /// Keeps the read runtime alive for this participant's lifetime,
    /// independently of the queue (UAF regression: tests/runtime_uaf.rs).
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedPoller {
    /// Poll for the next message.
    ///
    /// The closure receives `(index, appender_id, data)` and returns a
    /// `NextMove` directing the cursor. The data slice is a flyweight view
    /// into mapped memory — do not retain it across poll calls.
    ///
    /// Panics from within the handler are caught, stored in a thread-local,
    /// and re-raised after the C frame unwinds, avoiding undefined behaviour
    /// from unwinding through FFI.
    ///
    /// Recursive `poll()` calls from within the handler are not supported
    /// (the thread-local panic slot would be overwritten).
    ///
    /// # Allocation
    ///
    /// Zero heap allocations on the success path. The closure is passed by
    /// value on the stack; the `Callback` struct and trampoline are
    /// stack-allocated. C error-code mapping uses borrowed static diagnostics.
    pub fn poll<H: FnMut(i64, u8, &[u8]) -> NextMove>(
        &mut self,
        mut handler: H,
    ) -> Result<PollResult, MmapError> {
        let mut cb = Callback {
            func: call_handler::<H>,
            data: &mut handler as *mut H as *mut std::ffi::c_void,
        };

        // SAFETY: self exclusively borrows a live poller with its runtime retained. cb and
        // handler are stack locals, paired with the correct trampoline. C neither
        // retains their pointers nor calls the handler concurrently; the trampoline
        // catches panics, and payload/cursor borrows cannot escape the callback.
        let result = unsafe {
            ffi::mmapq_indexed_poller_poll(
                self.inner,
                Some(
                    c_trampoline
                        as unsafe extern "C" fn(
                            i64,
                            u8,
                            *const std::ffi::c_void,
                            i32,
                            *mut std::ffi::c_void,
                        ) -> i32,
                ),
                &mut cb as *mut Callback as *mut std::ffi::c_void,
            )
        };

        // Re-raise any panic captured inside the trampoline.
        // SAFETY: This thread owns the slot. take ends its temporary mutable borrow before
        // resume_unwind runs; no reference to the cell escapes this closure.
        POLL_PANIC.with(|p| unsafe {
            if let Some(e) = (*p.get()).take() {
                resume_unwind(e);
            }
        });

        match result {
            ffi::MMAPQ_POLL_CORRUPT => Err(MmapError::Corrupt("committed payload data is corrupt".into())),
            // SAFETY: Polling has returned and the handle remains live; this only reads its error code.
            ffi::MMAPQ_POLL_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_poller_last_error(self.inner) })),
            _ => Ok(PollResult::from_ffi(result)),
        }
    }

    /// Move the cursor to a specific index.
    /// Returns `true` if the position is valid (not archived, not too far ahead).
    pub fn move_to_index(&mut self, index: i64) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_move_to_index(self.inner, index) != 0 }
    }

    /// Move the cursor to one past the last committed entry.
    /// After this call, `poll()` returns `EndOfStream` until a new message arrives.
    pub fn move_to_end(&mut self) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_move_to_end(self.inner) != 0 }
    }

    /// Move the cursor to the last committed entry.
    /// Returns `false` if the queue is empty.
    pub fn move_to_last(&mut self) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_move_to_last(self.inner) != 0 }
    }

    /// Move the cursor to the earliest valid entry (start index).
    pub fn move_to_start(&mut self) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_move_to_start(self.inner) != 0 }
    }

    /// The current cursor position.
    pub fn current_index(&self) -> i64 {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_current_index(self.inner) }
    }

    /// The lowest readable index (0 for non-archivable queues).
    pub fn start_index(&self) -> i64 {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_start_index(self.inner) }
    }

    /// Check whether a committed entry exists at the given index.
    pub fn exists(&self, index: i64) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_exists(self.inner, index) != 0 }
    }
}

impl Drop for IndexedPoller {
    fn drop(&mut self) {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_poller_close(self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use crate::queue::QueueConfig;
    use crate::runtime::Runtime;
    use crate::types::{NextMove, PollResult};
    use crate::IndexedQueue;
    use crate::IndexedPoller;

    fn setup(name: &str) -> (IndexedQueue, IndexedPoller, Runtime) {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-poller").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = IndexedQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let p = q.create_poller().unwrap();
        (q, p, rt)
    }

    #[test]
    fn test_poll_end_of_stream_when_empty() {
        let (_q, mut p, _rt) = setup("test_poll_empty");
        let result = p
            .poll(|_idx, _aid, _data| NextMove::Stop)
            .expect("poll should succeed");
        assert_eq!(result, PollResult::EndOfStream);
    }

    #[test]
    fn test_poll_one_message() {
        let (q, mut p, _rt) = setup("test_poll_one");
        let mut a = q.create_appender().unwrap();
        a.append(b"hello").unwrap();
        let mut count = 0;
        let result = p
            .poll(|_idx, _aid, data| {
                assert_eq!(data, b"hello");
                count += 1;
                NextMove::Advance
            })
            .expect("poll should succeed");
        assert_eq!(result, PollResult::Advanced);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_poll_multiple_messages() {
        let (q, mut p, _rt) = setup("test_poll_multi");
        let mut a = q.create_appender().unwrap();
        a.append(b"msg1").unwrap();
        a.append(b"msg2").unwrap();
        a.append(b"msg3").unwrap();

        let mut messages: Vec<String> = Vec::new();
        loop {
            let advanced = p
                .poll(|_idx, _aid, data| {
                    messages.push(String::from_utf8_lossy(data).to_string());
                    NextMove::Advance
                })
                .expect("poll should succeed");
            if advanced == PollResult::EndOfStream {
                break;
            }
        }
        assert_eq!(messages, vec!["msg1", "msg2", "msg3"]);
    }

    #[test]
    fn test_poll_handler_stop() {
        let (q, mut p, _rt) = setup("test_poll_stop");
        let mut a = q.create_appender().unwrap();
        a.append(b"first").unwrap();
        a.append(b"second").unwrap();

        let result = p
            .poll(|_idx, _aid, data| {
                assert_eq!(data, b"first");
                NextMove::Stop
            })
            .expect("poll should succeed");
        assert_eq!(result, PollResult::Stopped);
    }
}
