//! Batch poller — sequential cursor-based batch consumer.
//!
//! Each `poll()` lends a batch cursor to a stack-backed callback. Advance with
//! `next()` and process each borrowed payload before advancing again.
//!
//! Thread-confined (`!Send + !Sync`). One per reader thread.

use std::any::Any;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use crate::batch::BatchMessageIter;
use crate::error::{map_rc, MmapError};
use crate::ffi;
use crate::types::{NextMove, PollResult};

thread_local! {
    static BATCH_POLL_PANIC: UnsafeCell<Option<Box<dyn Any + Send>>> =
        const { UnsafeCell::new(None) };
}

struct BatchCallback {
    func: fn(*mut std::ffi::c_void, i64, &mut ffi::BatchReadingContext) -> i32,
    data: *mut std::ffi::c_void,
}

fn call_batch_handler<H: for<'batch> FnMut(i64, BatchMessageIter<'batch>) -> NextMove>(
    data: *mut std::ffi::c_void,
    batch_index: i64,
    ctx: &mut ffi::BatchReadingContext,
) -> i32 {
    // SAFETY: poll pairs this monomorphized function with &mut handler of exactly H.
    // C invokes it synchronously; no other access to that handler overlaps this call.
    let handler: &mut H = unsafe { &mut *(data as *mut H) };
    // SAFETY: Only the C batch trampoline calls this private helper, with an AVAILABLE
    // context owned by the live, exclusively borrowed poller. H's for<'batch>
    // bound prevents retaining the cursor outside this callback.
    let iter = unsafe { BatchMessageIter::new(ctx) };
    handler(batch_index, iter) as i32
}

/// # Safety
///
/// C must supply a live, aligned BatchCallback and an exclusive, initialized
/// AVAILABLE context from the poller. The owner and runtime must stay alive
/// until return. C must call synchronously and not retain the Rust callback.
unsafe extern "C" fn c_batch_trampoline(
    batch_index: i64,
    context: *mut ffi::BatchReadingContext,
    ctx: *mut std::ffi::c_void,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The trampoline contract supplies these aligned, live pointers;
        // the context and callback are exclusively used for this invocation.
        unsafe {
            let cb: &mut BatchCallback = &mut *(ctx as *mut BatchCallback);
            (cb.func)(cb.data, batch_index, &mut *context)
        }
    }));
    match result {
        Ok(m) => m,
        Err(e) => {
            // SAFETY: The cell belongs to this thread; the raw write does not expose a reference
            // to the slot. The panic is saved only after the handler has unwound.
            BATCH_POLL_PANIC.with(|p| unsafe { *p.get() = Some(e) });
            NextMove::Stop as i32
        }
    }
}

/// Sequential batch poller. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchPoller {
    pub(crate) inner: *mut ffi::IndexedBatchPoller,
    /// Keeps the read runtime alive for this participant's lifetime.
    /// Forwarded from the parent queue at creation; None = sync mapping.
    pub(crate) _runtime: Option<crate::runtime::Runtime>,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedBatchPoller {
    /// Poll for the next batch, delivering it to the handler.
    ///
    /// The handler receives `(batch_index, BatchMessageIter<'_>)`. The
    /// iterator yields `(&[u8], u8)` pairs — the message payload and the
    /// appender ID that wrote the batch. Iteration starts at message 0.
    ///
    /// Returns Ok(PollResult) on success, Err(MmapError) on mapping timeout.
    /// Panics are caught and re-raised after the C frame unwinds.
    pub fn poll<H: for<'batch> FnMut(i64, BatchMessageIter<'batch>) -> NextMove>(
        &mut self,
        mut handler: H,
    ) -> Result<PollResult, MmapError> {
        let mut cb = BatchCallback {
            func: call_batch_handler::<H>,
            data: &mut handler as *mut H as *mut std::ffi::c_void,
        };

        // SAFETY: self exclusively borrows a live poller with its runtime retained. cb and
        // handler are stack locals, paired with the correct trampoline. C neither
        // retains their pointers nor calls the handler concurrently; the trampoline
        // catches panics, and payload/cursor borrows cannot escape the callback.
        let result = unsafe {
            ffi::mmapq_indexed_batch_poller_poll(
                self.inner,
                Some(c_batch_trampoline
                    as unsafe extern "C" fn(
                        i64,
                        *mut ffi::BatchReadingContext,
                        *mut std::ffi::c_void,
                    ) -> i32),
                &mut cb as *mut BatchCallback as *mut std::ffi::c_void,
            )
        };

        // SAFETY: This thread owns the slot. take ends its temporary mutable borrow before
        // resume_unwind runs; no reference to the cell escapes this closure.
        BATCH_POLL_PANIC.with(|p| unsafe {
            if let Some(e) = (*p.get()).take() {
                resume_unwind(e);
            }
        });

        match result {
            // SAFETY: Polling has returned and the handle remains live; this only reads its error code.
            ffi::MMAPQ_POLL_ERROR => Err(map_rc(unsafe { ffi::mmapq_indexed_batch_poller_last_error(self.inner) })),
            _ => Ok(PollResult::from_ffi(result)),
        }
    }

    /// Move the cursor to a specific batch index.
    pub fn move_to_index(&mut self, index: i64) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_move_to_index(self.inner, index) != 0 }
    }

    /// Move the cursor to one past the last committed batch.
    pub fn move_to_end(&mut self) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_move_to_end(self.inner) != 0 }
    }

    /// Move the cursor to the last committed batch.
    pub fn move_to_last_existing_batch(&mut self) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_move_to_last_existing_batch(self.inner) != 0 }
    }

    /// Move the cursor to the earliest valid batch.
    pub fn move_to_start(&mut self) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_move_to_start(self.inner) != 0 }
    }

    /// The current batch index.
    pub fn current_batch_index(&self) -> i64 {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_current_batch_index(self.inner) }
    }

    /// The lowest readable batch index.
    pub fn start_index(&self) -> i64 {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_start_index(self.inner) }
    }

    /// Return true if a committed batch exists at the given index.
    pub fn exists(&self, batch_index: i64) -> bool {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_exists(self.inner, batch_index) != 0 }
    }
}

impl Drop for IndexedBatchPoller {
    fn drop(&mut self) {
        // SAFETY: The poller is owned and thread-confined; safe callbacks cannot retain a
        // payload/cursor or overlap another operation on this handle. Its runtime
        // remains live during lookup/reposition, and Drop closes it exactly once.
        unsafe { ffi::mmapq_indexed_batch_poller_close(self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::IndexedBatchQueue;
    use crate::queue::QueueConfig;
    use crate::runtime::Runtime;
    use crate::types::NextMove;
    use crate::types::PollResult;

    fn setup(name: &str) -> (IndexedBatchQueue, super::IndexedBatchPoller, Runtime) {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-bpoller").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = IndexedBatchQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let p = q.create_poller().unwrap();
        (q, p, rt)
    }

    #[test]
    fn test_batch_poll_end_of_stream() {
        let (_q, mut p, _rt) = setup("test_bpoll_empty");
        let result = p
            .poll(|_bi, _iter| NextMove::Advance)
            .expect("poll should succeed");
        assert_eq!(result, PollResult::EndOfStream);
    }

    #[test]
    fn test_batch_poll_one_message() {
        let (q, mut p, _rt) = setup("test_bpoll_one");
        let mut a = q.create_appender().unwrap();
        let mut batch = a.start_batch().expect("start batch");
        batch.append(b"hello").expect("append");
        batch.commit().expect("commit");
        drop(a);

        let mut count = 0;
        let result = p
            .poll(|_bi, mut iter| {
                while let Some((_data, _aid)) = iter.next().expect("batch iteration") {
                    count += 1;
                }
                NextMove::Advance
            })
            .expect("poll");
        assert_eq!(result, PollResult::Advanced);
        assert_eq!(count, 1);
    }
}
