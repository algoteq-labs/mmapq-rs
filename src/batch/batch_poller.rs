//! Batch poller — sequential cursor-based batch consumer.
//!
//! Each `poll()` delivers one batch. The handler receives a mutable reference
//! to a BatchReadingContext containing a BatchPayloadIterator positioned before
//! the first message. The handler calls batch_iterator_advance() to iterate
//! messages, then returns a NextMove directive (Advance, Stop, or Retreat).
//!
//! Thread-confined (`!Send + !Sync`). One per reader thread.

use std::any::Any;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use crate::error::MmapError;
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

fn call_batch_handler<H: FnMut(i64, &mut ffi::BatchReadingContext) -> NextMove>(
    data: *mut std::ffi::c_void,
    batch_index: i64,
    ctx: &mut ffi::BatchReadingContext,
) -> i32 {
    let handler: &mut H = unsafe { &mut *(data as *mut H) };
    handler(batch_index, ctx) as i32
}

unsafe extern "C" fn c_batch_trampoline(
    batch_index: i64,
    context: *mut ffi::BatchReadingContext,
    ctx: *mut std::ffi::c_void,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let cb: &mut BatchCallback = &mut *(ctx as *mut BatchCallback);
        (cb.func)(cb.data, batch_index, &mut *context)
    }));
    match result {
        Ok(m) => m,
        Err(e) => {
            BATCH_POLL_PANIC.with(|p| unsafe { *p.get() = Some(e) });
            NextMove::Stop as i32
        }
    }
}

/// Sequential batch poller. Thread-confined (`!Send + !Sync`).
pub struct IndexedBatchPoller {
    pub(crate) inner: *mut ffi::IndexedBatchPoller,
    pub(crate) _not_send: PhantomData<*mut ()>,
}

impl IndexedBatchPoller {
    /// Poll for the next batch, delivering it to the handler.
    ///
    /// The handler receives (batch_index, &mut BatchReadingContext). The
    /// BatchReadingContext's batch_iterator starts before message 0 — call
    /// batch_iterator_advance() before the first current_data() call.
    ///
    /// Returns Ok(PollResult) on success, Err(MmapError) on mapping timeout.
    /// Panics are caught and re-raised after the C frame unwinds.
    pub fn poll<H: FnMut(i64, &mut ffi::BatchReadingContext) -> NextMove>(
        &mut self,
        mut handler: H,
    ) -> Result<PollResult, MmapError> {
        let mut cb = BatchCallback {
            func: call_batch_handler::<H>,
            data: &mut handler as *mut H as *mut std::ffi::c_void,
        };

        let result = unsafe {
            ffi::indexed_batch_poller_poll(
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

        BATCH_POLL_PANIC.with(|p| unsafe {
            if let Some(e) = (*p.get()).take() {
                resume_unwind(e);
            }
        });

        match result {
            ffi::POLL_ERROR => Err(MmapError::MapTimedOut),
            _ => Ok(PollResult::from_ffi(result)),
        }
    }

    /// Move the cursor to a specific batch index.
    pub fn move_to_index(&mut self, index: i64) -> bool {
        unsafe { ffi::indexed_batch_poller_move_to_index(self.inner, index) != 0 }
    }

    /// Move the cursor to one past the last committed batch.
    pub fn move_to_end(&mut self) -> bool {
        unsafe { ffi::indexed_batch_poller_move_to_end(self.inner) != 0 }
    }

    /// Move the cursor to the last committed batch.
    pub fn move_to_last_existing_batch(&mut self) -> bool {
        unsafe { ffi::indexed_batch_poller_move_to_last_existing_batch(self.inner) != 0 }
    }

    /// Move the cursor to the earliest valid batch.
    pub fn move_to_start(&mut self) -> bool {
        unsafe { ffi::indexed_batch_poller_move_to_start(self.inner) != 0 }
    }

    /// The current batch index.
    pub fn current_batch_index(&self) -> i64 {
        unsafe { ffi::indexed_batch_poller_current_batch_index(self.inner) }
    }

    /// The lowest readable batch index.
    pub fn start_index(&self) -> i64 {
        unsafe { ffi::indexed_batch_poller_start_index(self.inner) }
    }

    /// Return true if a committed batch exists at the given index.
    pub fn exists(&self, batch_index: i64) -> bool {
        unsafe { ffi::indexed_batch_poller_exists(self.inner, batch_index) != 0 }
    }
}

impl Drop for IndexedBatchPoller {
    fn drop(&mut self) {
        unsafe { ffi::indexed_batch_poller_close(self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::IndexedBatchQueue;
    use crate::ffi;
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
            .poll(|_bi, _ctx| NextMove::Advance)
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
            .poll(|_bi, ctx| {
                while unsafe { ffi::batch_iterator_advance(&mut ctx.batch_iterator) != 0 } {
                    count += 1;
                }
                NextMove::Advance
            })
            .expect("poll");
        assert_eq!(result, PollResult::Advanced);
        assert_eq!(count, 1);
    }
}
