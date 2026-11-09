use std::marker::PhantomData;
use std::slice;

use crate::ffi;
use crate::error::{map_rc, MmapError};

/// Zero-copy iterator over messages in a single batch.
///
/// Wraps the C `BatchPayloadIterator` and yields `(&[u8], u8)` pairs —
/// the message payload (borrowed until the next cursor step)
/// and the appender ID that wrote the batch.
///
/// Each payload borrows the current exclusive cursor step. Process it before
/// advancing again; its lifetime does not extend to the whole batch.
/// Mapping and framing errors are returned without allocating.
pub struct BatchMessageIter<'a> {
    iterator: *mut ffi::BatchPayloadIterator,
    appender_id: u8,
    _lifetime: PhantomData<&'a mut ffi::BatchReadingContext>,
}

impl<'a> BatchMessageIter<'a> {
    /// Create a new iterator from a mutable reference to a
    /// `BatchReadingContext` whose `batch_iterator` has been
    /// initialised by a read or poll call.
    ///
    /// The iterator starts **before** message 0 — the first call to
    /// `next()` advances to the first message.
    ///
    /// # Safety
    ///
    /// `ctx.iterator` must be non-null and initialized by a successful C read
    /// or poll. Its reader/poller and runtime must remain alive for `'a`.
    /// No other access path may advance, reset, exhaust, or close that cursor
    /// during this borrow. Advancing this cursor may remap its payload; the
    /// current step's mapping must remain valid until that step's borrow ends.
    pub(crate) unsafe fn new(ctx: &'a mut ffi::BatchReadingContext) -> Self {
        BatchMessageIter {
            iterator: ctx.iterator,
            appender_id: ctx.appender_id,
            _lifetime: PhantomData,
        }
    }

    /// Create a new iterator from a raw pointer to `BatchReadingContext`.
    ///
    /// # Safety
    ///
    /// `ctx` must point to a valid `BatchReadingContext` whose
    /// `iterator` has been initialised by a read or poll call.
    /// The caller must establish the same owner lifetime and exclusive cursor
    /// access as `new()` for `'a`; raw pointers do not carry that lifetime.
    /// `ctx` must be aligned and readable during construction. Each payload
    /// mapping must remain valid for its step borrow, not the entire batch.
    pub(crate) unsafe fn from_raw(ctx: *mut ffi::BatchReadingContext) -> Self {
        // SAFETY: The caller guarantees ctx is a live, aligned context and
        // establishes the owner lifetime/exclusive access recorded by PhantomData.
        unsafe {
            BatchMessageIter {
                iterator: (*ctx).iterator,
                appender_id: (*ctx).appender_id,
                _lifetime: PhantomData,
            }
        }
    }
}

impl BatchMessageIter<'_> {
    /// Advance without copying. Failure remains sticky until the batch is read again.
    #[inline]
    pub fn next(&mut self) -> Result<Option<(&[u8], u8)>, MmapError> {
        let mut data = std::ptr::null();
        let mut length = 0;
        // SAFETY: Construction guarantees a live, exclusively held C cursor and owner.
        // The mutable step borrow excludes live payload slices; outputs are writable locals.
        let result = unsafe { ffi::mmapq_batch_iterator_next(self.iterator, &mut data, &mut length) };
        match result {
            0 => Ok(None),
            1 => {
                // SAFETY: The checked C step returned a positive length within the current mapping.
                // The slice borrows this exclusive cursor step; it cannot survive another step.
                // Committed bytes must not be externally overwritten or truncated.
                let payload = unsafe { slice::from_raw_parts(data as *const u8, length as usize) };
                Ok(Some((payload, self.appender_id)))
            }
            error => Err(map_rc(error)),
        }
    }
}

impl BatchMessageIter<'_> {
    /// Return the raw C `BatchPayloadIterator` pointer for use with
    /// `BatchWriter::append_remaining_from()`.
    ///
    /// Useful in echo-server patterns where the first message is inspected
    /// and the remaining messages are piped directly through C (zero-copy).
    ///
    /// # Safety
    ///
    /// Returns `*mut` from `&mut self` — the caller gains mutable access to
    /// the C iterator through this pointer. The returned pointer must not
    /// be used concurrently with any other access to this iterator. Any later
    /// use must also keep its owner alive and must not advance/reset it while
    /// a payload slice is borrowed. The raw pointer does not retain the borrow.
    pub unsafe fn as_raw_iterator(&mut self) -> *mut ffi::BatchPayloadIterator {
        self.iterator
    }
}

impl<'a> std::fmt::Debug for BatchMessageIter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchMessageIter")
            .field("appender_id", &self.appender_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::batch_queue::IndexedBatchQueue;
    use crate::queue::QueueConfig;
    use crate::runtime::Runtime;
    use crate::types::{NextMove, PollResult};

    // covers: IndexedBatchQueue::create
    // covers: IndexedBatchQueue::create_poller
    #[test]
    fn test_batch_iter_empty_queue_returns_none() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-batch-iter-empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = IndexedBatchQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut p = q.create_poller().unwrap();

        let mut messages: Vec<Vec<u8>> = Vec::new();
        let result = p
            .poll(|_bi, mut iter| {
                while let Some((data, _aid)) = iter.next().expect("batch iteration") {
                    messages.push(data.to_vec());
                }
                NextMove::Advance
            })
            .expect("poll");
        assert_eq!(result, PollResult::EndOfStream);
        assert!(messages.is_empty());
        drop(p);
        drop(q);
    }

    // covers: BatchMessageIter::next
    #[test]
    fn test_batch_iter_yields_all_messages() {
        let rt = Runtime::region_mapper().unwrap();
        let dir = std::env::temp_dir().join("mmap-rs-test-batch-iter-all");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create base dir");
        let q = IndexedBatchQueue::create(
            QueueConfig::new(dir.to_str().unwrap(), "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let mut a = q.create_appender().unwrap();
        {
            let mut batch = a.start_batch().expect("start batch");
            batch.append(b"msg-a").expect("append");
            batch.append(b"msg-bb").expect("append");
            batch.append(b"msg-ccc").expect("append");
            batch.commit().expect("commit");
        }
        drop(a);

        let mut p = q.create_poller().unwrap();
        let mut messages: Vec<Vec<u8>> = Vec::new();
        let result = p
            .poll(|_bi, mut iter| {
                while let Some((data, _aid)) = iter.next().expect("batch iteration") {
                    messages.push(data.to_vec());
                }
                NextMove::Advance
            })
            .expect("poll");
        assert_eq!(result, PollResult::Advanced);
        assert_eq!(messages.len(), 3);
        assert_eq!(&messages[0], b"msg-a");
        assert_eq!(&messages[1], b"msg-bb");
        assert_eq!(&messages[2], b"msg-ccc");
        drop(p);
        drop(q);
    }
}
