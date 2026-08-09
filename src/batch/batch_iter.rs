use std::marker::PhantomData;
use std::slice;

use crate::ffi;

/// Zero-copy iterator over messages in a single batch.
///
/// Wraps the C `BatchPayloadIterator` and yields `(&[u8], u8)` pairs —
/// the message payload (lifetime-bound to the containing reader/poller)
/// and the appender ID that wrote the batch.
///
/// The lifetime `'a` is tied to the `BatchReadingContext` or the
/// reader/poller that owns the mapped region. The returned byte slices
/// are valid for `'a` — they point into memory-mapped payload data that
/// is stable as long as the reader/poller exists.
///
/// # Panics
///
/// The `Iterator` impl does not panic. The underlying C functions may
/// abort on internal invariant violations (never on caller input).
pub struct BatchMessageIter<'a> {
    iterator: *mut ffi::BatchPayloadIterator,
    appender_id: u8,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> BatchMessageIter<'a> {
    /// Create a new iterator from a mutable reference to a
    /// `BatchReadingContext` whose `batch_iterator` has been
    /// initialised by a read or poll call.
    ///
    /// The iterator starts **before** message 0 — the first call to
    /// `next()` advances to the first message.
    pub fn new(ctx: &'a mut ffi::BatchReadingContext) -> Self {
        BatchMessageIter {
            iterator: &mut ctx.batch_iterator,
            appender_id: ctx.appender_id,
            _lifetime: PhantomData,
        }
    }

    /// Create a new iterator from a raw pointer to `BatchReadingContext`.
    ///
    /// # Safety
    ///
    /// `ctx` must point to a valid `BatchReadingContext` whose
    /// `batch_iterator` has been initialised by a read or poll call.
    /// The mapped memory backing the returned slices must remain mapped
    /// (i.e. the reader/poller must not be dropped or re-wrapped) for
    /// the lifetime `'a`.
    pub unsafe fn from_raw(ctx: *mut ffi::BatchReadingContext) -> Self {
        BatchMessageIter {
            iterator: std::ptr::addr_of_mut!((*ctx).batch_iterator),
            appender_id: (*ctx).appender_id,
            _lifetime: PhantomData,
        }
    }
}

impl<'a> Iterator for BatchMessageIter<'a> {
    type Item = (&'a [u8], u8);

    fn next(&mut self) -> Option<Self::Item> {
        let advanced = unsafe { ffi::batch_iterator_advance(self.iterator) };
        if advanced == 0 {
            return None;
        }
        let data = unsafe { ffi::batch_iterator_current_data_ffi(self.iterator) };
        let length = unsafe { ffi::batch_iterator_current_length_ffi(self.iterator) };
        if length <= 0 {
            return None;
        }
        let slice = unsafe { slice::from_raw_parts(data as *const u8, length as usize) };
        Some((slice, self.appender_id))
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
    /// Returns `*mut` from `&self` — the caller gains mutable access to
    /// the C iterator through this pointer. The returned pointer must not
    /// be used concurrently with any other access to this iterator.
    pub unsafe fn as_raw_iterator(&self) -> *mut ffi::BatchPayloadIterator {
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
            .poll(|_bi, iter| {
                for (data, _aid) in iter {
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
            .poll(|_bi, iter| {
                for (data, _aid) in iter {
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
