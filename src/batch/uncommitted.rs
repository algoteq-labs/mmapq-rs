//! Uncommitted-batch iterator — recovery surface for the batch lane.
//!
//! `UncommittedBatches` wraps the C appender-owned
//! `MmapqUncommittedBatchIterator` and yields `(batch_offset,
//! BatchMessageIter)` pairs for batches staged but never committed, from
//! the archive line onward to the frozen end of stream (snapshot at the
//! factory call). Each yielded offset is refilled as committable: pass it
//! to `IndexedBatchAppender::commit_batch()` to publish the batch — any
//! subset, any order, same session or after rejoin. Not committing = skip.
//!
//! Discarded batches reappear here after a restart — the walker reports
//! them and refill makes them committable; recovery code decides (owner
//! ruling 8). In-session, a discarded batch's offset is never committable
//! (`commit_batch` returns `Err(InvalidArgument)` — discard is final).
//!
//! The `&mut IndexedBatchAppender` borrow enforces the
//! no-append-while-iterating rule at compile time. Archiving may remove
//! batches the walk has not reached, even after an undo; a zero archive
//! line means a corrupt appender meta (pre-0.2.0 queues are refused at
//! open with `SchemaMismatch`).

use std::marker::PhantomData;

use crate::batch::batch_appender::IndexedBatchAppender;
use crate::batch::batch_iter::BatchMessageIter;
use crate::error::{map_rc, MmapError};
use crate::ffi;

/// Iterator over a snapshot of this appender's uncommitted (staged or
/// discarded) batches.
///
/// Created by [`IndexedBatchAppender::uncommitted_batches`]. The yielded
/// `BatchMessageIter` borrows this iterator — drop it before calling
/// `next()` again (the C message cursor is re-initialised per step).
/// Appender-owned in C: no destroy; dropping this guard only releases the
/// borrow.
pub struct UncommittedBatches<'a> {
    iterator: *mut ffi::UncommittedBatchIterator,
    /// Scratch context re-pointed at the C message cursor per step; the
    /// yielded `BatchMessageIter` borrows it, tying each step's message
    /// iteration to the `&mut self` borrow.
    ctx: ffi::BatchReadingContext,
    _phantom: PhantomData<&'a mut IndexedBatchAppender>,
}

impl<'a> UncommittedBatches<'a> {
    /// Wrap a successfully created appender-owned C iterator.
    ///
    /// The caller (the checked factory) establishes the live C iterator and
    /// the exclusive appender borrow recorded by `PhantomData`.
    /// `appender_id` labels the yielded messages (this lane's payloads).
    pub(crate) fn new(iterator: *mut ffi::UncommittedBatchIterator, appender_id: u8) -> Self {
        // SAFETY: BatchReadingContext is a repr(C) POD (integers + one pointer);
        // all-zero is a valid bit pattern for every field, including the
        // reserved bytes and the null cursor (never dereferenced before a
        // successful step re-points it).
        let mut ctx: ffi::BatchReadingContext = unsafe { std::mem::zeroed() };
        ctx.appender_id = appender_id;
        UncommittedBatches {
            iterator,
            ctx,
            _phantom: PhantomData,
        }
    }

    /// Advance to the next uncommitted batch in the snapshot.
    ///
    /// Returns `Ok(Some((batch_offset, messages)))` for a batch
    /// (`batch_offset` is the value to pass to
    /// `IndexedBatchAppender::commit_batch()`), `Ok(None)` when the snapshot
    /// is exhausted, or `Err` on a negative `MMAPQ_*` code (`MapTimedOut`
    /// is retryable with unchanged state — the next call re-yields the same
    /// batch).
    pub fn next(&mut self) -> Result<Option<(i64, BatchMessageIter<'_>)>, MmapError> {
        let mut batch_offset = 0;
        let mut message_count = 0;
        let mut msgs = std::ptr::null_mut();
        // SAFETY: Construction guarantees a live, appender-owned C iterator and the
        // exclusive appender borrow; the mutable step borrow excludes any live
        // BatchMessageIter (and its payload slices) from prior steps. Outputs are
        // writable aligned locals.
        let rc = unsafe {
            ffi::mmapq_uncommitted_batch_next(
                self.iterator,
                &mut batch_offset,
                &mut message_count,
                &mut msgs,
            )
        };
        match rc {
            0 => Ok(None),
            1 => {
                self.ctx.batch_offset = batch_offset;
                self.ctx.message_count = message_count;
                self.ctx.iterator = msgs;
                // SAFETY: The successful step re-initialised the appender-owned C
                // cursor (msgs) positioned before message 0; it stays live under the
                // exclusive appender borrow. The scratch ctx borrow ties the yielded
                // iterator to this &mut self, so no competing step can advance,
                // re-initialise, or close the cursor while it is alive.
                let iter = unsafe { BatchMessageIter::new(&mut self.ctx) };
                Ok(Some((batch_offset, iter)))
            }
            error => Err(map_rc(error)),
        }
    }
}
