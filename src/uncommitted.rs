//! Uncommitted-message iterator — recovery surface for the single-message
//! lane.
//!
//! `UncommittedIter` wraps the C appender-owned `MmapqUncommittedPayloadIterator`
//! and yields `(offset, &[u8])` pairs for payloads appended but never
//! committed, from the archive line onward (snapshot at the factory call —
//! appends/commits after the call are invisible). Each yielded offset is
//! refilled as committable: pass it to `IndexedAppender::commit()` to
//! publish the message — any subset, any order, same session or after
//! rejoin. Not committing an offset skips it.
//!
//! The `&mut IndexedAppender` borrow enforces the no-append-while-iterating
//! rule at compile time (C documents it). Archiving may remove messages the walk
//! has not reached, even after an undo; a zero archive line means a corrupt
//! appender meta (pre-0.2.0 queues are refused at open with
//! `SchemaMismatch`).

use std::marker::PhantomData;
use std::slice;

use crate::appender::IndexedAppender;
use crate::error::{map_rc, MmapError};
use crate::ffi;

/// Iterator over a snapshot of this appender's uncommitted payloads.
///
/// Created by [`IndexedAppender::uncommitted_iter`]. The payload slice of
/// each yielded pair borrows the current step — process or copy it before
/// calling `next()` again; it does not survive the next step, a factory
/// re-call, or appender close. Appender-owned in C: no destroy; dropping
/// this guard only releases the borrow.
pub struct UncommittedIter<'a> {
    iterator: *mut ffi::UncommittedPayloadIterator,
    _phantom: PhantomData<&'a mut IndexedAppender>,
}

impl<'a> UncommittedIter<'a> {
    /// Wrap a successfully created appender-owned C iterator.
    ///
    /// The caller (the checked factory) establishes the live C iterator and
    /// the exclusive appender borrow recorded by `PhantomData`; storing the
    /// pointer itself dereferences nothing.
    pub(crate) fn new(iterator: *mut ffi::UncommittedPayloadIterator) -> Self {
        UncommittedIter {
            iterator,
            _phantom: PhantomData,
        }
    }

    /// Advance to the next uncommitted payload in the snapshot.
    ///
    /// Returns `Ok(Some((offset, payload)))` for a message (`offset` is the
    /// record start — the value to pass to `IndexedAppender::commit()`),
    /// `Ok(None)` when the snapshot is exhausted, or `Err` on a negative
    /// `MMAPQ_*` code (`MapTimedOut` is retryable with unchanged state —
    /// the next call re-yields the same message).
    pub fn next(&mut self) -> Result<Option<(i64, &[u8])>, MmapError> {
        let mut data = std::ptr::null();
        let mut length = 0;
        let mut offset = 0;
        // SAFETY: Construction guarantees a live, appender-owned C iterator and the
        // exclusive appender borrow; the mutable step borrow excludes live payload
        // slices from prior steps. Outputs are writable aligned locals.
        let rc = unsafe {
            ffi::mmapq_uncommitted_iterator_next(self.iterator, &mut data, &mut length, &mut offset)
        };
        match rc {
            0 => Ok(None),
            1 => {
                // SAFETY: The checked C step returned a positive length within a live
                // mapping. The slice borrows this exclusive iterator step; it cannot
                // survive another step, a factory re-call, or appender close.
                let payload = unsafe { slice::from_raw_parts(data as *const u8, length as usize) };
                Ok(Some((offset, payload)))
            }
            error => Err(map_rc(error)),
        }
    }
}
