//! `#[repr(C)]` enum types matching the C library's ABI.
//!
//! These enums are exchanged across the FFI boundary and must match the
//! discriminant values defined in the C headers exactly.

/// Result of a single `IndexedPoller::poll()` call.
///
/// Mirrors the C `PollResult` enum.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollResult {
    /// No message at the current index. The polelr will retry on the next call.
    EndOfStream = 0,
    /// A message was processed and the cursor advanced to the next index.
    Advanced = 1,
    /// A message was processed but the cursor was held at the same index
    /// (handler returned `NextMove::Stop`).
    Stopped = 2,
    /// A message was processed and the cursor moved back one position
    /// (handler returned `NextMove::Retreat`).
    Retreated = 3,
    /// The current index is below the queue's start index (messages have
    /// been archived).
    Archived = 4,
    /// A mapping timeout or I/O error occurred.
    Error = 5,
}

impl PollResult {
    /// Convert a raw `i32` from the C library to a `PollResult`.
    pub fn from_ffi(val: i32) -> Self {
        match val {
            0 => PollResult::EndOfStream,
            1 => PollResult::Advanced,
            2 => PollResult::Stopped,
            3 => PollResult::Retreated,
            4 => PollResult::Archived,
            5 => PollResult::Error,
            _ => PollResult::Error,
        }
    }
}

/// Direction returned by the poll handler to control the cursor.
///
/// Mirrors the C `NextMove` enum.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextMove {
    /// Move the cursor forward to the next index after processing.
    Advance = 0,
    /// Hold the cursor at the current index.
    Stop = 1,
    /// Move the cursor backward one position.
    Retreat = 2,
}

impl From<NextMove> for i32 {
    fn from(m: NextMove) -> i32 {
        m as i32
    }
}

/// Availability of a single message entry at a given index.
///
/// Mirrors the C `EntryStatus` enum.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// The entry exists and is readable.
    Available = 0,
    /// No entry has been committed at this index yet.
    EndOfStream = 1,
    /// The entry was archived and is no longer accessible.
    Archived = 2,
    /// A mapping timeout or I/O error occurred.
    Error = 3,
}

/// Direction returned by the batch poll handler.
///
/// Mirrors the C `BatchNextMove` enum.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchNextMove {
    /// Advance to the next message within the current batch.
    Advance = 0,
    /// Skip to the next batch entirely.
    AdvanceBatch = 1,
    /// Stay at the current message.
    Stop = 2,
    /// Move back to the previous batch.
    RetreatBatch = 3,
}

impl From<BatchNextMove> for i32 {
    fn from(m: BatchNextMove) -> i32 {
        m as i32
    }
}

/// Result of a single `IndexedBatchPoller::poll()` call.
///
/// Mirrors the C `BatchPollResult` enum.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchPollResult {
    /// No batch is available at the current index.
    EndOfStream = 0,
    /// One message within the current batch was processed and the cursor
    /// advanced to the next message (handler returned `BatchNextMove::Advance`).
    AdvancedMessage = 1,
    /// The batch ended or the handler returned `BatchNextMove::AdvanceBatch`;
    /// cursor moved to the next batch.
    AdvancedBatch = 2,
    /// The handler returned `BatchNextMove::Stop`; cursor held.
    Stopped = 3,
    /// The handler returned `BatchNextMove::RetreatBatch`; cursor moved to
    /// the previous batch.
    RetreatedBatch = 4,
    /// The batch at the current index has been archived.
    Archived = 5,
    /// A mapping timeout or I/O error occurred.
    Error = 6,
}

impl BatchPollResult {
    /// Convert a raw `i32` from the C library to a `BatchPollResult`.
    pub fn from_ffi(val: i32) -> Self {
        match val {
            0 => BatchPollResult::EndOfStream,
            1 => BatchPollResult::AdvancedMessage,
            2 => BatchPollResult::AdvancedBatch,
            3 => BatchPollResult::Stopped,
            4 => BatchPollResult::RetreatedBatch,
            5 => BatchPollResult::Archived,
            6 => BatchPollResult::Error,
            _ => BatchPollResult::Error,
        }
    }
}

/// Availability of a batch entry at a given index.
///
/// Mirrors the C `BatchEntryStatus` enum.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchEntryStatus {
    /// The batch exists and is readable.
    Available = 0,
    /// No batch has been committed at this index yet.
    EndOfStream = 1,
    /// The batch was archived and is no longer accessible.
    Archived = 2,
    /// A mapping timeout or I/O error occurred.
    Error = 3,
}
