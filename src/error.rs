//! Error types returned by mmap-c-rs operations.

use std::fmt;

/// Errors that can occur during queue operations.
///
/// Only `MapTimedOut` is reliably recoverable. The remaining variants
/// indicate programming errors, configuration issues, or I/O failures
/// and are typically fatal.
#[derive(Debug)]
pub enum MmapError {
    /// The async mapper runtime did not complete the mapping within the
    /// configured timeout. The operation can be retried.
    MapTimedOut,

    /// The backing file does not exist (queue not yet created or already
    /// archived).
    FileNotFound,

    /// The read position exceeds the current file size.
    BeyondFileSize,

    /// All 256 appender IDs are in use.
    PoolExhausted,

    /// The requested appender ID is already acquired by another appender.
    IdInUse,

    /// The queue has been closed and cannot accept new operations.
    QueueClosed,

    /// The queue data is corrupted or inconsistent.
    Corrupt(String),

    /// An OS-level I/O error occurred (open, mmap, ftruncate, etc.).
    Io(std::io::Error),

    /// An internal invariant violation or unexpected condition.
    Internal(String),

    /// The runtime failed to stop within the configured timeout.
    RuntimeCloseTimeout,
}

impl fmt::Display for MmapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MmapError::MapTimedOut => write!(f, "mapping timed out — runtime may be overloaded"),
            MmapError::FileNotFound => write!(f, "backing file does not exist"),
            MmapError::BeyondFileSize => write!(f, "position beyond current file size"),
            MmapError::PoolExhausted => write!(f, "all 256 appender IDs are in use"),
            MmapError::IdInUse => write!(f, "requested appender ID is already acquired"),
            MmapError::QueueClosed => write!(f, "queue has been closed"),
            MmapError::Corrupt(msg) => write!(f, "corruption: {}", msg),
            MmapError::Io(e) => write!(f, "I/O error: {}", e),
            MmapError::Internal(msg) => write!(f, "internal error: {}", msg),
            MmapError::RuntimeCloseTimeout => write!(f, "runtime failed to stop within timeout"),
        }
    }
}

impl std::error::Error for MmapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MmapError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for MmapError {
    fn from(e: std::io::Error) -> Self {
        MmapError::Io(e)
    }
}
