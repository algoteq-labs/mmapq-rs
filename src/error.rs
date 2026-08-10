//! Error types returned by mmapq-rs operations.

use std::fmt;

/// Errors that can occur during queue operations.
///
/// Only `MapTimedOut` is reliably recoverable. The remaining variants
/// indicate programming errors, configuration issues, or I/O failures
/// and are typically fatal.
#[derive(Debug)]
#[allow(missing_docs)]
pub enum MmapError {
    /// The async mapper runtime did not complete the mapping within the
    /// configured timeout. The operation can be retried.
    MapTimedOut,

    /// The backing file does not exist (queue not yet created or already
    /// archived).
    FileNotFound,

    /// An argument is invalid (e.g., zero-length data, NULL pointer).
    InvalidArgument,

    /// The queue has been closed and cannot accept new operations.
    QueueClosed,

    /// The queue data is corrupted or inconsistent.
    Corrupt(String),

    /// Schema version or queue type mismatch on open (wrong queue kind,
    /// or file from an incompatible version).
    SchemaMismatch,

    /// I/O error — metadata file write failure (rw_meta_open failed).
    /// Retry is safe: appender meta writes are idempotent.
    Eio,

    /// An OS-level I/O error occurred (open, mmap, ftruncate, etc.).
    Io(std::io::Error),

    /// An internal invariant violation or unexpected condition.
    Internal(String),

    /// The runtime failed to stop within the configured timeout.
    RuntimeCloseTimeout,

    /// The destination queue already exists and allow_append was not set.
    Exists,

    /// A mapped appender ID is already in use and on_conflict is Abort.
    IdConflict,

    /// Internal allocation failure (calloc, strdup, etc.).
    /// The caller should back off and alert — do not retry hot-loop.
    OutOfMemory,
}

impl fmt::Display for MmapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MmapError::MapTimedOut => write!(f, "mapping timed out — runtime may be overloaded"),
            MmapError::InvalidArgument => write!(f, "invalid argument (zero-length data, NULL pointer, etc.)"),
            MmapError::FileNotFound => write!(f, "backing file does not exist"),
            MmapError::QueueClosed => write!(f, "queue has been closed"),
            MmapError::Corrupt(msg) => write!(f, "corruption: {}", msg),
            MmapError::SchemaMismatch => write!(f, "schema version or queue type mismatch (wrong queue kind, or file from an incompatible version)"),
            MmapError::Eio => write!(f, "I/O error — metadata file write failure (rw_meta_open failed)"),
            MmapError::Io(e) => write!(f, "I/O error: {}", e),
            MmapError::Internal(msg) => write!(f, "internal error: {}", msg),
            MmapError::RuntimeCloseTimeout => write!(f, "runtime failed to stop within timeout"),
            MmapError::Exists => write!(f, "destination queue already exists"),
            MmapError::IdConflict => write!(f, "mapped appender ID is already in use"),
            MmapError::OutOfMemory => write!(f, "internal allocation failure — back off, do not retry hot-loop"),
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

/// Map a C return code to the appropriate `MmapError` variant.
///
/// Call sites that receive a negative `i64` or `i32` from FFI should pass it
/// through this function to get a consistent error type.
pub(crate) fn map_rc(rc: i32) -> MmapError {
    match rc {
        -10 => MmapError::Eio,
        -9 => MmapError::OutOfMemory,
        -8 => MmapError::IdConflict,
        -7 => MmapError::FileNotFound,
        -6 => MmapError::Exists,
        -5 => MmapError::SchemaMismatch,
        -4 => MmapError::Corrupt("on-disk corruption detected".into()),
        -3 => MmapError::QueueClosed,
        -2 => MmapError::InvalidArgument,
        -1 => MmapError::MapTimedOut,
        _  => MmapError::Internal(format!("unexpected C return code: {}", rc)),
    }
}

impl From<std::io::Error> for MmapError {
    fn from(e: std::io::Error) -> Self {
        MmapError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_rc_eio() {
        assert!(matches!(map_rc(-10), MmapError::Eio));
    }

    #[test]
    fn test_map_rc_all_codes() {
        let cases = [
            (-1, MmapError::MapTimedOut),
            (-2, MmapError::InvalidArgument),
            (-3, MmapError::QueueClosed),
            (-4, MmapError::Corrupt("".into())),
            (-5, MmapError::SchemaMismatch),
            (-6, MmapError::Exists),
            (-7, MmapError::FileNotFound),
            (-8, MmapError::IdConflict),
            (-9, MmapError::OutOfMemory),
            (-10, MmapError::Eio),
        ];
        for (rc, expected) in &cases {
            let actual = map_rc(*rc);
            assert_eq!(std::mem::discriminant(&actual), std::mem::discriminant(expected));
        }
    }

    #[test]
    fn test_map_rc_unknown() {
        assert!(matches!(map_rc(-99), MmapError::Internal(_)));
    }

    #[test]
    fn test_map_rc_non_negative_returns_internal() {
        assert!(matches!(map_rc(0), MmapError::Internal(_)));
        assert!(matches!(map_rc(42), MmapError::Internal(_)));
    }
}
