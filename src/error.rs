//! Error types returned by mmapq-rs operations.

use std::fmt;
use std::borrow::Cow;

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
    /// archived, or its base directory does not exist).
    FileNotFound,

    /// An argument is invalid (e.g., zero-length data, NULL pointer).
    InvalidArgument,

    /// The queue has been closed and cannot accept new operations.
    QueueClosed,

    /// The queue data is corrupted or inconsistent.
    Corrupt(Cow<'static, str>),

    /// Schema version or queue type mismatch on open (wrong queue kind,
    /// or file from an incompatible version), or on-disk geometry not
    /// supported by this build/host (a pre-CAND-38-rule geometry, or a
    /// page-size mismatch: a queue is portable only to hosts whose page
    /// size divides its region_size).
    SchemaMismatch,

    /// I/O error on a queue file or directory: a write failed, or it cannot
    /// be created or opened (permission denied, a path component that is not
    /// a directory, a read-only or full filesystem). Fix the cause, then
    /// retry; appender meta writes are idempotent.
    Eio,

    /// An OS-level I/O error occurred (open, mmap, ftruncate, etc.).
    Io(std::io::Error),

    /// An internal invariant violation or unexpected condition.
    Internal(Cow<'static, str>),

    /// The runtime failed to stop within the configured timeout.
    RuntimeCloseTimeout,

    /// The destination queue already exists and allow_append was not set.
    Exists,

    /// A mapped appender ID is already in use and on_conflict is Abort.
    IdConflict,

    /// Internal allocation failure (calloc, strdup, etc.).
    /// The caller should back off and alert — do not retry hot-loop.
    OutOfMemory,

    /// Queue meta file exists but is not yet fully initialised (transient, retry).
    NotReady,

    UnexpectedCode(i32),
    CallbackAborted,
}

/// Resolve the human-readable string for an `MMAPQ_*` error code.
///
/// The C library is the single source of truth for code-backed messages
/// (`src/mmapq_strerror.c`): this forwards to it so the text can never
/// diverge from C by construction. Returns a `'static` borrow of a C
/// static string literal — no allocation, no failure path.
///
/// # Safety
///
/// `mmapq_strerror` returns a pointer to a static string literal for
/// every input, including unknown codes (it returns "unknown error"), so
/// the `CStr` it produces is valid for `'static`. This is discharged
/// entirely by the C contract; callers need no `unsafe`.
fn c_strerror(rc: i32) -> &'static str {
    // SAFETY: mmapq_strerror returns a pointer to a static string literal
    // for every rc (src/mmapq_strerror.c), including unknown codes.
    unsafe { std::ffi::CStr::from_ptr(crate::ffi::mmapq_strerror(rc)) }
        .to_str()
        .unwrap_or("unknown error")
}

impl fmt::Display for MmapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Code-backed variants forward their text to C — the C
            // `switch` in mmapq_strerror.c is the source of truth. Codes
            // match `map_rc` below (values are ABI in mmap_platform.h).
            MmapError::MapTimedOut => write!(f, "{}", c_strerror(-1)),
            MmapError::InvalidArgument => write!(f, "{}", c_strerror(-2)),
            MmapError::QueueClosed => write!(f, "{}", c_strerror(-3)),
            MmapError::SchemaMismatch => write!(f, "{}", c_strerror(-5)),
            MmapError::Exists => write!(f, "{}", c_strerror(-6)),
            MmapError::FileNotFound => write!(f, "{}", c_strerror(-7)),
            MmapError::IdConflict => write!(f, "{}", c_strerror(-8)),
            MmapError::OutOfMemory => write!(f, "{}", c_strerror(-9)),
            MmapError::Eio => write!(f, "{}", c_strerror(-10)),
            MmapError::CallbackAborted => write!(f, "{}", c_strerror(-12)),
            MmapError::NotReady => write!(f, "{}", c_strerror(-11)),
            // Corrupt maps to ECORRUPT but keeps its borrowed or owned payload.
            MmapError::Corrupt(msg) => write!(f, "corruption: {}", msg),
            // Rust-owned variants — no C code; text lives here.
            MmapError::Io(e) => write!(f, "I/O error: {}", e),
            MmapError::Internal(msg) => write!(f, "internal error: {}", msg),
            MmapError::UnexpectedCode(code) => write!(f, "unexpected C return code: {}", code),
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

/// Map a C return code to the appropriate `MmapError` variant.
///
/// Call sites that receive a negative `i64` or `i32` from FFI should pass it
/// through this function to get a consistent error type.
#[cold]
#[inline(never)]
pub(crate) fn map_rc(rc: i32) -> MmapError {
    match rc {
        -12 => MmapError::CallbackAborted,
        -11 => MmapError::NotReady,
        -10 => MmapError::Eio,
        -9 => MmapError::OutOfMemory,
        -8 => MmapError::IdConflict,
        -7 => MmapError::FileNotFound,
        -6 => MmapError::Exists,
        -5 => MmapError::SchemaMismatch,
        // Corrupt keeps a borrowed or owned payload, but its default text comes
        // from C so the two cannot drift. Call sites with a more specific
        // message construct Corrupt directly.
        -4 => MmapError::Corrupt(c_strerror(-4).into()),
        -3 => MmapError::QueueClosed,
        -2 => MmapError::InvalidArgument,
        -1 => MmapError::MapTimedOut,
        _  => MmapError::UnexpectedCode(rc),
    }
}

/// Validate that a `usize` length fits in an `i32`.
///
/// Returns `InvalidArgument` if `len > i32::MAX as usize`, which would
/// silently truncate if cast directly. The C library's payload length
/// field is `int32_t`, so any value exceeding `i32::MAX` is unreachable.
pub(crate) fn checked_len(len: usize) -> Result<i32, MmapError> {
    i32::try_from(len).map_err(|_| MmapError::InvalidArgument)
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
            (-12, MmapError::CallbackAborted),
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
            (-11, MmapError::NotReady),
        ];
        for (rc, expected) in &cases {
            let actual = map_rc(*rc);
            assert_eq!(std::mem::discriminant(&actual), std::mem::discriminant(expected));
        }
    }

    #[test]
    fn test_map_rc_unknown() {
        assert!(matches!(map_rc(-99), MmapError::UnexpectedCode(_)));
    }

    #[test]
    fn test_map_rc_non_negative_returns_unexpected_code() {
        assert!(matches!(map_rc(0), MmapError::UnexpectedCode(_)));
        assert!(matches!(map_rc(42), MmapError::UnexpectedCode(_)));
    }

    #[test]
    fn test_checked_len_zero() {
        assert_eq!(checked_len(0).unwrap(), 0);
    }

    #[test]
    fn test_checked_len_i32_max() {
        assert_eq!(checked_len(i32::MAX as usize).unwrap(), i32::MAX);
    }

    #[test]
    fn test_checked_len_i32_max_plus_one() {
        let err = checked_len((i32::MAX as usize) + 1).unwrap_err();
        assert!(matches!(err, MmapError::InvalidArgument));
    }

    #[test]
    fn test_checked_len_large() {
        let err = checked_len(5_000_000_000_usize).unwrap_err();
        assert!(matches!(err, MmapError::InvalidArgument));
    }

    /// Every code-backed Display variant forwards to C (mmapq_strerror.c).
    ///
    /// Driven off the code list rather than a hand-written expectation
    /// table so it cannot become a mirror that silently drifts. Each code
    /// must yield a non-empty, non-unknown string distinct from every
    /// other code.
    #[test]
    fn test_c_strerror_all_codes_distinct() {
        let codes = [
            -1, // ETIMEDOUT
            -2, // EINVAL
            -3, // ECLOSED
            -4, // ECORRUPT
            -5, // ESCHEMA
            -6, // EEXIST
            -7, // ENOENT
            -8, // EIDCONFLICT
            -9, // ENOMEM
            -10, // EIO
            -11, // ENOTREADY
        ];
        let mut seen = std::collections::HashSet::new();
        for rc in codes {
            let s = c_strerror(rc);
            assert!(!s.is_empty(), "rc {} must not be empty", rc);
            assert_ne!(s, "unknown error", "rc {} must map to a known message", rc);
            assert!(
                seen.insert(s),
                "rc {} message '{s}' duplicates another code — messages must be distinct",
                rc
            );
        }
    }

    #[test]
    fn test_c_strerror_unknown_code() {
        // Unknown codes still return a static literal, never NULL/panic.
        assert_eq!(c_strerror(-999), "unknown error");
    }

    #[test]
    fn test_c_strerror_keyword_spots() {
        // A few keyword spot-checks to guard the forwarding wiring (the
        // anti-mirror test above proves distinctness, these prove content).
        assert!(c_strerror(-1).contains("tim"));
        assert!(c_strerror(-3).contains("closed"));
        assert!(c_strerror(-9).contains("allocation"));
    }

    #[test]
    fn test_display_code_backed_variants_forward_to_c() {
        // The whole point of (b): no code-backed Display text is hand-written.
        // If these ever fail, a code-backed arm was left on a literal here
        // instead of forwarding to c_strerror.
        // Exhaustive over the ten code-backed variants: spot-checking a
        // subset would leave the unchecked arms free to regress to a
        // hand-written literal, which is the exact failure 17c(b) removes.
        let pairs: [(MmapError, i32); 10] = [
            (MmapError::MapTimedOut, -1),
            (MmapError::InvalidArgument, -2),
            (MmapError::QueueClosed, -3),
            (MmapError::SchemaMismatch, -5),
            (MmapError::Exists, -6),
            (MmapError::FileNotFound, -7),
            (MmapError::IdConflict, -8),
            (MmapError::OutOfMemory, -9),
            (MmapError::Eio, -10),
            (MmapError::NotReady, -11),
        ];
        for (variant, rc) in pairs {
            assert_eq!(
                variant.to_string(),
                c_strerror(rc),
                "Display for rc {rc} is not forwarding to C"
            );
        }
        // Corrupt keeps its Rust payload.
        assert_eq!(MmapError::Corrupt("boom".into()).to_string(), "corruption: boom");
    }
}
