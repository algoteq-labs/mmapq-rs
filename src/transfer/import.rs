//! Start index mode for queue transfer operations.

/// Controls how start indices are mapped during transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartIndexMode {
    /// Rebase the start index to 0 in the output queue.
    Rebase,
    /// Preserve the original start index from the source.
    Preserve,
}
