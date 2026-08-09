//! Batch queue wrappers — batch-aware equivalents of the single-message API.
//!
//! A batch queue groups multiple messages into a single commit. The appender
//! starts a batch, appends messages, completes it, and commits atomically.
//! Pollers see the entire batch as one unit and can iterate individual messages.
//!
//! ## Batch lifecycle
//!
//! 1. `start_batch()` → returns a `BatchWriter<'_>`
//! 2. `BatchWriter::append(data)` — add messages
//! 3. `BatchWriter::commit()` — finalises and commits the batch (consumes writer)
//! 4. Dropping `BatchWriter` without `commit()` auto-rolls back

pub mod batch_queue;
pub mod batch_appender;
pub mod batch_poller;
pub mod batch_reader;
pub mod batch_archiver;
/// Zero-copy batch message iterator (`BatchMessageIter`).
///
/// Wraps the C `BatchPayloadIterator` to provide safe iteration over
/// messages in a single batch. Yields `(&[u8], u8)` pairs.
pub mod batch_iter;

pub use batch_queue::IndexedBatchQueue;
pub use batch_appender::{IndexedBatchAppender, BatchWriter};
pub use batch_poller::IndexedBatchPoller;
pub use batch_reader::{IndexedBatchReader, BatchMessage};
pub use batch_archiver::IndexedBatchArchiver;
pub use batch_iter::BatchMessageIter;
