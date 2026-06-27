//! Queue transfer — export, import, and copy queues to/from portable stream files.
//!
//! `QueueTransfer` exports messages from a queue reader to a flat binary stream
//! file, and imports them into a new or existing queue. The stream format is
//! compatible with the C library's `queue_transfer_export` / `queue_transfer_import`.

pub mod export;
pub mod import;
pub mod copy;

pub use export::{QueueTransfer, BatchQueueTransfer};
pub use import::StartIndexMode;
