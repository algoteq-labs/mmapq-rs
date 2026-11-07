#![warn(missing_docs)]
//! # mmapq-rs
//!
//! Idiomatic Rust bindings for **mmapq** — a low-latency, memory-mapped,
//! shared-memory indexed queue for inter-process messaging on Linux.
//!
//! ## Architecture
//!
//! Messages are stored in two file types:
//! - **Header file** (one per queue): 8-byte atomically-committed entries
//!   (HEADER_SIZE) spaced 512 bytes apart (vertical walk), each containing
//!   an appender ID and payload offset.
//! - **Payload files** (one per appender): [length prefix][payload bytes] framing.
//!
//! ## Thread safety
//!
//! | Type          | Send | Sync | Notes |
//! |---------------|------|------|-------|
//! | `Runtime`     | ✅   | ✅   | Background thread, mutex-guarded registry |
//! | `IndexedQueue`| ✅   | ✅   | Config holder, re-entrant factories |
//! | Participants  | ❌   | ❌   | One per thread, owns cursor/position |
//!
//! ## Quick start
//!
//! Use a persistent filesystem for retained history. Commit publishes to
//! readers but does not guarantee power-loss durability. Consumers must
//! save their own progress and seek explicitly after reopening.
//!
//! Zero-copy reader methods currently take shared borrows: copy a message
//! before another read or transfer on the same reader. Copy each batch
//! iterator payload before advancing; mapped regions may be reused.
//!
//! ```no_run
//! use mmapq::{IndexedQueue, NextMove, QueueConfig, Runtime};
//!
//! std::fs::create_dir_all("./data").unwrap();
//! let rt = Runtime::region_mapper().unwrap();
//! let q = IndexedQueue::create(
//!     QueueConfig::new("./data", "my_queue")
//!         .write_runtime(&rt)
//!         .read_runtime(&rt),
//! ).unwrap();
//!
//! // Writer
//! let mut appender = q.create_appender().unwrap();
//! appender.append(b"hello").unwrap();
//!
//! // Reader
//! let mut poller = q.create_poller().unwrap();
//! poller.poll(|_idx, _aid, data| {
//!     println!("got: {:?}", std::str::from_utf8(data));
//!     NextMove::Advance
//! }).unwrap();
//! ```
//!
//! ## Examples
//!
//! - `echo_server` — cross-process echo server for latency benchmarking
//! - `latency_bench` — round-trip latency benchmark (internal + external modes)
//! - `batch_echo_server` / `batch_latency_bench` — batch queue equivalents

#[doc(hidden)]
pub mod ffi;
pub mod error;
pub mod types;
pub mod runtime;
pub mod queue;
pub mod appender;
pub mod uncommitted;
pub mod poller;
pub mod reader;
pub mod archiver;
pub mod batch;
pub mod transfer;

pub use error::MmapError;
pub use types::{
    BatchEntryStatus, BatchPollResult, EntryStatus, NextMove, PollResult,
};
pub use runtime::{Runtime, RuntimeConfig, IdleStrategy};
pub use queue::{IndexedQueue, QueueConfig};
pub use appender::IndexedAppender;
pub use uncommitted::UncommittedIter;
pub use poller::IndexedPoller;
pub use reader::{IndexedReader, Message, ReadResult};
pub use archiver::IndexedArchiver;
pub use transfer::*;
pub use batch::{
    IndexedBatchQueue, IndexedBatchAppender, IndexedBatchPoller,
    IndexedBatchReader, IndexedBatchArchiver, BatchWriter, BatchMessage,
    BatchMessageIter, UncommittedBatches,
};
