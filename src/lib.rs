#![warn(missing_docs)]
//! # mmapq-rs
//!
//! Idiomatic Rust bindings for **mmapq** — a low-latency, memory-mapped,
//! shared-memory indexed queue for inter-process messaging on Linux.
//!
//! ## Architecture
//!
//! Messages are stored in two file types:
//! - **Header file** (one per queue): 64-byte atomically-committed entries
//!   spaced 512 bytes apart (vertical walk), each containing an appender ID
//!   and payload offset.
//! - **Payload files** (one per appender): [length prefix][payload bytes] framing.
//!
//! The queue is split into two logical roles:
//! - **Request queue**: writer appends, server reads/echoes
//! - **Response queue**: server appends echo, reader polls
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
//! ```no_run
//! use mmapq::{IndexedQueue, NextMove, QueueConfig, Runtime};
//!
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
pub use poller::IndexedPoller;
pub use reader::{IndexedReader, Message};
pub use archiver::IndexedArchiver;
pub use transfer::*;
pub use batch::{
    IndexedBatchQueue, IndexedBatchAppender, IndexedBatchPoller,
    IndexedBatchReader, IndexedBatchArchiver, BatchWriter, BatchMessage,
    BatchMessageIter,
};
