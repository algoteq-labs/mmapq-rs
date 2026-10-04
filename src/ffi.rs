#![allow(non_camel_case_types, non_snake_case, dead_code, missing_docs)]
// ^^ missing_docs is suppressed because this is an FFI declarations file
//    (raw bindings; higher-level wrapper contracts live in their modules).

//! Raw C ABI declarations, not a safe ownership layer.
//!
//! # Safety
//!
//! Callers of the foreign functions must satisfy the corresponding contracts in
//! the vendored C headers under c/include. Handles must come from the matching
//! successful C factory and remain live until their matching close/destroy;
//! copied pointer values do not acquire ownership. Output pointers must be
//! writable/aligned, buffers valid for their supplied length, and strings
//! NUL-terminated. Retained runtime pointers must outlive their participants.
//! Serialize operations on thread-confined handles. Returned mmap pointers
//! have the accessor's invalidation lifetime, not an arbitrary Rust lifetime;
//! do not mutate/truncate committed backing storage while slices exist.
//!
//! Foreign declarations are unsafe to call even though their syntax is `pub fn`
//! inside `extern "C"`. Their callback types below likewise do not enforce
//! Rust lifetimes. See doc/unsafe-audit.md for unresolved wrapper invariants.

use libc::{c_char, c_int, c_void};

/* ── Opaque types ─────────────────────────────────────────────────────────── */

pub enum Runtime {}
pub enum IndexedQueue {}
pub enum IndexedAppender {}
pub enum IndexedPoller {}
pub enum IndexedReader {}
pub enum IndexedArchiver {}
pub enum IndexedBatchQueue {}
pub enum IndexedBatchAppender {}
pub enum IndexedBatchPoller {}
pub enum IndexedBatchReader {}
pub enum IndexedBatchArchiver {}
pub enum MutableBatch {}
/// Opaque — struct definition is private to C (`src/internal/`).
/// All access through `mmapq_batch_iterator_advance`, `mmapq_batch_iterator_current_data_ffi`,
/// etc. Zero-sized in Rust — never allocated or accessed directly.
#[repr(C)]
pub struct BatchPayloadIterator {
    _private: [u8; 0],
}
pub enum PayloadIterator {}
pub enum UncommittedPayloadIterator {}
pub enum ExecList {}
pub enum QueueRegionAccessor {}

pub type IdleStrategy = c_void;
pub type Executable = c_void;

/* ── Enums (values must match C headers) ──────────────────────────────────── */

pub const WRAP_SUCCESS: c_int = 0;
pub const WRAP_MAP_TIMED_OUT: c_int = 1;
pub const WRAP_FILE_NOT_FOUND: c_int = 2;
pub const WRAP_UNWRAPPED: c_int = 4;

pub const HEADER_NULL: u64 = 0;
pub const HEADER_NULL_INDEX: i64 = -1;

pub const MMAPQ_POLL_END_OF_STREAM: c_int = 0;
pub const MMAPQ_POLL_ADVANCED: c_int = 1;
pub const MMAPQ_POLL_STOPPED: c_int = 2;
pub const MMAPQ_POLL_RETREATED: c_int = 3;
pub const MMAPQ_POLL_ARCHIVED: c_int = 4;
pub const MMAPQ_POLL_ERROR: c_int = 5;
pub const MMAPQ_POLL_CORRUPT: c_int = 6;

pub const MMAPQ_NEXT_ADVANCE: c_int = 0;
pub const MMAPQ_NEXT_STOP: c_int = 1;
pub const MMAPQ_NEXT_RETREAT: c_int = 2;

pub const MMAPQ_ENTRY_AVAILABLE: c_int = 0;
pub const MMAPQ_ENTRY_END_OF_STREAM: c_int = 1;
pub const MMAPQ_ENTRY_ARCHIVED: c_int = 2;
pub const MMAPQ_ENTRY_ERROR: c_int = 3;

pub const MMAPQ_BATCH_ENTRY_AVAILABLE: c_int = 0;
pub const MMAPQ_BATCH_ENTRY_END_OF_STREAM: c_int = 1;
pub const MMAPQ_BATCH_ENTRY_ARCHIVED: c_int = 2;
pub const MMAPQ_BATCH_ENTRY_ERROR: c_int = 3;

pub const QUEUE_TRANSFER_REBASE: c_int = 0;
pub const QUEUE_TRANSFER_PRESERVE: c_int = 1;

pub const APPENDER_ID_AUTO: i32 = -1;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictBehavior {
    Auto = 0,
    Abort = 1,
}

/// Appender-ID mapping callback.
///
/// # Safety
/// ctx must match the callback's live state for each synchronous invocation.
/// The callback must not unwind across C or retain borrowed C storage.
pub type AppenderIdMapper = Option<
    unsafe extern "C" fn(source_aid: u16, ctx: *mut c_void) -> i32,
>;

/* ── Callback types ───────────────────────────────────────────────────────── */

/// Borrowed payload callback.
///
/// # Safety
/// data must be readable for positive length bytes in one live mapping for the
/// call. ctx must match the callback state; neither may escape or be used to
/// reenter the same poller. Catch Rust panics before returning across C.
pub type MessageHandler = Option<
    unsafe extern "C" fn(
        index: i64,
        appender_id: u8,
        data: *const c_void,
        length: i32,
        ctx: *mut c_void,
    ) -> c_int,
>;

/// Callback invoked by `mmapq_indexed_batch_poller_poll()` for each batch.
///
/// Receives the batch index, a pointer to the BatchReadingContext (embedded
/// BatchPayloadIterator starts before message 0), and the user-supplied ctx.
///
/// Must return a NextMove cast to c_int (MMAPQ_NEXT_ADVANCE=0, MMAPQ_NEXT_STOP=1,
/// MMAPQ_NEXT_RETREAT=2).
/// # Safety
/// context must reference a live, exclusively available C batch cursor whose
/// owner/runtime outlive the callback. Do not retain it or a payload across
/// invalidation. ctx must match live callback state; do not unwind across C.
pub type BatchHandler = Option<
    unsafe extern "C" fn(
        batch_index: i64,
        context: *mut BatchReadingContext,
        ctx: *mut c_void,
    ) -> c_int,
>;

/// Single-message filter; export may evaluate an entry more than once.
///
/// # Safety
/// data must remain readable for length bytes during the call, and ctx must
/// match live callback state. Do not retain data or unwind across C.
pub type QueueFilterFn = Option<
    unsafe extern "C" fn(
        index: i64,
        data: *const c_void,
        length: i32,
        ctx: *mut c_void,
    ) -> c_int,
>;

/// Batch-message filter; export may evaluate a message more than once.
///
/// # Safety
/// data must remain readable for length bytes during the call, and ctx must
/// match live callback state. Do not retain data or unwind across C.
pub type BatchQueueFilterFn = Option<
    unsafe extern "C" fn(
        batch_index: i64,
        msg_index: i32,
        msg_count: i32,
        data: *const c_void,
        length: i32,
        ctx: *mut c_void,
    ) -> c_int,
>;

/* ── ReadingContext (mirrors C struct) ────────────────────────────────────── */

#[repr(C)]
#[derive(Debug)]
pub struct ReadingContext {
    pub index: i64,
    pub appender_id: u8,
    pub msg_offset: i64,
    pub data: *const c_void,
    pub length: i32,
    pub status: c_int,
}

/// Result of a batch read or batch poll operation.
///
/// Mirrors C's `BatchReadingContext` (indexed_batch_reader.h).
/// Populated by `mmapq_indexed_batch_reader_read()` and `mmapq_indexed_batch_poller_poll()`.
///
/// The `iterator` points to a BatchPayloadIterator positioned **before**
/// message 0 — call `mmapq_batch_iterator_advance()` before accessing data.
///
/// Fields are valid when `status == MMAPQ_BATCH_ENTRY_AVAILABLE`.
#[repr(C)]
pub struct BatchReadingContext {
    /// The batch index that was read or polled.
    pub index: i64,
    /// Availability status (MMAPQ_BATCH_ENTRY_AVAILABLE, _END_OF_STREAM, etc.).
    pub status: c_int,
    /// Payload file offset where the batch header begins.
    pub batch_offset: i64,
    /// Number of messages in the batch.
    pub message_count: i32,
    /// Per-lane appender ID that wrote this batch (0–255, same encoding as
    /// single-message `ReadingContext.appender_id`). Used by transfer copy
    /// functions to preserve lane assignment across batch queue copies.
    pub appender_id: u8,
    _reserved: [u8; 3],
    /// Pointer to iterator positioned before message 0.
    pub iterator: *mut BatchPayloadIterator,
}

/* ── Runtime ──────────────────────────────────────────────────────────────── */

extern "C" {
    /// Return a human-readable string for an `MMAPQ_*` error code.
    ///
    /// Returns a pointer to a static string literal for every input,
    /// including unknown codes ("unknown error"). No allocation, no
    /// failure path. See `src/mmapq_strerror.c` and `mmap_platform.h`.
    pub fn mmapq_strerror(rc: c_int) -> *const c_char;

    pub fn mmapq_runtime_create(
        name: *const c_char,
        idle: *mut IdleStrategy,
        close_timeout_ns: u64,
        on_close: Option<unsafe extern "C" fn(*mut Runtime, *mut c_void)>,
        on_close_ctx: *mut c_void,
        out: *mut *mut Runtime,
    ) -> c_int;

    pub fn mmapq_runtime_destroy(rt: *mut Runtime);

    pub fn mmapq_runtime_clone(rt: *mut Runtime, out: *mut *mut Runtime) -> c_int;

    pub fn mmapq_runtime_close(rt: *mut Runtime);

    pub fn mmapq_runtime_register(rt: *mut Runtime, exec: *mut Executable) -> c_int;

    pub fn mmapq_runtime_unregister_and_wait(rt: *mut Runtime, exec: *mut Executable);

    pub fn mmapq_runtime_set_affinity(rt: *mut Runtime, cpu_core: c_int) -> c_int;

    pub fn mmapq_runtime_is_closed(rt: *const Runtime) -> c_int;

    pub fn mmapq_runtime_region_mapper(out: *mut *mut Runtime) -> c_int;

    pub fn mmapq_runtime_metrics_processor(out: *mut *mut Runtime) -> c_int;

    pub fn mmapq_idle_strategy_busy_spin_create(out: *mut *mut IdleStrategy) -> c_int;

    pub fn mmapq_idle_strategy_yield_create(out: *mut *mut IdleStrategy) -> c_int;

    pub fn mmapq_idle_strategy_sleep_create(
        min_sleep_ns: u64,
        max_sleep_ns: u64,
        out: *mut *mut IdleStrategy,
    ) -> c_int;

    pub fn mmapq_idle_strategy_backoff_create(
        spin_count: c_int,
        yield_count: c_int,
        max_sleep_ns: u64,
        out: *mut *mut IdleStrategy,
    ) -> c_int;

    pub fn mmapq_idle_strategy_destroy(strategy: *mut IdleStrategy);
}

/* ── IndexedQueue ─────────────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_queue_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        regions_to_map_ahead: c_int,
        max_file_size: i64,
        start_index: i64,
        timeout_ns: u64,
        archivable: c_int,
        write_runtime: *mut Runtime,
        read_runtime: *mut Runtime,
        out_queue: *mut *mut IndexedQueue,
    ) -> i32;

    pub fn mmapq_indexed_queue_create_default(
        directory: *const c_char,
        queue_name: *const c_char,
        write_runtime: *mut Runtime,
        read_runtime: *mut Runtime,
        out_queue: *mut *mut IndexedQueue,
    ) -> i32;

    pub fn mmapq_indexed_queue_close(q: *mut IndexedQueue);

    pub fn mmapq_indexed_queue_create_appender(
        q: *mut IndexedQueue,
        out: *mut *mut IndexedAppender,
    ) -> i32;

    pub fn mmapq_indexed_queue_create_appender_with_id(
        q: *mut IndexedQueue,
        appender_id: u8,
        out: *mut *mut IndexedAppender,
    ) -> i32;

    pub fn mmapq_indexed_queue_create_poller(
        q: *mut IndexedQueue,
        out: *mut *mut IndexedPoller,
    ) -> i32;

    pub fn mmapq_indexed_queue_create_reader(
        q: *mut IndexedQueue,
        out: *mut *mut IndexedReader,
    ) -> i32;

    pub fn mmapq_indexed_queue_create_archiver(
        q: *mut IndexedQueue,
        out: *mut *mut IndexedArchiver,
    ) -> i32;
}

/* ── IndexedAppender ──────────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_appender_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        max_file_size: i64,
        timeout_ns: u64,
        archivable: c_int,
        runtime: *mut Runtime,
        start_index: i64,
        out: *mut *mut IndexedAppender,
    ) -> i32;

    pub fn mmapq_indexed_appender_create_with_id(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        max_file_size: i64,
        timeout_ns: u64,
        archivable: c_int,
        runtime: *mut Runtime,
        appender_id: u8,
        start_index: i64,
        out: *mut *mut IndexedAppender,
    ) -> i32;

    pub fn mmapq_indexed_appender_append(
        a: *mut IndexedAppender,
        data: *const c_void,
        length: i32,
    ) -> i64;

    pub fn mmapq_indexed_appender_append_uncommitted(
        a: *mut IndexedAppender,
        data: *const c_void,
        length: i32,
    ) -> i64;

    pub fn mmapq_indexed_appender_commit(
        a: *mut IndexedAppender,
        payload_offset: i64,
    ) -> i64;

    pub fn mmapq_indexed_appender_close(a: *mut IndexedAppender);

    pub fn mmapq_indexed_appender_id(a: *const IndexedAppender) -> u8;

    pub fn mmapq_indexed_appender_current_index(a: *const IndexedAppender) -> i64;

    pub fn mmapq_indexed_appender_start_offset(a: *const IndexedAppender) -> i64;

    pub fn mmapq_indexed_appender_iterator(
        a: *mut IndexedAppender,
        start_offset: i64,
        end_offset: i64,
    ) -> *mut PayloadIterator;

    pub fn mmapq_indexed_appender_uncommitted_iterator(
        a: *mut IndexedAppender,
        start_offset: i64,
        end_offset: i64,
    ) -> *mut UncommittedPayloadIterator;
}

/* ── IndexedPoller ────────────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_poller_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
        out: *mut *mut IndexedPoller,
    ) -> i32;

    pub fn mmapq_indexed_poller_poll(
        p: *mut IndexedPoller,
        handler: MessageHandler,
        ctx: *mut c_void,
    ) -> c_int;

    pub fn mmapq_indexed_poller_move_to_index(
        p: *mut IndexedPoller,
        index: i64,
    ) -> c_int;

    pub fn mmapq_indexed_poller_move_to_end(p: *mut IndexedPoller) -> c_int;

    pub fn mmapq_indexed_poller_move_to_last(p: *mut IndexedPoller) -> c_int;

    pub fn mmapq_indexed_poller_move_to_start(p: *mut IndexedPoller) -> c_int;

    pub fn mmapq_indexed_poller_close(p: *mut IndexedPoller);

    pub fn mmapq_indexed_poller_current_index(p: *const IndexedPoller) -> i64;

    pub fn mmapq_indexed_poller_start_index(p: *mut IndexedPoller) -> i64;

    pub fn mmapq_indexed_poller_exists(p: *mut IndexedPoller, index: i64) -> c_int;

    pub fn mmapq_indexed_poller_last_error(p: *const IndexedPoller) -> c_int;
}

/* ── IndexedReader ────────────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_reader_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
        out: *mut *mut IndexedReader,
    ) -> i32;

    pub fn mmapq_indexed_reader_read(
        r: *mut IndexedReader,
        index: i64,
        ctx: *mut ReadingContext,
    ) -> *mut ReadingContext;

    pub fn mmapq_indexed_reader_read_first(
        r: *mut IndexedReader,
        ctx: *mut ReadingContext,
    ) -> *mut ReadingContext;

    pub fn mmapq_indexed_reader_read_last(
        r: *mut IndexedReader,
        ctx: *mut ReadingContext,
    ) -> *mut ReadingContext;

    pub fn mmapq_indexed_reader_close(r: *mut IndexedReader);

    pub fn mmapq_indexed_reader_last_index(r: *mut IndexedReader) -> i64;

    pub fn mmapq_indexed_reader_start_index(r: *mut IndexedReader) -> i64;

    pub fn mmapq_indexed_reader_entry_status(
        r: *mut IndexedReader,
        index: i64,
    ) -> c_int;

    pub fn mmapq_indexed_reader_last_error(r: *const IndexedReader) -> c_int;
}

/* ── IndexedArchiver ──────────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_archiver_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        runtime: *mut Runtime,
        out: *mut *mut IndexedArchiver,
    ) -> i32;

    pub fn mmapq_indexed_archiver_archive_up_to_index(
        a: *mut IndexedArchiver,
        index: i64,
    ) -> c_int;

    pub fn mmapq_indexed_archiver_archivable_files(
        a: *mut IndexedArchiver,
        paths_out: *mut *mut c_char,
        max_paths: c_int,
    ) -> c_int;

    pub fn mmapq_indexed_archiver_print_summary(
        a: *mut IndexedArchiver,
        sb: *mut c_char,
        n: c_int,
    ) -> c_int;

    pub fn mmapq_indexed_archiver_start_offset(
        a: *mut IndexedArchiver,
        appender_id: u8,
        out_offset: *mut i64,
    ) -> c_int;

    pub fn mmapq_indexed_archiver_close(a: *mut IndexedArchiver);
}

/* ── IndexedBatchQueue ────────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_batch_queue_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        regions_to_map_ahead: c_int,
        max_file_size: i64,
        start_index: i64,
        timeout_ns: u64,
        archivable: c_int,
        write_runtime: *mut Runtime,
        read_runtime: *mut Runtime,
        out: *mut *mut IndexedBatchQueue,
    ) -> i32;

    pub fn mmapq_indexed_batch_queue_create_default(
        directory: *const c_char,
        queue_name: *const c_char,
        write_runtime: *mut Runtime,
        read_runtime: *mut Runtime,
        out: *mut *mut IndexedBatchQueue,
    ) -> i32;

    pub fn mmapq_indexed_batch_queue_close(q: *mut IndexedBatchQueue);

    pub fn mmapq_indexed_batch_queue_create_appender(
        q: *mut IndexedBatchQueue,
        out: *mut *mut IndexedBatchAppender,
    ) -> i32;

    pub fn mmapq_indexed_batch_queue_create_appender_with_id(
        q: *mut IndexedBatchQueue,
        appender_id: u8,
        out: *mut *mut IndexedBatchAppender,
    ) -> i32;

    pub fn mmapq_indexed_batch_queue_create_poller(
        q: *mut IndexedBatchQueue,
        out: *mut *mut IndexedBatchPoller,
    ) -> i32;

    pub fn mmapq_indexed_batch_queue_create_reader(
        q: *mut IndexedBatchQueue,
        out: *mut *mut IndexedBatchReader,
    ) -> i32;

    pub fn mmapq_indexed_batch_queue_create_archiver(
        q: *mut IndexedBatchQueue,
        out: *mut *mut IndexedBatchArchiver,
    ) -> i32;

}

/* ── IndexedBatchAppender ─────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_batch_appender_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        max_file_size: i64,
        timeout_ns: u64,
        archivable: c_int,
        runtime: *mut Runtime,
        start_index: i64,
        out: *mut *mut IndexedBatchAppender,
    ) -> i32;

    pub fn mmapq_indexed_batch_appender_start_batch_checked(appender: *mut IndexedBatchAppender, out: *mut *mut MutableBatch) -> c_int;

    pub fn mmapq_indexed_batch_appender_start_batch(
        a: *mut IndexedBatchAppender,
    ) -> *mut MutableBatch;

    pub fn mmapq_indexed_batch_appender_commit_batch(
        a: *mut IndexedBatchAppender,
        batch_offset: i64,
    ) -> i64;

    pub fn mmapq_indexed_batch_appender_discard_batch(
        a: *mut IndexedBatchAppender,
        batch_offset: i64,
    );

    pub fn mmapq_indexed_batch_appender_append_and_commit(
        a: *mut IndexedBatchAppender,
        data: *const c_void,
        length: i32,
    ) -> i64;

    pub fn mmapq_mutable_batch_complete(b: *mut MutableBatch) -> i64;

    pub fn mmapq_mutable_batch_append(
        b: *mut MutableBatch,
        data: *const c_void,
        length: i32,
    ) -> i32;

    pub fn mmapq_mutable_batch_append_reserved(
        b: *mut MutableBatch,
        length: i32,
        out_data: *mut *mut c_void,
    ) -> i32;

    pub fn mmapq_mutable_batch_append_remaining_from(
        b: *mut MutableBatch,
        it: *mut BatchPayloadIterator,
    ) -> i32;

    pub fn mmapq_mutable_batch_get(
        b: *mut MutableBatch,
        message_index: i32,
        out_length: *mut i32,
    ) -> *const c_void;

    pub fn mmapq_mutable_batch_iterator(
        b: *mut MutableBatch,
        start_message_index: i32,
    ) -> *mut BatchPayloadIterator;

    pub fn mmapq_mutable_batch_message_count(b: *const MutableBatch) -> i32;

    pub fn mmapq_indexed_batch_appender_id(a: *const IndexedBatchAppender) -> u8;

    pub fn mmapq_indexed_batch_appender_close(a: *mut IndexedBatchAppender);
}

/* ── IndexedBatchPoller ───────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_batch_poller_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
        out: *mut *mut IndexedBatchPoller,
    ) -> i32;

    pub fn mmapq_indexed_batch_poller_poll(
        p: *mut IndexedBatchPoller,
        handler: BatchHandler,
        ctx: *mut c_void,
    ) -> c_int;

    pub fn mmapq_indexed_batch_poller_move_to_index(
        p: *mut IndexedBatchPoller,
        index: i64,
    ) -> c_int;

    pub fn mmapq_indexed_batch_poller_move_to_end(p: *mut IndexedBatchPoller) -> c_int;

    pub fn mmapq_indexed_batch_poller_move_to_last_existing_batch(
        p: *mut IndexedBatchPoller,
    ) -> c_int;

    pub fn mmapq_indexed_batch_poller_move_to_start(p: *mut IndexedBatchPoller) -> c_int;

    pub fn mmapq_indexed_batch_poller_close(p: *mut IndexedBatchPoller);

    pub fn mmapq_indexed_batch_poller_current_batch_index(
        p: *const IndexedBatchPoller,
    ) -> i64;

    pub fn mmapq_indexed_batch_poller_start_index(
        p: *mut IndexedBatchPoller,
    ) -> i64;

    pub fn mmapq_indexed_batch_poller_exists(
        p: *mut IndexedBatchPoller,
        batch_index: i64,
    ) -> c_int;

    pub fn mmapq_indexed_batch_poller_last_error(p: *const IndexedBatchPoller) -> c_int;
}

/* ── IndexedBatchReader ───────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_batch_reader_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
        out: *mut *mut IndexedBatchReader,
    ) -> i32;

    pub fn mmapq_indexed_batch_reader_read(
        r: *mut IndexedBatchReader,
        index: i64,
        ctx: *mut BatchReadingContext,
    ) -> *mut BatchReadingContext;

    pub fn mmapq_indexed_batch_reader_read_first(
        r: *mut IndexedBatchReader,
        ctx: *mut BatchReadingContext,
    ) -> *mut BatchReadingContext;

    pub fn mmapq_indexed_batch_reader_read_last(
        r: *mut IndexedBatchReader,
        ctx: *mut BatchReadingContext,
    ) -> *mut BatchReadingContext;

    pub fn mmapq_indexed_batch_reader_close(r: *mut IndexedBatchReader);

    pub fn mmapq_indexed_batch_reader_last_index(r: *mut IndexedBatchReader) -> i64;

    pub fn mmapq_indexed_batch_reader_start_index(r: *mut IndexedBatchReader) -> i64;

    pub fn mmapq_indexed_batch_reader_entry_status(
        r: *mut IndexedBatchReader,
        index: i64,
    ) -> c_int;

    pub fn mmapq_indexed_batch_reader_last_error(r: *const IndexedBatchReader) -> c_int;
}

/* ── IndexedBatchArchiver ─────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_indexed_batch_archiver_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        runtime: *mut Runtime,
        out: *mut *mut IndexedBatchArchiver,
    ) -> i32;

    pub fn mmapq_indexed_batch_archiver_archive_up_to_index(
        a: *mut IndexedBatchArchiver,
        index: i64,
    ) -> c_int;

    pub fn mmapq_indexed_batch_archiver_archivable_files(
        a: *mut IndexedBatchArchiver,
        paths_out: *mut *mut c_char,
        max_paths: c_int,
    ) -> c_int;

    pub fn mmapq_indexed_batch_archiver_close(a: *mut IndexedBatchArchiver);
}

/* ── QueueTransfer ────────────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_queue_transfer_export(
        reader: *mut IndexedReader,
        out: *mut libc::FILE,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: QueueFilterFn,
        exclude: QueueFilterFn,
        ctx: *mut c_void,
    ) -> i64;

    pub fn mmapq_queue_transfer_export_chunked(
        reader: *mut IndexedReader,
        base_path: *const c_char,
        start: i64,
        end: i64,
        chunk_size_mb: c_int,
        start_index_mode: c_int,
        include: QueueFilterFn,
        exclude: QueueFilterFn,
        ctx: *mut c_void,
    ) -> i64;

    pub fn mmapq_queue_transfer_import(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index_mode: c_int,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;

    pub fn mmapq_queue_transfer_import_batch_stream(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index_mode: c_int,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;

    pub fn mmapq_queue_transfer_copy(
        reader: *mut IndexedReader,
        dest_dir: *const c_char,
        dest_queue: *const c_char,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: QueueFilterFn,
        exclude: QueueFilterFn,
        ctx: *mut c_void,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;

    pub fn mmapq_queue_transfer_copy_batch_to_normal(
        reader: *mut IndexedBatchReader,
        dest_dir: *const c_char,
        dest_queue: *const c_char,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: BatchQueueFilterFn,
        exclude: BatchQueueFilterFn,
        ctx: *mut c_void,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;
}

/* ── BatchQueueTransfer ───────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_batch_queue_transfer_export(
        reader: *mut IndexedBatchReader,
        out: *mut libc::FILE,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: BatchQueueFilterFn,
        exclude: BatchQueueFilterFn,
        ctx: *mut c_void,
    ) -> i64;

    pub fn mmapq_batch_queue_transfer_export_chunked(
        reader: *mut IndexedBatchReader,
        base_path: *const c_char,
        start: i64,
        end: i64,
        chunk_size_mb: c_int,
        start_index_mode: c_int,
        include: QueueFilterFn,
        exclude: QueueFilterFn,
        ctx: *mut c_void,
    ) -> i64;

    pub fn mmapq_batch_queue_transfer_import(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index_mode: c_int,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;

    pub fn mmapq_batch_queue_transfer_import_flat_stream(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index_mode: c_int,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;

    pub fn mmapq_batch_queue_transfer_copy(
        reader: *mut IndexedBatchReader,
        dest_dir: *const c_char,
        dest_queue: *const c_char,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: BatchQueueFilterFn,
        exclude: BatchQueueFilterFn,
        ctx: *mut c_void,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;

    pub fn mmapq_batch_queue_transfer_copy_normal_to_batch(
        reader: *mut IndexedReader,
        dest_dir: *const c_char,
        dest_queue: *const c_char,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: BatchQueueFilterFn,
        exclude: BatchQueueFilterFn,
        ctx: *mut c_void,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
        allow_append: c_int,
        mapper: AppenderIdMapper,
        mapper_ctx: *mut c_void,
        on_conflict: ConflictBehavior,
    ) -> i64;
}

/* ── BatchPayloadIterator ─────────────────────────────────────────────────── */

extern "C" {
    pub fn mmapq_batch_iterator_read_message_count(
        it: *mut BatchPayloadIterator,
        batch_offset: i64,
    ) -> i32;

    pub fn mmapq_batch_iterator_probe_batch(
        it: *mut BatchPayloadIterator,
        offset: i64,
    ) -> i64;

    pub fn mmapq_batch_iterator_init(
        it: *mut BatchPayloadIterator,
        batch_offset: i64,
        message_count: i32,
        start_msg_index: i32,
    ) -> *mut BatchPayloadIterator;

    pub fn mmapq_batch_iterator_advance(it: *mut BatchPayloadIterator) -> c_int;
    pub fn mmapq_batch_iterator_next(iterator: *mut BatchPayloadIterator, data: *mut *const c_void, length: *mut i32) -> c_int;

    pub fn mmapq_batch_iterator_current_data_ffi(
        it: *const BatchPayloadIterator,
    ) -> *const c_void;

    pub fn mmapq_batch_iterator_current_length_ffi(
        it: *const BatchPayloadIterator,
    ) -> i32;
}

// layout_tests deleted — BatchPayloadIterator is now opaque.
// Struct layout is no longer load-bearing across the ABI.
