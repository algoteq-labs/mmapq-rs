#![allow(non_camel_case_types, non_snake_case, dead_code, missing_docs)]
// ^^ missing_docs is suppressed because this is an FFI declarations file
//    (not public API — public wrapper types are documented in their modules).

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
/// Opaque representation of C's `BatchPayloadIterator` (80 bytes).
///
/// **Layout** (x86-64 System V ABI, verified via GDB):
///   `queue_name(8) + payload_acc(8) + payload_buf(16: data(8)+capacity(4)+pad(4))`
///   `+ batch_offset(8) + message_count(4) + current_msg_index(4)`
///   `+ current_msg_offset(8) + current_msg_length(4)`
///   `+ next_message_position(8) + exhausted(4) + trailing_pad(4)`
///   = **80 bytes** total.
///
/// All field access goes through C functions (`batch_iterator_advance`, etc.)
/// — never access the raw bytes directly.
#[repr(C)]
#[derive(Debug)]
pub struct BatchPayloadIterator {
    _data: [u64; 10], // 80 bytes
}
pub enum PayloadIterator {}
pub enum UncommittedPayloadIterator {}
pub enum QueueMeta {}
pub enum AppenderIdPool {}
pub enum AppenderMeta {}
pub enum ExecList {}
pub enum QueueRegionAccessor {}

pub type IdleStrategy = c_void;
pub type Executable = c_void;

/* ── Enums (values must match C headers) ──────────────────────────────────── */

pub const WRAP_SUCCESS: c_int = 0;
pub const WRAP_MAP_TIMED_OUT: c_int = 1;
pub const WRAP_FILE_NOT_FOUND: c_int = 2;
pub const WRAP_BEYOND_FILE_SIZE: c_int = 3;
pub const WRAP_UNWRAPPED: c_int = 4;

pub const HEADER_NULL: u64 = 0;
pub const HEADER_NULL_INDEX: i64 = -1;

pub const POLL_END_OF_STREAM: c_int = 0;
pub const POLL_ADVANCED: c_int = 1;
pub const POLL_STOPPED: c_int = 2;
pub const POLL_RETREATED: c_int = 3;
pub const POLL_ARCHIVED: c_int = 4;
pub const POLL_ERROR: c_int = 5;

pub const NEXT_ADVANCE: c_int = 0;
pub const NEXT_STOP: c_int = 1;
pub const NEXT_RETREAT: c_int = 2;

pub const ENTRY_AVAILABLE: c_int = 0;
pub const ENTRY_END_OF_STREAM: c_int = 1;
pub const ENTRY_ARCHIVED: c_int = 2;
pub const ENTRY_ERROR: c_int = 3;

pub const BATCH_ENTRY_AVAILABLE: c_int = 0;
pub const BATCH_ENTRY_END_OF_STREAM: c_int = 1;
pub const BATCH_ENTRY_ARCHIVED: c_int = 2;
pub const BATCH_ENTRY_ERROR: c_int = 3;

pub const QUEUE_TRANSFER_REBASE: c_int = 0;
pub const QUEUE_TRANSFER_PRESERVE: c_int = 1;

/* ── Callback types ───────────────────────────────────────────────────────── */

pub type MessageHandler = Option<
    unsafe extern "C" fn(
        index: i64,
        appender_id: u8,
        data: *const c_void,
        length: i32,
        ctx: *mut c_void,
    ) -> c_int,
>;

/// Callback invoked by `indexed_batch_poller_poll()` for each batch.
///
/// Receives the batch index, a pointer to the BatchReadingContext (embedded
/// BatchPayloadIterator starts before message 0), and the user-supplied ctx.
///
/// Must return a NextMove cast to c_int (NEXT_ADVANCE=0, NEXT_STOP=1,
/// NEXT_RETREAT=2).
pub type BatchHandler = Option<
    unsafe extern "C" fn(
        batch_index: i64,
        context: *mut BatchReadingContext,
        ctx: *mut c_void,
    ) -> c_int,
>;

pub type QueueFilterFn = Option<
    unsafe extern "C" fn(
        index: i64,
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
/// Populated by `indexed_batch_reader_read()` and `indexed_batch_poller_poll()`.
///
/// The `batch_iterator` starts **before** message 0 — call
/// `batch_iterator_advance()` before accessing data.
///
/// Fields are valid when `status == BATCH_ENTRY_AVAILABLE`.
#[repr(C)]
#[derive(Debug)]
pub struct BatchReadingContext {
    /// The batch index that was read or polled.
    pub index: i64,
    /// Availability status (BATCH_ENTRY_AVAILABLE, _END_OF_STREAM, etc.).
    pub status: c_int,
    /// Payload file offset where the batch header begins.
    pub batch_offset: i64,
    /// Number of messages in the batch.
    pub message_count: i32,
    /// Iterator positioned before message 0 — advance before reading.
    pub batch_iterator: BatchPayloadIterator,
}

/* ── Runtime ──────────────────────────────────────────────────────────────── */

extern "C" {
    pub fn runtime_create(
        name: *const c_char,
        idle: *mut IdleStrategy,
        close_timeout_ns: u64,
        on_close: Option<unsafe extern "C" fn(*mut Runtime, *mut c_void)>,
        on_close_ctx: *mut c_void,
    ) -> *mut Runtime;

    pub fn runtime_destroy(rt: *mut Runtime);

    pub fn runtime_clone(rt: *mut Runtime) -> *mut Runtime;

    pub fn runtime_close(rt: *mut Runtime);

    pub fn runtime_register(rt: *mut Runtime, exec: *mut Executable) -> c_int;

    pub fn runtime_unregister_and_wait(rt: *mut Runtime, exec: *mut Executable);

    pub fn runtime_set_affinity(rt: *mut Runtime, cpu_core: c_int) -> c_int;

    pub fn runtime_is_closed(rt: *const Runtime) -> c_int;

    pub fn runtime_region_mapper() -> *mut Runtime;

    pub fn runtime_metrics_processor() -> *mut Runtime;

    pub fn idle_strategy_busy_spin_create() -> *mut IdleStrategy;

    pub fn idle_strategy_yield_create() -> *mut IdleStrategy;

    pub fn idle_strategy_sleep_create(
        min_sleep_ns: u64,
        max_sleep_ns: u64,
    ) -> *mut IdleStrategy;

    pub fn idle_strategy_backoff_create(
        spin_count: c_int,
        yield_count: c_int,
        max_sleep_ns: u64,
    ) -> *mut IdleStrategy;

    pub fn idle_strategy_destroy(strategy: *mut IdleStrategy);
}

/* ── IndexedQueue ─────────────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_queue_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        regions_to_map_ahead: c_int,
        max_file_size: i64,
        start_index: i64,
        timeout_ns: u64,
        write_runtime: *mut Runtime,
        read_runtime: *mut Runtime,
    ) -> *mut IndexedQueue;

    pub fn indexed_queue_create_default(
        directory: *const c_char,
        queue_name: *const c_char,
        write_runtime: *mut Runtime,
        read_runtime: *mut Runtime,
    ) -> *mut IndexedQueue;

    pub fn indexed_queue_close(q: *mut IndexedQueue);

    pub fn indexed_queue_create_appender(q: *mut IndexedQueue) -> *mut IndexedAppender;

    pub fn indexed_queue_create_appender_with_id(
        q: *mut IndexedQueue,
        appender_id: u8,
    ) -> *mut IndexedAppender;

    pub fn indexed_queue_create_poller(q: *mut IndexedQueue) -> *mut IndexedPoller;

    pub fn indexed_queue_create_reader(q: *mut IndexedQueue) -> *mut IndexedReader;

    pub fn indexed_queue_create_archiver(q: *mut IndexedQueue) -> *mut IndexedArchiver;

    pub fn indexed_queue_region_size(q: *const IndexedQueue) -> i32;

    pub fn indexed_queue_max_file_size(q: *const IndexedQueue) -> i64;

    pub fn indexed_queue_start_index(q: *const IndexedQueue) -> i64;
}

/* ── IndexedAppender ──────────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_appender_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        max_file_size: i64,
        timeout_ns: u64,
        runtime: *mut Runtime,
    ) -> *mut IndexedAppender;

    pub fn indexed_appender_create_with_id(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        max_file_size: i64,
        timeout_ns: u64,
        runtime: *mut Runtime,
        appender_id: u8,
    ) -> *mut IndexedAppender;

    pub fn indexed_appender_append(
        a: *mut IndexedAppender,
        data: *const c_void,
        length: i32,
    ) -> i64;

    pub fn indexed_appender_append_uncommitted(
        a: *mut IndexedAppender,
        data: *const c_void,
        length: i32,
    ) -> i64;

    pub fn indexed_appender_commit(
        a: *mut IndexedAppender,
        payload_offset: i64,
    ) -> i64;

    pub fn indexed_appender_close(a: *mut IndexedAppender);

    pub fn indexed_appender_id(a: *const IndexedAppender) -> u8;

    pub fn indexed_appender_current_index(a: *const IndexedAppender) -> i64;

    pub fn indexed_appender_start_offset(a: *const IndexedAppender) -> i64;

    pub fn indexed_appender_iterator(
        a: *mut IndexedAppender,
        start_offset: i64,
        end_offset: i64,
    ) -> *mut PayloadIterator;

    pub fn indexed_appender_uncommitted_iterator(
        a: *mut IndexedAppender,
        start_offset: i64,
        end_offset: i64,
    ) -> *mut UncommittedPayloadIterator;
}

/* ── IndexedPoller ────────────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_poller_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
    ) -> *mut IndexedPoller;

    pub fn indexed_poller_poll(
        p: *mut IndexedPoller,
        handler: MessageHandler,
        ctx: *mut c_void,
    ) -> c_int;

    pub fn indexed_poller_move_to_index(
        p: *mut IndexedPoller,
        index: i64,
    ) -> c_int;

    pub fn indexed_poller_move_to_end(p: *mut IndexedPoller) -> c_int;

    pub fn indexed_poller_move_to_last(p: *mut IndexedPoller) -> c_int;

    pub fn indexed_poller_move_to_start(p: *mut IndexedPoller) -> c_int;

    pub fn indexed_poller_close(p: *mut IndexedPoller);

    pub fn indexed_poller_current_index(p: *const IndexedPoller) -> i64;

    pub fn indexed_poller_start_index(p: *mut IndexedPoller) -> i64;

    pub fn indexed_poller_exists(p: *mut IndexedPoller, index: i64) -> c_int;
}

/* ── IndexedReader ────────────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_reader_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
    ) -> *mut IndexedReader;

    pub fn indexed_reader_read(
        r: *mut IndexedReader,
        index: i64,
        ctx: *mut ReadingContext,
    ) -> *mut ReadingContext;

    pub fn indexed_reader_read_first(
        r: *mut IndexedReader,
        ctx: *mut ReadingContext,
    ) -> *mut ReadingContext;

    pub fn indexed_reader_read_last(
        r: *mut IndexedReader,
        ctx: *mut ReadingContext,
    ) -> *mut ReadingContext;

    pub fn indexed_reader_close(r: *mut IndexedReader);

    pub fn indexed_reader_last_index(r: *mut IndexedReader) -> i64;

    pub fn indexed_reader_start_index(r: *mut IndexedReader) -> i64;

    pub fn indexed_reader_entry_status(
        r: *mut IndexedReader,
        index: i64,
    ) -> c_int;

    pub fn indexed_reader_queue_meta(r: *mut IndexedReader) -> *mut QueueMeta;

    pub fn indexed_reader_directory(r: *mut IndexedReader) -> *const c_char;

    pub fn indexed_reader_queue_name(r: *mut IndexedReader) -> *const c_char;
}

/* ── IndexedArchiver ──────────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_archiver_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        runtime: *mut Runtime,
    ) -> *mut IndexedArchiver;

    pub fn indexed_archiver_archive_up_to_index(
        a: *mut IndexedArchiver,
        index: i64,
    );

    pub fn indexed_archiver_archivable_files(
        a: *mut IndexedArchiver,
        paths_out: *mut *mut c_char,
        max_paths: c_int,
    ) -> c_int;

    pub fn indexed_archiver_print_summary(
        a: *mut IndexedArchiver,
        sb: *mut c_char,
        n: c_int,
    ) -> c_int;

    pub fn indexed_archiver_start_offset(
        a: *mut IndexedArchiver,
        appender_id: u8,
        out_offset: *mut i64,
    ) -> c_int;

    pub fn indexed_archiver_close(a: *mut IndexedArchiver);
}

/* ── IndexedBatchQueue ────────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_batch_queue_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        regions_to_map_ahead: c_int,
        max_file_size: i64,
        start_index: i64,
        timeout_ns: u64,
        write_runtime: *mut Runtime,
        read_runtime: *mut Runtime,
    ) -> *mut IndexedBatchQueue;

    pub fn indexed_batch_queue_close(q: *mut IndexedBatchQueue);

    pub fn indexed_batch_queue_create_appender(
        q: *mut IndexedBatchQueue,
    ) -> *mut IndexedBatchAppender;

    pub fn indexed_batch_queue_create_poller(
        q: *mut IndexedBatchQueue,
    ) -> *mut IndexedBatchPoller;

    pub fn indexed_batch_queue_create_reader(
        q: *mut IndexedBatchQueue,
    ) -> *mut IndexedBatchReader;

    pub fn indexed_batch_queue_create_archiver(
        q: *mut IndexedBatchQueue,
    ) -> *mut IndexedBatchArchiver;

    pub fn indexed_batch_queue_region_size(q: *const IndexedBatchQueue) -> i32;

    pub fn indexed_batch_queue_max_file_size(q: *const IndexedBatchQueue) -> i64;

    pub fn indexed_batch_queue_start_index(q: *const IndexedBatchQueue) -> i64;
}

/* ── IndexedBatchAppender ─────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_batch_appender_create(
        directory: *const c_char,
        queue_name: *const c_char,
        region_size: i32,
        ring_size: c_int,
        max_file_size: i64,
        timeout_ns: u64,
        runtime: *mut Runtime,
    ) -> *mut IndexedBatchAppender;

    pub fn indexed_batch_appender_start_batch(
        a: *mut IndexedBatchAppender,
    ) -> *mut MutableBatch;

    pub fn indexed_batch_appender_commit_batch(
        a: *mut IndexedBatchAppender,
        batch_offset: i64,
    ) -> i64;

    pub fn indexed_batch_appender_discard_batch(
        a: *mut IndexedBatchAppender,
        batch_offset: i64,
    );

    pub fn indexed_batch_appender_append_and_commit(
        a: *mut IndexedBatchAppender,
        data: *const c_void,
        length: i32,
    ) -> i64;

    pub fn mutable_batch_complete(b: *mut MutableBatch) -> i64;

    pub fn mutable_batch_append(
        b: *mut MutableBatch,
        data: *const c_void,
        length: i32,
    ) -> i32;

    pub fn mutable_batch_append_reserved(
        b: *mut MutableBatch,
        length: i32,
        out_data: *mut *mut c_void,
    ) -> i32;

    pub fn mutable_batch_append_remaining_from(
        b: *mut MutableBatch,
        it: *mut BatchPayloadIterator,
    ) -> i32;

    pub fn mutable_batch_get(
        b: *mut MutableBatch,
        message_index: i32,
        out_length: *mut i32,
    ) -> *const c_void;

    pub fn mutable_batch_iterator(
        b: *mut MutableBatch,
        start_message_index: i32,
    ) -> *mut BatchPayloadIterator;

    pub fn mutable_batch_message_count(b: *const MutableBatch) -> i32;

    pub fn indexed_batch_appender_id(a: *const IndexedBatchAppender) -> u8;

    pub fn indexed_batch_appender_close(a: *mut IndexedBatchAppender);
}

/* ── IndexedBatchPoller ───────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_batch_poller_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
    ) -> *mut IndexedBatchPoller;

    pub fn indexed_batch_poller_poll(
        p: *mut IndexedBatchPoller,
        handler: BatchHandler,
        ctx: *mut c_void,
    ) -> c_int;

    pub fn indexed_batch_poller_move_to_index(
        p: *mut IndexedBatchPoller,
        index: i64,
    ) -> c_int;

    pub fn indexed_batch_poller_move_to_end(p: *mut IndexedBatchPoller) -> c_int;

    pub fn indexed_batch_poller_move_to_last_existing_batch(
        p: *mut IndexedBatchPoller,
    ) -> c_int;

    pub fn indexed_batch_poller_move_to_start(p: *mut IndexedBatchPoller) -> c_int;

    pub fn indexed_batch_poller_close(p: *mut IndexedBatchPoller);

    pub fn indexed_batch_poller_current_batch_index(
        p: *const IndexedBatchPoller,
    ) -> i64;

    pub fn indexed_batch_poller_start_index(
        p: *mut IndexedBatchPoller,
    ) -> i64;

    pub fn indexed_batch_poller_exists(
        p: *mut IndexedBatchPoller,
        batch_index: i64,
    ) -> c_int;
}

/* ── IndexedBatchReader ───────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_batch_reader_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        timeout_ns: u64,
        runtime: *mut Runtime,
    ) -> *mut IndexedBatchReader;

    pub fn indexed_batch_reader_read(
        r: *mut IndexedBatchReader,
        index: i64,
        ctx: *mut BatchReadingContext,
    ) -> *mut BatchReadingContext;

    pub fn indexed_batch_reader_read_first(
        r: *mut IndexedBatchReader,
        ctx: *mut BatchReadingContext,
    ) -> *mut BatchReadingContext;

    pub fn indexed_batch_reader_read_last(
        r: *mut IndexedBatchReader,
        ctx: *mut BatchReadingContext,
    ) -> *mut BatchReadingContext;

    pub fn indexed_batch_reader_close(r: *mut IndexedBatchReader);

    pub fn indexed_batch_reader_last_index(r: *mut IndexedBatchReader) -> i64;

    pub fn indexed_batch_reader_start_index(r: *mut IndexedBatchReader) -> i64;

    pub fn indexed_batch_reader_entry_status(
        r: *mut IndexedBatchReader,
        index: i64,
    ) -> c_int;

    pub fn indexed_batch_reader_queue_meta(
        r: *mut IndexedBatchReader,
    ) -> *mut QueueMeta;

    pub fn indexed_batch_reader_directory(
        r: *mut IndexedBatchReader,
    ) -> *const c_char;

    pub fn indexed_batch_reader_queue_name(
        r: *mut IndexedBatchReader,
    ) -> *const c_char;
}

/* ── IndexedBatchArchiver ─────────────────────────────────────────────────── */

extern "C" {
    pub fn indexed_batch_archiver_create(
        directory: *const c_char,
        queue_name: *const c_char,
        ring_size: c_int,
        runtime: *mut Runtime,
    ) -> *mut IndexedBatchArchiver;

    pub fn indexed_batch_archiver_archive_up_to_index(
        a: *mut IndexedBatchArchiver,
        index: i64,
    );

    pub fn indexed_batch_archiver_archivable_files(
        a: *mut IndexedBatchArchiver,
        paths_out: *mut *mut c_char,
        max_paths: c_int,
    ) -> c_int;

    pub fn indexed_batch_archiver_print_summary(
        a: *mut IndexedBatchArchiver,
        sb: *mut c_char,
        n: c_int,
    ) -> c_int;

    pub fn indexed_batch_archiver_close(a: *mut IndexedBatchArchiver);
}

/* ── QueueTransfer ────────────────────────────────────────────────────────── */

extern "C" {
    pub fn queue_transfer_export(
        reader: *mut IndexedReader,
        out: *mut libc::FILE,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: QueueFilterFn,
        exclude: QueueFilterFn,
        ctx: *mut c_void,
    ) -> i64;

    pub fn queue_transfer_export_chunked(
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

    pub fn queue_transfer_import(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index: i64,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
    ) -> i64;

    pub fn queue_transfer_import_batch_stream(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index: i64,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
    ) -> i64;

    pub fn queue_transfer_copy(
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
    ) -> i64;
}

/* ── BatchQueueTransfer ───────────────────────────────────────────────────── */

extern "C" {
    pub fn batch_queue_transfer_export(
        reader: *mut IndexedBatchReader,
        out: *mut libc::FILE,
        start: i64,
        end: i64,
        start_index_mode: c_int,
        include: QueueFilterFn,
        exclude: QueueFilterFn,
        ctx: *mut c_void,
    ) -> i64;

    pub fn batch_queue_transfer_export_chunked(
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

    pub fn batch_queue_transfer_import(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index: i64,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
    ) -> i64;

    pub fn batch_queue_transfer_import_flat_stream(
        in_file: *mut libc::FILE,
        directory: *const c_char,
        queue_name: *const c_char,
        start_index: i64,
        write_rt: *mut Runtime,
        read_rt: *mut Runtime,
    ) -> i64;

    pub fn batch_queue_transfer_copy(
        reader: *mut IndexedBatchReader,
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
    ) -> i64;
}

/* ── Utility / queue meta accessors ───────────────────────────────────────── */

extern "C" {
    pub fn queue_meta_open_ro(
        directory: *const c_char,
        queue_name: *const c_char,
    ) -> *mut QueueMeta;

    pub fn queue_meta_close(m: *mut QueueMeta);

    pub fn queue_meta_region_size(m: *const QueueMeta) -> i32;

    pub fn queue_meta_max_file_size(m: *const QueueMeta) -> i64;

    pub fn queue_meta_start_index(m: *const QueueMeta) -> i64;

    pub fn queue_meta_is_archivable(m: *const QueueMeta) -> c_int;

    pub fn appender_id_pool_open(
        directory: *const c_char,
        queue_name: *const c_char,
    ) -> *mut AppenderIdPool;

    pub fn appender_id_pool_close(pool: *mut AppenderIdPool);

    pub fn appender_meta_start_offset(am: *const AppenderMeta) -> i64;

    pub fn appender_meta_open_rw(
        directory: *const c_char,
        queue_name: *const c_char,
        appender_id: u8,
    ) -> *mut AppenderMeta;

    pub fn appender_meta_close(am: *mut AppenderMeta);

    pub fn header_found(
        header_acc: *mut c_void,
        buf: *mut c_void,
        index: i64,
        queue_name: *const c_char,
    ) -> c_int;
}

/* ── BatchPayloadIterator ─────────────────────────────────────────────────── */

extern "C" {
    pub fn batch_iterator_read_message_count(
        it: *mut BatchPayloadIterator,
        batch_offset: i64,
    ) -> i32;

    pub fn batch_iterator_probe_batch(
        it: *mut BatchPayloadIterator,
        offset: i64,
    ) -> i64;

    pub fn batch_iterator_init(
        it: *mut BatchPayloadIterator,
        batch_offset: i64,
        message_count: i32,
        start_msg_index: i32,
    ) -> *mut BatchPayloadIterator;

    pub fn batch_iterator_advance(it: *mut BatchPayloadIterator) -> c_int;

    pub fn batch_iterator_has_current(it: *const BatchPayloadIterator) -> c_int;

    pub fn batch_iterator_current_data_ffi(
        it: *const BatchPayloadIterator,
    ) -> *const c_void;

    pub fn batch_iterator_current_length_ffi(
        it: *const BatchPayloadIterator,
    ) -> i32;
}
