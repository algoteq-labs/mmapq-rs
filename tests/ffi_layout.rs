//! Verify that Rust FFI struct layouts match C.
//!
//! Struct layout mismatches between Rust and C cause silent memory corruption
//! (SIGSEGV at runtime, not compile-time errors). These tests catch layout
//! drift by checking field offsets and struct sizes against expected values
//! derived from the C headers.
//!
//! When adding new fields to any FFI struct, determine the C layout with:
//!   printf("offsetof(struct, field) = %zu\n", offsetof(struct, field));
//! Then update the expected constants below.

use std::mem::size_of;
use mmapq::ffi;

/// Compute the byte offset of a field within a struct.
fn offset_of<T, F>(obj: &T, field_ptr: *const F) -> usize {
    let obj_addr = obj as *const T as *const u8 as usize;
    let field_addr = field_ptr as *const u8 as usize;
    field_addr - obj_addr
}

// ── BatchPayloadIterator ──────────────────────────────────────────────────
//
// C source: include/batch_payload_iterator.h
//
// typedef struct {
//     const char     *queue_name;         //  0: ptr    (8)
//     RegionAccessor *payload_acc;        //  8: ptr    (8)
//     Buffer          payload_buf;        // 16: struct (data:ptr(8) + capacity:i32(4)) + pad(4)
//     int64_t         batch_offset;       // 32: i64    (8)
//     int32_t         message_count;      // 40: i32    (4)
//     int32_t         current_msg_index;  // 44: i32    (4)
//     int64_t         current_msg_offset; // 48: i64    (8)
//     int32_t         current_msg_length; // 56: i32    (4)
//     int64_t         next_message_position; // 64: i64 (8)
//     int             exhausted;         // 72: i32    (4) + trailing pad(4)
// } BatchPayloadIterator;                 // total: 80 bytes
//
// The Rust struct uses `_data: [u64; 10]` which is also 80 bytes.
// Field access is through C functions only, so per-field offsets don't matter
// inside this struct — only the total size is critical.

#[test]
fn verify_batch_payload_iterator_size() {
    assert_eq!(
        size_of::<ffi::BatchPayloadIterator>(),
        80,
        "BatchPayloadIterator must be 80 bytes to match C struct"
    );
}

// ── BatchReadingContext ───────────────────────────────────────────────────
//
// C source: include/indexed_batch_reader.h
//
// typedef struct {
//     int64_t              index;           //  0: i64  (8)
//     BatchEntryStatus     status;          //  8: i32  (4)
//     int64_t              batch_offset;    // 16: i64  (8) — padded from 12
//     int32_t              message_count;   // 24: i32  (4)
//     uint8_t              appender_id;     // 28: u8   (1)
//     uint8_t              reserved[3];     // 29: u8[3](3)
//     BatchPayloadIterator batch_iterator;  // 32: struct (80 bytes)
// } BatchReadingContext;                     // total: 112 bytes
//
// On x86-64 with GCC, the System V ABI requires 8-byte alignment for int64_t
// and [u64;10], inserting padding after status (offset 8→16).
// appender_id + reserved fill the gap before batch_iterator at offset 32.

#[test]
fn verify_batch_reading_context_layout() {
    // Use a zero-initialized dummy to compute offsets at runtime.
    let ctx: ffi::BatchReadingContext = unsafe { std::mem::zeroed() };

    assert_eq!(
        offset_of(&ctx, &ctx.index), 0,
        "BatchReadingContext.index must be at offset 0"
    );
    assert_eq!(
        offset_of(&ctx, &ctx.status), 8,
        "BatchReadingContext.status must be at offset 8"
    );
    assert_eq!(
        offset_of(&ctx, &ctx.batch_offset), 16,
        "BatchReadingContext.batch_offset must be at offset 16 (padded for i64)"
    );
    assert_eq!(
        offset_of(&ctx, &ctx.message_count), 24,
        "BatchReadingContext.message_count must be at offset 24"
    );
    assert_eq!(
        offset_of(&ctx, &ctx.appender_id), 28,
        "BatchReadingContext.appender_id must be at offset 28"
    );
    assert_eq!(
        offset_of(&ctx, &ctx.batch_iterator), 32,
        "BatchReadingContext.batch_iterator must be at offset 32"
    );
    assert_eq!(
        size_of::<ffi::BatchReadingContext>(), 112,
        "BatchReadingContext must be 112 bytes"
    );
}
