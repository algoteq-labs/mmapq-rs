//! Layout gate for the FFI boundary structs. C is the source of truth
//! (`c/include/indexed_reader.h:54-61`, `c/include/indexed_batch_reader.h:94-102`);
//! the mirrors live at `src/ffi.rs:176-185` and `src/ffi.rs:197-213`. Every
//! field is asserted twice: against the expected ABI literal (documents the
//! ABI; flags a Rust-side edit) and against a C probe compiled from the real
//! headers at test time (flags a C-side edit, where a literal-only gate
//! would stay green).

use std::collections::HashMap;
use std::process::Command;
use mmapq::ffi;

/// Prints `name value` lines of sizeof/alignof/offsetof for every field,
/// compiled against the vendored headers so the gate anchors on the source
/// of truth, not on hand-copied numbers.
const PROBE_C: &str = r#"
#include <stdio.h>
#include <stddef.h>
#include <stdalign.h>
#include "indexed_reader.h"
#include "indexed_batch_reader.h"

#define P(name, value) printf("%s %zu\n", name, (size_t)(value))

int main(void) {
    P("rc.size", sizeof(MmapqReadingContext));
    P("rc.align", alignof(MmapqReadingContext));
    P("rc.index", offsetof(MmapqReadingContext, index));
    P("rc.appender_id", offsetof(MmapqReadingContext, appender_id));
    P("rc.msg_offset", offsetof(MmapqReadingContext, msg_offset));
    P("rc.data", offsetof(MmapqReadingContext, data));
    P("rc.length", offsetof(MmapqReadingContext, length));
    P("rc.status", offsetof(MmapqReadingContext, status));
    P("brc.size", sizeof(MmapqBatchReadingContext));
    P("brc.align", alignof(MmapqBatchReadingContext));
    P("brc.index", offsetof(MmapqBatchReadingContext, index));
    P("brc.status", offsetof(MmapqBatchReadingContext, status));
    P("brc.batch_offset", offsetof(MmapqBatchReadingContext, batch_offset));
    P("brc.message_count", offsetof(MmapqBatchReadingContext, message_count));
    P("brc.appender_id", offsetof(MmapqBatchReadingContext, appender_id));
    P("brc.reserved", offsetof(MmapqBatchReadingContext, reserved));
    P("brc.iterator", offsetof(MmapqBatchReadingContext, iterator));
    return 0;
}
"#;

/// Compile and run PROBE_C against the vendored headers; parse its
/// `name value` output. `name` keeps concurrent tests' temp dirs apart.
fn c_layout(name: &str) -> HashMap<String, usize> {
    let directory = std::env::temp_dir().join(format!(
        "mmapq-ffi-layout-{name}-{}", std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let source = directory.join("probe.c");
    std::fs::write(&source, PROBE_C).unwrap();
    let binary = directory.join("probe");
    // Honor $CC so the probe uses the same compiler as the library build
    // (build.rs, via the cc crate); irrelevant today — gcc and clang agree
    // on this ABI.
    let compiler = std::env::var("CC").unwrap_or_else(|_| "cc".to_owned());
    let compiled = Command::new(compiler)
        .arg("-std=gnu11")
        .arg("-I").arg(concat!(env!("CARGO_MANIFEST_DIR"), "/c/include"))
        .arg(&source)
        .arg("-o").arg(&binary)
        .output().unwrap();
    if !compiled.status.success() {
        let message = String::from_utf8_lossy(&compiled.stderr).into_owned();
        let _ = std::fs::remove_dir_all(&directory);
        panic!("{message}");
    }
    let output = Command::new(&binary).output().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8_lossy(&output.stdout).lines().map(|line| {
        let mut parts = line.split_whitespace();
        (parts.next().unwrap().to_owned(), parts.next().unwrap().parse().unwrap())
    }).collect()
}

/// Assert the expected ABI literal, the Rust layout, and the C header layout
/// all agree. `rust` is `None` for C-only fields (Rust's `_reserved` is
/// private; the C-side assertion pins its offset directly).
fn check(c: &HashMap<String, usize>, name: &str, expected: usize, rust: Option<usize>) {
    if let Some(rust) = rust {
        assert_eq!(expected, rust, "{name}: expected ABI vs Rust (src/ffi.rs)");
    }
    let c_value = *c.get(name).unwrap_or_else(|| panic!("{name}: missing from C probe output"));
    assert_eq!(expected, c_value, "{name}: expected ABI vs C header");
}

#[test]
fn reading_context_layout_matches_c() {
    let c = c_layout("rc");
    // SAFETY: all-zero is a valid bit pattern for every field (integers and
    // raw pointers only) and only field addresses are read. Precedent:
    // src/batch/batch_queue.rs:145. `offset_of!` would do this without
    // unsafe but needs rustc 1.77; the toolchain is 1.75 (accepted
    // constraint).
    let ctx: ffi::ReadingContext = unsafe { std::mem::zeroed() };
    let base = std::ptr::addr_of!(ctx) as usize;
    check(&c, "rc.size", 40, Some(std::mem::size_of_val(&ctx)));
    check(&c, "rc.align", 8, Some(std::mem::align_of_val(&ctx)));
    check(&c, "rc.index", 0, Some(std::ptr::addr_of!(ctx.index) as usize - base));
    check(&c, "rc.appender_id", 8, Some(std::ptr::addr_of!(ctx.appender_id) as usize - base));
    check(&c, "rc.msg_offset", 16, Some(std::ptr::addr_of!(ctx.msg_offset) as usize - base));
    check(&c, "rc.data", 24, Some(std::ptr::addr_of!(ctx.data) as usize - base));
    check(&c, "rc.length", 32, Some(std::ptr::addr_of!(ctx.length) as usize - base));
    check(&c, "rc.status", 36, Some(std::ptr::addr_of!(ctx.status) as usize - base));
}

#[test]
fn batch_reading_context_layout_matches_c() {
    let c = c_layout("brc");
    // SAFETY: as above — integers and one raw pointer; addresses only.
    let ctx: ffi::BatchReadingContext = unsafe { std::mem::zeroed() };
    let base = std::ptr::addr_of!(ctx) as usize;
    check(&c, "brc.size", 40, Some(std::mem::size_of_val(&ctx)));
    check(&c, "brc.align", 8, Some(std::mem::align_of_val(&ctx)));
    check(&c, "brc.index", 0, Some(std::ptr::addr_of!(ctx.index) as usize - base));
    check(&c, "brc.status", 8, Some(std::ptr::addr_of!(ctx.status) as usize - base));
    check(&c, "brc.batch_offset", 16, Some(std::ptr::addr_of!(ctx.batch_offset) as usize - base));
    check(&c, "brc.message_count", 24, Some(std::ptr::addr_of!(ctx.message_count) as usize - base));
    check(&c, "brc.appender_id", 28, Some(std::ptr::addr_of!(ctx.appender_id) as usize - base));
    check(&c, "brc.reserved", 29, None);
    check(&c, "brc.iterator", 32, Some(std::ptr::addr_of!(ctx.iterator) as usize - base));
}
