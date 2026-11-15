//! 0.2.1: an appender that cannot create, open or map the next queue file
//! gets `Eio` at once, not `MapTimedOut` after the mapping timeout, and the
//! same call succeeds once the cause is gone. The cause here is a directory
//! sitting where the next file must go (opening it fails with EISDIR), so
//! the tests behave the same as root and as an ordinary user.

use std::time::{Duration, Instant};

use mmapq::batch::IndexedBatchQueue;
use mmapq::error::MmapError;
use mmapq::queue::QueueConfig;
use mmapq::runtime::Runtime;
use mmapq::IndexedQueue;

const REGION: i32 = 32 * 1024;
const FILE: i64 = REGION as i64 * 8; // 1000-byte messages: 256 per payload file
const TIMEOUT_NS: u64 = 2_000_000_000;

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("mmap-rs-write-path").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn config(base: &std::path::Path, rt: &Runtime) -> QueueConfig {
    QueueConfig::new(base.to_str().unwrap(), "q")
        .region_size(REGION)
        .max_file_size(FILE)
        .timeout(TIMEOUT_NS)
        .write_runtime(rt)
        .read_runtime(rt)
}

/// Runs `op`, which must fail with `Eio` well inside the mapping timeout.
fn assert_eio_at_once<T: std::fmt::Debug>(op: impl FnOnce() -> Result<T, MmapError>) {
    let t = Instant::now();
    let r = op();
    assert!(matches!(r, Err(MmapError::Eio)), "expected Eio, got {r:?}");
    assert!(t.elapsed() < Duration::from_secs(1), "took {:?}", t.elapsed());
}

#[test]
fn next_payload_file_unusable_is_eio() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("payload");
    let q = IndexedQueue::create(config(&base, &rt)).unwrap();
    let mut a = q.create_appender().unwrap();
    let blocker = base.join("q").join("payload_0_1");
    std::fs::create_dir(&blocker).unwrap();
    let msg = [7u8; 1000];
    for i in 0..256 {
        assert_eq!(a.append(&msg).unwrap(), i);
    }
    assert_eio_at_once(|| a.append(&msg));
    assert_eio_at_once(|| a.append(&msg)); // a retry fails the same way
    std::fs::remove_dir(&blocker).unwrap();
    assert_eq!(a.append(&msg).unwrap(), 256);
}

#[test]
fn next_header_file_unusable_is_eio() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("header");
    // 10 free slots left in header file 0; small messages
    let q = IndexedQueue::create(config(&base, &rt).start_index(32768 - 10)).unwrap();
    let mut a = q.create_appender().unwrap();
    let blocker = base.join("q").join("header_1");
    std::fs::create_dir(&blocker).unwrap();
    for i in 0..10 {
        assert_eq!(a.append(b"12345678").unwrap(), 32768 - 10 + i);
    }
    assert_eio_at_once(|| a.append(b"12345678"));
    std::fs::remove_dir(&blocker).unwrap();
    assert_eq!(a.append(b"12345678").unwrap(), 32768);
}

#[test]
fn next_payload_file_unusable_is_eio_batch() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("batch");
    let q = IndexedBatchQueue::create(config(&base, &rt)).unwrap();
    let mut a = q.create_appender().unwrap();
    let blocker = base.join("q").join("payload_0_1");
    std::fs::create_dir(&blocker).unwrap();
    let msg = [7u8; 1000];
    for i in 0..256 {
        assert_eq!(a.append_and_commit(&msg).unwrap(), i);
    }
    assert_eio_at_once(|| a.append_and_commit(&msg));
    std::fs::remove_dir(&blocker).unwrap();
    assert_eq!(a.append_and_commit(&msg).unwrap(), 256);
}
