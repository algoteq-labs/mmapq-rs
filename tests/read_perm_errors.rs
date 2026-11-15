//! 0.2.1: a consumer that reaches a header or payload file it cannot read
//! gets `Eio` at once — not `Ok(EndOfStream)` (an unreadable header file
//! used to stop a poller silently, as if caught up) and not `Corrupt` (an
//! unreadable payload file) — and carries on once the file is readable.
//! The cause here is a symlink loop where the file was (opening it fails
//! with ELOOP), so the tests behave the same as root and as an ordinary
//! user.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use mmapq::batch::IndexedBatchQueue;
use mmapq::error::MmapError;
use mmapq::queue::QueueConfig;
use mmapq::reader::ReadResult;
use mmapq::runtime::Runtime;
use mmapq::types::{NextMove, PollResult};
use mmapq::IndexedQueue;

const REGION: i32 = 32 * 1024;
const FILE: i64 = REGION as i64 * 8; // 32,768 headers, or 256 1000-byte messages, per file
const TIMEOUT_NS: u64 = 2_000_000_000;
const HDR_SI: i64 = 32768 - 10; // header file 0 has 10 slots

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("mmap-rs-read-perm").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn config(base: &Path, rt: &Runtime) -> QueueConfig {
    QueueConfig::new(base.to_str().unwrap(), "q")
        .region_size(REGION)
        .max_file_size(FILE)
        .timeout(TIMEOUT_NS)
        .write_runtime(rt)
        .read_runtime(rt)
}

/// Replaces `file` with a symlink to itself; returns a guard that restores it.
struct Loop {
    path: PathBuf,
    aside: PathBuf,
}

impl Loop {
    fn new(base: &Path, file: &str) -> Loop {
        let path = base.join("q").join(file);
        let aside = base.join("q").join(format!("{file}.aside"));
        std::fs::rename(&path, &aside).unwrap();
        std::os::unix::fs::symlink(file, &path).unwrap();
        Loop { path, aside }
    }
    fn restore(self) {
        std::fs::remove_file(&self.path).unwrap();
        std::fs::rename(&self.aside, &self.path).unwrap();
    }
}

fn assert_eio_at_once<T: std::fmt::Debug>(op: impl FnOnce() -> Result<T, MmapError>) {
    let t = Instant::now();
    let r = op();
    assert!(matches!(r, Err(MmapError::Eio)), "expected Eio, got {r:?}");
    assert!(t.elapsed() < Duration::from_secs(1), "took {:?}", t.elapsed());
}

/// Polls until the poll does not advance; returns (outcome, next index).
fn drain(p: &mut mmapq::IndexedPoller, next: &mut i64) -> Result<PollResult, MmapError> {
    loop {
        let r = p.poll(|i, _, _| {
            assert_eq!(i, *next);
            *next += 1;
            NextMove::Advance
        });
        if !matches!(r, Ok(PollResult::Advanced)) {
            return r;
        }
    }
}

#[test]
fn unreadable_header_file_is_eio_not_end_of_stream() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("header");
    let q = IndexedQueue::create(config(&base, &rt).start_index(HDR_SI)).unwrap();
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..20 {
            assert_eq!(a.append(b"12345678").unwrap(), HDR_SI + i);
        }
    }
    let blocked = Loop::new(&base, "header_1");
    let mut p = q.create_poller().unwrap();
    let mut r = q.create_reader().unwrap();
    let mut next = HDR_SI;
    assert_eio_at_once(|| drain(&mut p, &mut next));
    assert_eq!(next, 32768); // header file 0 delivered, then Eio
    assert_eio_at_once(|| drain(&mut p, &mut next)); // a retry fails the same way
    assert_eio_at_once(|| r.read(32770).map(|_| ()));
    assert_eq!(r.last_index(), -1); // cannot see past the unreadable file
    blocked.restore();
    assert!(matches!(drain(&mut p, &mut next), Ok(PollResult::EndOfStream)));
    assert_eq!(next, HDR_SI + 20);
    assert!(matches!(r.read(32770), Ok(ReadResult::Available(_))));
    assert_eq!(r.last_index(), HDR_SI + 19);
}

#[test]
fn unreadable_payload_file_is_eio_not_corrupt() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("payload");
    let q = IndexedQueue::create(config(&base, &rt)).unwrap();
    let msg = [7u8; 1000];
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..300 {
            assert_eq!(a.append(&msg).unwrap(), i);
        }
    }
    let blocked = Loop::new(&base, "payload_0_1");
    let mut p = q.create_poller().unwrap();
    let mut r = q.create_reader().unwrap();
    let mut next = 0;
    assert_eio_at_once(|| drain(&mut p, &mut next));
    assert_eq!(next, 256); // payload file 0 delivered, then Eio
    assert_eio_at_once(|| r.read(299).map(|_| ()));
    assert_eq!(r.last_index(), 299); // the headers are readable
    blocked.restore();
    assert!(matches!(drain(&mut p, &mut next), Ok(PollResult::EndOfStream)));
    assert_eq!(next, 300);
}

#[test]
fn unreadable_header_file_is_eio_batch() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("batch");
    let q = IndexedBatchQueue::create(config(&base, &rt).start_index(HDR_SI)).unwrap();
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..20 {
            assert_eq!(a.append_and_commit(b"12345678").unwrap(), HDR_SI + i);
        }
    }
    let blocked = Loop::new(&base, "header_1");
    let mut p = q.create_poller().unwrap();
    let mut next = HDR_SI;
    let mut drain = |p: &mut mmapq::batch::IndexedBatchPoller| loop {
        let r = p.poll(|i, _| {
            assert_eq!(i, next);
            next += 1;
            NextMove::Advance
        });
        if !matches!(r, Ok(PollResult::Advanced)) {
            return r;
        }
    };
    assert_eio_at_once(|| drain(&mut p));
    blocked.restore();
    assert!(matches!(drain(&mut p), Ok(PollResult::EndOfStream)));
    assert_eq!(next, HDR_SI + 20);
}
