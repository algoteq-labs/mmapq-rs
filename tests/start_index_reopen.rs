//! 0.2.1: `start_index` applies only when the queue is created. Reopening an
//! existing queue with a larger value appends after its last entry (it used
//! to append at the given index, past empty slots no poller reaches), and an
//! appender whose rejoin meets a payload file it cannot read gets `Eio`, not
//! `Corrupt`. The unreadable file is a symlink loop (ELOOP), so the test
//! behaves the same as root and as an ordinary user.

use std::path::{Path, PathBuf};

use mmapq::error::MmapError;
use mmapq::queue::QueueConfig;
use mmapq::runtime::Runtime;
use mmapq::types::{NextMove, PollResult};
use mmapq::IndexedQueue;

const REGION: i32 = 32 * 1024;
const FILE: i64 = REGION as i64 * 8;
const TIMEOUT_NS: u64 = 2_000_000_000;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("mmap-rs-start-index").join(name);
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

#[test]
fn reopen_with_higher_start_index_appends_at_tail() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("reopen");
    {
        let q = IndexedQueue::create(config(&base, &rt)).unwrap();
        let mut a = q.create_appender().unwrap();
        for i in 0..3 {
            assert_eq!(a.append(b"msg").unwrap(), i);
        }
    }
    let q = IndexedQueue::create(config(&base, &rt).start_index(10)).unwrap();
    {
        let mut a = q.create_appender().unwrap();
        assert_eq!(a.append(b"msg").unwrap(), 3);
    }
    let mut p = q.create_poller().unwrap();
    let mut next = 0i64;
    loop {
        let r = p.poll(|i, _, _| {
            assert_eq!(i, next);
            next += 1;
            NextMove::Advance
        });
        if !matches!(r, Ok(PollResult::Advanced)) {
            assert!(matches!(r, Ok(PollResult::EndOfStream)), "{r:?}");
            break;
        }
    }
    assert_eq!(next, 4);
    assert_eq!(q.create_reader().unwrap().last_index(), 3);
}

#[test]
fn join_over_unreadable_payload_file_is_eio_not_corrupt() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("join-eio");
    let q = IndexedQueue::create(config(&base, &rt)).unwrap();
    {
        let mut a = q.create_appender_with_id(7).unwrap();
        assert_eq!(a.append_uncommitted(b"msg").unwrap(), 64); // first offset of a new lane
    }
    let path = base.join("q").join("payload_7_0");
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink("payload_7_0", &path).unwrap();
    let r = q.create_appender_with_id(7);
    assert!(matches!(r, Err(MmapError::Eio)), "expected Eio, got {:?}", r.err());
}
