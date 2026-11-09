//! Regression tests for the Runtime use-after-free (review finding 3.1).
//!
//! Two drop orders must be sound:
//!   1. Queue outlives the user's Runtime binding — the queue must keep the
//!      runtime alive via its own stored clones.
//!   2. Participants (appender/poller/reader) outlive both the queue and the
//!      user's Runtime binding — each participant must hold its own clone.
//!
//! Pre-fix, both scenarios dereference a freed Runtime: abort under debug
//! asserts, UB under NDEBUG. This file is a separate integration-test binary
//! so a regression kills only this process, reported as one test failure.

use mmapq::{IndexedQueue, QueueConfig, Runtime, RuntimeConfig};

fn temp_queue_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "mmap-uaf-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_string()
}

/// Scenario 1: the user's Runtime binding is dropped while the queue lives.
/// The queue's stored clones must keep the C runtime alive for participant
/// creation and appends.
// covers: RuntimeConfig::close_timeout_ns
#[test]
fn queue_outlives_user_runtime_binding() {
    let dir = temp_queue_dir("s1");

    let q = {
        let rt = Runtime::new(RuntimeConfig::new().name("uaf-s1").close_timeout_ns(100_000_000)).unwrap();
        IndexedQueue::create(
            QueueConfig::new(&dir, "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap()
    }; // user's `rt` binding dropped here

    let mut a = q.create_appender().unwrap();
    let idx = a.append(b"survives-runtime-drop").unwrap();
    assert_eq!(idx, 0);
}

/// Scenario 2: the queue itself is dropped while participants live.
/// Each participant's own Runtime clone must keep the C runtime alive
/// independently of the queue.
#[test]
fn participants_outlive_queue_and_runtime_binding() {
    let dir = temp_queue_dir("s2");

    let (mut a, mut p) = {
        let rt = Runtime::new(RuntimeConfig::new().name("uaf-s2")).unwrap();
        let q = IndexedQueue::create(
            QueueConfig::new(&dir, "q")
                .write_runtime(&rt)
                .read_runtime(&rt),
        )
        .unwrap();
        let a = q.create_appender().unwrap();
        let p = q.create_poller().unwrap();
        (a, p)
    }; // both `rt` and `q` dropped here — participants must stand alone

    let idx = a.append(b"survives-queue-drop").unwrap();
    assert_eq!(idx, 0);

    let mut seen = 0;
    let res = p
        .poll(|_idx, _aid, data: &[u8]| {
            assert_eq!(data, b"survives-queue-drop");
            seen += 1;
            mmapq::NextMove::Advance
        })
        .unwrap();
    assert!(matches!(res, mmapq::PollResult::Advanced));
    assert_eq!(seen, 1);
}
