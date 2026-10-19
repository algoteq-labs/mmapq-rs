//! Recovery-surface tests (CAND-49 §5.5): uncommitted iterators,
//! complete_uncommitted/commit_batch, join_completed_open_batch.
//! Shapes mirror the C pins in test/test_indexed_queue.c and
//! test/test_indexed_batch_queue.c (offsets are exact: a fresh lane starts
//! at payload offset 64).

use mmapq::batch::IndexedBatchQueue;
use mmapq::error::MmapError;
use mmapq::queue::QueueConfig;
use mmapq::runtime::Runtime;
use mmapq::types::{NextMove, PollResult};
use mmapq::IndexedQueue;

fn setup_single(name: &str) -> (IndexedQueue, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = std::env::temp_dir().join("mmap-rs-test-recovery").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("failed to create test base directory");
    let q = IndexedQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    (q, rt)
}

fn setup_batch(name: &str) -> (IndexedBatchQueue, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = std::env::temp_dir().join("mmap-rs-test-recovery").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("failed to create test base directory");
    let q = IndexedBatchQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    (q, rt)
}

/// Mirrors test_uncommitted_iterator_finds_uncommitted: one committed, one
/// uncommitted; the iterator yields exactly the uncommitted one, with its
/// exact offset and bytes; after commit it yields nothing.
#[test]
fn uncommitted_iter_finds_uncommitted() {
    let (q, _rt) = setup_single("single_finds_uncommitted");
    let mut a = q.create_appender().unwrap();

    let idx = a.append(b"committed").unwrap();
    assert_eq!(idx, 0);
    let off = a.append_uncommitted(b"uncommitted").unwrap();
    assert_eq!(off, 77); // 64 (start of stream) + 4 (len prefix) + 9 ("committed")

    {
        let mut it = a.uncommitted_iter().unwrap();
        let first = it.next().unwrap().expect("one uncommitted message");
        assert_eq!(first.0, 77);
        assert_eq!(first.1, b"uncommitted");
        assert!(it.next().unwrap().is_none(), "snapshot exhausted after one");
    }

    let idx = a.commit(off).unwrap();
    assert_eq!(idx, 1);

    let mut it = a.uncommitted_iter().unwrap();
    assert!(it.next().unwrap().is_none(), "nothing uncommitted after commit");
}

/// Snapshot discipline: messages appended after the factory call are
/// invisible to that iterator.
#[test]
fn uncommitted_iter_is_a_call_time_snapshot() {
    let (q, _rt) = setup_single("single_snapshot");
    let mut a = q.create_appender().unwrap();

    let off1 = a.append_uncommitted(b"one").unwrap();
    assert_eq!(off1, 64);
    let mut it = a.uncommitted_iter().unwrap();

    let first = it.next().unwrap().expect("first message");
    assert_eq!(first.0, 64);
    assert_eq!(first.1, b"one");
    drop(it);

    // A second iterator (a new snapshot) after a second append sees both.
    let off2 = a.append_uncommitted(b"two").unwrap();
    assert_eq!(off2, 71); // 64 + 4 + 3
    let mut it2 = a.uncommitted_iter().unwrap();
    assert_eq!(it2.next().unwrap().unwrap().0, 64);
    assert_eq!(it2.next().unwrap().unwrap().0, 71);
    assert!(it2.next().unwrap().is_none());
}

/// Recovery across a rejoin: uncommitted messages survive the appender
/// closing; the rejoined appender's iterator refills them as committable.
/// Commit validation: unknown/already-committed offsets are EINVAL.
#[test]
fn uncommitted_iter_rejoin_and_commit_validation() {
    let (q, _rt) = setup_single("single_rejoin");
    let off_a;
    let off_b;
    {
        let mut a = q.create_appender().unwrap();
        off_a = a.append_uncommitted(b"msg-a").unwrap();
        assert_eq!(off_a, 64);
        off_b = a.append_uncommitted(b"msg-bb").unwrap();
        assert_eq!(off_b, 73); // 64 + 4 + 5
        // Committing an offset never written uncommitted is EINVAL.
        assert!(matches!(a.commit(8), Err(MmapError::InvalidArgument)));
    } // appender closes ("restart")

    let mut a = q.create_appender().unwrap();
    {
        let mut it = a.uncommitted_iter().unwrap();
        let first = it.next().unwrap().expect("msg-a");
        assert_eq!(first.0, 64);
        assert_eq!(first.1, b"msg-a");
        let second = it.next().unwrap().expect("msg-bb");
        assert_eq!(second.0, 73);
        assert_eq!(second.1, b"msg-bb");
        assert!(it.next().unwrap().is_none());
    }

    // Refilled by the walk: both offsets committable after the rejoin.
    let idx = a.commit(off_b).unwrap();
    assert_eq!(idx, 0);
    let idx = a.commit(off_a).unwrap();
    assert_eq!(idx, 1);
    // Already committed: EINVAL.
    assert!(matches!(a.commit(off_a), Err(MmapError::InvalidArgument)));
}

/// The staged path: complete_uncommitted stages without publishing;
/// commit_batch publishes by offset (the gap fix — the staged/recovered
/// commit path).
#[test]
fn batch_complete_uncommitted_then_commit_batch() {
    let (q, _rt) = setup_batch("batch_staged_commit");
    let mut a = q.create_appender().unwrap();

    let off;
    {
        let mut w = a.start_batch().unwrap();
        assert_eq!(w.append(b"a").unwrap(), 0);
        assert_eq!(w.append(b"bb").unwrap(), 1);
        assert_eq!(w.append(b"ccc").unwrap(), 2);
        off = w.complete_uncommitted().unwrap();
    }
    assert_eq!(off, 64); // first batch of a fresh lane

    // Staged is not visible yet.
    let mut p = q.create_poller().unwrap();
    let r = p.poll(|_bi, _iter| NextMove::Advance).unwrap();
    assert_eq!(r, PollResult::EndOfStream);
    drop(p);

    let idx = a.commit_batch(off).unwrap();
    assert_eq!(idx, 0);
    // Already committed: EINVAL. Never staged: EINVAL.
    assert!(matches!(a.commit_batch(off), Err(MmapError::InvalidArgument)));
    assert!(matches!(a.commit_batch(8), Err(MmapError::InvalidArgument)));

    let mut p = q.create_poller().unwrap();
    let mut messages: Vec<Vec<u8>> = Vec::new();
    let r = p
        .poll(|_bi, mut iter| {
            while let Some((data, _aid)) = iter.next().expect("batch iteration") {
                messages.push(data.to_vec());
            }
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Advanced);
    assert_eq!(messages.len(), 3);
    assert_eq!(&messages[0], b"a");
    assert_eq!(&messages[1], b"bb");
    assert_eq!(&messages[2], b"ccc");
}

/// A discarded batch is final in-session (commit_batch EINVAL) but reappears
/// in recovery after a restart — the walker reports it and refill makes it
/// committable; recovery code decides (owner ruling 8).
#[test]
fn discarded_batch_reappears_in_recovery() {
    let (q, _rt) = setup_batch("batch_discard_recovery");
    {
        let mut a = q.create_appender().unwrap();
        {
            let mut w = a.start_batch().unwrap();
            w.append(b"d1").unwrap();
            w.append(b"d2").unwrap();
        } // drop without commit: discard-only (never publish)
        // Discard is final in-session.
        assert!(matches!(a.commit_batch(64), Err(MmapError::InvalidArgument)));
    } // "restart"

    let mut a = q.create_appender().unwrap();
    {
        let mut batches = a.uncommitted_batches().unwrap();
        let (off, mut msgs) = batches.next().unwrap().expect("discarded batch reported");
        assert_eq!(off, 64);
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        while let Some((data, _aid)) = msgs.next().expect("message iteration") {
            payloads.push(data.to_vec());
        }
        assert_eq!(payloads.len(), 2);
        assert_eq!(&payloads[0], b"d1");
        assert_eq!(&payloads[1], b"d2");
        drop(msgs);
        assert!(batches.next().unwrap().is_none(), "exactly one staged batch");
    }

    // Refill made the discarded offset committable; recovery code decides.
    let idx = a.commit_batch(64).unwrap();
    assert_eq!(idx, 0);
}

/// A trailing open batch (never completed — the "crash" shape) is completed
/// to staged at the rejoin; join_completed_open_batch reports it once
/// (read-and-clear) and the walker yields it.
/// Mirrors test_batch_de_mode2_rejoin_completes_open_batch.
#[test]
fn rejoin_completes_open_batch() {
    let (q, _rt) = setup_batch("batch_join_completion");
    let big = [b'A'; 200];
    {
        let mut a = q.create_appender().unwrap();
        let mut w = a.start_batch().unwrap();
        for _ in 0..3 {
            w.append(&big).unwrap();
        }
        // "Crash" with the batch open: leak the guard so Drop cannot discard.
        std::mem::forget(w);
    } // appender closes with the batch still open on disk

    let mut a = q.create_appender().unwrap();
    assert_eq!(a.join_completed_open_batch(), Some(64));
    assert_eq!(a.join_completed_open_batch(), None, "read-and-clear");

    // The position moved past the completed batch: the next batch publishes
    // at index 0.
    {
        let mut w = a.start_batch().unwrap();
        w.append(b"y").unwrap();
        let idx = w.commit().unwrap();
        assert_eq!(idx, 0);
    }

    // The walker yields the join-completed batch (count 3, exact bytes).
    let mut batches = a.uncommitted_batches().unwrap();
    let (off, mut msgs) = batches.next().unwrap().expect("join-completed batch reported");
    assert_eq!(off, 64);
    for _ in 0..3 {
        let (data, _aid) = msgs.next().unwrap().expect("message");
        assert_eq!(data.len(), 200);
        assert!(data.iter().all(|&b| b == b'A'));
    }
    assert!(msgs.next().unwrap().is_none());
    drop(msgs);
    assert!(batches.next().unwrap().is_none());
}
