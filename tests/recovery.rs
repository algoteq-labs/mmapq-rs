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
// covers: IndexedAppender::uncommitted_iter
// covers: UncommittedIter::next
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
// covers: BatchWriter::complete_uncommitted
// covers: IndexedBatchAppender::commit_batch
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
// covers: IndexedBatchAppender::uncommitted_batches
// covers: UncommittedBatches::next
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
// covers: IndexedBatchAppender::join_completed_open_batch
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

/* ── CAND-51b F-REC-3 wrapper pins ─────────────────────────────────────── */

fn setup_batch_archivable(name: &str) -> (IndexedBatchQueue, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = std::env::temp_dir().join("mmap-rs-test-recovery").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("failed to create test base directory");
    let q = IndexedBatchQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .archivable(true)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    (q, rt)
}

/// Mirrors the C §3.5 overtaken pre-check (test_batch_uncommitted_dg_line_
/// respected): a staged batch whose offset an archive has overtaken is
/// rejected with EINVAL — final. (ORPHANED recovery is a C-only pin: the
/// pre-store fault that creates ORPHANED is unreachable from the wrapper.)
#[test]
fn batch_commit_batch_overtaken_is_rejected() {
    let (q, _rt) = setup_batch_archivable("batch_overtaken_commit");
    let mut a = q.create_appender().unwrap();

    let off_b1;
    {
        let mut w = a.start_batch().unwrap();
        w.append(b"b1").unwrap();
        off_b1 = w.complete_uncommitted().unwrap();
    }
    assert_eq!(off_b1, 64);
    {
        let mut w = a.start_batch().unwrap();
        w.append(b"b2").unwrap();
        assert_eq!(w.commit().unwrap(), 0);
    }

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(1).unwrap();   // boundary past B1
    drop(arch);

    assert!(matches!(
        a.commit_batch(off_b1),
        Err(MmapError::InvalidArgument)
    ));
    // Never published: the archive moved start_index past B2 (idx 0), so a
    // fresh poller starts at 1 and sees EOS — nothing live remains.
    let mut p = q.create_poller().unwrap();
    let r = p.poll(|_bi, _iter| NextMove::Advance).unwrap();
    assert_eq!(r, PollResult::EndOfStream);
}

/// Mirrors test_batch_commit_out_of_order_multi_legal: stage A then B,
/// commit B then A — both legal (membership is the gate); exact indices.
#[test]
fn batch_commit_out_of_order_multi_legal() {
    let (q, _rt) = setup_batch("batch_out_of_order_commit");
    let mut a = q.create_appender().unwrap();

    let off_a;
    {
        let mut w = a.start_batch().unwrap();
        w.append(b"a").unwrap();
        off_a = w.complete_uncommitted().unwrap();
    }
    let off_b;
    {
        let mut w = a.start_batch().unwrap();
        w.append(b"b").unwrap();
        off_b = w.complete_uncommitted().unwrap();
    }
    assert_eq!(off_a, 64);
    assert_eq!(off_b, 73); // 64 + 4 (count) + 4 (len) + 1 ("a")

    assert_eq!(a.commit_batch(off_b).unwrap(), 0);
    assert_eq!(a.commit_batch(off_a).unwrap(), 1);
}

/// Batch archiver forward + undo through the wrapper: archiving advances
/// the read boundary (poll below it → Archived), undo restores it.
/// (reclaim_files has no wrapper binding — C-only for both queue types.)
// covers: IndexedBatchArchiver::archivable_files
#[test]
fn batch_archiver_forward_and_undo() {
    let (q, _rt) = setup_batch_archivable("batch_archiver_forward_undo");
    let mut a = q.create_appender().unwrap();
    for (i, msg) in [b"m0".as_slice(), b"m1", b"m2"].iter().enumerate() {
        let mut w = a.start_batch().unwrap();
        w.append(msg).unwrap();
        assert_eq!(w.commit().unwrap(), i as i64);
    }

    // Poller created BEFORE the archive: cursor 0. After archiving to 2 the
    // cursor is below start_index → Archived; after the undo the poll reads
    // m1/m2 (m0 already served).
    let mut p = q.create_poller().unwrap();
    let r = p.poll(|_bi, mut iter| {
        let (data, _aid) = iter.next().unwrap().expect("one message");
        assert_eq!(data.to_vec(), b"m0".to_vec());
        NextMove::Advance
    }).unwrap();
    assert_eq!(r, PollResult::Advanced);

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(2).unwrap();
    // archivable_files binding: nothing is wholly below the boundary at the
    // default geometry (everything is in payload file 0).
    assert_eq!(arch.archivable_files().unwrap(), Vec::<String>::new());
    let r = p.poll(|_bi, _iter| NextMove::Advance).unwrap();
    assert_eq!(r, PollResult::Archived);   // cursor 1 < start_index 2

    arch.archive_up_to_index(0).unwrap();  // undo
    drop(arch);

    let mut seen: Vec<Vec<u8>> = Vec::new();
    for _ in 0..2 {
        let r = p.poll(|_bi, mut iter| {
            let (data, _aid) = iter.next().unwrap().expect("one message");
            seen.push(data.to_vec());
            NextMove::Advance
        }).unwrap();
        assert_eq!(r, PollResult::Advanced);
    }
    assert_eq!(seen, vec![b"m1".to_vec(), b"m2".to_vec()]);
}

/// An empty batch cannot complete nor stage (EINVAL; the writer is
/// dropped/reusable — Rust consumes the handle either way).
#[test]
fn batch_empty_commit_and_stage_rejected() {
    let (q, _rt) = setup_batch("batch_empty_rejected");
    let mut a = q.create_appender().unwrap();
    {
        let w = a.start_batch().unwrap();
        assert!(matches!(w.commit(), Err(MmapError::InvalidArgument)));
    }
    {
        let w = a.start_batch().unwrap();
        assert!(matches!(
            w.complete_uncommitted(),
            Err(MmapError::InvalidArgument)
        ));
    }
    // The lane is usable afterwards.
    let mut w = a.start_batch().unwrap();
    w.append(b"ok").unwrap();
    assert_eq!(w.commit().unwrap(), 0);
}
