//! Cursor-move coverage on archivable queues, single and batch pollers.
//!
//! Every queue below holds entries (or batches) 0..9 and is archived up to
//! 5, so `start_index == 5`. The pinned behaviors follow the archiving
//! guide: `move_to_start` lands on `start_index`; `move_to_index` refuses
//! an archived index without moving the cursor; a poll below `start_index`
//! returns `Archived`, never calls the handler, and holds the cursor;
//! `move_to_end` parks one past the last entry until an append publishes
//! there.

use mmapq::batch::IndexedBatchQueue;
use mmapq::queue::QueueConfig;
use mmapq::runtime::Runtime;
use mmapq::types::{NextMove, PollResult};
use mmapq::IndexedQueue;

fn base_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("mmap-rs-cursor").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create base dir");
    dir
}

/// Archivable single-message queue with entries "m0".."m9", archived to 5.
fn setup_single_archived(name: &str) -> (IndexedQueue, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = base_dir(name);
    let q = IndexedQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .archivable(true)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..10 {
            let msg = format!("m{i}");
            assert_eq!(a.append(msg.as_bytes()).unwrap(), i);
        }
    }
    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(5).unwrap();
    drop(arch);
    (q, rt)
}

/// Archivable batch queue with batches 0..9 (one "b<i>" message each),
/// archived to 5.
fn setup_batch_archived(name: &str) -> (IndexedBatchQueue, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = base_dir(name);
    let q = IndexedBatchQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .archivable(true)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..10 {
            let msg = format!("b{i}");
            let mut w = a.start_batch().unwrap();
            w.append(msg.as_bytes()).unwrap();
            assert_eq!(w.commit().unwrap(), i);
        }
    }
    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(5).unwrap();
    drop(arch);
    (q, rt)
}

// covers: IndexedPoller::move_to_start
// covers: IndexedPoller::current_index
// covers: IndexedPoller::start_index
// covers: IndexedPoller::exists
#[test]
fn single_move_to_start_lands_on_start_index() {
    let (q, _rt) = setup_single_archived("single_start");

    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_index(7), "move to a live index first");
    assert!(p.move_to_start());
    assert_eq!(p.start_index(), 5);
    assert_eq!(p.current_index(), 5);
    assert!(!p.exists(4), "archived entry is not readable");
    assert!(p.exists(5), "start_index entry is readable");
    assert!(!p.exists(10), "not yet appended");

    let mut delivered = -1;
    let r = p
        .poll(|idx, _aid, data| {
            assert_eq!(idx, 5);
            assert_eq!(data, b"m5");
            delivered = idx;
            NextMove::Stop
        })
        .unwrap();
    assert_eq!(r, PollResult::Stopped);
    assert_eq!(delivered, 5);
    assert_eq!(p.current_index(), 5, "Stop holds the cursor");
}

/// A cursor positioned before archiving stays put: move_to_index refuses
/// the archived target, and the next poll reports Archived without calling
/// the handler or moving the cursor.
#[test]
fn single_move_to_index_refuses_archived_index() {
    let rt = Runtime::region_mapper().unwrap();
    let dir = base_dir("single_refused");
    let q = IndexedQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .archivable(true)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..10 {
            let msg = format!("m{i}");
            assert_eq!(a.append(msg.as_bytes()).unwrap(), i);
        }
    }
    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_index(2), "position below the later archive line");

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(5).unwrap();
    drop(arch);

    assert!(!p.move_to_index(3), "an archived index is refused");
    assert_eq!(p.current_index(), 2, "a refused move leaves the cursor");

    let mut handler_calls = 0;
    let r = p
        .poll(|_idx, _aid, _data| {
            handler_calls += 1;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Archived);
    assert_eq!(handler_calls, 0, "Archived never calls the handler");
    assert_eq!(p.current_index(), 2, "Archived holds the cursor");
}

// covers: IndexedPoller::move_to_end
#[test]
fn single_move_to_index_then_end_delivers_appended_index() {
    let (q, _rt) = setup_single_archived("single_index_end");

    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_index(7));
    let mut delivered = -1;
    let r = p
        .poll(|idx, _aid, data| {
            assert_eq!(idx, 7);
            assert_eq!(data, b"m7");
            delivered = idx;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Advanced);
    assert_eq!(delivered, 7);

    assert!(p.move_to_end());
    assert_eq!(p.current_index(), 10, "one past the last entry");
    let mut handler_calls = 0;
    let r = p
        .poll(|_idx, _aid, _data| {
            handler_calls += 1;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::EndOfStream);
    assert_eq!(handler_calls, 0, "EndOfStream never calls the handler");

    {
        let mut a = q.create_appender().unwrap();
        assert_eq!(a.append(b"m10").unwrap(), 10);
    }
    let mut delivered = -1;
    let r = p
        .poll(|idx, _aid, data| {
            assert_eq!(idx, 10);
            assert_eq!(data, b"m10");
            delivered = idx;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Advanced);
    assert_eq!(delivered, 10);
}

// covers: IndexedPoller::move_to_last
#[test]
fn single_move_to_last_delivers_last_entry() {
    let (q, _rt) = setup_single_archived("single_last");

    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_last());
    assert_eq!(p.current_index(), 9);
    let mut delivered = -1;
    let r = p
        .poll(|idx, _aid, data| {
            assert_eq!(idx, 9);
            assert_eq!(data, b"m9");
            delivered = idx;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Advanced);
    assert_eq!(delivered, 9);
}

// covers: IndexedBatchPoller::move_to_start
// covers: IndexedBatchPoller::start_index
// covers: IndexedBatchPoller::exists
#[test]
fn batch_move_to_start_lands_on_start_index() {
    let (q, _rt) = setup_batch_archived("batch_start");

    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_index(7), "move to a live batch first");
    assert!(p.move_to_start());
    assert_eq!(p.start_index(), 5);
    assert_eq!(p.current_batch_index(), 5);
    assert!(!p.exists(4), "archived batch is not readable");
    assert!(p.exists(5), "start_index batch is readable");
    assert!(!p.exists(10), "batch not yet appended");

    let mut delivered = -1;
    let r = p
        .poll(|bi, mut iter| {
            assert_eq!(bi, 5);
            let (data, _aid) = iter.next().unwrap().expect("one message");
            assert_eq!(data, b"b5");
            delivered = bi;
            NextMove::Stop
        })
        .unwrap();
    assert_eq!(r, PollResult::Stopped);
    assert_eq!(delivered, 5);
    assert_eq!(p.current_batch_index(), 5, "Stop holds the cursor");
}

/// Batch counterpart of single_move_to_index_refuses_archived_index.
// covers: IndexedBatchPoller::move_to_index
#[test]
fn batch_move_to_index_refuses_archived_index() {
    let rt = Runtime::region_mapper().unwrap();
    let dir = base_dir("batch_refused");
    let q = IndexedBatchQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .archivable(true)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..10 {
            let msg = format!("b{i}");
            let mut w = a.start_batch().unwrap();
            w.append(msg.as_bytes()).unwrap();
            assert_eq!(w.commit().unwrap(), i);
        }
    }
    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_index(2), "position below the later archive line");

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(5).unwrap();
    drop(arch);

    assert!(!p.move_to_index(3), "an archived batch index is refused");
    assert_eq!(p.current_batch_index(), 2, "a refused move leaves the cursor");

    let mut handler_calls = 0;
    let r = p
        .poll(|_bi, _iter| {
            handler_calls += 1;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Archived);
    assert_eq!(handler_calls, 0, "Archived never calls the handler");
    assert_eq!(p.current_batch_index(), 2, "Archived holds the cursor");
}

// covers: IndexedBatchPoller::move_to_end
#[test]
fn batch_move_to_index_then_end_delivers_appended_batch() {
    let (q, _rt) = setup_batch_archived("batch_index_end");

    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_index(7));
    let mut delivered = -1;
    let r = p
        .poll(|bi, mut iter| {
            assert_eq!(bi, 7);
            let (data, _aid) = iter.next().unwrap().expect("one message");
            assert_eq!(data, b"b7");
            delivered = bi;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Advanced);
    assert_eq!(delivered, 7);

    assert!(p.move_to_end());
    assert_eq!(p.current_batch_index(), 10, "one past the last batch");
    let mut handler_calls = 0;
    let r = p
        .poll(|_bi, _iter| {
            handler_calls += 1;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::EndOfStream);
    assert_eq!(handler_calls, 0, "EndOfStream never calls the handler");

    {
        let mut a = q.create_appender().unwrap();
        let mut w = a.start_batch().unwrap();
        w.append(b"b10").unwrap();
        assert_eq!(w.commit().unwrap(), 10);
    }
    let mut delivered = -1;
    let r = p
        .poll(|bi, mut iter| {
            assert_eq!(bi, 10);
            let (data, _aid) = iter.next().unwrap().expect("one message");
            assert_eq!(data, b"b10");
            delivered = bi;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Advanced);
    assert_eq!(delivered, 10);
}

// covers: IndexedBatchPoller::move_to_last_existing_batch
#[test]
fn batch_move_to_last_existing_batch_delivers_last_batch() {
    let (q, _rt) = setup_batch_archived("batch_last_existing");

    let mut p = q.create_poller().unwrap();
    assert!(p.move_to_last_existing_batch());
    assert_eq!(p.current_batch_index(), 9);
    let mut delivered = -1;
    let r = p
        .poll(|bi, mut iter| {
            assert_eq!(bi, 9);
            let (data, _aid) = iter.next().unwrap().expect("one message");
            assert_eq!(data, b"b9");
            delivered = bi;
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(r, PollResult::Advanced);
    assert_eq!(delivered, 9);
}
