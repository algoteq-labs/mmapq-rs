//! reclaim_files binding tests: the mark -> reclaim deletion protocol
//! through the wrapper on both queue types, at the 32 KiB region/file
//! geometry (one payload/header file per region — the smallest legal
//! geometry; a fresh lane starts at payload offset 64).
//!
//! Offsets are exact: a 1000-byte message occupies 1004 framed bytes
//! (4-byte length prefix), a 1000-byte one-message batch occupies 1008
//! (4-byte count + 4 + 1000).

use mmapq::batch::IndexedBatchQueue;
use mmapq::error::MmapError;
use mmapq::queue::QueueConfig;
use mmapq::runtime::Runtime;
use mmapq::{IndexedQueue, ReadResult};

/// 1000 deterministic bytes: the message index in the first bytes, the
/// rest a fixed pattern.
fn payload(i: usize) -> Vec<u8> {
    let mut v = vec![0xa5u8; 1000];
    let head = format!("msg-{i:03}");
    v[..head.len()].copy_from_slice(head.as_bytes());
    v
}

fn temp_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("mmap-rs-test-reclaim-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("failed to create test base directory");
    dir.to_str().unwrap().to_string()
}

fn setup_single(tag: &str) -> (IndexedQueue, String, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = temp_dir(tag);
    let q = IndexedQueue::create(
        QueueConfig::new(&dir, "q")
            .archivable(true)
            .region_size(32768)
            .max_file_size(32768)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    (q, dir, rt)
}

fn setup_batch(tag: &str) -> (IndexedBatchQueue, String, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = temp_dir(tag);
    let q = IndexedBatchQueue::create(
        QueueConfig::new(&dir, "q")
            .archivable(true)
            .region_size(32768)
            .max_file_size(32768)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    (q, dir, rt)
}

fn file_names(paths: &[String]) -> Vec<String> {
    let mut names: Vec<String> = paths
        .iter()
        .map(|p| {
            std::path::Path::new(p)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/* Geometry at region = file = 32768, 100 messages of 1000 bytes
 * (1004 framed each), single appender (lane 0):
 *   file 0 [0, 32768):   msgs 0..=31  at 64 + 1004*i   (last ends 32192;
 *                        576 bytes left < 1004 -> padding)
 *   file 1 [32768, 65536): msgs 32..=63 at 32768 + 1004*(i-32) (ends 64896;
 *                        640 left -> padding)
 *   file 2 [65536, 98304): msgs 64..=95; msg 79 ends at 65536 + 1004*16
 *                        = 81600
 *   file 3 [98304, ...): msgs 96..=99
 * archive_up_to_index(80) -> boundary 81600 (end of msg 79; live msg 80
 * starts exactly there, nothing is lowered). Payload files wholly below
 * 81600: payload_0_0 [0, 32768) and payload_0_1 [32768, 65536); payload_0_2
 * straddles. header_0 covers indices 0..=4095 (8-byte entries, 4096 per
 * 32-KiB header block) — not wholly below start_index 80, not listed. */

// covers: IndexedArchiver::reclaim_files
// covers: IndexedArchiver::archivable_files
#[test]
fn single_positive() {
    let (q, _dir, _rt) = setup_single("single_positive");
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..100usize {
            assert_eq!(a.append(&payload(i)).unwrap(), i as i64);
        }
    }

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(80).unwrap();
    let list = arch.archivable_files().unwrap();
    assert_eq!(
        file_names(&list),
        vec!["payload_0_0".to_string(), "payload_0_1".to_string()]
    );

    assert_eq!(arch.reclaim_files(&list).unwrap(), list.len());
    for p in &list {
        assert!(!std::path::Path::new(p).exists(), "{p} should be deleted");
    }
    assert_eq!(arch.archivable_files().unwrap(), Vec::<String>::new());
    assert_eq!(arch.reclaim_files(&list).unwrap(), 0);

    let mut r = q.create_reader().unwrap();
    for i in 80..=99usize {
        match r.read(i as i64).unwrap() {
            ReadResult::Available(m) => assert_eq!(m.to_vec(), payload(i), "message {i}"),
            other => panic!("read({i}) returned {other:?}, expected Available"),
        }
    }
}

// covers: IndexedArchiver::reclaim_files
// covers: IndexedArchiver::archivable_files
#[test]
fn single_undo_protects() {
    let (q, _dir, _rt) = setup_single("single_undo");
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..100usize {
            assert_eq!(a.append(&payload(i)).unwrap(), i as i64);
        }
    }

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(80).unwrap();
    let list = arch.archivable_files().unwrap();
    assert!(!list.is_empty());

    arch.archive_up_to_index(0).unwrap(); // undo — restores [0, 80)

    assert_eq!(arch.reclaim_files(&list).unwrap(), 0);
    for p in &list {
        assert!(std::path::Path::new(p).exists(), "{p} must survive the undo");
    }

    let mut r = q.create_reader().unwrap();
    match r.read(0).unwrap() {
        ReadResult::Available(m) => assert_eq!(m.to_vec(), payload(0)),
        other => panic!("read(0) returned {other:?}, expected Available"),
    }
}

// covers: IndexedArchiver::reclaim_files
#[test]
fn single_unlisted_path_untouched() {
    let (q, dir, _rt) = setup_single("single_unlisted");
    // One append creates the queue directory and its files.
    {
        let mut a = q.create_appender().unwrap();
        assert_eq!(a.append(&payload(0)).unwrap(), 0);
    }
    let unlisted = std::path::Path::new(&dir).join("q").join("not_a_queue_file");
    std::fs::write(&unlisted, b"not a queue file").unwrap();

    let arch = q.create_archiver().unwrap();
    assert_eq!(arch.reclaim_files(&[unlisted.to_str().unwrap()]).unwrap(), 0);
    assert!(unlisted.exists(), "an unmarked file is never deleted");
    assert_eq!(arch.reclaim_files::<&str>(&[]).unwrap(), 0);
}

// covers: IndexedArchiver::reclaim_files
#[test]
fn single_nul_rejected() {
    let (q, _dir, _rt) = setup_single("single_nul");
    let arch = q.create_archiver().unwrap();
    assert!(matches!(
        arch.reclaim_files(&["a\0b"]),
        Err(MmapError::InvalidArgument)
    ));
}

/* Batch twin, one 1000-byte message per batch (1008 framed bytes:
 * 4-byte count + 4-byte length + 1000):
 *   file 0: batches 0..=31 at 64 + 1008*i    (last ends 32320; 448 -> pad)
 *   file 1: batches 32..=63 at 32768 + 1008*(i-32) (ends 65024; 512 -> pad)
 *   file 2: batches 64..=95; batch 79 ends at 65536 + 1008*16 = 81664
 *   file 3: batches 96..=99
 * archive_up_to_index(80) -> boundary 81664; payload_0_0 and payload_0_1
 * wholly below it, payload_0_2 straddles, header_0 not below 80. */

// covers: IndexedBatchArchiver::reclaim_files
// covers: IndexedBatchArchiver::archivable_files
#[test]
fn batch_positive() {
    let (q, _dir, _rt) = setup_batch("batch_positive");
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..100usize {
            let mut w = a.start_batch().unwrap();
            w.append(&payload(i)).unwrap();
            assert_eq!(w.commit().unwrap(), i as i64);
        }
    }

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(80).unwrap();
    let list = arch.archivable_files().unwrap();
    assert_eq!(
        file_names(&list),
        vec!["payload_0_0".to_string(), "payload_0_1".to_string()]
    );

    assert_eq!(arch.reclaim_files(&list).unwrap(), list.len());
    for p in &list {
        assert!(!std::path::Path::new(p).exists(), "{p} should be deleted");
    }
    assert_eq!(arch.archivable_files().unwrap(), Vec::<String>::new());
    assert_eq!(arch.reclaim_files(&list).unwrap(), 0);

    let mut r = q.create_reader().unwrap();
    for i in 80..=99usize {
        match r.read(i as i64).unwrap() {
            ReadResult::Available(ref mut bm) => {
                assert_eq!(bm.message_count(), 1);
                let mut msgs = bm.iter();
                let (data, _aid) = msgs.next().unwrap().expect("one message");
                assert_eq!(data.to_vec(), payload(i), "batch {i}");
                assert!(msgs.next().unwrap().is_none());
            }
            other => panic!("read({i}) returned {other:?}, expected Available"),
        }
    }
}

// covers: IndexedBatchArchiver::reclaim_files
// covers: IndexedBatchArchiver::archivable_files
#[test]
fn batch_undo_protects() {
    let (q, _dir, _rt) = setup_batch("batch_undo");
    {
        let mut a = q.create_appender().unwrap();
        for i in 0..100usize {
            let mut w = a.start_batch().unwrap();
            w.append(&payload(i)).unwrap();
            assert_eq!(w.commit().unwrap(), i as i64);
        }
    }

    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(80).unwrap();
    let list = arch.archivable_files().unwrap();
    assert!(!list.is_empty());

    arch.archive_up_to_index(0).unwrap(); // undo

    assert_eq!(arch.reclaim_files(&list).unwrap(), 0);
    for p in &list {
        assert!(std::path::Path::new(p).exists(), "{p} must survive the undo");
    }

    let mut r = q.create_reader().unwrap();
    match r.read(0).unwrap() {
        ReadResult::Available(ref mut bm) => {
            let mut msgs = bm.iter();
            let (data, _aid) = msgs.next().unwrap().expect("one message");
            assert_eq!(data.to_vec(), payload(0));
            assert!(msgs.next().unwrap().is_none());
        }
        other => panic!("read(0) returned {other:?}, expected Available"),
    }
}

/// Geometry for the listing-size tests: 32 KiB regions and files, one
/// 30,000-byte message per file (it does not fit beside another), so N
/// messages fill payload files 0..N-1. Archiving up to N-1 leaves message
/// N-1 live; the lane's start_offset is the end of message N-2, inside file
/// N-2, so exactly payload_0_0 .. payload_0_{N-3} (N-2 files) are listed.
fn big_listing_names(n: usize) -> Vec<String> {
    let mut v: Vec<String> = (0..n - 2).map(|k| format!("payload_0_{k}")).collect();
    v.sort();
    v
}

fn base_names(paths: &[String]) -> Vec<String> {
    let mut v: Vec<String> = paths
        .iter()
        .map(|p| std::path::Path::new(p).file_name().unwrap().to_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

// covers: IndexedArchiver::archivable_files
#[test]
fn single_listing_past_the_first_array_is_complete() {
    let (q, _dir, _rt) = setup_single("big-single");
    let msg = vec![7u8; 30000];
    let mut last = -1;
    {
        let mut a = q.create_appender().unwrap();
        for _ in 0..1100 {
            last = a.append(&msg).unwrap();
        }
    }
    assert_eq!(last, 1099);
    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(1099).unwrap();
    let list = arch.archivable_files().unwrap();
    assert_eq!(list.len(), 1098);
    assert_eq!(base_names(&list), big_listing_names(1100));
}

// covers: IndexedBatchArchiver::archivable_files
#[test]
fn batch_listing_past_the_first_array_is_complete() {
    let rt = Runtime::region_mapper().unwrap();
    let dir = temp_dir("big-batch");
    let q = IndexedBatchQueue::create(
        QueueConfig::new(&dir, "q")
            .archivable(true)
            .region_size(32768)
            .max_file_size(32768)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    let msg = vec![7u8; 30000];
    let mut last = -1;
    {
        let mut a = q.create_appender().unwrap();
        for _ in 0..1100 {
            last = a.append_and_commit(&msg).unwrap();
        }
    }
    assert_eq!(last, 1099);
    let mut arch = q.create_archiver().unwrap();
    arch.archive_up_to_index(1099).unwrap();
    let list = arch.archivable_files().unwrap();
    assert_eq!(list.len(), 1098);
    assert_eq!(base_names(&list), big_listing_names(1100));
}
