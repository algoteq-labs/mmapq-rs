//! Tests for the archivable creation parameter.
//!
//! Verifies that creating a queue with `archivable(true)` sets byte 24
//! in the meta file to 1, and that the default (false) sets it to 0.

use std::io::{Read, Seek, SeekFrom};
use mmapq::{IndexedQueue, QueueConfig, Runtime, RuntimeConfig};

fn temp_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("mmap-arch-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_string()
}

fn read_byte24(dir: &str) -> u8 {
    let path = std::path::PathBuf::from(dir).join("q").join("meta");
    let mut f = std::fs::File::open(&path).unwrap();
    f.seek(SeekFrom::Start(24)).unwrap();
    let mut byte = [0u8; 1];
    f.read_exact(&mut byte).unwrap();
    byte[0]
}

// covers: QueueConfig::archivable
// covers: IndexedQueue::create
#[test]
fn archivable_true_sets_byte24() {
    let dir = temp_dir("true");
    let rt = Runtime::new(RuntimeConfig::new().name("arch-t")).unwrap();
    let q = IndexedQueue::create(
        QueueConfig::new(&dir, "q")
            .write_runtime(&rt)
            .read_runtime(&rt)
            .archivable(true),
    )
    .unwrap();
    let mut a = q.create_appender().unwrap();
    a.append(b"x").unwrap();
    drop(a);
    drop(q);
    assert_eq!(read_byte24(&dir), 1);
    drop(rt);
}

#[test]
fn archivable_default_is_false() {
    let dir = temp_dir("false");
    let rt = Runtime::new(RuntimeConfig::new().name("arch-f")).unwrap();
    let q = IndexedQueue::create(
        QueueConfig::new(&dir, "q")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    let mut a = q.create_appender().unwrap();
    a.append(b"x").unwrap();
    drop(a);
    drop(q);
    assert_eq!(read_byte24(&dir), 0);
    drop(rt);
}

fn single(dir: &str, rt: &Runtime, archivable: bool) -> IndexedQueue {
    IndexedQueue::create(
        QueueConfig::new(dir, "q").write_runtime(rt).read_runtime(rt).archivable(archivable),
    )
    .unwrap()
}

fn batch(dir: &str, rt: &Runtime, archivable: bool) -> mmapq::IndexedBatchQueue {
    mmapq::IndexedBatchQueue::create(
        QueueConfig::new(dir, "q").write_runtime(rt).read_runtime(rt).archivable(archivable),
    )
    .unwrap()
}

// covers: IndexedArchiver::archivable
#[test]
fn archiver_archivable_reports_the_stored_flag() {
    let rt = Runtime::new(RuntimeConfig::new().name("arch-ck")).unwrap();
    for flag in [true, false] {
        let dir = temp_dir(&format!("ck-{flag}"));
        let q = single(&dir, &rt, flag);
        let mut a = q.create_appender().unwrap();
        a.append(b"x").unwrap();
        let arch = q.create_archiver().unwrap();
        assert_eq!(arch.archivable().unwrap(), flag);
        assert_eq!(arch.archivable().unwrap(), flag); // the cached answer is stable
    }
}

// covers: IndexedArchiver::archivable
#[test]
fn archiver_archivable_retryable_until_first_appender() {
    let rt = Runtime::new(RuntimeConfig::new().name("arch-rt")).unwrap();
    let dir = temp_dir("ck-retry");
    let q = single(&dir, &rt, true);
    let arch = q.create_archiver().unwrap();
    assert!(matches!(arch.archivable(), Err(mmapq::MmapError::FileNotFound)));
    let mut a = q.create_appender().unwrap();
    a.append(b"x").unwrap();
    assert_eq!(arch.archivable().unwrap(), true); // same archiver, retried
}

// covers: IndexedBatchArchiver::archivable
#[test]
fn batch_archiver_archivable_reports_the_stored_flag() {
    let rt = Runtime::new(RuntimeConfig::new().name("barch-ck")).unwrap();
    for flag in [true, false] {
        let dir = temp_dir(&format!("bck-{flag}"));
        let q = batch(&dir, &rt, flag);
        let mut a = q.create_appender().unwrap();
        let mut b = a.start_batch().unwrap();
        b.append(b"x").unwrap();
        b.commit().unwrap();
        let arch = q.create_archiver().unwrap();
        assert_eq!(arch.archivable().unwrap(), flag);
    }
}

// covers: IndexedBatchArchiver::archivable
#[test]
fn batch_archiver_archivable_retryable_until_first_appender() {
    let rt = Runtime::new(RuntimeConfig::new().name("barch-rt")).unwrap();
    let dir = temp_dir("bck-retry");
    let q = batch(&dir, &rt, true);
    let arch = q.create_archiver().unwrap();
    assert!(matches!(arch.archivable(), Err(mmapq::MmapError::FileNotFound)));
    let _a = q.create_appender().unwrap();
    assert_eq!(arch.archivable().unwrap(), true);
}
