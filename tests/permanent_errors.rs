//! 0.2.1 PERMANENT-NOT-TRANSIENT: a permanent failure is never
//! `MapTimedOut` — a missing base directory is `FileNotFound`, a base path
//! that runs through a regular file is `Eio`.

use mmapq::batch::IndexedBatchQueue;
use mmapq::error::MmapError;
use mmapq::queue::QueueConfig;
use mmapq::runtime::Runtime;
use mmapq::IndexedQueue;

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("mmap-rs-permanent").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn config(base: &std::path::Path, rt: &Runtime) -> QueueConfig {
    QueueConfig::new(base.to_str().unwrap(), "q")
        .write_runtime(rt)
        .read_runtime(rt)
}

#[test]
fn missing_base_dir_is_file_not_found() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("missing").join("absent");
    let q = IndexedQueue::create(config(&base, &rt)).unwrap();
    assert!(matches!(q.create_appender(), Err(MmapError::FileNotFound)));
    let bq = IndexedBatchQueue::create(config(&base, &rt)).unwrap();
    assert!(matches!(bq.create_appender(), Err(MmapError::FileNotFound)));
    assert!(!base.exists());
}

#[test]
fn path_through_a_file_is_eio() {
    let rt = Runtime::region_mapper().unwrap();
    let base = scratch("file").join("file");
    std::fs::write(&base, b"").unwrap();
    let q = IndexedQueue::create(config(&base, &rt)).unwrap();
    assert!(matches!(q.create_appender(), Err(MmapError::Eio)));
}
