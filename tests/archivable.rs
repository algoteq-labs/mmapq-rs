//! Tests for the archivable creation parameter.
//!
//! Verifies that creating a queue with `archivable(true)` sets byte 24
//! in the meta file to 1, and that the default (false) sets it to 0.

use std::io::{Read, Seek, SeekFrom};
use mmap_c_rs::{IndexedQueue, QueueConfig, Runtime, RuntimeConfig};

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
