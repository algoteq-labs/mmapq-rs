//! Regression tests for queue_type discriminator (review finding, C meta change).
//!
//! Verifies that opening a queue with the wrong participant kind is refused
//! (e.g., batch poller on a normal queue, or normal poller on a batch queue).

use mmapq::{IndexedQueue, QueueConfig, Runtime, RuntimeConfig};

fn temp_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("mmap-qt-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_string()
}

#[test]
fn normal_queue_rejects_batch_participant() {
    let dir = temp_dir("normal_rejects_batch");
    let rt = Runtime::new(RuntimeConfig::new().name("qt")).unwrap();

    // Create a normal queue
    let q = IndexedQueue::create(
        QueueConfig::new(&dir, "q").write_runtime(&rt).read_runtime(&rt),
    )
    .unwrap();
    let mut a = q.create_appender().unwrap();
    a.append(b"msg").unwrap();
    drop(a);
    drop(q);

    // Try to open the same directory with batch participants — must fail
    let bq = mmapq::batch::IndexedBatchQueue::create(
        QueueConfig::new(&dir, "q").write_runtime(&rt).read_runtime(&rt),
    )
    .unwrap();
    let ba = bq.create_appender();
    assert!(ba.is_err(), "batch appender on normal queue must fail");
    drop(bq);
    drop(rt);
}

#[test]
fn batch_queue_rejects_normal_participant() {
    let dir = temp_dir("batch_rejects_normal");
    let rt = Runtime::new(RuntimeConfig::new().name("qt2")).unwrap();

    // Create a batch queue — batch appender creates meta with QUEUE_TYPE_BATCH
    let bq = mmapq::batch::IndexedBatchQueue::create(
        QueueConfig::new(&dir, "q").write_runtime(&rt).read_runtime(&rt),
    )
    .unwrap();
    let mut ba = bq.create_appender().unwrap();
    {
        let mut w = ba.start_batch().unwrap();
        w.append(b"msg").unwrap();
        w.commit().unwrap();
    }
    drop(ba);
    drop(bq);

    // Try to open the same directory with normal participants — must fail
    let nq = IndexedQueue::create(
        QueueConfig::new(&dir, "q").write_runtime(&rt).read_runtime(&rt),
    )
    .unwrap();
    let na = nq.create_appender();
    assert!(na.is_err(), "normal appender on batch queue must fail");
    drop(nq);
    drop(rt);
}
