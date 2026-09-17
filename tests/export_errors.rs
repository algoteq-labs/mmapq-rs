use mmapq::{IndexedQueue, QueueConfig, MmapError};
use mmapq::batch::IndexedBatchQueue;
use mmapq::transfer::{BatchQueueTransfer, QueueTransfer, StartIndexMode};

#[test]
fn export_reports_buffered_write_failure() {
    let directory = std::env::temp_dir().join(format!("mmapq-export-errors-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let queue = IndexedQueue::create(QueueConfig::new(directory.to_str().unwrap(), "single")).unwrap();
    let mut appender = queue.create_appender().unwrap();
    let index = appender.append(b"message").unwrap();
    let reader = queue.create_reader().unwrap();
    let result = QueueTransfer::export(&reader, "/dev/full", index, index, StartIndexMode::Rebase);
    assert!(matches!(result, Err(MmapError::Io(_))), "{result:?}");
    drop(reader);
    drop(appender);
    drop(queue);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn batch_export_reports_buffered_write_failure() {
    let directory = std::env::temp_dir().join(format!("mmapq-batch-export-errors-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let queue = IndexedBatchQueue::create(QueueConfig::new(directory.to_str().unwrap(), "batch")).unwrap();
    let mut appender = queue.create_appender().unwrap();
    let index = appender.append_and_commit(b"message").unwrap();
    let reader = queue.create_reader().unwrap();
    let result = BatchQueueTransfer::export(&reader, "/dev/full", index, index, StartIndexMode::Rebase);
    assert!(matches!(result, Err(MmapError::Io(_))), "{result:?}");
    drop(reader);
    drop(appender);
    drop(queue);
    std::fs::remove_dir_all(directory).unwrap();
}
