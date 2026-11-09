use mmapq::{IndexedQueue, QueueConfig, MmapError};
use mmapq::batch::IndexedBatchQueue;
use mmapq::transfer::{BatchQueueTransfer, QueueTransfer, StartIndexMode};
use std::os::unix::fs::FileExt;

// covers: IndexedBatchAppender::append_and_commit
#[test]
fn batch_export_preserves_corruption_when_close_also_fails() {
    let directory = std::env::temp_dir().join(format!("mmapq-export-corruption-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let queue = IndexedBatchQueue::create(QueueConfig::new(directory.to_str().unwrap(), "batch")).unwrap();
    let mut appender = queue.create_appender().unwrap();
    let index = appender.append_and_commit(b"message").unwrap();
    let mut reader = queue.create_reader().unwrap();
    let offset = reader.read(index).unwrap().available().unwrap().batch_offset();
    let payload = std::fs::OpenOptions::new().write(true)
        .open(directory.join("batch").join(format!("payload_{}_0", appender.id()))).unwrap();
    payload.write_all_at(&0_i32.to_le_bytes(), offset as u64 + 4).unwrap();
    drop(payload);
    for output in [directory.join("export.mmapq"), "/dev/full".into()] {
        let result = BatchQueueTransfer::export(&mut reader, output.to_str().unwrap(), index, index, StartIndexMode::Rebase);
        assert!(matches!(result, Err(MmapError::Corrupt(_))), "{result:?}");
    }
    drop(reader);
    drop(appender);
    drop(queue);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn export_reports_buffered_write_failure() {
    let directory = std::env::temp_dir().join(format!("mmapq-export-errors-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let queue = IndexedQueue::create(QueueConfig::new(directory.to_str().unwrap(), "single")).unwrap();
    let mut appender = queue.create_appender().unwrap();
    let index = appender.append(b"message").unwrap();
    let mut reader = queue.create_reader().unwrap();
    let result = QueueTransfer::export(&mut reader, "/dev/full", index, index, StartIndexMode::Rebase);
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
    let mut reader = queue.create_reader().unwrap();
    let result = BatchQueueTransfer::export(&mut reader, "/dev/full", index, index, StartIndexMode::Rebase);
    assert!(matches!(result, Err(MmapError::Io(_))), "{result:?}");
    drop(reader);
    drop(appender);
    drop(queue);
    std::fs::remove_dir_all(directory).unwrap();
}
