use std::path::PathBuf;

use mmap_c_rs::{
    transfer::QueueTransfer, IndexedQueue, NextMove, PollResult, QueueConfig,
};

fn create_queue(basename: &str) -> (IndexedQueue, PathBuf) {
    let dir = std::env::temp_dir().join("mmap-rs-transfer-test").join(basename);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let q = IndexedQueue::create(QueueConfig::new(dir.to_str().unwrap(), "q")).unwrap();
    (q, dir)
}

#[test]
fn test_export_import_roundtrip() {
    let (q_src, src_dir) = create_queue("export_roundtrip");

    // Append 100 messages
    let mut appender = q_src.create_appender().unwrap();
    for i in 0..100 {
        let msg = format!("transfer-msg-{}", i);
        appender.append(msg.as_bytes()).unwrap();
    }
    let last_idx = appender.current_index();
    drop(appender);

    // Create a reader for export
    let reader = q_src.create_reader().unwrap();

    // Export to file
    let export_path = src_dir.join("export.dat");
    let count = QueueTransfer::export(
        &reader,
        export_path.to_str().unwrap(),
        0,
        last_idx,
    )
    .expect("export should succeed");
    assert_eq!(count, 100, "should export 100 messages");

    drop(reader);
    drop(q_src);

    // Delete the original queue — we will re-import
    std::fs::remove_dir_all(src_dir.join("q")).ok();

    // Import into a NEW queue directory
    let import_dir = std::env::temp_dir()
        .join("mmap-rs-transfer-test")
        .join("import_dir");
    let _ = std::fs::remove_dir_all(&import_dir);
    std::fs::create_dir_all(&import_dir).unwrap();

    let import_count = QueueTransfer::import(
        export_path.to_str().unwrap(),
        import_dir.to_str().unwrap(),
        "restored",
        0,
    )
    .expect("import should succeed");
    assert_eq!(import_count, 100, "should import 100 messages");

    // Verify the imported queue has all messages
    let q_restored =
        IndexedQueue::create(QueueConfig::new(import_dir.to_str().unwrap(), "restored")).unwrap();
    let mut poller = q_restored.create_poller().unwrap();

    let mut received = 0;
    for _ in 0..100 {
        loop {
            let result = poller
                .poll(|_idx, _aid, data| {
                    let s = String::from_utf8_lossy(data);
                    assert!(
                        s.starts_with("transfer-msg-"),
                        "unexpected message: {:?}",
                        s
                    );
                    received += 1;
                    NextMove::Advance
                })
                .expect("poll");
            if result == PollResult::Advanced {
                break;
            }
            if result == PollResult::EndOfStream {
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
        }
    }
    assert_eq!(received, 100, "should verify all 100 messages");

    drop(poller);
    drop(q_restored);
}
