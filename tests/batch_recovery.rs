//! Regression pin for A6: batch appender recovery must refuse a truncated
//! end-of-stream scan — a corrupt message length inside the last committed
//! batch — instead of silently recovering to a wrong append position
//! (C pins: test_batch_recovery_refuses_* in test_indexed_batch_queue.c).

use mmapq::batch::IndexedBatchQueue;
use mmapq::{MmapError, QueueConfig, Runtime, RuntimeConfig};

fn temp_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("mmap-br-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_string()
}

#[test]
fn batch_recovery_refuses_truncated_scan() {
    let dir = temp_dir("truncated_scan");
    let rt = Runtime::new(RuntimeConfig::new().name("brt")).unwrap();

    let bq = IndexedBatchQueue::create(
        QueueConfig::new(&dir, "q").write_runtime(&rt).read_runtime(&rt),
    )
    .unwrap();
    let mut ba = bq.create_appender().unwrap();
    for b in 0..2i64 {
        let mut w = ba.start_batch().unwrap();
        for i in 0..3i32 {
            assert_eq!(w.append(format!("b{b}-m{i}-padding").as_bytes()).unwrap(), i);
        }
        assert_eq!(w.commit().unwrap(), b);
    }
    drop(ba);

    // Overwrite the length prefix of message 1 of the last committed batch:
    // 64 start-of-stream + 55 first batch (4-byte count + 3*(4+13)) +
    // 4 count + 17 first message (4+13) = offset 140.
    use std::io::{Seek, SeekFrom, Write};
    let payload = format!("{dir}/q/payload_0_0");
    let mut f = std::fs::OpenOptions::new().write(true).open(&payload).unwrap();
    f.seek(SeekFrom::Start(140)).unwrap();
    f.write_all(&(-2147483632i32).to_le_bytes()).unwrap(); // 0x80000010 as int32
    drop(f);

    let res = bq.create_appender();
    assert!(
        matches!(res, Err(MmapError::Corrupt(_))),
        "recovery after a truncated scan must fail with Corrupt"
    );
    drop(bq);
    drop(rt);
}
