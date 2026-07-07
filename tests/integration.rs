use std::path::PathBuf;

use mmapq::{
    IndexedQueue, NextMove, PollResult, QueueConfig, Runtime,
};

fn setup_queue(name: &str) -> (IndexedQueue, Runtime) {
    let rt = Runtime::region_mapper().unwrap();
    let dir = std::env::temp_dir().join("mmap-rs-int").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let q = IndexedQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();
    (q, rt)
}

/// Returns the path to the C echo_server binary.
#[allow(dead_code)]
fn echo_server_path() -> PathBuf {
    // Look relative to the Rust project
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("c");
    p.push("echo_server");
    p
}

#[test]
fn test_single_process_append_poll_1000() {
    let (q, _rt) = setup_queue("int_append_1k");

    // Append 1000 messages
    let mut writer = q.create_appender().unwrap();
    for i in 0..1000 {
        let msg = format!("msg-{}", i);
        let idx = writer.append(msg.as_bytes()).unwrap();
        assert_eq!(idx, i, "expected sequential indices");
    }
    drop(writer);

    // Poll all 1000 messages back
    let mut reader = q.create_poller().unwrap();
    let mut count = 0;
    loop {
        let result = reader
            .poll(|_idx, _aid, data| {
                let s = String::from_utf8_lossy(data);
                assert_eq!(s, format!("msg-{}", count));
                count += 1;
                NextMove::Advance
            })
            .unwrap();
        if result == PollResult::EndOfStream {
            break;
        }
    }
    assert_eq!(count, 1000, "should have received all 1000 messages");
}

#[test]
fn test_single_process_uncommitted_append() {
    let (q, _rt) = setup_queue("int_uncommitted");

    let mut writer = q.create_appender().unwrap();
    let mut offsets = Vec::new();

    for i in 0..10 {
        let msg = format!("uncommitted-{}", i);
        let offset = writer.append_uncommitted(msg.as_bytes()).unwrap();
        offsets.push(offset);
    }

    // Commit all
    let mut indices = Vec::new();
    for &offset in &offsets {
        let idx = writer.commit(offset).unwrap();
        indices.push(idx);
    }
    drop(writer);

    // Read back with reader
    let reader = q.create_reader().unwrap();
    for (i, &expected_idx) in indices.iter().enumerate() {
        let msg = reader.read(expected_idx).unwrap().expect("message should exist");
        assert_eq!(msg.index(), expected_idx);
        let s = String::from_utf8_lossy(msg.data());
        assert_eq!(s, format!("uncommitted-{}", i));
    }
}

#[test]
fn test_single_process_multiple_appenders() {
    let (q, _rt) = setup_queue("int_multi_app");

    // Create two appenders
    let mut a1 = q.create_appender().unwrap();
    let mut a2 = q.create_appender().unwrap();

    a1.append(b"from-a1").unwrap();
    a2.append(b"from-a2").unwrap();
    a1.append(b"from-a1-again").unwrap();
    a2.append(b"from-a2-again").unwrap();
    drop(a1);
    drop(a2);

    // Poll all 4 messages — order is interleaved (determined by CAS)
    let mut reader = q.create_poller().unwrap();
    let mut received: Vec<String> = Vec::new();
    loop {
        let result = reader
            .poll(|_idx, _aid, data| {
                received.push(String::from_utf8_lossy(data).to_string());
                NextMove::Advance
            })
            .unwrap();
        if result == PollResult::EndOfStream {
            break;
        }
    }
    assert_eq!(received.len(), 4);
    assert!(received.contains(&"from-a1".to_string()));
    assert!(received.contains(&"from-a2".to_string()));
    assert!(received.contains(&"from-a1-again".to_string()));
    assert!(received.contains(&"from-a2-again".to_string()));
}

#[test]
fn test_single_process_move_to_index() {
    let (q, _rt) = setup_queue("int_move_to");

    let mut writer = q.create_appender().unwrap();
    writer.append(b"first").unwrap();
    writer.append(b"second").unwrap();
    writer.append(b"third").unwrap();
    drop(writer);

    let mut reader = q.create_poller().unwrap();

    // Skip to index 1 and read it
    assert!(reader.move_to_index(1));
    let result = reader
        .poll(|_idx, _aid, data| {
            assert_eq!(data, b"second");
            NextMove::Advance
        })
        .unwrap();
    assert_eq!(result, PollResult::Advanced);
}

#[test]
fn test_reader_metadata() {
    let (q, _rt) = setup_queue("int_reader_meta");

    let mut writer = q.create_appender().unwrap();
    writer.append(b"a").unwrap();
    writer.append(b"b").unwrap();
    writer.append(b"c").unwrap();
    let last_idx = writer.append(b"d").unwrap();
    drop(writer);

    let reader = q.create_reader().unwrap();

    // Check last_index
    assert_eq!(reader.last_index(), last_idx);

    // Check individual reads
    for i in 0..=last_idx {
        let msg = reader.read(i).unwrap().expect("message should exist");
        assert_eq!(msg.index(), i);
    }

    // Read beyond last — should be None
    let beyond = reader.read(last_idx + 1).unwrap();
    assert!(beyond.is_none(), "read past end should be None");
}

#[test]
fn test_poll_corrupt_payload_raises_error() {
    use std::io::{Write, Seek};
    use mmapq::MmapError;

    let rt = Runtime::region_mapper().unwrap();
    let dir = std::env::temp_dir().join("mmap-rs-corrupt");
    let name = "q";
    let qdir = dir.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&qdir).unwrap();
    let q = IndexedQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), name)
            .write_runtime(&rt)
            .read_runtime(&rt),
    )
    .unwrap();

    let mut writer = q.create_appender().unwrap();
    writer.append(b"hello").unwrap();
    drop(writer);

    // Corrupt the 4-byte length prefix at offset 64 in payload_0_0
    let payload_path = qdir.join("payload_0_0");
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(&payload_path)
        .unwrap();
    f.seek(std::io::SeekFrom::Start(64)).unwrap();
    f.write_all(b"\x00\x00\x00\x00").unwrap();
    drop(f);

    let mut poller = q.create_poller().unwrap();
    let result = poller.poll(|_idx, _aid, _data| NextMove::Advance);
    assert!(result.is_err(), "poll should return Err on corrupt payload");
    match result {
        Err(MmapError::Corrupt(_)) => {} // expected
        Err(other) => panic!("expected Corrupt error, got {:?}", other),
        Ok(_) => panic!("expected Err, got Ok"),
    }
    drop(poller);
    drop(q);
}
