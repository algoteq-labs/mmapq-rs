use mmapq::{IndexedQueue, QueueConfig};
use mmapq::transfer::{QueueTransfer, StartIndexMode, ConflictBehavior};

#[test]
fn mapper_panic_is_resumed_after_c_cleanup() {
    if std::env::var_os("MMAPQ_MAPPER_PANIC_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "mapper_panic_is_resumed_after_c_cleanup", "--nocapture"])
            .env("MMAPQ_MAPPER_PANIC_CHILD", "1").output().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        return;
    }
    let directory = std::env::temp_dir().join(format!("mmapq-mapper-panic-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let queue = IndexedQueue::create(QueueConfig::new(directory.to_str().unwrap(), "source")).unwrap();
    let mut appender = queue.create_appender().unwrap();
    assert_eq!(appender.append(b"message").unwrap(), 0);
    let mut reader = queue.create_reader().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        QueueTransfer::copy(&mut reader, directory.to_str().unwrap(), "destination",
            0, 0, StartIndexMode::Rebase, false,
            Some(Box::new(|_| panic!("mapper sentinel"))), ConflictBehavior::Auto)
    }));
    let payload = result.expect_err("mapper panic must be resumed");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"mapper sentinel"));
    let destination = IndexedQueue::create(QueueConfig::new(directory.to_str().unwrap(), "destination")).unwrap();
    let mut destination_reader = destination.create_reader().unwrap();
    assert!(matches!(destination_reader.read(0).unwrap(), mmapq::ReadResult::EndOfStream));
    let mut destination_appender = destination.create_appender_with_id(0).unwrap();
    assert_eq!(destination_appender.append(b"after failure").unwrap(), 0);
    drop(destination_appender);
    drop(destination_reader);
    drop(destination);
    drop(reader);
    drop(appender);
    drop(queue);
    std::fs::remove_dir_all(directory).unwrap();
}
