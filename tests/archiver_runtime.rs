//! Archivers must retain custom read runtimes through lazy use and C teardown.

use mmapq::batch::IndexedBatchQueue;
use mmapq::{IndexedQueue, QueueConfig, Runtime, RuntimeConfig};
use std::process::Command;
use std::time::{Duration, Instant};

// A dangling runtime can abort or hang inside C. Isolate each scenario and
// bound its execution so a regression cannot take down or stall the suite.
fn isolated(name: &str, run: impl FnOnce()) {
    if std::env::var("MMAPQ_ARCHIVER_RUNTIME_CHILD").as_deref() == Ok(name) {
        run();
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("MMAPQ_ARCHIVER_RUNTIME_CHILD", name)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "{name}: child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("{name}: archiver lifetime scenario timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

macro_rules! lifetime_test {
    ($name:ident, $queue:ty, $append:ident, $initialized:expr, $use_after:expr) => {
        #[test]
        fn $name() {
            isolated(stringify!($name), || {
                for asynchronous in [false, true] {
                    let dir = std::env::temp_dir().join(format!(
                        "mmapq-{}-{}-{asynchronous}", stringify!($name), std::process::id()
                    ));
                    let _ = std::fs::remove_dir_all(&dir);
                    std::fs::create_dir_all(&dir).unwrap();
                    let runtime = asynchronous.then(|| {
                        Runtime::new(RuntimeConfig::new().name(stringify!($name))).unwrap()
                    });
                    // Writes remain synchronous: no write-runtime owner can hide
                    // a failure to retain the configured read runtime.
                    let mut config = QueueConfig::new(dir.to_str().unwrap(), "q").archivable(true);
                    if let Some(rt) = &runtime {
                        config = config.read_runtime(rt);
                    }
                    let queue = <$queue>::create(config).unwrap();
                    let mut appender = queue.create_appender().unwrap();
                    assert_eq!(appender.$append(b"first").unwrap(), 0);
                    assert_eq!(appender.$append(b"second").unwrap(), 1);
                    drop(appender);
                    let mut archiver = queue.create_archiver().unwrap();
                    if $initialized {
                        archiver.archive_up_to_index(1).unwrap();
                    }
                    drop(queue);
                    drop(runtime);
                    if $use_after {
                        archiver.archive_up_to_index(2).unwrap();
                        assert!(archiver.archivable_files().unwrap().is_empty());
                    }
                    drop(archiver);
                    if $use_after {
                        let queue = <$queue>::create(QueueConfig::new(dir.to_str().unwrap(), "q")).unwrap();
                        let mut reader = queue.create_reader().unwrap();
                        assert!(matches!(reader.read(0).unwrap(), mmapq::ReadResult::Archived));
                        assert!(matches!(reader.read(1).unwrap(), mmapq::ReadResult::Archived));
                    }
                    std::fs::remove_dir_all(dir).unwrap();
                }
            });
        }
    };
}

lifetime_test!(normal_lazy, IndexedQueue, append, false, true);
lifetime_test!(normal_initialized, IndexedQueue, append, true, true);
lifetime_test!(normal_unused, IndexedQueue, append, false, false);
lifetime_test!(batch_lazy, IndexedBatchQueue, append_and_commit, false, true);
lifetime_test!(batch_initialized, IndexedBatchQueue, append_and_commit, true, true);
lifetime_test!(batch_unused, IndexedBatchQueue, append_and_commit, false, false);
