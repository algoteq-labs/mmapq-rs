use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Barrier};

use mmapq::{IndexedQueue, NextMove, PollResult, QueueConfig, Runtime, RuntimeConfig};

#[test]
fn shared_factory_participants_discover_late_metadata() {
    for asynchronous in [false, true] {
        let directory = std::env::temp_dir().join(format!(
            "mmapq-shared-factory-{}-{asynchronous}", std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let runtime = asynchronous.then(|| Runtime::new(RuntimeConfig::new()).unwrap());
        let mut config = QueueConfig::new(directory.to_str().unwrap(), "queue")
            .region_size(32768).ring_size(16).max_file_size(32768 * 8)
            .start_index(17);
        if let Some(runtime) = &runtime {
            config = config.write_runtime(runtime).read_runtime(runtime);
        }
        let queue = Arc::new(IndexedQueue::create(config).unwrap());
        let ready = Arc::new(Barrier::new(5));
        let published = Arc::new(Barrier::new(5));
        let mut workers = Vec::new();
        for id in 1..=4_u8 {
            let queue = Arc::clone(&queue);
            let ready = Arc::clone(&ready);
            let published = Arc::clone(&published);
            workers.push(std::thread::spawn(move || {
                // A panic before the barriers would leave the main thread and
                // the other workers waiting forever; catch it, reach both
                // barriers, then re-raise it so the test fails instead of hanging.
                let before = catch_unwind(AssertUnwindSafe(|| {
                    let mut reader = queue.create_reader().unwrap();
                    let mut poller = queue.create_poller().unwrap();
                    assert!(matches!(reader.read(17).unwrap(), mmapq::ReadResult::EndOfStream));
                    assert_eq!(poller.poll(|_, _, _| panic!("not published")).unwrap(), PollResult::EndOfStream);
                    (reader, poller)
                }));
                ready.wait();
                published.wait();
                let (mut reader, mut poller) = before.unwrap_or_else(|panic| resume_unwind(panic));
                assert_eq!(reader.start_index(), 17);
                assert_eq!(reader.read(17).unwrap().available().unwrap().data(), b"seed");
                assert_eq!(poller.poll(|index, _, payload| {
                    assert_eq!(index, 17);
                    assert_eq!(payload, b"seed");
                    NextMove::Advance
                }).unwrap(), PollResult::Advanced);
                let mut appender = queue.create_appender_with_id(id).unwrap();
                let index = appender.append(&[id]).unwrap();
                assert_eq!(reader.read(index).unwrap().available().unwrap().data(), &[id]);
                index
            }));
        }
        ready.wait();
        assert!(!directory.join("queue/meta").exists());
        let mut appender = queue.create_appender_with_id(0).unwrap();
        assert_eq!(appender.append(b"seed").unwrap(), 17);
        published.wait();
        let mut indices: Vec<_> = workers.into_iter().map(|worker| worker.join().unwrap()).collect();
        indices.sort_unstable();
        assert_eq!(indices, [18, 19, 20, 21]);
        drop(appender);
        drop(queue);
        drop(runtime);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
