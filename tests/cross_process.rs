use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use mmapq::{IndexedQueue, NextMove, PollResult, QueueConfig};

/// Find or build the C echo_server binary.
fn c_echo_server_path() -> PathBuf {
    let c_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("c");

    // Check if already built
    let candidate = c_dir.join("echo_server");
    if candidate.exists() {
        return candidate;
    }

    // Build it via make
    let status = Command::new("make")
        .args(["echo_server"])
        .current_dir(&c_dir)
        .status()
        .expect("failed to run make for echo_server");
    assert!(status.success(), "make echo_server failed");
    candidate
}

/// Start the C echo_server subprocess.
fn start_c_echo_server(dir: &str, req: &str, res: &str, sync: bool) -> Child {
    let server_path = c_echo_server_path();
    let mut cmd = Command::new(&server_path);
    cmd.args([dir, req, res, "-1", "-1"]); // cpu=-1, mapper_cpu=-1
    if sync {
        cmd.arg("--sync");
    }
    let child = cmd.spawn().expect("failed to spawn echo_server");
    // Give it time to start
    std::thread::sleep(Duration::from_millis(500));
    child
}

#[test]
fn test_rust_client_c_echo_server() {
    let dir = std::env::temp_dir()
        .join("mmap-rs-cross-test")
        .join("c_echo");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");
    let dir_str = dir.to_str().unwrap();

    let mut server = start_c_echo_server(dir_str, "req", "res", true);

    let req_cfg = QueueConfig::new(dir_str, "req");
    let res_cfg = QueueConfig::new(dir_str, "res");

    let q_req = IndexedQueue::create(req_cfg).expect("create req queue");
    let q_res = IndexedQueue::create(res_cfg).expect("create res queue");

    let mut appender = q_req.create_appender().expect("create appender");
    let mut poller = q_res.create_poller().expect("create poller");
    poller.move_to_end();

    // Send 100 messages
    for i in 0..100 {
        let msg = format!("msg-{}", i);
        appender.append(msg.as_bytes()).expect("append");
    }

    // Receive all echoes
    let mut received = 0;
    for _ in 0..100 {
        loop {
            let result = poller
                .poll(|_idx, _aid, data| {
                    let s = String::from_utf8_lossy(data);
                    assert!(
                        s.starts_with("msg-"),
                        "unexpected response: {:?}",
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
                std::thread::sleep(Duration::from_micros(100));
            }
        }
    }

    assert_eq!(received, 100, "should have received 100 echo responses");

    // Send shutdown
    appender.append(&[0u8; 6]).expect("shutdown append");

    drop(appender);
    drop(poller);
    drop(q_req);
    drop(q_res);

    // Wait briefly then kill
    std::thread::sleep(Duration::from_millis(200));
    let _ = server.kill();
}

#[test]
fn test_rust_client_c_batch_echo_server() {
    let dir = std::env::temp_dir()
        .join("mmap-rs-cross-test")
        .join("c_batch_echo");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");
    let dir_str = dir.to_str().unwrap();

    // Build batch_echo_server (always rebuild to avoid stale binaries)
    let c_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("c");
    let batch_server = c_dir.join("batch_echo_server");
    let status = Command::new("make")
        .args(["batch_echo_server"])
        .current_dir(&c_dir)
        .status()
        .expect("make batch_echo_server failed");
    assert!(status.success(), "make batch_echo_server failed");

    let mut server = Command::new(&batch_server)
        .args([dir_str, "breq", "bres", "-1", "-1", "--sync"])
        .spawn()
        .expect("spawn batch_echo_server");
    std::thread::sleep(Duration::from_millis(500));

    // Use batch queue
    let req = QueueConfig::new(dir_str, "breq");
    let res = QueueConfig::new(dir_str, "bres");
    let q_req =
        mmapq::batch::IndexedBatchQueue::create(req).expect("create batch req queue");
    let q_res =
        mmapq::batch::IndexedBatchQueue::create(res).expect("create batch res queue");

    let mut appender = q_req.create_appender().expect("create appender");
    let mut poller = q_res.create_poller().expect("create poller");
    poller.move_to_end();

    // Send 50 batch messages
    for i in 0..50 {
        let msg = format!("batch-{}", i);
        let mut batch = appender.start_batch().expect("start batch");
        batch.append(msg.as_bytes()).expect("append");
        batch.commit().expect("commit");
    }

    // Receive echoes
    let mut received = 0;
    for _ in 0..50 {
        loop {
            let result = poller
                .poll(|_b_idx, ctx| {
                    if unsafe { mmapq::ffi::batch_iterator_advance(&mut ctx.batch_iterator) == 0 } {
                        return NextMove::Advance;
                    }
                    let len = unsafe { mmapq::ffi::batch_iterator_current_length_ffi(&ctx.batch_iterator) };
                    let data = unsafe {
                        std::slice::from_raw_parts(
                            mmapq::ffi::batch_iterator_current_data_ffi(&ctx.batch_iterator) as *const u8,
                            len as usize,
                        )
                    };
                    let s = String::from_utf8_lossy(data);
                    assert!(
                        s.starts_with("batch-"),
                        "unexpected response: {:?}",
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
                std::thread::sleep(Duration::from_micros(100));
            }
        }
    }

    assert_eq!(received, 50, "should have received 50 batch echo responses");

    // Send shutdown
    let mut batch = appender.start_batch().expect("start batch");
    batch.append(&[0u8; 6]).expect("shutdown append");
    batch.commit().expect("commit");

    drop(appender);
    drop(poller);
    drop(q_req);
    drop(q_res);

    std::thread::sleep(Duration::from_millis(200));
    let _ = server.kill();
}
