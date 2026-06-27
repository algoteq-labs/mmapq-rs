use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use mmap_c_rs::{IndexedQueue, NextMove, QueueConfig, Runtime};

const SHUTDOWN_SEQ: u32 = 0;

/// Wire format: [instance_id: u16][sequence: u32][payload: bytes]
#[allow(dead_code)]
#[repr(packed)]
struct LatencyHeader {
    instance_id: u16,
    sequence: u32,
}

fn parse_message(data: &[u8]) -> (u16, u32, &[u8]) {
    let instance_id = u16::from_be_bytes([data[0], data[1]]);
    let sequence = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);
    let payload = &data[6..];
    (instance_id, sequence, payload)
}

fn make_config(dir: &str, name: &str, write_rt: Option<&Runtime>, read_rt: Option<&Runtime>) -> QueueConfig {
    let mut cfg = QueueConfig::new(dir, name);
    if let Some(rt) = write_rt {
        cfg = cfg.write_runtime(rt);
    }
    if let Some(rt) = read_rt {
        cfg = cfg.read_runtime(rt);
    }
    cfg
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: {} <directory> <request_queue> <response_queue> [cpu_core] [mapper_cpu] [--sync]",
            args[0]
        );
        std::process::exit(1);
    }

    let dir = &args[1];
    let req_name = &args[2];
    let res_name = &args[3];
    let mapper_cpu: i32 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(-1);

    let use_sync = args.iter().any(|a| a == "--sync" || a == "-s");
    let use_mixed = args.iter().any(|a| a == "--mixed" || a == "-m");

    let mode_str = if use_sync {
        "SYNCHRONOUS"
    } else if use_mixed {
        "MIXED (Sync-Read, Async-Write)"
    } else {
        "ASYNCHRONOUS"
    };

    println!("Echo Server started...");
    println!("  Queues: {} -> {} ({})", req_name, res_name, mode_str);

    let write_rt: Option<Runtime> = if use_sync {
        None
    } else {
        let rt = Runtime::region_mapper().expect("region_mapper");
        if mapper_cpu >= 0 {
            rt.set_affinity(mapper_cpu as u32).ok();
        }
        Some(rt)
    };

    let read_rt: Option<Runtime> = if use_sync {
        None
    } else if use_mixed {
        None
    } else {
        write_rt.clone()
    };

    let q_req = IndexedQueue::create(make_config(dir, req_name, write_rt.as_ref(), read_rt.as_ref()))
        .expect("create request queue");
    let q_res = IndexedQueue::create(make_config(dir, res_name, write_rt.as_ref(), read_rt.as_ref()))
        .expect("create response queue");

    let mut poller = q_req.create_poller().expect("create poller");
    let mut appender = q_res.create_appender().expect("create appender");

    let shutdown = Arc::new(AtomicBool::new(false));

    println!("Waiting for requests...");

    let sd = Arc::clone(&shutdown);
    while !shutdown.load(Ordering::Acquire) {
        let _result = poller
            .poll(|_idx, _aid, data| {
                let (_inst, seq, _payload) = parse_message(data);
                if seq == SHUTDOWN_SEQ {
                    sd.store(true, Ordering::Release);
                }
                // Echo the entire payload back
                appender.append(data).expect("echo append failed");
                NextMove::Advance
            })
            .expect("poll failed");

        // No delay — tight spin matching C echo_server behaviour.
        // poll() returns immediately when there's no message.
    }

    println!("Shutdown requested. Closing...");
    drop(appender);
    drop(poller);
    drop(q_req);
    drop(q_res);
}
