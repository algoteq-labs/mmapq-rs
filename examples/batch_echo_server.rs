//! Batch echo server — listens on a request batch queue and echoes each batch
//! onto a response batch queue. Supports `--sync` and `--mixed` modes.
//!
//! Usage:
//!   batch_echo_server <directory> <request_queue> <response_queue>
//!                     [cpu_core] [mapper_cpu] [--sync] [--mixed]

use std::cell::Cell;

use mmapq::{
    batch::{IndexedBatchAppender, IndexedBatchQueue},
    ffi, NextMove, QueueConfig, Runtime,
};

const SHUTDOWN_SEQ: u32 = 0;

fn pin_thread(cpu: i32) {
    if cpu < 0 {
        return;
    }
    let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_SET(cpu as usize, &mut cpuset) };
    let ret = unsafe {
        libc::pthread_setaffinity_np(
            libc::pthread_self(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &cpuset as *const libc::cpu_set_t,
        )
    };
    if ret != 0 {
        eprintln!("Warning: failed to pin to core {} (errno: {})", cpu, ret);
    }
}

fn make_config(
    dir: &str,
    name: &str,
    write_rt: Option<&Runtime>,
    read_rt: Option<&Runtime>,
) -> QueueConfig {
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
    let cpu_core: i32 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(-1);
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

    println!("Batch Echo Server started...");
    println!("  Queues: {} -> {} ({})", req_name, res_name, mode_str);
    if cpu_core >= 0 {
        println!("  Core:   {}", cpu_core);
    }
    if !use_sync && mapper_cpu >= 0 {
        println!("  Mapper: {}", mapper_cpu);
    }

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

    pin_thread(cpu_core);

    let q_req = IndexedBatchQueue::create(make_config(dir, req_name, write_rt.as_ref(), read_rt.as_ref()))
        .expect("create request queue");
    let q_res = IndexedBatchQueue::create(make_config(dir, res_name, write_rt.as_ref(), read_rt.as_ref()))
        .expect("create response queue");

    let mut poller = q_req.create_poller().expect("create poller");
    let mut appender: IndexedBatchAppender = q_res.create_appender().expect("create appender");

    let shutdown = Cell::new(false);

    println!("Waiting for requests...");

    while !shutdown.get() {
        let _result = poller
            .poll(|_b_idx, ctx| {
                // Advance to first message, check for shutdown
                if unsafe { ffi::batch_iterator_advance(&mut ctx.batch_iterator) == 0 } {
                    return NextMove::Advance;
                }
                let len = unsafe { ffi::batch_iterator_current_length_ffi(&ctx.batch_iterator) };
                let data =
                    unsafe { std::slice::from_raw_parts(ffi::batch_iterator_current_data_ffi(&ctx.batch_iterator) as *const u8, len as usize) };

                if data.len() >= 6 {
                    let seq = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);
                    if seq == SHUTDOWN_SEQ {
                        shutdown.set(true);
                    }
                }

                // Echo: append msg 0 manually, then append_remaining_from for msg 1..N
                let mut writer = appender.start_batch().expect("start batch");
                writer.append(data).expect("append msg 0");
                writer.append_remaining_from(&mut ctx.batch_iterator);
                writer.commit().expect("commit");

                NextMove::Advance
            })
            .expect("poll failed");
    }

    println!("Shutdown requested. Closing...");
    drop(appender);
    drop(poller);
    drop(q_req);
    drop(q_res);
}
