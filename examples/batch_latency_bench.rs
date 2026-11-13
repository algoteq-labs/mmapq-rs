//! Batch round-trip latency benchmark — measures end-to-end latency through
//! batch queues using an echo server (internal or external).
//!
//! Usage:
//!   batch_latency_bench <iterations> [warmup] [msg_size]
//!                       [client_cpu] [server_cpu] [mapper_cpu]
//!                       [--external] [--sync] [--mixed] [--dir <path>]

use std::sync::atomic::{AtomicBool, Ordering};

use mmapq::batch::{IndexedBatchAppender, IndexedBatchPoller, IndexedBatchQueue};
use mmapq::{NextMove, PollResult, QueueConfig, Runtime};

fn pin_thread(cpu: i32) {
    if cpu < 0 {
        return;
    }
    // SAFETY: On supported Linux targets cpu_set_t is an integer bitset; all-zero
    // bytes represent a valid empty set.
    let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: cpuset is an initialized, exclusively borrowed bitset. This libc
    // implementation uses bounds-checked array indexing for the supplied CPU.
    unsafe { libc::CPU_SET(cpu as usize, &mut cpuset) };
    // SAFETY: pthread_self supplies the live calling thread; cpuset is initialized
    // and readable for its exact size during this synchronous affinity call.
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

fn monotonic_nanos() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is an initialized, aligned, writable timespec; clock_gettime
    // writes only that object and does not retain its address.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts); }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

const SHUTDOWN_SEQ: u32 = 0;

const LATENCY_HEADER_SIZE: usize = 6;

fn write_latency_header(buffer: &mut [u8], instance_id: u16, sequence: u32) {
    let buffer = &mut buffer[..LATENCY_HEADER_SIZE];
    buffer[..2].copy_from_slice(&instance_id.to_le_bytes());
    buffer[2..LATENCY_HEADER_SIZE].copy_from_slice(&sequence.to_le_bytes());
}

struct BatchLatencyProbe {
    msg_buffer: Vec<u8>,
    instance_id: u16,
    current_seq: u32,
    _request_queue: IndexedBatchQueue,
    _response_queue: IndexedBatchQueue,
    request_appender: IndexedBatchAppender,
    response_poller: IndexedBatchPoller,
    response_received: AtomicBool,
}

impl BatchLatencyProbe {
    fn create(instance_id: u16, msg_size: usize, dir: &str, write_rt: Option<&Runtime>, read_rt: Option<&Runtime>) -> Self {
        assert!(msg_size >= LATENCY_HEADER_SIZE, "msg_size must be at least 6 bytes");
        let req_cfg = QueueConfig::new(dir, "request");
        let req_cfg = match write_rt { Some(rt) => req_cfg.write_runtime(rt), None => req_cfg };
        let req_cfg = match read_rt { Some(rt) => req_cfg.read_runtime(rt), None => req_cfg };
        let res_cfg = QueueConfig::new(dir, "response");
        let res_cfg = match write_rt { Some(rt) => res_cfg.write_runtime(rt), None => res_cfg };
        let res_cfg = match read_rt { Some(rt) => res_cfg.read_runtime(rt), None => res_cfg };

        let q_req = IndexedBatchQueue::create(req_cfg).expect("create request queue");
        let q_res = IndexedBatchQueue::create(res_cfg).expect("create response queue");
        let appender = q_req.create_appender().expect("create appender");
        let mut poller = q_res.create_poller().expect("create poller");
        poller.move_to_end();

        Self {
            msg_buffer: vec![0u8; msg_size],
            instance_id,
            current_seq: 0,
            _request_queue: q_req,
            _response_queue: q_res,
            request_appender: appender,
            response_poller: poller,
            response_received: AtomicBool::new(false),
        }
    }

    fn execute(&mut self) -> u64 {
        self.response_received.store(false, Ordering::Relaxed);
        self.current_seq += 1;

        write_latency_header(&mut self.msg_buffer, self.instance_id, self.current_seq);

        let start = monotonic_nanos();

        let mut batch = self.request_appender.start_batch().expect("start batch");
        batch.append(&self.msg_buffer).expect("batch append");
        batch.commit().expect("batch commit");

        let poller = &mut self.response_poller;
        let received = &self.response_received;
        let my_id = self.instance_id;
        let my_seq = self.current_seq;

        while !received.load(Ordering::Relaxed) {
            let _ = poller.poll(|_b_idx, mut iter| {
                while let Some((data, _aid)) = iter.next().expect("batch iteration") {
                    if data.len() >= 6 {
                        let rid = u16::from_le_bytes([data[0], data[1]]);
                        let rseq = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
                        if rid == my_id && rseq == my_seq {
                            received.store(true, Ordering::Relaxed);
                        }
                    }
                }
                NextMove::Advance
            });
        }

        monotonic_nanos() - start
    }
}

struct LatencyStats {
    mean: f64,
    min: u64,
    max: u64,
    p50: u64,
    p90: u64,
    p95: u64,
    p99: u64,
    p999: u64,
    p9999: u64,
    p99999: u64,
    throughput: f64,
}

fn compute_stats(results: &[u64], elapsed_ns: u64) -> LatencyStats {
    let total: u64 = results.iter().sum();
    let mean = total as f64 / results.len() as f64;
    let mut sorted = results.to_vec();
    sorted.sort_unstable();
    let len = sorted.len();
    LatencyStats {
        mean,
        min:   sorted[0],
        max:   sorted[len - 1],
        p50:   sorted[(len as f64 * 0.50)   as usize],
        p90:   sorted[(len as f64 * 0.90)   as usize],
        p95:   sorted[(len as f64 * 0.95)   as usize],
        p99:   sorted[(len as f64 * 0.99)   as usize],
        p999:  sorted[(len as f64 * 0.999)  as usize],
        p9999: sorted[(len as f64 * 0.9999) as usize],
        p99999:sorted[(len as f64 * 0.99999)as usize],
        throughput: len as f64 / (elapsed_ns as f64 / 1_000_000_000.0),
    }
}


fn print_oneline(s: &LatencyStats, n: usize) {
    println!("BENCH: iterations={} mean={:.1} min={} p50={} p90={} p99={} p99.9={} p99.99={} p99.999={}",
        n, s.mean, s.min, s.p50, s.p90, s.p99, s.p999, s.p9999, s.p99999);
}

fn print_multiline(s: &LatencyStats, n: usize) {
    println!("\nBatch Round-Trip Latency Statistics (nanoseconds):");
    println!("------------------------------------------");
    println!("Iterations:   {:>12}", n);
    println!("Throughput: {:>12.2} ops/sec", s.throughput);
    println!("Average:      {:>12.2}", s.mean);
    println!("Min (p0):     {:>12}", s.min);
    println!("Max (p100):   {:>12}", s.min.max(s.max));
    println!("------------------------------------------");
    println!("P50:          {:>12}", s.p50);
    println!("P90:          {:>12}", s.p90);
    println!("P95:          {:>12}", s.p95);
    println!("P99:          {:>12}", s.p99);
    println!("P99.9:        {:>12}", s.p999);
    println!("P99.99:       {:>12}", s.p9999);
    println!("P99.999:      {:>12}", s.p99999);
    println!("------------------------------------------");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <iterations> <warmup> <msg_size> [client_cpu] [server_cpu] [mapper_cpu] [options]", args[0]);
        eprintln!("  options: --external  External server mode");
        eprintln!("           --sync     Synchronous mode (no mapper runtime)");
        eprintln!("           --mixed    Mixed mode (async write, sync read)");
        eprintln!("           --oneline  Single-line batch latency summary for scripting");
        eprintln!("           --dir <d>  Queue base directory (default: /tmp)");
        std::process::exit(1);
    }

    let mut pos = Vec::new();
    let mut i = 1;
    let mut external_server = false;
    let mut use_sync = false;
    let mut use_mixed = false;
    let mut oneline_mode = false;
    let mut dir = "/tmp".to_string();

    while i < args.len() {
        match args[i].as_str() {
            "--external" | "-e" => { external_server = true; i += 1; }
            "--sync" | "-s" => { use_sync = true; i += 1; }
            "--mixed" | "-m" => { use_mixed = true; i += 1; }
            "--oneline" | "-1" => { oneline_mode = true; i += 1; }
            "--dir" | "-d" => { if i + 1 < args.len() { dir = args[i + 1].clone(); i += 2; } else { i += 1; } }
            _ => { pos.push(args[i].clone()); i += 1; }
        }
    }

    let iterations: i32 = pos.get(0).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let warmup: i32 = pos.get(1).and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let msg_size: usize = pos.get(2).and_then(|s| s.parse().ok()).unwrap_or(64);
    if msg_size < LATENCY_HEADER_SIZE {
        eprintln!("msg_size must be at least 6 bytes (got {msg_size})");
        std::process::exit(1);
    }
    let client_cpu: i32 = pos.get(3).and_then(|s| s.parse().ok()).unwrap_or(-1);
    let server_cpu: i32 = pos.get(4).and_then(|s| s.parse().ok()).unwrap_or(-1);
    let mapper_cpu: i32 = pos.get(5).and_then(|s| s.parse().ok()).unwrap_or(-1);

    let mode_str = if use_sync {
        "SYNCHRONOUS"
    } else if use_mixed {
        "MIXED (Async-Write, Sync-Read)"
    } else {
        "ASYNCHRONOUS"
    };

    println!("Starting Batch Round-Trip Latency Benchmark...");
    println!("  Config: Iterations={}, Warmup={}, MsgSize={}", iterations, warmup, msg_size);
    println!("  Mode:   {} ({})", if external_server { "EXTERNAL SERVER" } else { "INTERNAL SERVER" }, mode_str);

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

    if !external_server {
        let s_dir = dir.clone();
        let s_wrt = write_rt.clone();
        let s_rdt = read_rt.clone();
        std::thread::spawn(move || {
            pin_thread(server_cpu);
            internal_server(&s_dir, s_wrt, s_rdt);
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        println!("  Internal Batch Server started (core {})",
            if server_cpu >= 0 { format!("{}", server_cpu) } else { "any".into() });
    }

    let mut probe = BatchLatencyProbe::create(1, msg_size, &dir, write_rt.as_ref(), read_rt.as_ref());

    pin_thread(client_cpu);

    println!("Warming up...");
    for _ in 0..warmup {
        probe.execute();
    }

    println!("Measuring...");
    let mut results = vec![0u64; iterations as usize];

    let start_bench = monotonic_nanos();
    for i in 0..iterations {
        results[i as usize] = probe.execute();
    }
    let end_bench = monotonic_nanos();

    let s = compute_stats(&results, end_bench - start_bench);
    if oneline_mode {
        print_oneline(&s, results.len());
    } else {
        print_multiline(&s, results.len());
    }

    send_shutdown(&mut probe);
}

fn send_shutdown(probe: &mut BatchLatencyProbe) {
    probe.response_received.store(false, Ordering::Relaxed);
    let saved_seq = probe.current_seq;
    probe.current_seq = SHUTDOWN_SEQ;

    write_latency_header(&mut probe.msg_buffer, probe.instance_id, SHUTDOWN_SEQ);

    let mut batch = probe.request_appender.start_batch().expect("shutdown start batch");
    batch.append(&probe.msg_buffer).expect("shutdown append");
    batch.commit().expect("shutdown commit");

    let timeout_at = monotonic_nanos() + 2_000_000_000;
    let poller = &mut probe.response_poller;
    let received = &probe.response_received;
    while !received.load(Ordering::Relaxed) && monotonic_nanos() < timeout_at {
        let _ = poller.poll(|_b_idx, mut iter| {
            while let Some((data, _aid)) = iter.next().expect("batch iteration") {
                if data.len() >= 6 {
                    let rseq = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
                    if rseq == SHUTDOWN_SEQ {
                        received.store(true, Ordering::Relaxed);
                    }
                }
            }
            NextMove::Advance
        });
    }

    probe.current_seq = saved_seq;
}

/// Internal batch echo server thread.
fn internal_server(dir: &str, write_rt: Option<Runtime>, read_rt: Option<Runtime>) {
    let req_cfg = QueueConfig::new(dir, "request");
    let req_cfg = match write_rt.as_ref() { Some(rt) => req_cfg.write_runtime(rt), None => req_cfg };
    let req_cfg = match read_rt.as_ref() { Some(rt) => req_cfg.read_runtime(rt), None => req_cfg };
    let res_cfg = QueueConfig::new(dir, "response");
    let res_cfg = match write_rt.as_ref() { Some(rt) => res_cfg.write_runtime(rt), None => res_cfg };
    let res_cfg = match read_rt.as_ref() { Some(rt) => res_cfg.read_runtime(rt), None => res_cfg };

    let q_req = IndexedBatchQueue::create(req_cfg).expect("server: create request queue");
    let q_res = IndexedBatchQueue::create(res_cfg).expect("server: create response queue");

    let mut poller = q_req.create_poller().expect("server: create poller");
    let mut appender = q_res.create_appender().expect("server: create appender");

    let shutdown = std::cell::Cell::new(false);
    loop {
        let result = poller
            .poll(|_b_idx, mut iter| {
                if let Some(data) = iter.next().expect("batch iteration").map(|(d, _a)| d) {
                    if data.len() >= 6 {
                        let seq = u32::from_le_bytes([
                            data[2], data[3], data[4], data[5],
                        ]);
                        if seq == SHUTDOWN_SEQ {
                            shutdown.set(true);
                        }
                    }

                    let mut writer = appender.start_batch().expect("server: start batch");
                    writer.append(data).expect("server: append msg 0");
                    writer.append_remaining_from_iter(&mut iter).expect("server: zero-copy echo");
                    writer.commit().expect("server: commit");
                }
                NextMove::Advance
            });

        if shutdown.get() {
            break;
        }

        match result {
            Ok(PollResult::EndOfStream) => {
                core::hint::spin_loop();
            }
            _ => {}
        }
    }

    drop(appender);
    drop(poller);
    drop(q_req);
    drop(q_res);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_request_and_shutdown_without_changing_payload() {
        for size in [6, 64] {
            let mut buffer = vec![0xa5; size];
            write_latency_header(&mut buffer, 0x1234, 0x56789abc);
            assert_eq!(&buffer[..6], &[0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc]);
            assert!(buffer[6..].iter().all(|&byte| byte == 0xa5));
            write_latency_header(&mut buffer, 0x1234, SHUTDOWN_SEQ);
            assert_eq!(&buffer[..6], &[0x12, 0x34, 0, 0, 0, 0]);
            assert!(buffer[6..].iter().all(|&byte| byte == 0xa5));
        }
    }

    #[test]
    fn rejects_short_message_before_queue_creation() {
        for size in 0..6 {
            let dir = std::env::temp_dir().join(format!(
                "mmapq-BatchLatencyProbe-short-{}-{size}", std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            // Construction alone reproduces missing validation without executing
            // the out-of-bounds header writes in the unfixed implementation.
            let result = std::panic::catch_unwind(|| {
                BatchLatencyProbe::create(1, size, dir.to_str().unwrap(), None, None)
            });
            let empty = std::fs::read_dir(&dir).unwrap().next().is_none();
            std::fs::remove_dir_all(&dir).unwrap();
            assert!(result.is_err(), "size {size} was accepted");
            let panic = result.err().unwrap();
            let message = panic.downcast_ref::<String>().map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("");
            assert!(message.contains("msg_size must be at least 6 bytes"), "{message}");
            assert!(empty, "invalid size created queue files");
        }
    }
}
