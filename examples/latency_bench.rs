use std::sync::atomic::{AtomicBool, Ordering};

use mmapq::{IndexedAppender, IndexedPoller, IndexedQueue, NextMove, PollResult, QueueConfig, Runtime};

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

fn monotonic_nanos() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts); }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

const SHUTDOWN_SEQ: u32 = 0;

#[repr(packed)]
struct LatencyHeader {
    instance_id: u16,
    sequence: u32,
}

struct LatencyProbe {
    msg_buffer: Vec<u8>,
    instance_id: u16,
    current_seq: u32,
    _request_queue: IndexedQueue,
    _response_queue: IndexedQueue,
    request_appender: IndexedAppender,
    response_poller: IndexedPoller,
    response_received: AtomicBool,
}

impl LatencyProbe {
    fn create(instance_id: u16, msg_size: usize, dir: &str, write_rt: Option<&Runtime>, read_rt: Option<&Runtime>) -> Self {
        let req_cfg = QueueConfig::new(dir, "request");
        let req_cfg = match write_rt { Some(rt) => req_cfg.write_runtime(rt), None => req_cfg };
        let req_cfg = match read_rt { Some(rt) => req_cfg.read_runtime(rt), None => req_cfg };
        let res_cfg = QueueConfig::new(dir, "response");
        let res_cfg = match write_rt { Some(rt) => res_cfg.write_runtime(rt), None => res_cfg };
        let res_cfg = match read_rt { Some(rt) => res_cfg.read_runtime(rt), None => res_cfg };

        let q_req = IndexedQueue::create(req_cfg).expect("create request queue");
        let q_res = IndexedQueue::create(res_cfg).expect("create response queue");
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

        let hdr = unsafe { &mut *(self.msg_buffer.as_mut_ptr() as *mut LatencyHeader) };
        hdr.instance_id = self.instance_id.to_be();
        hdr.sequence = self.current_seq.to_be();

        let start = monotonic_nanos();

        self.request_appender.append(&self.msg_buffer).expect("append");

        let poller = &mut self.response_poller;
        let received = &self.response_received;
        let my_id = self.instance_id;
        let my_seq = self.current_seq;

        while !received.load(Ordering::Relaxed) {
            let _ = poller.poll(|_idx, _aid, data| {
                if data.len() >= 6 {
                    let rid = u16::from_be_bytes([data[0], data[1]]);
                    let rseq = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);
                    if rid == my_id && rseq == my_seq {
                        received.store(true, Ordering::Relaxed);
                    }
                }
                NextMove::Advance
            });
        }

        monotonic_nanos() - start
    }

}

fn format_duration(ns: u64) -> String {
    if ns < 1000 {
        format!("{} ns", ns)
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1000.0)
    } else {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    }
}

fn print_stats(results: &[u64], elapsed_ns: u64, label: &str) {
    if results.is_empty() {
        return;
    }
    let mut sorted = results.to_vec();
    sorted.sort_unstable();

    let total: u64 = sorted.iter().sum();
    let mean = total as f64 / sorted.len() as f64;

    let p50 = sorted[(sorted.len() as f64 * 0.50) as usize];
    let p90 = sorted[(sorted.len() as f64 * 0.90) as usize];
    let p99 = sorted[(sorted.len() as f64 * 0.99) as usize];
    let p999 = sorted[(sorted.len() as f64 * 0.999) as usize];
    let p9999 = sorted[(sorted.len() as f64 * 0.9999) as usize];
    let p99999 = sorted[(sorted.len() as f64 * 0.99999) as usize];
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];

    let throughput = (sorted.len() as f64) / (elapsed_ns as f64 / 1_000_000_000.0);

    println!("\n{}", label);
    println!("  {:>6} samples", sorted.len());
    println!("  Throughput: {:.0} ops/sec", throughput);
    println!("  Latency:");
    println!("    Mean:    {}", format_duration(mean as u64));
    println!("    Min:     {}", format_duration(min));
    println!("    P50:     {}", format_duration(p50));
    println!("    P90:     {}", format_duration(p90));
    println!("    P99:     {}", format_duration(p99));
    println!("    P99.9:   {}", format_duration(p999));
    println!("    P99.99:  {}", format_duration(p9999));
    println!("    P99.999: {}", format_duration(p99999));
    println!("    Max:     {}", format_duration(max));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <iterations> <warmup> <msg_size> [client_cpu] [server_cpu] [mapper_cpu] [options]", args[0]);
        eprintln!("  options: --external  External server mode");
        eprintln!("           --sync     Synchronous mode (no mapper runtime)");
        eprintln!("           --mixed    Mixed mode (async write, sync read)");
        eprintln!("           --bench    Machine-readable output for regression tracking");
        eprintln!("           --dir <d>  Queue base directory (default: /tmp)");
        std::process::exit(1);
    }

    let mut pos = Vec::new();
    let mut i = 1;
    let mut external_server = false;
    let mut use_sync = false;
    let mut use_mixed = false;
    let mut bench_mode = false;
    let mut dir = "/tmp".to_string();

    while i < args.len() {
        match args[i].as_str() {
            "--external" | "-e" => { external_server = true; i += 1; }
            "--sync" | "-s" => { use_sync = true; i += 1; }
            "--mixed" | "-m" => { use_mixed = true; i += 1; }
            "--bench" | "-b" => { bench_mode = true; i += 1; }
            "--dir" | "-d" => { if i + 1 < args.len() { dir = args[i + 1].clone(); i += 2; } else { i += 1; } }
            _ => { pos.push(args[i].clone()); i += 1; }
        }
    }

    let iterations: i32 = pos.get(0).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let warmup: i32 = pos.get(1).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let msg_size: usize = pos.get(2).and_then(|s| s.parse().ok()).unwrap_or(64);
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

    println!("Starting Round-Trip Latency Benchmark...");
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
        std::thread::sleep(std::time::Duration::from_millis(100));
        println!("  Internal Server started (core {})", if server_cpu >= 0 { format!("{}", server_cpu) } else { "any".into() });
    }

    let mut probe = LatencyProbe::create(1, msg_size, &dir, write_rt.as_ref(), read_rt.as_ref());

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

    if bench_mode {
        let total: u64 = results.iter().sum();
        let mean = total as f64 / results.len() as f64;
        let mut sorted = results.to_vec();
        sorted.sort_unstable();
        let p50 = sorted[(sorted.len() as f64 * 0.50) as usize];
        let p99 = sorted[(sorted.len() as f64 * 0.99) as usize];
        println!("BENCH: iterations={} mean={:.1} p50={} p99={} min={} max={}",
            results.len(), mean, p50, p99, sorted[0], sorted[sorted.len()-1]);
    } else {
        print_stats(&results, end_bench - start_bench, "Round-Trip Latency Statistics (nanoseconds):");
    }

    send_shutdown(&mut probe);
}

fn send_shutdown(probe: &mut LatencyProbe) {
    probe.response_received.store(false, Ordering::Relaxed);
    let saved_seq = probe.current_seq;
    probe.current_seq = SHUTDOWN_SEQ;

    let hdr = unsafe { &mut *(probe.msg_buffer.as_mut_ptr() as *mut LatencyHeader) };
    hdr.instance_id = probe.instance_id.to_be();
    hdr.sequence = SHUTDOWN_SEQ.to_be();

    probe.request_appender.append(&probe.msg_buffer).expect("shutdown append");

    let timeout_at = monotonic_nanos() + 2_000_000_000;
    let poller = &mut probe.response_poller;
    let received = &probe.response_received;
    while !received.load(Ordering::Relaxed) && monotonic_nanos() < timeout_at {
        let _ = poller.poll(|_idx, _aid, data| {
            if data.len() >= 6 {
                let rseq = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);
                if rseq == SHUTDOWN_SEQ {
                    received.store(true, Ordering::Relaxed);
                }
            }
            NextMove::Advance
        });
    }

    probe.current_seq = saved_seq;
}

/// Internal echo server thread: reads from request queue, echoes to response queue.
fn internal_server(dir: &str, write_rt: Option<Runtime>, read_rt: Option<Runtime>) {
    let req_cfg = QueueConfig::new(dir, "request");
    let req_cfg = match write_rt.as_ref() { Some(rt) => req_cfg.write_runtime(rt), None => req_cfg };
    let req_cfg = match read_rt.as_ref() { Some(rt) => req_cfg.read_runtime(rt), None => req_cfg };
    let res_cfg = QueueConfig::new(dir, "response");
    let res_cfg = match write_rt.as_ref() { Some(rt) => res_cfg.write_runtime(rt), None => res_cfg };
    let res_cfg = match read_rt.as_ref() { Some(rt) => res_cfg.read_runtime(rt), None => res_cfg };

    let q_req = IndexedQueue::create(req_cfg).expect("server: create request queue");
    let q_res = IndexedQueue::create(res_cfg).expect("server: create response queue");

    let mut poller = q_req.create_poller().expect("server: create poller");
    let mut appender = q_res.create_appender().expect("server: create appender");

    let shutdown = std::cell::Cell::new(false);
    loop {
        let result = poller
            .poll(|_idx, _aid, data| {
                if data.len() >= 6 {
                    let seq = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);
                    if seq == SHUTDOWN_SEQ {
                        shutdown.set(true);
                    }
                }
                appender.append(data).expect("server: echo append");
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
