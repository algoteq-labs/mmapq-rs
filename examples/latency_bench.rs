use std::cell::Cell;
use std::time::{Duration, Instant};

use mmap_c_rs::{IndexedQueue, NextMove, PollResult, QueueConfig, Runtime};

#[allow(dead_code)]
/// Pin the current thread to the given CPU core.
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

const SHUTDOWN_SEQ: u32 = 0;

#[allow(dead_code)]
#[repr(packed)]
struct LatencyHeader {
    instance_id: u16,
    sequence: u32,
}

fn make_message(instance_id: u16, sequence: u32, payload: &[u8]) -> Vec<u8> {
    let hdr = LatencyHeader {
        instance_id: instance_id.to_be(),
        sequence: sequence.to_be(),
    };
    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(&hdr as *const LatencyHeader as *const u8, 6)
    };
    [hdr_bytes, payload].concat()
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
        eprintln!("  options: --external  External server mode (spawn echo_server)");
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

    let name_req = "request";
    let name_res = "response";

    // ── Runtime setup ────────────────────────────────────────────────────
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

    // ── Start internal server thread ──────────────────────────────────────
    if !external_server {
        let s_dir = dir.clone();
        let s_wrt = write_rt.clone();
        let s_rdt = read_rt.clone();
        std::thread::spawn(move || {
            pin_thread(server_cpu);
            internal_server(&s_dir, name_req, name_res, s_wrt, s_rdt);
        });
        std::thread::sleep(Duration::from_millis(100));
        println!("  Internal Server started (core {})", if server_cpu >= 0 { format!("{}", server_cpu) } else { "any".into() });
    }

    // ── Create probe: request appender + response poller ──────────────────
    let req_cfg = make_config(&dir, name_req, write_rt.as_ref(), read_rt.as_ref());
    let res_cfg = make_config(&dir, name_res, write_rt.as_ref(), read_rt.as_ref());

    let q_req = IndexedQueue::create(req_cfg).expect("create request queue");
    let q_res = IndexedQueue::create(res_cfg).expect("create response queue");

    let mut appender = q_req.create_appender().expect("create appender");
    let mut poller = q_res.create_poller().expect("create poller");

    // Move to end to skip any stale messages
    poller.move_to_end();

    // Pin client thread to its core (after runtime creation to prevent inheritance)
    pin_thread(client_cpu);

    let payload = vec![b'A' as u8; msg_size];
    let instance_id: u16 = 1;

    // ── Warmup ────────────────────────────────────────────────────────────
    println!("Warming up...");
    for seq in 1..=warmup {
        let msg = make_message(instance_id, seq as u32, &payload);
        appender.append(&msg).expect("warmup append");
        loop {
            let done = poller
                .poll(|_idx, _aid, _data| NextMove::Advance)
                .expect("warmup poll");
            if done == PollResult::Advanced {
                break;
            }
            std::thread::yield_now();
        }
    }

    // ── Measure ───────────────────────────────────────────────────────────
    // Start timing AFTER append, matching C benchmark methodology.
    println!("Measuring...");
    let mut results = vec![0u64; iterations as usize];
    results.fill(0);

    let start_bench = Instant::now();

    for i in 0..iterations {
        let seq = (i + 1) as u32;
        let msg = make_message(instance_id, seq, &payload);
        appender.append(&msg).expect("measure append");

        let t0 = Instant::now();
        loop {
            let done = poller
                .poll(|_idx, _aid, _data| NextMove::Advance)
                .expect("measure poll");
            if done == PollResult::Advanced {
                break;
            }
            std::thread::yield_now();
        }
        let t1 = Instant::now();
        results[i as usize] = t1.duration_since(t0).as_nanos() as u64;
    }

    let end_bench = Instant::now();
    let elapsed_ns = end_bench.duration_since(start_bench).as_nanos() as u64;

    // ── Print statistics ─────────────────────────────────────────────────
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
        print_stats(&results, elapsed_ns, "Round-Trip Latency Statistics (nanoseconds):");
    }

    // ── Send shutdown ────────────────────────────────────────────────────
    let shutdown_msg = make_message(instance_id, SHUTDOWN_SEQ, &[]);
    appender.append(&shutdown_msg).expect("shutdown append");

    drop(appender);
    drop(poller);
    drop(q_req);
    drop(q_res);
}

/// Internal echo server thread: reads from request queue, echoes to response queue.
fn internal_server(
    dir: &str,
    req_name: &str,
    res_name: &str,
    write_rt: Option<Runtime>,
    read_rt: Option<Runtime>,
) {
    let q_req =
        IndexedQueue::create(make_config(dir, req_name, write_rt.as_ref(), read_rt.as_ref()))
            .expect("server: create request queue");
    let q_res =
        IndexedQueue::create(make_config(dir, res_name, write_rt.as_ref(), read_rt.as_ref()))
            .expect("server: create response queue");

    let mut poller = q_req.create_poller().expect("server: create poller");
    let mut appender = q_res.create_appender().expect("server: create appender");

    let shutdown = Cell::new(false);
    loop {
        let result = poller
            .poll(|_idx, _aid, data| {
                let (_inst, seq, _payload) = parse_message(data);
                if seq == SHUTDOWN_SEQ {
                    shutdown.set(true);
                }
                appender.append(data).expect("server: echo append");
                NextMove::Advance
            })
            .expect("server: poll");

        if shutdown.get() {
            break;
        }

        if result == PollResult::EndOfStream {
            // Server tight-spins — always ready for messages.
        }
    }

    drop(appender);
    drop(poller);
    drop(q_req);
    drop(q_res);
}
