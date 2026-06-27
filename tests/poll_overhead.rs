use std::time::Instant;

use mmap_c_rs::{IndexedQueue, NextMove, PollResult, QueueConfig};

#[test]
fn test_poll_overhead_and_append_poll() {
    let dir = std::env::temp_dir().join("mmap-rs-poll-overhead");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let q = IndexedQueue::create(QueueConfig::new(dir.to_str().unwrap(), "q")).unwrap();
    let mut appender = q.create_appender().unwrap();
    let mut poller = q.create_poller().unwrap();

    // Warmup: consume one message so poller is past it
    appender.append(b"warmup").unwrap();
    loop {
        if poller.poll(|_i, _a, _d| NextMove::Advance).unwrap() == PollResult::Advanced {
            break;
        }
    }

    // ── Measure poll() overhead on empty queue ────────────────────────
    let mut results = Vec::new();
    for _ in 0..5000 {
        let t0 = Instant::now();
        let r = poller.poll(|_i, _a, _d| NextMove::Advance).unwrap();
        let t1 = Instant::now();
        results.push(t1.duration_since(t0).as_nanos());
        assert_eq!(r, PollResult::EndOfStream);
    }
    let total: u128 = results.iter().sum();
    let mean = total as f64 / results.len() as f64;
    println!("poll() overhead (empty): mean={:.0}ns", mean);

    // ── Measure append + poll same-thread round-trip ──────────────────
    let mut results2 = Vec::new();
    for _ in 0..5000 {
        appender.append(b"hello").unwrap();
        let t0 = Instant::now();
        loop {
            if poller.poll(|_i, _a, _d| NextMove::Advance).unwrap() == PollResult::Advanced {
                break;
            }
        }
        let t1 = Instant::now();
        results2.push(t1.duration_since(t0).as_nanos());
    }
    let total2: u128 = results2.iter().sum();
    let mean2 = total2 as f64 / results2.len() as f64;
    let min2 = *results2.iter().min().unwrap();
    let max2 = *results2.iter().max().unwrap();
    println!("append+poll (same thread): mean={:.0}ns min={}ns max={}ns", mean2, min2, max2);

    // If same-thread is fast but cross-thread is slow, the bottleneck is scheduling
    println!(">>> If same-thread is ~400ns but cross-thread is ~50us,");
    println!(">>> the bottleneck is server thread scheduling, not Rust overhead.");
}
