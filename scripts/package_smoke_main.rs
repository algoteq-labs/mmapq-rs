use mmapq::{IndexedQueue, NextMove, QueueConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("mmapq-package-smoke-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let q = IndexedQueue::create(QueueConfig::new(dir.to_str().unwrap(), "smoke"))?;
    let mut a = q.create_appender()?;
    assert_eq!(a.append(b"one")?, 0);
    assert_eq!(a.append(b"two")?, 1);
    let mut p = q.create_poller()?;
    let mut got: Vec<(i64, Vec<u8>)> = Vec::new();
    for _ in 0..2 {
        p.poll(|index, _appender_id, data| {
            got.push((index, data.to_vec()));
            NextMove::Advance
        })?;
    }
    assert_eq!(got, vec![(0, b"one".to_vec()), (1, b"two".to_vec())]);
    drop(p);
    drop(a);
    drop(q);
    std::fs::remove_dir_all(&dir)?;
    println!("smoke ok");
    Ok(())
}
