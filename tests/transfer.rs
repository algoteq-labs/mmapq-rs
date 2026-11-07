use std::path::{Path, PathBuf};

use mmapq::{
    transfer::{QueueTransfer, BatchQueueTransfer, StartIndexMode, ConflictBehavior},
    batch::IndexedBatchQueue,
    IndexedQueue, QueueConfig,
    MmapError,
};
use mmapq::ffi;

fn create_queue(basename: &str) -> (IndexedQueue, PathBuf) {
    let dir = std::env::temp_dir().join("mmap-rs-transfer-test").join(basename);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let q = IndexedQueue::create(QueueConfig::new(dir.to_str().unwrap(), "q")).unwrap();
    (q, dir)
}

fn create_batch_queue(basename: &str) -> (IndexedBatchQueue, PathBuf) {
    let dir = std::env::temp_dir().join("mmap-rs-transfer-test").join(basename);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let q = IndexedBatchQueue::create(QueueConfig::new(dir.to_str().unwrap(), "bq")).unwrap();
    (q, dir)
}

fn verify_normal(dest: &Path, expected_si: i64, expected_li: i64) {
    let q = IndexedQueue::create(QueueConfig::new(dest.to_str().unwrap(), "q")).unwrap();
    let mut r = q.create_reader().unwrap();
    assert_eq!(r.start_index(), expected_si);
    assert_eq!(r.last_index(), expected_li);
    assert!(r.read(expected_si).unwrap().available().is_some());
    drop(r);
    drop(q);
}

fn verify_batch(dest: &Path, expected_si: i64, expected_li: i64) {
    let q = IndexedBatchQueue::create(QueueConfig::new(dest.to_str().unwrap(), "q")).unwrap();
    let mut r = q.create_reader().unwrap();
    assert_eq!(r.start_index(), expected_si);
    assert_eq!(r.last_index(), expected_li);
    drop(r);
    drop(q);
}

#[test]
fn test_export_import_roundtrip() {
    let (q_src, src_dir) = create_queue("xfer_roundtrip");
    let mut a = q_src.create_appender().unwrap();
    for i in 0..100 { a.append(format!("msg-{}", i).as_bytes()).unwrap(); }
    let li = a.current_index();
    drop(a);
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, li, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    std::fs::remove_dir_all(src_dir.join("q")).ok();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("xfer_import");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 100);
}

#[test]
fn test_export_preserve() {
    let (q_src, src_dir) = create_queue("export_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"msg").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    let n = QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, li, StartIndexMode::Preserve).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);

    // Import back and verify index space
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("export_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
    verify_normal(&dest, 0, 0);
}

#[test]
fn test_import_preserve() {
    let (q_src, src_dir) = create_queue("import_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"a").unwrap(); a.append(b"b").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, li, StartIndexMode::Preserve).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("import_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 2);
    verify_normal(&dest, si, si + 1);
}

#[test]
fn test_import_allow_append_false() {
    let (q_src, src_dir) = create_queue("import_no_append");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"msg").unwrap(); let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, li, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("import_no_append_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    let e = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).expect_err("exists");
    assert!(matches!(e, MmapError::Exists));
}

#[test]
fn test_import_allow_append_true() {
    let (q_src, src_dir) = create_queue("import_append");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"msg").unwrap(); let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, li, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("import_append_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Rebase, true, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn test_copy_roundtrip() {
    let (q_src, _src_dir) = create_queue("copy_roundtrip");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"c1").unwrap(); a.append(b"c2").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("copy_roundtrip_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy(&mut r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 2);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_preserve() {
    let (q_src, _src_dir) = create_queue("copy_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"x").unwrap(); a.append(b"y").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("copy_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy(&mut r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 2);
    verify_normal(&dest, si, si + 1);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_allow_append() {
    let (q_src, _src_dir) = create_queue("copy_append");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"new").unwrap(); let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("copy_append_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    QueueTransfer::copy(&mut r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    let n = QueueTransfer::copy(&mut r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, true, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);
}

#[test]
fn test_batch_export_import_roundtrip() {
    let (q_src, src_dir) = create_batch_queue("batch_xfer");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"msg").unwrap(); b.commit().unwrap(); }
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    BatchQueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_import");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn test_batch_export_preserve() {
    let (q_src, src_dir) = create_batch_queue("batch_export_preserve");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"msg").unwrap(); b.commit().unwrap(); }
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    let n = BatchQueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Preserve).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);

    // Import back and verify index space
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_export_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
    verify_batch(&dest, 0, 0);
}

#[test]
fn test_batch_import_preserve() {
    let (q_src, src_dir) = create_batch_queue("batch_import_preserve");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"msg").unwrap(); b.commit().unwrap(); }
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    BatchQueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_import_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn test_batch_copy_roundtrip() {
    let (q_src, _src_dir) = create_batch_queue("batch_copy");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"bc1").unwrap(); b.commit().unwrap(); }
    let mut r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_copy_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy(&mut r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);
}

#[test]
fn test_batch_copy_preserve() {
    let (q_src, _src_dir) = create_batch_queue("batch_copy_preserve");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"bcp").unwrap(); b.commit().unwrap(); }
    let mut r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_copy_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy(&mut r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 1);
    verify_batch(&dest, si, si);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_batch_to_normal() {
    let (bq_src, _src_dir) = create_batch_queue("b2n");
    let mut a = bq_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"m1").unwrap(); b.append(b"m2").unwrap(); b.commit().unwrap(); }
    let mut r = bq_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("b2n_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy_from_batch(&mut r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 2);
    drop(r); drop(bq_src);
}

#[test]
fn test_copy_batch_to_normal_preserve() {
    let (bq_src, _src_dir) = create_batch_queue("b2n_preserve");
    let mut a = bq_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"m1").unwrap(); b.append(b"m2").unwrap(); b.commit().unwrap(); }
    let mut r = bq_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("b2n_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy_from_batch(&mut r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 2);
    verify_normal(&dest, si, si + 1);
    drop(r); drop(bq_src);
}

#[test]
fn test_copy_normal_to_batch() {
    let (q_src, _src_dir) = create_queue("n2b");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"n2b_1").unwrap(); a.append(b"n2b_2").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("n2b_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy_from_normal(&mut r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 2);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_normal_to_batch_preserve() {
    let (q_src, _src_dir) = create_queue("n2b_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"n2b_1").unwrap(); a.append(b"n2b_2").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("n2b_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy_from_normal(&mut r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Preserve, false, None, ConflictBehavior::Auto).unwrap();
    assert_eq!(n, 2);
    verify_batch(&dest, si, si + 1);
    drop(r); drop(q_src);
}

// ── Cross-type PRESERVE with non-zero start_index ─────────────────────────

#[test]
fn test_copy_batch_to_normal_preserve_nonzero() {
    let dir = std::env::temp_dir().join("mmap-rs-transfer-test").join("b2n_nonzero");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let q = IndexedBatchQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "bq").start_index(100),
    ).unwrap();
    let mut a = q.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"hello").unwrap(); b.commit().unwrap(); }
    let mut r = q.create_reader().unwrap();
    assert_eq!(r.start_index(), 100);
    assert_eq!(r.last_index(), 100);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("b2n_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy_from_batch(
        &mut r, dest.to_str().unwrap(), "q", 100, 100, StartIndexMode::Preserve, false,
        None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 1);
    verify_normal(&dest, 100, 100);

    drop(r); drop(q);
}

#[test]
fn test_copy_normal_to_batch_preserve_nonzero() {
    let dir = std::env::temp_dir().join("mmap-rs-transfer-test").join("n2b_nonzero");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let q = IndexedQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q").start_index(100),
    ).unwrap();
    let mut a = q.create_appender().unwrap();
    a.append(b"world").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q.create_reader().unwrap();
    assert_eq!(r.start_index(), 100);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("n2b_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy_from_normal(
        &mut r, dest.to_str().unwrap(), "q", 100, li, StartIndexMode::Preserve, false,
        None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 1);
    verify_batch(&dest, 100, 100);

    drop(r); drop(q);
}

#[test]
fn test_import_batch_stream_preserve_nonzero() {
    let dir = std::env::temp_dir().join("mmap-rs-transfer-test").join("rust_ibs_nonzero");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let q = IndexedBatchQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "bq").start_index(100),
    ).unwrap();
    let mut a = q.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"test").unwrap(); b.commit().unwrap(); }
    let mut r = q.create_reader().unwrap();

    let ep = dir.join("export.dat");
    BatchQueueTransfer::export(&mut r, ep.to_str().unwrap(), 100, 100, StartIndexMode::Preserve).unwrap();
    drop(r); drop(q);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("rust_ibs_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import_from_batch(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Preserve, false,
        None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 1);
    verify_normal(&dest, 100, 100);
}

#[test]
fn test_import_flat_to_batch_preserve_nonzero() {
    let dir = std::env::temp_dir().join("mmap-rs-transfer-test").join("rust_ifb_nonzero");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let q = IndexedQueue::create(
        QueueConfig::new(dir.to_str().unwrap(), "q").start_index(100),
    ).unwrap();
    let mut a = q.create_appender().unwrap();
    a.append(b"test").unwrap();
    let li = a.current_index(); drop(a);
    let mut r = q.create_reader().unwrap();

    let ep = dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 100, li, StartIndexMode::Preserve).unwrap();
    drop(r); drop(q);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("rust_ifb_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import_from_flat(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q", StartIndexMode::Preserve, false,
        None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 1);
    verify_batch(&dest, 100, 100);
}

// ── Appender ID preservation tests ────────────────────────────────────────

#[test]
fn test_import_preserves_appender_ids() {
    let (q_src, src_dir) = create_queue("aid_preserve");
    let mut a1 = q_src.create_appender().unwrap();
    let mut a2 = q_src.create_appender().unwrap();
    let id1 = a1.id();
    let id2 = a2.id();
    assert_ne!(id1, id2);

    a1.append(b"from-a1").unwrap();
    a2.append(b"from-a2").unwrap();
    a1.append(b"from-a1-2").unwrap();
    drop(a1); drop(a2);

    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 2, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("aid_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q",
        StartIndexMode::Rebase, false, None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 3);

    // Verify both appender IDs are present in the imported queue
    let q_dest = IndexedQueue::create(
        QueueConfig::new(dest.to_str().unwrap(), "q"),
    ).unwrap();
    let mut r_dest = q_dest.create_reader().unwrap();
    let mut seen_id1 = false;
    let mut seen_id2 = false;
    for i in 0..=r_dest.last_index() {
        let msg = r_dest.read(i).unwrap().available().unwrap();
        if msg.appender_id() == id1 { seen_id1 = true; }
        if msg.appender_id() == id2 { seen_id2 = true; }
    }
    assert!(seen_id1 && seen_id2);
    drop(r_dest); drop(q_dest);
}

#[test]
fn test_import_merge_via_mapper() {
    let (q_src, src_dir) = create_queue("aid_merge");
    let mut a1 = q_src.create_appender().unwrap();
    let mut a2 = q_src.create_appender().unwrap();
    a1.append(b"from-a1").unwrap();
    a2.append(b"from-a2").unwrap();
    drop(a1); drop(a2);

    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 1, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("aid_merge_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();

    // Mapper that returns None (auto-assign) for all
    let mapper: Box<dyn Fn(u16) -> Option<u8>> = Box::new(|_| None);
    let n = QueueTransfer::import(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q",
        StartIndexMode::Rebase, false, Some(mapper), ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 2);
}

#[test]
fn test_import_auto_on_conflict() {
    // Create destination with appender ID 0 in use
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("aid_auto_conflict");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let q_dest = IndexedQueue::create(QueueConfig::new(dest.to_str().unwrap(), "q")).unwrap();
    let _existing = q_dest.create_appender_with_id(0).unwrap();

    // Create source with appender ID 0
    let (q_src, src_dir) = create_queue("aid_auto_src");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"hello").unwrap();
    drop(a);
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);

    // Import with MMAPQ_CONFLICT_AUTO — should auto-assign since ID 0 is taken
    let n = QueueTransfer::import(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q",
        StartIndexMode::Rebase, true, None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 1);

    // Verify the imported entry got a different appender ID (not 0, which is held)
    let q_dest2 = IndexedQueue::create(
        QueueConfig::new(dest.to_str().unwrap(), "q"),
    ).unwrap();
    let mut r_dest = q_dest2.create_reader().unwrap();
    let msg = r_dest.read(0).unwrap().available().unwrap();
    assert_ne!(msg.appender_id(), 0, "auto-assign should not use the held ID 0");
    drop(r_dest); drop(q_dest2);

    drop(_existing);
    drop(q_dest);
}

#[test]
fn test_import_abort_on_conflict() {
    // Create destination with appender ID 0 in use
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("aid_abort");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let q_dest = IndexedQueue::create(QueueConfig::new(dest.to_str().unwrap(), "q")).unwrap();
    let _existing = q_dest.create_appender_with_id(0).unwrap();

    // Create source with appender ID 0
    let (q_src, src_dir) = create_queue("aid_abort_src");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"hello").unwrap();
    drop(a);
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);

    // Import with MMAPQ_CONFLICT_ABORT — should get IdConflict error
    let e = QueueTransfer::import(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q",
        StartIndexMode::Rebase, true, None, ConflictBehavior::Abort,
    ).expect_err("expected IdConflict");
    assert!(matches!(e, MmapError::IdConflict));
    drop(_existing);
    drop(q_dest);
}

#[test]
fn test_batch_import_preserves_appender_ids() {
    let (q_src, src_dir) = create_batch_queue("batch_aid");
    let mut a1 = q_src.create_appender().unwrap();
    let mut a2 = q_src.create_appender().unwrap();
    let id1 = a1.id();
    let id2 = a2.id();
    assert_ne!(id1, id2);

    { let mut b = a1.start_batch().unwrap(); b.append(b"a1").unwrap(); b.commit().unwrap(); }
    { let mut b = a2.start_batch().unwrap(); b.append(b"a2").unwrap(); b.commit().unwrap(); }
    drop(a1); drop(a2);

    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    BatchQueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 1, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_aid_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q",
        StartIndexMode::Rebase, false, None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 2);

    // Verify via reader
    let q_dest = IndexedBatchQueue::create(
        QueueConfig::new(dest.to_str().unwrap(), "q"),
    ).unwrap();
    let mut r_dest = q_dest.create_reader().unwrap();
    let _b0 = r_dest.read(0).unwrap().available().unwrap();
    let got0 = r_dest.appender_id();
    let _b1 = r_dest.read(1).unwrap().available().unwrap();
    let got1 = r_dest.appender_id();
    assert!((got0 == id1 && got1 == id2) || (got0 == id2 && got1 == id1));
    drop(r_dest); drop(q_dest);
}

#[test]
fn test_batch_import_abort_on_conflict() {
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_abort");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let q_dest = IndexedBatchQueue::create(
        QueueConfig::new(dest.to_str().unwrap(), "q"),
    ).unwrap();
    let _existing = q_dest.create_appender_with_id(0).unwrap();

    let (q_src, src_dir) = create_batch_queue("batch_abort_src");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"hello").unwrap(); b.commit().unwrap(); }
    drop(a);
    let mut r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    BatchQueueTransfer::export(&mut r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);

    let e = BatchQueueTransfer::import(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q",
        StartIndexMode::Rebase, true, None, ConflictBehavior::Abort,
    ).expect_err("expected IdConflict");
    assert!(matches!(e, MmapError::IdConflict));
    drop(_existing);
    drop(q_dest);
}
#[test]
fn test_export_with_filter() {
    let (q_src, src_dir) = create_queue("export_filter");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"keep-1").unwrap();
    a.append(b"discard").unwrap();
    a.append(b"keep-2").unwrap();
    let li = a.current_index();
    drop(a);

    /// # Safety
    /// data must be readable for positive length bytes for this invocation.
    /// The C exporter supplies a live mapped payload and does not retain the callback.
    unsafe extern "C" fn keep_filter(
        _idx: i64, data: *const std::ffi::c_void,
        length: i32, _ctx: *mut std::ffi::c_void,
    ) -> i32 {
        // SAFETY: The exporter supplies a validated message in one live mapping;
        // this slice is used only for starts_with and never retained.
        let slice = unsafe { std::slice::from_raw_parts(data as *const u8, length as usize) };
        if slice.starts_with(b"keep") { 1 } else { 0 }
    }

    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export_filtered.dat");
    let cpath = std::ffi::CString::new(ep.to_str().unwrap()).unwrap();
    // SAFETY: cpath and the static mode are NUL-terminated and live for fopen;
    // the returned FILE is checked before use.
    let file = unsafe {
        libc::fopen(
            cpath.as_ptr(),
            b"wb ".as_ptr() as *const _,
        )
    };
    assert!(!file.is_null());

    // SAFETY: The reader remains owned and has no live borrowed messages. file is a
    // checked writable FILE; keep_filter is synchronous, does not retain the
    // payload, and does not panic. No other access to the reader overlaps export.
    let n = unsafe {
        ffi::mmapq_queue_transfer_export(
            r.as_ptr(), file, 0, li, 0,
            Some(keep_filter as unsafe extern "C" fn(_, _, _, _) -> _),
            None,
            std::ptr::null_mut(),
        )
    };
    // SAFETY: Export only borrows file; this is its single close after the callback
    // has returned and before reading the output through the import API.
    unsafe { libc::fclose(file) };
    assert_eq!(n, 2);

    drop(r); drop(q_src);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("export_filter_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q",
        StartIndexMode::Rebase, false, None, ConflictBehavior::Auto,
    ).unwrap();
    assert_eq!(n, 2);
}
