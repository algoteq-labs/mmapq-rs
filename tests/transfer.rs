use std::path::{Path, PathBuf};

use mmapq::{
    transfer::{QueueTransfer, BatchQueueTransfer, StartIndexMode},
    batch::IndexedBatchQueue,
    IndexedQueue, QueueConfig,
    MmapError,
};

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
    let r = q.create_reader().unwrap();
    assert_eq!(r.start_index(), expected_si);
    assert_eq!(r.last_index(), expected_li);
    assert!(r.read(expected_si).unwrap().is_some());
    drop(r);
    drop(q);
}

fn verify_batch(dest: &Path, expected_si: i64, expected_li: i64) {
    let q = IndexedBatchQueue::create(QueueConfig::new(dest.to_str().unwrap(), "q")).unwrap();
    let r = q.create_reader().unwrap();
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
    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&r, ep.to_str().unwrap(), 0, li, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    std::fs::remove_dir_all(src_dir.join("q")).ok();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("xfer_import");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Rebase, false).unwrap();
    assert_eq!(n, 100);
}

#[test]
fn test_export_preserve() {
    let (q_src, src_dir) = create_queue("export_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"msg").unwrap();
    let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    let n = QueueTransfer::export(&r, ep.to_str().unwrap(), 0, li, StartIndexMode::Preserve).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);

    // Import back and verify index space
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("export_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Preserve, false).unwrap();
    assert_eq!(n, 1);
    verify_normal(&dest, 0, 0);
}

#[test]
fn test_import_preserve() {
    let (q_src, src_dir) = create_queue("import_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"a").unwrap(); a.append(b"b").unwrap();
    let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&r, ep.to_str().unwrap(), 0, li, StartIndexMode::Preserve).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("import_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", si, StartIndexMode::Preserve, false).unwrap();
    assert_eq!(n, 2);
    verify_normal(&dest, si, si + 1);
}

#[test]
fn test_import_allow_append_false() {
    let (q_src, src_dir) = create_queue("import_no_append");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"msg").unwrap(); let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&r, ep.to_str().unwrap(), 0, li, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("import_no_append_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Rebase, false).unwrap();
    let e = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Rebase, false).expect_err("exists");
    assert!(matches!(e, MmapError::Exists));
}

#[test]
fn test_import_allow_append_true() {
    let (q_src, src_dir) = create_queue("import_append");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"msg").unwrap(); let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    QueueTransfer::export(&r, ep.to_str().unwrap(), 0, li, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("import_append_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Rebase, false).unwrap();
    let n = QueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Rebase, true).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn test_copy_roundtrip() {
    let (q_src, _src_dir) = create_queue("copy_roundtrip");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"c1").unwrap(); a.append(b"c2").unwrap();
    let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("copy_roundtrip_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy(&r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, false).unwrap();
    assert_eq!(n, 2);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_preserve() {
    let (q_src, _src_dir) = create_queue("copy_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"x").unwrap(); a.append(b"y").unwrap();
    let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("copy_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy(&r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Preserve, false).unwrap();
    assert_eq!(n, 2);
    verify_normal(&dest, si, si + 1);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_allow_append() {
    let (q_src, _src_dir) = create_queue("copy_append");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"new").unwrap(); let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("copy_append_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    QueueTransfer::copy(&r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, false).unwrap();
    let n = QueueTransfer::copy(&r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, true).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);
}

#[test]
fn test_batch_export_import_roundtrip() {
    let (q_src, src_dir) = create_batch_queue("batch_xfer");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"msg").unwrap(); b.commit().unwrap(); }
    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    BatchQueueTransfer::export(&r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_import");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Rebase, false).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn test_batch_export_preserve() {
    let (q_src, src_dir) = create_batch_queue("batch_export_preserve");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"msg").unwrap(); b.commit().unwrap(); }
    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    let n = BatchQueueTransfer::export(&r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Preserve).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);

    // Import back and verify index space
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_export_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Preserve, false).unwrap();
    assert_eq!(n, 1);
    verify_batch(&dest, 0, 0);
}

#[test]
fn test_batch_import_preserve() {
    let (q_src, src_dir) = create_batch_queue("batch_import_preserve");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"msg").unwrap(); b.commit().unwrap(); }
    let r = q_src.create_reader().unwrap();
    let ep = src_dir.join("export.dat");
    BatchQueueTransfer::export(&r, ep.to_str().unwrap(), 0, 0, StartIndexMode::Rebase).unwrap();
    drop(r); drop(q_src);
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_import_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import(ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 0, StartIndexMode::Preserve, false).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn test_batch_copy_roundtrip() {
    let (q_src, _src_dir) = create_batch_queue("batch_copy");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"bc1").unwrap(); b.commit().unwrap(); }
    let r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_copy_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy(&r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Rebase, false).unwrap();
    assert_eq!(n, 1);
    drop(r); drop(q_src);
}

#[test]
fn test_batch_copy_preserve() {
    let (q_src, _src_dir) = create_batch_queue("batch_copy_preserve");
    let mut a = q_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"bcp").unwrap(); b.commit().unwrap(); }
    let r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("batch_copy_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy(&r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Preserve, false).unwrap();
    assert_eq!(n, 1);
    verify_batch(&dest, si, si);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_batch_to_normal() {
    let (bq_src, _src_dir) = create_batch_queue("b2n");
    let mut a = bq_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"m1").unwrap(); b.append(b"m2").unwrap(); b.commit().unwrap(); }
    let r = bq_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("b2n_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy_from_batch(&r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Rebase, false).unwrap();
    assert_eq!(n, 2);
    drop(r); drop(bq_src);
}

#[test]
fn test_copy_batch_to_normal_preserve() {
    let (bq_src, _src_dir) = create_batch_queue("b2n_preserve");
    let mut a = bq_src.create_appender().unwrap();
    { let mut b = a.start_batch().unwrap(); b.append(b"m1").unwrap(); b.append(b"m2").unwrap(); b.commit().unwrap(); }
    let r = bq_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("b2n_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy_from_batch(&r, dest.to_str().unwrap(), "q", 0, 0, StartIndexMode::Preserve, false).unwrap();
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
    let r = q_src.create_reader().unwrap();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("n2b_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy_from_normal(&r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Rebase, false).unwrap();
    assert_eq!(n, 2);
    drop(r); drop(q_src);
}

#[test]
fn test_copy_normal_to_batch_preserve() {
    let (q_src, _src_dir) = create_queue("n2b_preserve");
    let mut a = q_src.create_appender().unwrap();
    a.append(b"n2b_1").unwrap(); a.append(b"n2b_2").unwrap();
    let li = a.current_index(); drop(a);
    let r = q_src.create_reader().unwrap();
    let si = r.start_index();
    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("n2b_preserve_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy_from_normal(&r, dest.to_str().unwrap(), "q", 0, li, StartIndexMode::Preserve, false).unwrap();
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
    let r = q.create_reader().unwrap();
    assert_eq!(r.start_index(), 100);
    assert_eq!(r.last_index(), 100);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("b2n_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::copy_from_batch(
        &r, dest.to_str().unwrap(), "q", 100, 100, StartIndexMode::Preserve, false,
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
    let r = q.create_reader().unwrap();
    assert_eq!(r.start_index(), 100);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("n2b_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::copy_from_normal(
        &r, dest.to_str().unwrap(), "q", 100, li, StartIndexMode::Preserve, false,
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
    let r = q.create_reader().unwrap();

    let ep = dir.join("export.dat");
    BatchQueueTransfer::export(&r, ep.to_str().unwrap(), 100, 100, StartIndexMode::Preserve).unwrap();
    drop(r); drop(q);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("rust_ibs_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = QueueTransfer::import_from_batch(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 100, StartIndexMode::Preserve, false,
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
    let r = q.create_reader().unwrap();

    let ep = dir.join("export.dat");
    QueueTransfer::export(&r, ep.to_str().unwrap(), 100, li, StartIndexMode::Preserve).unwrap();
    drop(r); drop(q);

    let dest = std::env::temp_dir().join("mmap-rs-transfer-test").join("rust_ifb_nonzero_dest");
    let _ = std::fs::remove_dir_all(&dest);
    std::fs::create_dir_all(&dest).unwrap();
    let n = BatchQueueTransfer::import_from_flat(
        ep.to_str().unwrap(), dest.to_str().unwrap(), "q", 100, StartIndexMode::Preserve, false,
    ).unwrap();
    assert_eq!(n, 1);
    verify_batch(&dest, 100, 100);
}
