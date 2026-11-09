use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::os::unix::fs::FileExt;
use mmapq::{IndexedBatchQueue, IndexedQueue, MmapError, NextMove, PollResult, QueueConfig, Runtime, RuntimeConfig};
// The included error module resolves its C bindings through crate::ffi.
#[allow(unused_imports)] // IDE inspection misses the use in the #[path] module.
use mmapq::ffi;

#[allow(dead_code)]
#[path = "../src/error.rs"]
mod error_mapping;

struct CountingAllocator;
thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
// SAFETY: Allocation and deallocation delegate to System with unchanged layouts.
// Counters are thread-local Cells; bookkeeping does not allocate or retain pointers.
unsafe impl GlobalAlloc for CountingAllocator {
    /// # Safety
    /// The caller must supply a valid, nonzero allocation layout (GlobalAlloc contract).
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.try_with(Cell::get).unwrap_or(false) {
            let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        }
        // SAFETY: The GlobalAlloc caller supplies System's required valid layout.
        unsafe { System.alloc(layout) }
    }
    /// # Safety
    /// pointer must be a live allocation from this allocator with the same layout.
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: Every allocation delegates to System; the caller guarantees
        // this pointer/layout pair still owns that allocation and is freed once.
        unsafe { System.dealloc(pointer, layout) };
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn without_allocations<ResultValue>(operation: impl FnOnce() -> ResultValue) -> ResultValue {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|active| active.set(true));
    let result = operation();
    COUNTING.with(|active| active.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
    result
}

#[test]
fn error_construction_does_not_allocate() {
    without_allocations(|| {
        for code in [-1, -4, -12, -99] {
            std::hint::black_box(error_mapping::map_rc(code));
        }
    });
}

// covers: IndexedBatchQueue::create
// covers: IndexedBatchQueue::create_appender
// covers: IndexedBatchQueue::create_appender_with_id
// covers: IndexedBatchQueue::create_reader
// covers: BatchWriter::append
// covers: BatchWriter::append_reserved
// covers: BatchWriter::commit
// covers: BatchWriter::append_remaining_from_iter
// covers: BatchMessage::iter
// covers: BatchMessage::message_count
#[test]
fn region_reuse_and_bulk_copy_preserve_borrowed_payloads() {
    for asynchronous in [false, true] {
        let directory = std::env::temp_dir().join(format!("mmapq-lending-{}-{asynchronous}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let runtime = asynchronous.then(|| Runtime::new(RuntimeConfig::new()).unwrap());
        let config = |name| {
            let config = QueueConfig::new(directory.to_str().unwrap(), name)
                .region_size(32768).ring_size(16).max_file_size(32768 * 16);
            if let Some(runtime) = &runtime {
                config.write_runtime(runtime).read_runtime(runtime)
            } else { config }
        };
        let source = IndexedBatchQueue::create(config("source")).unwrap();
        for appender_id in [172, 173] {
            let mut appender = source.create_appender_with_id(appender_id).unwrap();
            let mut writer = appender.start_batch().unwrap();
            for sequence in 0..192_u8 {
                writer.append_reserved(1020).unwrap().fill(sequence);
            }
            assert_eq!(writer.commit().unwrap(), i64::from(appender_id - 172));
        }
        let destination = IndexedBatchQueue::create(config("destination")).unwrap();
        let mut destination_appender = destination.create_appender().unwrap();
        let mut reader = source.create_reader().unwrap();
        for index in [0, 1, 0] {
            let mut batch = reader.read(index).unwrap().available().unwrap();
            for _ in 0..2 {
                let mut cursor = batch.iter();
                without_allocations(|| {
                    for sequence in 0..192_u8 {
                        let (payload, appender_id) = cursor.next().unwrap_or_else(|error| panic!("async={asynchronous} index={index} sequence={sequence}: {error}")).unwrap();
                        std::thread::yield_now();
                        assert_eq!(appender_id, 172 + index as u8);
                        assert_eq!(payload, &[sequence; 1020]);
                    }
                    assert!(cursor.next().unwrap().is_none());
                    assert!(cursor.next().unwrap().is_none());
                });
            }
            let mut cursor = batch.iter();
            let mut writer = destination_appender.start_batch().unwrap();
            let (first, _) = cursor.next().unwrap().unwrap();
            assert_eq!(writer.append(first).unwrap(), 0);
            assert_eq!(writer.append_remaining_from_iter(&mut cursor).unwrap(), 191);
            writer.commit().unwrap();
        }
        let mut destination_reader = destination.create_reader().unwrap();
        for index in 0..3 {
            let mut batch = destination_reader.read(index).unwrap().available().unwrap();
            assert_eq!(batch.message_count(), 192);
            let mut cursor = batch.iter();
            for sequence in 0..192_u8 {
                assert_eq!(cursor.next().unwrap().unwrap().0, &[sequence; 1020]);
            }
            assert!(cursor.next().unwrap().is_none());
        }
        let normal = IndexedQueue::create(config("normal")).unwrap();
        let mut appender = normal.create_appender().unwrap();
        for sequence in 0..192_u8 {
            assert_eq!(appender.append(&[sequence; 1020]).unwrap(), i64::from(sequence));
        }
        let mut normal_reader = normal.create_reader().unwrap();
        for index in (0..192).chain((0..192).rev()) {
            without_allocations(|| {
                let message = normal_reader.read(index).unwrap().available().unwrap();
                std::thread::yield_now();
                assert_eq!(message.data(), &[index as u8; 1020]);
            });
        }
        drop(normal_reader);
        drop(appender);
        drop(normal);
        drop(destination_reader);
        drop(reader);
        drop(destination_appender);
        drop(destination);
        drop(source);
        drop(runtime);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

// covers: IndexedBatchAppender::id
// covers: BatchMessage::batch_offset
// covers: IndexedBatchPoller::current_batch_index
#[test]
fn corruption_is_allocation_free_and_cannot_advance_poller() {
    let directory = std::env::temp_dir().join(format!("mmapq-lending-error-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let queue = IndexedBatchQueue::create(QueueConfig::new(directory.to_str().unwrap(), "batch")).unwrap();
    let mut appender = queue.create_appender().unwrap();
    let mut writer = appender.start_batch().unwrap();
    writer.append(b"good").unwrap();
    writer.append(b"bad").unwrap();
    assert_eq!(writer.commit().unwrap(), 0);
    let mut reader = queue.create_reader().unwrap();
    let offset = reader.read(0).unwrap().available().unwrap().batch_offset();
    let file = std::fs::OpenOptions::new().write(true)
        .open(directory.join("batch").join(format!("payload_{}_0", appender.id()))).unwrap();
    file.write_all_at(&0_i32.to_le_bytes(), offset as u64 + 12).unwrap();
    let mut batch = reader.read(0).unwrap().available().unwrap();
    let mut cursor = batch.iter();
    without_allocations(|| {
        assert_eq!(cursor.next().unwrap().unwrap().0, b"good");
        assert!(matches!(cursor.next(), Err(MmapError::Corrupt(_))));
        assert!(matches!(cursor.next(), Err(MmapError::Corrupt(_))));
    });
    let mut poller = queue.create_poller().unwrap();
    for _ in 0..2 {
        assert_eq!(poller.poll(|_, mut cursor| {
            assert_eq!(cursor.next().unwrap().unwrap().0, b"good");
            assert!(matches!(cursor.next(), Err(MmapError::Corrupt(_))));
            NextMove::Advance
        }).unwrap(), PollResult::Corrupt);
        assert_eq!(poller.current_batch_index(), 0);
    }
    drop(poller);
    drop(reader);
    drop(appender);
    drop(queue);
    drop(file);
    std::fs::remove_dir_all(directory).unwrap();
}
