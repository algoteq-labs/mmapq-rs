//! Queue transfer — export, import, and copy queues to/from portable stream files.
//!
//! `QueueTransfer` exports/imports/copies single-message queues.
//! `BatchQueueTransfer` does the same for batch queues.

use std::ffi::CString;
use std::any::Any;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use crate::error::MmapError;
use crate::ffi;
use crate::reader::IndexedReader;
use crate::batch::batch_reader::IndexedBatchReader;

pub use ffi::ConflictBehavior;

/// A Rust closure that maps a source appender ID to a destination appender ID.
/// Return `None` for auto-assign, or `Some(id)` for a specific ID.
pub type AppenderIdMapperFn = Box<dyn Fn(u16) -> Option<u8>>;

struct MapperState {
    callback: Option<AppenderIdMapperFn>,
    panic: Option<Box<dyn Any + Send>>,
}

impl MapperState {
    fn resume(&mut self) {
        if let Some(payload) = self.panic.take() {
            resume_unwind(payload);
        }
    }
}

/// # Safety
///
/// ctx must point to the live, exclusively accessible MapperState paired by
/// setup_mapper, with a present callback. C must invoke it synchronously and
/// must not retain it or call concurrently. Panics are caught before returning.
unsafe extern "C" fn mapper_trampoline(source_aid: u16, ctx: *mut std::ffi::c_void) -> i32 {
    // SAFETY: setup_mapper pairs this trampoline with its live stack state;
    // the synchronous C transfer grants exclusive access until callback return.
    let state = unsafe { &mut *(ctx as *mut MapperState) };
    match catch_unwind(AssertUnwindSafe(|| state.callback.as_ref().unwrap()(source_aid))) {
        Ok(Some(appender_id)) => appender_id as i32,
        Ok(None) => ffi::APPENDER_ID_AUTO,
        Err(payload) => {
            state.panic = Some(payload);
            i32::MIN
        }
    }
}

fn setup_mapper(state: &mut MapperState) -> (ffi::AppenderIdMapper, *mut std::ffi::c_void) {
    if state.callback.is_some() {
        (Some(mapper_trampoline), state as *mut MapperState as *mut std::ffi::c_void)
    } else {
        (None, std::ptr::null_mut())
    }
}

/// Controls how start indices are mapped during transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartIndexMode {
    /// Rebase the start index to 0 in the output queue.
    Rebase,
    /// Preserve the original start index from the source.
    Preserve,
}

fn to_c_mode(mode: StartIndexMode) -> i32 {
    match mode {
        StartIndexMode::Rebase => ffi::QUEUE_TRANSFER_REBASE,
        StartIndexMode::Preserve => ffi::QUEUE_TRANSFER_PRESERVE,
    }
}

/// Stateless helpers for single-message queue transfer.
pub struct QueueTransfer;

impl QueueTransfer {
    /// Export entries `[from_index, to_index]` from a reader to a file.
    pub fn export(
        reader: &mut IndexedReader,
        output_path: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
    ) -> Result<u64, MmapError> {
        let path = CString::new(output_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;

        // SAFETY: path is a live NUL-terminated CString and the mode is a static C string.
        // The nullable FILE result is checked before any use.
        let file = unsafe { libc::fopen(path.as_ptr(), b"wb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);

        // SAFETY: The live reader is exclusively borrowed, excluding live payload slices.
        // file is open for writing and remains owned here; C neither closes it
        // nor retains pointers. No filter callbacks are supplied.
        let result = unsafe {
            ffi::mmapq_queue_transfer_export(
                reader.as_ptr(),
                file,
                from_index,
                to_index,
                mode,
                None, None,
                std::ptr::null_mut(),
            )
        };

        // SAFETY: file is the non-null FILE opened above; transfer borrows it and never
        // closes it. This is its single close, after all callbacks have returned.
        let close_result = unsafe { libc::fclose(file) };
        if result < 0 {
            return Err(crate::error::map_rc(result as i32));
        }
        if close_result != 0 {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        Ok(result as u64)
    }

    /// Import entries from a flat stream file into a queue.
    ///
    /// The start index is determined automatically:
    ///   REBASE  → 0
    ///   PRESERVE → the stream header's start_index
    ///
    /// `mapper` maps source appender IDs to destination IDs. Return `None` from
    /// the closure for auto-assign. Pass `None` for identity mapping.
    /// `on_conflict` controls behaviour when the mapped ID is already in use.
    pub fn import(
        stream_path: &str,
        directory: &str,
        queue_name: &str,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let path = CString::new(stream_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;
        let dir = CString::new(directory)
            .map_err(|_| MmapError::Internal("directory contains null byte".into()))?;
        let name = CString::new(queue_name)
            .map_err(|_| MmapError::Internal("queue name contains null byte".into()))?;

        // SAFETY: path is a live NUL-terminated CString and the mode is a static C string.
        // The nullable FILE result is checked before any use.
        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: file is live/open for reading and CString directory/name inputs stay alive.
        // The stack MapperState is paired with its trampoline, used synchronously,
        // and never retained by C. Panics are caught until transfer cleanup returns.
        let result = unsafe {
            ffi::mmapq_queue_transfer_import(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        // SAFETY: file is the non-null FILE opened above; C transfer leaves it open.
        // All callbacks have returned and this is the matching, single fclose.
        unsafe { libc::fclose(file) };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Import entries from a batch-format stream into a normal queue, flattening
    /// each batch's messages into individual entries.
    pub fn import_from_batch(
        stream_path: &str,
        directory: &str,
        queue_name: &str,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let path = CString::new(stream_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;
        let dir = CString::new(directory)
            .map_err(|_| MmapError::Internal("directory contains null byte".into()))?;
        let name = CString::new(queue_name)
            .map_err(|_| MmapError::Internal("queue name contains null byte".into()))?;

        // SAFETY: path is a live NUL-terminated CString and the mode is a static C string.
        // The nullable FILE result is checked before any use.
        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: file is live/open for reading and CString directory/name inputs stay alive.
        // The stack MapperState is paired with its trampoline, used synchronously,
        // and never retained by C. Panics are caught until transfer cleanup returns.
        let result = unsafe {
            ffi::mmapq_queue_transfer_import_batch_stream(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        // SAFETY: file is the non-null FILE opened above; C transfer leaves it open.
        // All callbacks have returned and this is the matching, single fclose.
        unsafe { libc::fclose(file) };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy entries from a source queue reader to a new destination queue.
    pub fn copy(
        reader: &mut IndexedReader,
        dest_dir: &str,
        dest_queue: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let dir = CString::new(dest_dir)
            .map_err(|_| MmapError::Internal("dest_dir contains null byte".into()))?;
        let name = CString::new(dest_queue)
            .map_err(|_| MmapError::Internal("dest_queue contains null byte".into()))?;

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: The live reader is exclusively borrowed while C advances its mappings.
        // CString destinations and the paired stack MapperState outlive this
        // synchronous call; C retains neither, and mapper panics wait for cleanup.
        let result = unsafe {
            ffi::mmapq_queue_transfer_copy(
                reader.as_ptr(), dir.as_ptr(), name.as_ptr(),
                from_index, to_index, mode,
                None, None, std::ptr::null_mut(),
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy batches from a batch queue reader and flatten each batch's messages
    /// into individual entries in a normal queue.
    pub fn copy_from_batch(
        reader: &mut IndexedBatchReader,
        dest_dir: &str,
        dest_queue: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let dir = CString::new(dest_dir)
            .map_err(|_| MmapError::Internal("dest_dir contains null byte".into()))?;
        let name = CString::new(dest_queue)
            .map_err(|_| MmapError::Internal("dest_queue contains null byte".into()))?;

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: The live reader is exclusively borrowed while C advances its mappings.
        // CString destinations and the paired stack MapperState outlive this
        // synchronous call; C retains neither, and mapper panics wait for cleanup.
        let result = unsafe {
            ffi::mmapq_queue_transfer_copy_batch_to_normal(
                reader.as_ptr(), dir.as_ptr(), name.as_ptr(),
                from_index, to_index, mode,
                None, None, std::ptr::null_mut(),
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }
}

/// Stateless helpers for batch queue transfer.
pub struct BatchQueueTransfer;

impl BatchQueueTransfer {
    /// Export batches `[from_index, to_index]` from a batch reader to a file.
    pub fn export(
        reader: &mut IndexedBatchReader,
        output_path: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
    ) -> Result<u64, MmapError> {
        let path = CString::new(output_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;

        // SAFETY: path is a live NUL-terminated CString and the mode is a static C string.
        // The nullable FILE result is checked before any use.
        let file = unsafe { libc::fopen(path.as_ptr(), b"wb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);

        // SAFETY: The live reader is exclusively borrowed, excluding live payload slices.
        // file is open for writing and remains owned here; C neither closes it
        // nor retains pointers. No filter callbacks are supplied.
        let result = unsafe {
            ffi::mmapq_batch_queue_transfer_export(
                reader.as_ptr(),
                file,
                from_index,
                to_index,
                mode,
                None, None,
                std::ptr::null_mut(),
            )
        };

        // SAFETY: file is the non-null FILE opened above; transfer borrows it and never
        // closes it. This is its single close, after all callbacks have returned.
        let close_result = unsafe { libc::fclose(file) };
        if result < 0 {
            return Err(crate::error::map_rc(result as i32));
        }
        if close_result != 0 {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        Ok(result as u64)
    }

    /// Import batch entries from a flat stream file into a batch queue.
    pub fn import(
        stream_path: &str,
        directory: &str,
        queue_name: &str,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let path = CString::new(stream_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;
        let dir = CString::new(directory)
            .map_err(|_| MmapError::Internal("directory contains null byte".into()))?;
        let name = CString::new(queue_name)
            .map_err(|_| MmapError::Internal("queue name contains null byte".into()))?;

        // SAFETY: path is a live NUL-terminated CString and the mode is a static C string.
        // The nullable FILE result is checked before any use.
        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: file is live/open for reading and CString directory/name inputs stay alive.
        // The stack MapperState is paired with its trampoline, used synchronously,
        // and never retained by C. Panics are caught until transfer cleanup returns.
        let result = unsafe {
            ffi::mmapq_batch_queue_transfer_import(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        // SAFETY: file is the non-null FILE opened above; C transfer leaves it open.
        // All callbacks have returned and this is the matching, single fclose.
        unsafe { libc::fclose(file) };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Import entries from a flat stream into a batch queue, placing each
    /// message into its own 1-element batch.
    pub fn import_from_flat(
        stream_path: &str,
        directory: &str,
        queue_name: &str,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let path = CString::new(stream_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;
        let dir = CString::new(directory)
            .map_err(|_| MmapError::Internal("directory contains null byte".into()))?;
        let name = CString::new(queue_name)
            .map_err(|_| MmapError::Internal("queue name contains null byte".into()))?;

        // SAFETY: path is a live NUL-terminated CString and the mode is a static C string.
        // The nullable FILE result is checked before any use.
        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: file is live/open for reading and CString directory/name inputs stay alive.
        // The stack MapperState is paired with its trampoline, used synchronously,
        // and never retained by C. Panics are caught until transfer cleanup returns.
        let result = unsafe {
            ffi::mmapq_batch_queue_transfer_import_flat_stream(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        // SAFETY: file is the non-null FILE opened above; C transfer leaves it open.
        // All callbacks have returned and this is the matching, single fclose.
        unsafe { libc::fclose(file) };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy batches from a source batch reader to a new destination batch queue.
    pub fn copy(
        reader: &mut IndexedBatchReader,
        dest_dir: &str,
        dest_queue: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let dir = CString::new(dest_dir)
            .map_err(|_| MmapError::Internal("dest_dir contains null byte".into()))?;
        let name = CString::new(dest_queue)
            .map_err(|_| MmapError::Internal("dest_queue contains null byte".into()))?;

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: The live reader is exclusively borrowed while C advances its mappings.
        // CString destinations and the paired stack MapperState outlive this
        // synchronous call; C retains neither, and mapper panics wait for cleanup.
        let result = unsafe {
            ffi::mmapq_batch_queue_transfer_copy(
                reader.as_ptr(), dir.as_ptr(), name.as_ptr(),
                from_index, to_index, mode,
                None, None, std::ptr::null_mut(),
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy entries from a normal queue reader and place each entry as a
    /// single-message batch in a batch queue.
    pub fn copy_from_normal(
        reader: &mut IndexedReader,
        dest_dir: &str,
        dest_queue: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
        allow_append: bool,
        mapper: Option<AppenderIdMapperFn>,
        on_conflict: ConflictBehavior,
    ) -> Result<u64, MmapError> {
        let dir = CString::new(dest_dir)
            .map_err(|_| MmapError::Internal("dest_dir contains null byte".into()))?;
        let name = CString::new(dest_queue)
            .map_err(|_| MmapError::Internal("dest_queue contains null byte".into()))?;

        let mode = to_c_mode(start_index_mode);
        let mut mapper_state = MapperState { callback: mapper, panic: None };
        let (cb, ctx) = setup_mapper(&mut mapper_state);

        // SAFETY: The live reader is exclusively borrowed while C advances its mappings.
        // CString destinations and the paired stack MapperState outlive this
        // synchronous call; C retains neither, and mapper panics wait for cleanup.
        let result = unsafe {
            ffi::mmapq_batch_queue_transfer_copy_normal_to_batch(
                reader.as_ptr(), dir.as_ptr(), name.as_ptr(),
                from_index, to_index, mode,
                None, None, std::ptr::null_mut(),
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        mapper_state.resume();
        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }
}
