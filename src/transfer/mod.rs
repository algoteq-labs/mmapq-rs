//! Queue transfer — export, import, and copy queues to/from portable stream files.
//!
//! `QueueTransfer` exports/imports/copies single-message queues.
//! `BatchQueueTransfer` does the same for batch queues.

use std::ffi::CString;

use crate::error::MmapError;
use crate::ffi;
use crate::reader::IndexedReader;
use crate::batch::batch_reader::IndexedBatchReader;

pub use ffi::ConflictBehavior;

/// A Rust closure that maps a source appender ID to a destination appender ID.
/// Return `None` for auto-assign, or `Some(id)` for a specific ID.
pub type AppenderIdMapperFn = Box<dyn Fn(u16) -> Option<u8>>;

unsafe extern "C" fn mapper_trampoline(source_aid: u16, ctx: *mut std::ffi::c_void) -> i32 {
    if ctx.is_null() {
        return source_aid as i32;
    }
    let cb = &*(ctx as *mut AppenderIdMapperFn);
    match cb(source_aid) {
        Some(id) => id as i32,
        None => ffi::APPENDER_ID_AUTO,
    }
}

/// Prepare a mapper callback and context for FFI.
/// Returns (callback_fn, context_ptr, box_to_keep_alive).
/// The box MUST live until the FFI call returns.
fn setup_mapper(mapper: Option<AppenderIdMapperFn>)
    -> (ffi::AppenderIdMapper, *mut std::ffi::c_void, Option<Box<AppenderIdMapperFn>>)
{
    match mapper {
        Some(f) => {
            let b = Box::new(f);
            let ptr = Box::as_ref(&b) as *const AppenderIdMapperFn as *mut std::ffi::c_void;
            (Some(mapper_trampoline as unsafe extern "C" fn(_, _) -> _),
             ptr,
             Some(b)) // keep alive
        }
        None => (None, std::ptr::null_mut(), None),
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
        reader: &IndexedReader,
        output_path: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
    ) -> Result<u64, MmapError> {
        let path = CString::new(output_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;

        let file = unsafe { libc::fopen(path.as_ptr(), b"wb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);

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

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
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

        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let (cb, ctx, _owned) = setup_mapper(mapper);

        let result = unsafe {
            ffi::mmapq_queue_transfer_import(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        unsafe { libc::fclose(file) };

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

        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let (cb, ctx, _owned) = setup_mapper(mapper);

        let result = unsafe {
            ffi::mmapq_queue_transfer_import_batch_stream(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy entries from a source queue reader to a new destination queue.
    pub fn copy(
        reader: &IndexedReader,
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
        let (cb, ctx, _owned) = setup_mapper(mapper);

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

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy batches from a batch queue reader and flatten each batch's messages
    /// into individual entries in a normal queue.
    pub fn copy_from_batch(
        reader: &IndexedBatchReader,
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
        let (cb, ctx, _owned) = setup_mapper(mapper);

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
        reader: &IndexedBatchReader,
        output_path: &str,
        from_index: i64,
        to_index: i64,
        start_index_mode: StartIndexMode,
    ) -> Result<u64, MmapError> {
        let path = CString::new(output_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;

        let file = unsafe { libc::fopen(path.as_ptr(), b"wb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);

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

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
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

        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let (cb, ctx, _owned) = setup_mapper(mapper);

        let result = unsafe {
            ffi::mmapq_batch_queue_transfer_import(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        unsafe { libc::fclose(file) };

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

        let file = unsafe { libc::fopen(path.as_ptr(), b"rb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let mode = to_c_mode(start_index_mode);
        let (cb, ctx, _owned) = setup_mapper(mapper);

        let result = unsafe {
            ffi::mmapq_batch_queue_transfer_import_flat_stream(
                file, dir.as_ptr(), name.as_ptr(),
                mode,
                std::ptr::null_mut(), std::ptr::null_mut(),
                allow_append as i32,
                cb, ctx, on_conflict,
            )
        };

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy batches from a source batch reader to a new destination batch queue.
    pub fn copy(
        reader: &IndexedBatchReader,
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
        let (cb, ctx, _owned) = setup_mapper(mapper);

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

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }

    /// Copy entries from a normal queue reader and place each entry as a
    /// single-message batch in a batch queue.
    pub fn copy_from_normal(
        reader: &IndexedReader,
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
        let (cb, ctx, _owned) = setup_mapper(mapper);

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

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(crate::error::map_rc(result as i32))
        }
    }
}
