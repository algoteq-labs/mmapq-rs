//! Queue export/import — serialise queues to flat binary streams and restore them.

use std::ffi::CString;

use crate::error::MmapError;
use crate::ffi;
use crate::reader::IndexedReader;
use crate::batch::batch_reader::IndexedBatchReader;

/// Stateless export/import helpers for single-message queues.
pub struct QueueTransfer;

impl QueueTransfer {
    /// Export entries `[from_index, to_index]` from a reader to a file.
    ///
    /// Writes a 64-byte `ExportMeta` header followed by framed entries:
    /// `[uint16 appender_id][int32 length][payload bytes]`.
    ///
    /// Returns the number of entries written.
    pub fn export(
        reader: &IndexedReader,
        output_path: &str,
        from_index: i64,
        to_index: i64,
    ) -> Result<u64, MmapError> {
        let path = CString::new(output_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;

        let file = unsafe { libc::fopen(path.as_ptr(), b"wb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let result = unsafe {
            ffi::queue_transfer_export(
                reader.as_ptr(),
                file,
                from_index,
                to_index,
                ffi::QUEUE_TRANSFER_REBASE,
                None,
                None,
                std::ptr::null_mut(),
            )
        };

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(MmapError::Io(std::io::Error::last_os_error()))
        }
    }

    /// Import entries from a flat stream file into a queue.
    ///
    /// Reads the `ExportMeta` header, creates (or opens) the destination queue,
    /// and appends each entry.
    ///
    /// Returns the number of entries imported.
    pub fn import(
        stream_path: &str,
        directory: &str,
        queue_name: &str,
        start_index: i64,
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

        let result = unsafe {
            ffi::queue_transfer_import(
                file,
                dir.as_ptr(),
                name.as_ptr(),
                start_index,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(MmapError::Io(std::io::Error::last_os_error()))
        }
    }
}

/// Stateless export/import helpers for batch queues.
pub struct BatchQueueTransfer;

impl BatchQueueTransfer {
    /// Export batches `[from_index, to_index]` from a batch reader to a file.
    pub fn export(
        reader: &IndexedBatchReader,
        output_path: &str,
        from_index: i64,
        to_index: i64,
    ) -> Result<u64, MmapError> {
        let path = CString::new(output_path)
            .map_err(|_| MmapError::Internal("path contains null byte".into()))?;

        let file = unsafe { libc::fopen(path.as_ptr(), b"wb\0".as_ptr() as *const _) };
        if file.is_null() {
            return Err(MmapError::Io(std::io::Error::last_os_error()));
        }

        let result = unsafe {
            ffi::batch_queue_transfer_export(
                reader.as_ptr(),
                file,
                from_index,
                to_index,
                ffi::QUEUE_TRANSFER_REBASE,
                None,
                None,
                std::ptr::null_mut(),
            )
        };

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(MmapError::Io(std::io::Error::last_os_error()))
        }
    }

    /// Import batch entries from a flat stream file into a batch queue.
    pub fn import(
        stream_path: &str,
        directory: &str,
        queue_name: &str,
        start_index: i64,
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

        let result = unsafe {
            ffi::batch_queue_transfer_import(
                file,
                dir.as_ptr(),
                name.as_ptr(),
                start_index,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };

        unsafe { libc::fclose(file) };

        if result >= 0 {
            Ok(result as u64)
        } else {
            Err(MmapError::Io(std::io::Error::last_os_error()))
        }
    }
}
