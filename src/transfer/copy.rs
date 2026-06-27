//! Direct queue-to-queue copy without intermediate file.
//!
//! Implements `QueueTransfer::copy` and `BatchQueueTransfer::copy` using
//! the C `queue_transfer_copy` / `batch_queue_transfer_copy` functions.
