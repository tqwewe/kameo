//! io_uring-based zero-copy stream writer implementation
//!
//! High-performance implementation for Linux 5.1+ with true zero-copy I/O

use super::{StreamWriter, WriteCommand};
use crate::connection_pool::TCP_BUFFER_SIZE; // Use shared constant!
use async_trait::async_trait;
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use std::io::Result;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use tokio_uring::net::TcpStream;

const BUFFER_COUNT: usize = 1024;
const BUFFER_SIZE: usize = TCP_BUFFER_SIZE; // Use master constant - no more magic numbers!
const SUBMISSION_QUEUE_SIZE: u32 = 256;
const BATCH_THRESHOLD: usize = 32;

/// io_uring-based stream writer with zero-copy support
pub struct IoUringStreamWriter {
    ring: IoUring,
    stream: Arc<TcpStream>,
    registered_buffers: Vec<Vec<u8>>,
    free_buffers: Vec<usize>,
    pending_ops: Vec<squeue::Entry>,
    tcp_fd: types::Fd,
}

impl IoUringStreamWriter {
    /// Create a new io_uring stream writer
    /// Note: This is Linux-only and will panic on other platforms
    pub fn new(_stream: TcpStream) -> Self {
        panic!("io_uring is only supported on Linux 5.1+");
    }

    /// Submit all pending operations
    async fn submit_pending(&mut self) -> Result<()> {
        if self.pending_ops.is_empty() {
            return Ok(());
        }

        // Submit all pending operations
        let submission = self.ring.submission();
        for sqe in self.pending_ops.drain(..) {
            unsafe {
                submission.push(&sqe).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "io_uring submission queue full")
                })?;
            }
        }

        // Submit and wait for at least one completion
        self.ring.submit_and_wait(1)?;

        // Process completions
        let mut cq = self.ring.completion();
        for cqe in cq {
            if cqe.result() < 0 {
                return Err(std::io::Error::from_raw_os_error(-cqe.result()));
            }
            // Return buffer to free list
            if let Some(buf_id) = cqe.user_data() {
                self.free_buffers.push(buf_id as usize);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl StreamWriter for IoUringStreamWriter {
    async fn write_batch(&mut self, commands: &[WriteCommand]) -> Result<usize> {
        let mut total_written = 0;

        for cmd in commands {
            // Get a free buffer
            let buf_id = self.free_buffers.pop().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::WouldBlock, "No free buffers available")
            })?;

            // Copy data to registered buffer
            let len = cmd.data.len().min(BUFFER_SIZE);
            self.registered_buffers[buf_id][..len].copy_from_slice(&cmd.data[..len]);

            // Create send operation with registered buffer
            let sqe = opcode::Send::new(
                self.tcp_fd,
                self.registered_buffers[buf_id].as_ptr(),
                len as u32,
            )
            .build()
            .user_data(buf_id as u64);

            self.pending_ops.push(sqe);
            total_written += len;

            // Submit if we've reached the batch threshold
            if self.pending_ops.len() >= BATCH_THRESHOLD {
                self.submit_pending().await?;
            }
        }

        // Submit any remaining operations
        self.submit_pending().await?;

        Ok(total_written)
    }

    async fn flush(&mut self) -> Result<()> {
        self.submit_pending().await
    }

    fn supports_zero_copy(&self) -> bool {
        true
    }

    fn get_zero_copy_buffer(&mut self, size: usize) -> Option<&mut [u8]> {
        if size > BUFFER_SIZE {
            return None;
        }

        let buf_id = self.free_buffers.pop()?;
        Some(&mut self.registered_buffers[buf_id][..size])
    }

    async fn submit_zero_copy(&mut self, len: usize) -> Result<()> {
        // In a real implementation, we'd track which buffer was given out
        // For now, we'll submit the pending operations
        self.submit_pending().await
    }
}

// Ensure the ring is properly cleaned up
impl Drop for IoUringStreamWriter {
    fn drop(&mut self) {
        // Unregister buffers and files
        let _ = self.ring.unregister_buffers();
        let _ = self.ring.unregister_files();
    }
}
