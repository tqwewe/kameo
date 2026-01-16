//! Platform abstraction layer for high-performance stream writing
//!
//! This module provides a trait-based abstraction over different I/O backends,
//! allowing optimal performance on each platform:
//! - Linux 5.1+: io_uring for zero-copy I/O
//! - Other platforms: Standard tokio async I/O

use async_trait::async_trait;
use std::io::Result;

/// A write command containing data to be sent
#[derive(Debug, Clone)]
pub struct WriteCommand {
    pub data: bytes::Bytes,
    pub priority: u8,
}

/// Platform-agnostic trait for high-performance stream writing
#[async_trait]
pub trait StreamWriter: Send + Sync {
    /// Write a batch of commands to the stream
    ///
    /// Implementations should optimize for batching and minimize syscalls.
    /// Returns the number of bytes written.
    async fn write_batch(&mut self, commands: &[WriteCommand]) -> Result<usize>;

    /// Flush any buffered data to the stream
    ///
    /// Ensures all pending writes are completed.
    async fn flush(&mut self) -> Result<()>;

    /// Check if the writer supports zero-copy operations
    fn supports_zero_copy(&self) -> bool {
        false
    }

    /// Get a buffer for zero-copy writing (if supported)
    ///
    /// Returns None if zero-copy is not supported.
    fn get_zero_copy_buffer(&mut self, _size: usize) -> Option<&mut [u8]> {
        None
    }

    /// Submit a zero-copy write operation
    ///
    /// The buffer must have been obtained from `get_zero_copy_buffer`.
    async fn submit_zero_copy(&mut self, _len: usize) -> Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Zero-copy not supported on this platform",
        ))
    }
}

// Platform-specific modules
#[cfg(all(target_os = "linux", feature = "io_uring", not(target_env = "musl")))]
pub mod io_uring_writer;

pub mod standard_writer;

// Export the appropriate implementation for the current platform
#[cfg(all(target_os = "linux", feature = "io_uring", not(target_env = "musl")))]
pub use io_uring_writer::IoUringStreamWriter as PlatformStreamWriter;

#[cfg(not(all(target_os = "linux", feature = "io_uring", not(target_env = "musl"))))]
pub use standard_writer::StandardStreamWriter as PlatformStreamWriter;
