//! Standard tokio-based stream writer implementation
//!
//! Used on platforms without io_uring support (macOS, Windows, older Linux)

use super::{StreamWriter, WriteCommand};
use crate::connection_pool::TCP_BUFFER_SIZE; // Use shared constant!
use async_trait::async_trait;
use std::io::Result;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// Standard stream writer using tokio's async I/O
pub struct StandardStreamWriter {
    stream: BufWriter<TcpStream>,
}

impl StandardStreamWriter {
    /// Create a new standard stream writer
    pub fn new(stream: TcpStream) -> Self {
        // Use TCP_BUFFER_SIZE from master constant - no more magic numbers!
        // This ensures the BufWriter can handle messages up to the streaming threshold
        let stream = BufWriter::with_capacity(TCP_BUFFER_SIZE, stream);
        Self { stream }
    }
}

#[async_trait]
impl StreamWriter for StandardStreamWriter {
    async fn write_batch(&mut self, commands: &[WriteCommand]) -> Result<usize> {
        if commands.is_empty() {
            return Ok(0);
        }

        // Use vectored I/O for efficient batch writing
        if commands.len() > 1 {
            // Create IoSlice array for vectored write - no allocation, just references
            let slices: Vec<std::io::IoSlice> = commands
                .iter()
                .map(|cmd| std::io::IoSlice::new(&cmd.data))
                .collect();

            // Write all slices in one syscall
            self.stream.write_vectored(&slices).await
        } else {
            // Single command, write directly
            self.stream.write_all(&commands[0].data).await?;
            Ok(commands[0].data.len())
        }
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await
    }

    fn supports_zero_copy(&self) -> bool {
        false
    }
}
