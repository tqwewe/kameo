//! High-performance streaming API for large messages
//!
//! This module provides a Rust-idiomatic streaming API for sending large messages
//! like PreBacktest (35MB+) with maximum performance. Streams bypass the ring buffer
//! and write directly to the socket for minimal latency.
//!
//! # Example
//! ```ignore
//! // Create a stream for sending PreBacktest messages
//! let mut stream = actor_ref.create_stream::<PreBacktest>("backtest_data").await?;
//!
//! // Write messages to the stream
//! stream.send(large_prebacktest).await?;
//! stream.send(another_prebacktest).await?;
//!
//! // Close the stream when done
//! stream.close().await?;
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use rkyv::{Archive, Serialize as RSerialize};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use super::type_hash::HasTypeHash;
use crate::actor::ActorId;
use crate::error::SendError as CoreSendError;
use kameo_remote::connection_pool::ConnectionHandle;

/// Information about a stream's current state for monitoring and cleanup
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Unique identifier for this stream
    pub stream_id: u64,
    /// Human-readable name describing the stream's purpose
    pub name: String,
    /// Number of chunks received so far
    pub chunks_received: usize,
    /// Total number of chunks expected (if known)
    pub expected_chunks: Option<u32>,
    /// Current memory usage in bytes
    pub memory_usage: usize,
    /// Total time since stream creation
    pub age: std::time::Duration,
    /// Time since last chunk was received
    pub last_activity: std::time::Duration,
}

/// Errors that can occur during streaming operations
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// Failed to serialize message data
    #[error("serialization failed")]
    SerializationFailed,
    /// Network connection was closed unexpectedly
    #[error("connection closed")]
    ConnectionClosed,
    /// Not connected to remote endpoint
    #[error("not connected")]
    NotConnected,
    /// Stream has been closed
    #[error("stream closed")]
    StreamClosed,
    /// Received invalid or corrupted frame data
    #[error("invalid frame")]
    InvalidFrame,
}

impl<M, E> From<StreamError> for CoreSendError<M, E> {
    fn from(err: StreamError) -> Self {
        match err {
            StreamError::SerializationFailed => CoreSendError::ActorStopped,
            StreamError::ConnectionClosed => CoreSendError::ActorStopped,
            StreamError::NotConnected => CoreSendError::ActorStopped,
            StreamError::StreamClosed => CoreSendError::ActorStopped,
            StreamError::InvalidFrame => CoreSendError::ActorStopped,
        }
    }
}

/// Stream ID generator for unique stream identification
static STREAM_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A high-performance stream for sending large messages
///
/// This stream writes directly to the socket, bypassing the ring buffer
/// for maximum performance with large messages.
#[derive(Debug)]
pub struct MessageStream<M> {
    /// Unique stream identifier
    stream_id: u64,
    /// The stream name/purpose
    name: String,
    /// Target actor ID
    actor_id: ActorId,
    /// Connection handle for direct socket access
    connection: ConnectionHandle,
    /// Type marker for the message type
    _phantom: PhantomData<M>,
}

impl<M> MessageStream<M>
where
    M: HasTypeHash
        + Send
        + Archive
        + for<'a> RSerialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    /// Create a new message stream
    pub(crate) fn new(name: String, actor_id: ActorId, connection: ConnectionHandle) -> Self {
        let stream_id = STREAM_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            stream_id,
            name,
            actor_id,
            connection,
            _phantom: PhantomData,
        }
    }

    /// Get the stream ID
    pub fn id(&self) -> u64 {
        self.stream_id
    }

    /// Get the stream name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Send a message through the stream
    ///
    /// This method serializes the message and writes it directly to the socket
    /// with proper framing. Large messages are automatically chunked.
    pub async fn send(&mut self, message: M) -> Result<(), StreamError> {
        // Get compile-time type hash
        let type_hash = M::TYPE_HASH.as_u32();

        // Serialize the message to AlignedVec (owned)
        let aligned_vec = rkyv::to_bytes::<rkyv::rancor::Error>(&message)
            .map_err(|_| StreamError::SerializationFailed)?;

        // Use zero-copy path directly with AlignedVec
        self.send_rkyv_aligned(type_hash, aligned_vec).await
    }

    /// Send raw bytes through the stream (for pre-serialized data)
    pub async fn send_raw(&mut self, type_hash: u32, data: &[u8]) -> Result<(), StreamError> {
        self.write_frame(type_hash, data).await
    }

    /// Send pre-serialized data with zero-copy (optimal for rkyv AlignedVec)
    pub async fn send_pre_serialized(
        &mut self,
        type_hash: u32,
        data: bytes::Bytes,
    ) -> Result<(), StreamError> {
        self.write_frame_owned(type_hash, data).await
    }

    /// Send pre-serialized rkyv data directly (zero-copy from AlignedVec)
    pub async fn send_rkyv_aligned(
        &mut self,
        type_hash: u32,
        aligned_vec: rkyv::util::AlignedVec,
    ) -> Result<(), StreamError> {
        // Convert AlignedVec to Bytes with zero-copy transfer of ownership
        let data = bytes::Bytes::from(aligned_vec.into_vec());
        self.write_frame_owned(type_hash, data).await
    }

    /// Close the stream
    ///
    /// This sends a stream end marker to the receiver
    pub async fn close(self) -> Result<(), StreamError> {
        // Create StreamHeader for end message
        let header = kameo_remote::StreamHeader {
            stream_id: self.stream_id,
            total_size: 0, // Not used for end
            chunk_size: 0,
            chunk_index: 0,
            type_hash: M::TYPE_HASH.as_u32(),
            actor_id: self.actor_id.into_u64(),
        };

        let header_bytes = header.to_bytes();
        let frame_size = 8 + header_bytes.len(); // Header + payload

        let mut frame = BytesMut::with_capacity(4 + frame_size);
        frame.put_u32(frame_size as u32);
        frame.put_u8(0x12); // MessageType::StreamEnd
        frame.put_u16(0); // Correlation ID (none for streaming)
        frame.put_slice(&[0u8; 5]); // Reserved
        frame.put_slice(&header_bytes);

        self.connection
            .send_bytes_zero_copy(frame.freeze())
            .map_err(|_| StreamError::ConnectionClosed)?;

        Ok(())
    }

    /// Write a frame to the stream (legacy method - copies data)
    async fn write_frame(&mut self, type_hash: u32, data: &[u8]) -> Result<(), StreamError> {
        // Convert to owned data for zero-copy path
        let owned_data = bytes::Bytes::copy_from_slice(data);
        self.write_frame_owned(type_hash, owned_data).await
    }

    /// Write a frame to the stream with owned data (zero-copy)
    async fn write_frame_owned(
        &mut self,
        type_hash: u32,
        data: bytes::Bytes,
    ) -> Result<(), StreamError> {
        const MAX_SINGLE_FRAME: usize = 256 * 1024; // 256KB threshold

        if data.len() <= MAX_SINGLE_FRAME {
            // Single frame for small messages
            self.write_single_frame_owned(type_hash, data).await
        } else {
            // Multiple frames for large messages - use vectored chunking
            self.write_chunked_frames_vectored(type_hash, data).await
        }
    }

    /// Write a single frame with owned data (zero-copy)
    async fn write_single_frame_owned(
        &mut self,
        type_hash: u32,
        data: bytes::Bytes,
    ) -> Result<(), StreamError> {
        // Create StreamHeader
        let header = kameo_remote::StreamHeader {
            stream_id: self.stream_id,
            total_size: data.len() as u64,
            chunk_size: data.len() as u32,
            chunk_index: 0,
            type_hash,
            actor_id: self.actor_id.into_u64(),
        };

        let header_bytes = header.to_bytes();
        let frame_size = 8 + header_bytes.len(); // Protocol header + StreamHeader (no payload copy needed)

        let mut frame_header = BytesMut::with_capacity(4 + frame_size);
        frame_header.put_u32((frame_size + data.len()) as u32); // Total size including payload
        frame_header.put_u8(0x11); // MessageType::StreamData
        frame_header.put_u16(0); // Correlation ID (none for streaming)
        frame_header.put_slice(&[0u8; 5]); // Reserved
        frame_header.put_slice(&header_bytes);

        // Use vectored write to avoid copying payload
        self.connection
            .write_bytes_vectored(frame_header.freeze(), data)
            .map_err(|_| StreamError::ConnectionClosed)?;

        Ok(())
    }

    /// Write chunked frames for large messages with vectored I/O (zero-copy)
    async fn write_chunked_frames_vectored(
        &mut self,
        type_hash: u32,
        data: bytes::Bytes,
    ) -> Result<(), StreamError> {
        const MAX_CHUNK_SIZE: usize = 256 * 1024; // 256KB chunks (matching kameo_remote)

        let total_chunks = data.len().div_ceil(MAX_CHUNK_SIZE);

        // Send StreamStart first
        let start_header = kameo_remote::StreamHeader {
            stream_id: self.stream_id,
            total_size: data.len() as u64,
            chunk_size: 0, // Not used for start
            chunk_index: 0,
            type_hash,
            actor_id: self.actor_id.into_u64(),
        };

        let header_bytes = start_header.to_bytes();
        let mut start_frame = BytesMut::with_capacity(4 + 8 + header_bytes.len());
        start_frame.put_u32((8 + header_bytes.len()) as u32);
        start_frame.put_u8(0x10); // MessageType::StreamStart
        start_frame.put_u16(0); // Correlation ID
        start_frame.put_slice(&[0u8; 5]); // Reserved
        start_frame.put_slice(&header_bytes);

        self.connection
            .send_bytes_zero_copy(start_frame.freeze())
            .map_err(|_| StreamError::ConnectionClosed)?;

        // Batch chunks for optimal vectored I/O (reduce syscalls)
        const CHUNKS_PER_BATCH: usize = 8; // Batch up to 8 chunks per vectored write
        let mut offset = 0;

        // Process chunks in batches
        for batch_start in (0..total_chunks).step_by(CHUNKS_PER_BATCH) {
            let batch_end = std::cmp::min(batch_start + CHUNKS_PER_BATCH, total_chunks);
            let mut batch_frames = Vec::with_capacity((batch_end - batch_start) * 2); // header + payload per chunk

            for chunk_index in batch_start..batch_end {
                let chunk_size = std::cmp::min(MAX_CHUNK_SIZE, data.len() - offset);
                let chunk_data = data.slice(offset..offset + chunk_size);

                let chunk_header = kameo_remote::StreamHeader {
                    stream_id: self.stream_id,
                    total_size: data.len() as u64,
                    chunk_size: chunk_size as u32,
                    chunk_index: chunk_index as u32,
                    type_hash,
                    actor_id: self.actor_id.into_u64(),
                };

                let header_bytes = chunk_header.to_bytes();
                let mut frame_header = BytesMut::with_capacity(4 + 8 + header_bytes.len());
                frame_header.put_u32((8 + header_bytes.len() + chunk_size) as u32);
                frame_header.put_u8(0x11); // MessageType::StreamData
                frame_header.put_u16(0); // Correlation ID
                frame_header.put_slice(&[0u8; 5]); // Reserved
                frame_header.put_slice(&header_bytes);

                batch_frames.push(frame_header.freeze());
                batch_frames.push(chunk_data);

                offset += chunk_size;
            }

            // Send entire batch in single vectored write (maximum efficiency)
            self.connection
                .write_owned_chunks(batch_frames)
                .map_err(|_| StreamError::ConnectionClosed)?;
        }

        // Send StreamEnd
        let end_header = kameo_remote::StreamHeader {
            stream_id: self.stream_id,
            total_size: data.len() as u64,
            chunk_size: 0,
            chunk_index: total_chunks as u32,
            type_hash,
            actor_id: self.actor_id.into_u64(),
        };

        let header_bytes = end_header.to_bytes();
        let mut end_frame = BytesMut::with_capacity(4 + 8 + header_bytes.len());
        end_frame.put_u32((8 + header_bytes.len()) as u32);
        end_frame.put_u8(0x12); // MessageType::StreamEnd
        end_frame.put_u16(0); // Correlation ID
        end_frame.put_slice(&[0u8; 5]); // Reserved
        end_frame.put_slice(&header_bytes);

        self.connection
            .send_bytes_zero_copy(end_frame.freeze())
            .map_err(|_| StreamError::ConnectionClosed)?;

        Ok(())
    }
}

/// Stream factory for creating message streams
#[allow(async_fn_in_trait)]
pub trait StreamFactory {
    /// Create a new message stream for sending messages of type M
    async fn create_stream<M>(&self, name: &str) -> Result<MessageStream<M>, StreamError>
    where
        M: HasTypeHash
            + Send
            + Archive
            + for<'a> RSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >;
}

/// Receiver side of a message stream with timeout and cleanup support
#[derive(Debug)]
pub struct MessageStreamReceiver<M> {
    stream_id: u64,
    name: String,
    /// Buffer for assembling chunked messages
    chunks: std::collections::BTreeMap<u32, Bytes>,
    expected_chunks: Option<u32>,
    total_size: Option<usize>,
    /// Creation timestamp for timeout tracking
    created_at: std::time::Instant,
    /// Last activity timestamp for timeout tracking
    last_activity: std::time::Instant,
    /// Maximum allowed memory usage for this stream
    max_memory_bytes: usize,
    _phantom: PhantomData<M>,
}

impl<M> MessageStreamReceiver<M>
where
    M: Archive,
    <M as Archive>::Archived: for<'a> rkyv::Deserialize<M, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    /// Create a new stream receiver with default timeout and memory limits
    pub fn new(stream_id: u64, name: String) -> Self {
        let now = std::time::Instant::now();
        Self {
            stream_id,
            name,
            chunks: Default::default(),
            expected_chunks: None,
            total_size: None,
            created_at: now,
            last_activity: now,
            max_memory_bytes: 32 * 1024 * 1024, // 32MB default limit per stream
            _phantom: PhantomData,
        }
    }

    /// Create a new stream receiver with custom memory limit
    pub fn new_with_limits(stream_id: u64, name: String, max_memory_bytes: usize) -> Self {
        let now = std::time::Instant::now();
        Self {
            stream_id,
            name,
            chunks: Default::default(),
            expected_chunks: None,
            total_size: None,
            created_at: now,
            last_activity: now,
            max_memory_bytes,
            _phantom: PhantomData,
        }
    }

    /// Check if this stream has timed out
    pub fn is_timed_out(&self, timeout_duration: std::time::Duration) -> bool {
        self.last_activity.elapsed() > timeout_duration
    }

    /// Check if this stream has exceeded its age limit
    pub fn is_too_old(&self, max_age: std::time::Duration) -> bool {
        self.created_at.elapsed() > max_age
    }

    /// Get current memory usage of this stream
    pub fn memory_usage(&self) -> usize {
        self.chunks.values().map(|chunk| chunk.len()).sum()
    }

    /// Check if this stream has exceeded its memory limit
    pub fn is_over_memory_limit(&self) -> bool {
        self.memory_usage() > self.max_memory_bytes
    }

    /// Get stream information for debugging
    pub fn stream_info(&self) -> StreamInfo {
        StreamInfo {
            stream_id: self.stream_id,
            name: self.name.clone(),
            chunks_received: self.chunks.len(),
            expected_chunks: self.expected_chunks,
            memory_usage: self.memory_usage(),
            age: self.created_at.elapsed(),
            last_activity: self.last_activity.elapsed(),
        }
    }

    /// Force cleanup of this stream (for timeout/memory limit enforcement)
    pub fn cleanup(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let info = self.stream_info();
        self.chunks.clear();
        self.expected_chunks = None;
        self.total_size = None;

        Err(format!(
            "Stream {} ({}) forcibly cleaned up - chunks: {}/{:?}, memory: {} bytes, age: {:?}, inactive: {:?}",
            info.stream_id, info.name, info.chunks_received, info.expected_chunks,
            info.memory_usage, info.age, info.last_activity
        ).into())
    }

    /// Process a single frame
    pub fn process_frame(
        &mut self,
        data: &[u8],
    ) -> Result<Option<M>, Box<dyn std::error::Error + Send + Sync>> {
        // Single frame - deserialize directly
        rkyv::from_bytes::<M, rkyv::rancor::Error>(data)
            .map(Some)
            .map_err(|e| format!("Failed to deserialize stream message: {:?}", e).into())
    }

    /// Process a chunked frame
    pub fn process_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        total_size: usize,
        chunk_data: Bytes,
    ) -> Result<Option<M>, Box<dyn std::error::Error + Send + Sync>> {
        // Update activity timestamp
        self.last_activity = std::time::Instant::now();

        // First chunk - set expectations
        if chunk_index == 0 {
            self.expected_chunks = Some(total_chunks);
            self.total_size = Some(total_size);
            self.chunks.clear();
        }

        // Store chunk
        self.chunks.insert(chunk_index, chunk_data);

        // Check memory limits after adding chunk
        if self.is_over_memory_limit() {
            let error_msg = format!(
                "Stream {} ({}) exceeded memory limit: {} bytes used, {} bytes max",
                self.stream_id,
                self.name,
                self.memory_usage(),
                self.max_memory_bytes
            );

            // Clear chunks to prevent further memory growth
            self.chunks.clear();
            self.expected_chunks = None;
            self.total_size = None;

            return Err(error_msg.into());
        }

        // Check if we have all chunks by verifying all indices 0..N-1 are present
        let expected_chunks = self.expected_chunks.unwrap_or(0);
        if expected_chunks > 0 && self.chunks.len() == expected_chunks as usize {
            // Verify that all chunk indices 0..N-1 are actually present (not just count)
            let all_chunks_present = (0..expected_chunks).all(|i| self.chunks.contains_key(&i));

            if all_chunks_present {
                // Assemble complete message - all chunks guaranteed to be present
                let mut complete_data = BytesMut::with_capacity(self.total_size.unwrap_or(0));

                for i in 0..total_chunks {
                    // Safe to unwrap because we verified all chunks are present
                    let chunk = self
                        .chunks
                        .remove(&i)
                        .expect("Chunk must be present after verification");
                    complete_data.put_slice(&chunk);
                }

                // Deserialize
                let message = rkyv::from_bytes::<M, rkyv::rancor::Error>(&complete_data)
                    .map_err(|e| format!("Failed to deserialize assembled message: {:?}", e))?;

                // Reset state
                self.expected_chunks = None;
                self.total_size = None;
                self.chunks.clear();

                Ok(Some(message))
            } else {
                // We have the right count but missing some indices - this indicates corruption
                let missing_indices: Vec<u32> = (0..expected_chunks)
                    .filter(|i| !self.chunks.contains_key(i))
                    .collect();
                let present_indices: Vec<u32> = self.chunks.keys().copied().collect();

                Err(format!(
                    "Stream corruption detected: expected {} chunks (0..{}), have {} chunks, but missing indices {:?}. Present indices: {:?}",
                    expected_chunks, expected_chunks - 1, self.chunks.len(), missing_indices, present_indices
                ).into())
            }
        } else {
            Ok(None)
        }
    }
}

// Extension trait for DistributedActorRef to add streaming support
use super::distributed_actor_ref::DistributedActorRef;

impl StreamFactory for DistributedActorRef<Box<super::kameo_transport::KameoTransport>> {
    async fn create_stream<M>(&self, name: &str) -> Result<MessageStream<M>, StreamError>
    where
        M: HasTypeHash
            + Send
            + Archive
            + for<'a> RSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        if let Some(ref connection) = self.connection {
            // Create a new stream - start message is sent when chunking begins
            Ok(MessageStream::new(
                name.to_string(),
                self.actor_id,
                connection.clone(),
            ))
        } else {
            Err(StreamError::NotConnected)
        }
    }
}
