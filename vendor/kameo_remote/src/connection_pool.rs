use bytes::Buf;
use futures::FutureExt;
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicUsize, Ordering};
use std::{collections::HashMap, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, warn};

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
use sha2::{Digest, Sha256};

use crate::{
    current_timestamp,
    framing,
    registry::{GossipRegistry, RegistryMessage},
    GossipError, Result,
};

// ==== SINGLE SOURCE OF TRUTH FOR ALL BUFFER SIZES ====
// THIS is the ONLY place we define the master buffer size!
// Change this ONE constant to adjust ALL buffer behavior system-wide.
pub const MASTER_BUFFER_SIZE: usize = 1024 * 1024; // 1MB - THE source of truth

// All other buffer sizes derive from the master constant - NO MAGIC NUMBERS!
pub const TCP_BUFFER_SIZE: usize = MASTER_BUFFER_SIZE; // BufWriter & io_uring buffer
pub const STREAM_CHUNK_SIZE: usize = MASTER_BUFFER_SIZE; // Streaming chunk size
pub const STREAMING_THRESHOLD: usize = MASTER_BUFFER_SIZE.saturating_sub(1024); // Just under buffer limit

// Ring buffer configuration (NUMBER OF SLOTS, not bytes!)
pub const RING_BUFFER_SLOTS: usize = 1024; // Number of WriteCommand slots in ring buffer
                                           // Note: Each slot can hold any size message - this is NOT a byte limit
                                           // Note: Streaming threshold ensures messages fit within TCP_BUFFER_SIZE
const CONTROL_RESERVED_SLOTS: usize = 32; // Reserved permits for control/tell/response lanes
const WRITER_MAX_LATENCY: Duration = Duration::from_micros(100);

fn control_reserved_slots(capacity: usize) -> usize {
    let min_reserved = CONTROL_RESERVED_SLOTS.min(capacity);
    let max_reserved = (capacity / 2).max(1);
    min_reserved.min(max_reserved)
}

fn should_flush(
    bytes_since_flush: usize,
    ring_empty: bool,
    elapsed: Duration,
    flush_threshold: usize,
    max_latency: Duration,
) -> bool {
    if bytes_since_flush == 0 {
        return false;
    }

    if bytes_since_flush >= flush_threshold {
        return true;
    }

    if ring_empty {
        return true;
    }

    elapsed >= max_latency
}

/// Buffer configuration that encapsulates all buffer-related settings
///
/// This struct ensures that the streaming threshold is always derived from the actual buffer size,
/// preventing the disconnection between buffer size and streaming decisions that causes message loss.
#[derive(Debug, Clone)]
pub struct BufferConfig {
    ring_buffer_slots: usize, // Number of message slots in ring buffer
    tcp_buffer_size: usize,   // Size in bytes for TCP buffers
    ask_inflight_limit: usize,
}

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
pub(crate) fn process_mock_request(request: &str) -> Vec<u8> {
    if let Some(payload) = request.strip_prefix("ECHO:") {
        return format!("ECHOED:{}", payload).into_bytes();
    }
    if let Some(payload) = request.strip_prefix("REVERSE:") {
        let reversed: String = payload.chars().rev().collect();
        return format!("REVERSED:{}", reversed).into_bytes();
    }
    if let Some(payload) = request.strip_prefix("COUNT:") {
        return format!("COUNTED:{} chars", payload.chars().count()).into_bytes();
    }
    if let Some(payload) = request.strip_prefix("HASH:") {
        let mut hasher = Sha256::new();
        hasher.update(payload.as_bytes());
        let digest = hasher.finalize();
        return format!("HASHED:{}", hex::encode(digest)).into_bytes();
    }

    format!(
        "RECEIVED:{} bytes, content: '{}'",
        request.len(),
        request
    )
    .into_bytes()
}

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
pub(crate) fn process_mock_request_payload(payload: &[u8]) -> Vec<u8> {
    if payload.len() == 4 {
        let value = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        return (value + 1).to_be_bytes().to_vec();
    }

    let request_str = String::from_utf8_lossy(payload);
    process_mock_request(&request_str)
}

impl BufferConfig {
    /// Create a new BufferConfig with validation
    ///
    /// # Arguments
    /// * `tcp_buffer_size` - The size of TCP buffers in bytes
    ///
    /// # Errors
    /// Returns an error if the buffer size is less than 256KB for safety
    pub fn new(tcp_buffer_size: usize) -> Result<Self> {
        // Enforce minimum size of 256KB for safety
        if tcp_buffer_size < 256 * 1024 {
            return Err(GossipError::InvalidConfig(format!(
                "TCP buffer must be at least 256KB, got {}KB",
                tcp_buffer_size / 1024
            )));
        }
        Ok(Self {
            ring_buffer_slots: RING_BUFFER_SLOTS, // Use constant for slots
            tcp_buffer_size,
            ask_inflight_limit: crate::config::DEFAULT_ASK_INFLIGHT_LIMIT,
        })
    }

    /// Get the ring buffer slot count (NOT bytes!)
    pub fn ring_buffer_slots(&self) -> usize {
        self.ring_buffer_slots
    }

    /// Max in-flight ask permits per connection.
    pub fn ask_inflight_limit(&self) -> usize {
        self.ask_inflight_limit
    }

    /// Override the default ask inflight limit.
    pub fn with_ask_inflight_limit(mut self, limit: usize) -> Self {
        self.ask_inflight_limit = limit;
        self
    }

    /// Calculate the streaming threshold based on buffer size
    ///
    /// The streaming threshold is always derived from the buffer size,
    /// leaving 1KB headroom for headers and serialization overhead.
    pub fn streaming_threshold(&self) -> usize {
        // Always derive from buffer size
        // Leave 1KB headroom for headers/overhead
        self.tcp_buffer_size.saturating_sub(1024)
    }

    /// Get the TCP buffer size for BufWriter and io_uring
    ///
    /// This ensures TCP buffers match the configured size, preventing bottlenecks.
    pub fn tcp_buffer_size(&self) -> usize {
        self.tcp_buffer_size
    }

    /// Create BufferConfig using the master buffer size (recommended)
    pub fn from_master() -> Self {
        Self {
            ring_buffer_slots: RING_BUFFER_SLOTS,
            tcp_buffer_size: MASTER_BUFFER_SIZE,
            ask_inflight_limit: crate::config::DEFAULT_ASK_INFLIGHT_LIMIT,
        }
    }
}

impl Default for BufferConfig {
    fn default() -> Self {
        // Use master constant instead of magic number!
        Self {
            ring_buffer_slots: RING_BUFFER_SLOTS,
            tcp_buffer_size: MASTER_BUFFER_SIZE,
            ask_inflight_limit: crate::config::DEFAULT_ASK_INFLIGHT_LIMIT,
        }
    }
}

/// Stream frame types for high-performance streaming protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameType {
    Data = 0x01,
    Ack = 0x02,
    Close = 0x03,
    Heartbeat = 0x04,
    TellAsk = 0x05,    // Regular tell/ask messages
    StreamData = 0x06, // Dedicated streaming data
}

/// Channel IDs for stream multiplexing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelId {
    TellAsk = 0x00,  // Regular tell/ask channel
    Stream1 = 0x01,  // Dedicated streaming channel 1
    Stream2 = 0x02,  // Dedicated streaming channel 2
    Stream3 = 0x03,  // Dedicated streaming channel 3
    Bulk = 0x04,     // Bulk data channel
    Priority = 0x05, // Priority streaming channel
    Global = 0xFF,   // Global channel for all operations
}

/// Stream frame flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameFlags {
    None = 0x00,
    More = 0x01,       // More frames to follow
    Compressed = 0x02, // Frame is compressed
    Encrypted = 0x04,  // Frame is encrypted
}

/// Stream frame header for structured messaging
#[derive(Debug, Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(derive(Debug))]
pub struct StreamFrameHeader {
    pub frame_type: u8,
    pub channel_id: u8,
    pub flags: u8,
    pub sequence_id: u16,
    pub payload_len: u32,
}

/// Lock-free connection state representation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ConnectionState {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Failed = 3,
}

impl From<u32> for ConnectionState {
    fn from(value: u32) -> Self {
        match value {
            0 => ConnectionState::Disconnected,
            1 => ConnectionState::Connecting,
            2 => ConnectionState::Connected,
            3 => ConnectionState::Failed,
            _ => ConnectionState::Failed,
        }
    }
}

/// Direction of the TCP connection relative to this node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionDirection {
    Inbound,
    Outbound,
}

/// Lock-free connection metadata
#[derive(Debug)]
pub struct LockFreeConnection {
    pub addr: SocketAddr,
    pub state: AtomicU32,       // ConnectionState
    pub last_used: AtomicUsize, // Timestamp
    pub bytes_written: AtomicUsize,
    pub bytes_read: AtomicUsize,
    pub failure_count: AtomicUsize,
    pub stream_handle: Option<Arc<LockFreeStreamHandle>>,
    pub(crate) correlation: Option<Arc<CorrelationTracker>>,
    pub direction: ConnectionDirection,
}

impl Clone for LockFreeConnection {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            state: AtomicU32::new(self.state.load(Ordering::Relaxed)),
            last_used: AtomicUsize::new(self.last_used.load(Ordering::Relaxed)),
            bytes_written: AtomicUsize::new(self.bytes_written.load(Ordering::Relaxed)),
            bytes_read: AtomicUsize::new(self.bytes_read.load(Ordering::Relaxed)),
            failure_count: AtomicUsize::new(self.failure_count.load(Ordering::Relaxed)),
            stream_handle: self.stream_handle.clone(),
            correlation: self.correlation.clone(),
            direction: self.direction,
        }
    }
}

impl LockFreeConnection {
    pub fn new(addr: SocketAddr, direction: ConnectionDirection) -> Self {
        Self {
            addr,
            state: AtomicU32::new(ConnectionState::Disconnected as u32),
            last_used: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            bytes_read: AtomicUsize::new(0),
            failure_count: AtomicUsize::new(0),
            stream_handle: None,
            correlation: Some(CorrelationTracker::new()),
            direction,
        }
    }

    pub fn get_state(&self) -> ConnectionState {
        self.state.load(Ordering::Acquire).into()
    }

    pub fn set_state(&self, state: ConnectionState) {
        self.state.store(state as u32, Ordering::Release);
    }

    pub fn try_set_state(&self, expected: ConnectionState, new: ConnectionState) -> bool {
        self.state
            .compare_exchange(
                expected as u32,
                new as u32,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn update_last_used(&self) {
        self.last_used
            .store(crate::current_timestamp() as usize, Ordering::Release);
    }

    pub fn increment_failure_count(&self) -> usize {
        self.failure_count.fetch_add(1, Ordering::AcqRel)
    }

    pub fn reset_failure_count(&self) {
        self.failure_count.store(0, Ordering::Release);
    }

    pub fn is_connected(&self) -> bool {
        self.get_state() == ConnectionState::Connected
    }

    pub fn is_failed(&self) -> bool {
        self.get_state() == ConnectionState::Failed
    }
}

/// Payloads for ring-buffered writes.
pub enum WritePayload {
    Single(bytes::Bytes),
    HeaderPayload {
        header: bytes::Bytes,
        payload: bytes::Bytes,
    },
    HeaderInline {
        header: [u8; 8],
        header_len: u8,
        payload: bytes::Bytes,
    },
    HeaderPooled {
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    },
    HeaderInlinePooled {
        header: [u8; 8],
        header_len: u8,
        prefix: Option<[u8; 8]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    },
    Buf(Box<dyn Buf + Send>),
}

impl std::fmt::Debug for WritePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WritePayload::Single(data) => f
                .debug_tuple("Single")
                .field(&data.len())
                .finish(),
            WritePayload::HeaderPayload { header, payload } => f
                .debug_struct("HeaderPayload")
                .field("header_len", &header.len())
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInline {
                header_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInline")
                .field("header_len", &header_len)
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            } => f
                .debug_struct("HeaderPooled")
                .field("header_len", &header.len())
                .field("prefix_len", &prefix.as_ref().map(|p| p.len()).unwrap_or(0))
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInlinePooled {
                header_len,
                prefix,
                prefix_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInlinePooled")
                .field("header_len", &header_len)
                .field(
                    "prefix_len",
                    &if prefix.is_some() { *prefix_len } else { 0 },
                )
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::Buf(_) => f.debug_tuple("Buf").field(&"<buf>").finish(),
        }
    }
}

// Safety: WritePayload is only moved across threads; it is not accessed concurrently.
unsafe impl Send for WritePayload {}
unsafe impl Sync for WritePayload {}

/// Write command for the lock-free ring buffer with zero-copy optimization
pub struct WriteCommand {
    pub channel_id: ChannelId,
    pub data: WritePayload,
    pub sequence: u64,
    pub permit: Option<OwnedSemaphorePermit>,
}

impl std::fmt::Debug for WriteCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteCommand")
            .field("channel_id", &self.channel_id)
            .field("data", &self.data)
            .field("sequence", &self.sequence)
            .field("permit", &self.permit.is_some())
            .finish()
    }
}

// Safety: WriteCommand is only moved across threads; it is not accessed concurrently.
unsafe impl Send for WriteCommand {}
unsafe impl Sync for WriteCommand {}

/// Zero-copy write command that references pre-allocated buffer slots
#[derive(Debug)]
pub struct ZeroCopyWriteCommand {
    pub channel_id: ChannelId,
    pub buffer_ptr: *const u8,
    pub len: usize,
    pub sequence: u64,
    pub buffer_id: usize, // For buffer pool management
}

// Safety: ZeroCopyWriteCommand is only used within the single writer task
unsafe impl Send for ZeroCopyWriteCommand {}
unsafe impl Sync for ZeroCopyWriteCommand {}

/// Memory pool for zero-allocation message handling
#[derive(Debug)]
pub struct MessageBufferPool {
    pool: Vec<Vec<u8>>,
    pool_size: usize,
    available_buffers: AtomicUsize,
    next_buffer_idx: AtomicUsize,
}

impl MessageBufferPool {
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(Vec::with_capacity(buffer_size));
        }

        Self {
            pool,
            pool_size,
            available_buffers: AtomicUsize::new(pool_size),
            next_buffer_idx: AtomicUsize::new(0),
        }
    }

    /// Get a buffer from the pool - returns None if pool is empty
    pub fn get_buffer(&self) -> Option<Vec<u8>> {
        let available = self.available_buffers.load(Ordering::Acquire);
        if available == 0 {
            return None;
        }

        let idx = self.next_buffer_idx.fetch_add(1, Ordering::AcqRel) % self.pool_size;

        // Try to claim this buffer
        if self.available_buffers.fetch_sub(1, Ordering::AcqRel) > 0 {
            // We successfully claimed a buffer
            unsafe {
                let buffer_ptr = self.pool.as_ptr().add(idx) as *mut Vec<u8>;
                let mut buffer = std::ptr::replace(buffer_ptr, Vec::new());
                buffer.clear(); // Reset length but keep capacity
                Some(buffer)
            }
        } else {
            // No buffers available, restore count
            self.available_buffers.fetch_add(1, Ordering::Release);
            None
        }
    }

    /// Return a buffer to the pool
    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear(); // Reset length but keep capacity

        let idx = self.next_buffer_idx.fetch_add(1, Ordering::AcqRel) % self.pool_size;

        unsafe {
            let buffer_ptr = self.pool.as_ptr().add(idx) as *mut Vec<u8>;
            *buffer_ptr = buffer;
        }

        self.available_buffers.fetch_add(1, Ordering::Release);
    }

    pub fn available_count(&self) -> usize {
        self.available_buffers.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.available_count() == 0
    }
}

/// Lock-free ring buffer for high-performance writes with proper memory ordering
#[derive(Debug)]
pub struct LockFreeRingBuffer {
    buffer: Vec<Option<WriteCommand>>,
    capacity: usize,
    // Cache-line aligned atomic counters to prevent false sharing
    write_index: AtomicUsize,
    read_index: AtomicUsize,
    pending_writes: AtomicUsize,
}

impl LockFreeRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: (0..capacity).map(|_| None).collect(),
            capacity,
            write_index: AtomicUsize::new(0),
            read_index: AtomicUsize::new(0),
            pending_writes: AtomicUsize::new(0),
        }
    }

    /// Try to push a write command - returns true if successful
    /// Uses proper memory ordering to prevent ABA problem
    pub fn try_push(&self, command: WriteCommand) -> bool {
        let current_writes = self.pending_writes.load(Ordering::Acquire);
        if current_writes >= self.capacity {
            return false; // Buffer full
        }

        let write_idx = self.write_index.fetch_add(1, Ordering::AcqRel) % self.capacity;

        // This is unsafe but fast - we're assuming single writer per slot
        // Using proper memory ordering ensures visibility across cores
        unsafe {
            let slot = self.buffer.as_ptr().add(write_idx) as *mut Option<WriteCommand>;
            *slot = Some(command);
        }

        // Release ordering ensures the write above is visible before incrementing pending count
        self.pending_writes.fetch_add(1, Ordering::Release);
        true
    }

    /// Try to pop a write command - returns None if empty
    /// Uses proper memory ordering to prevent race conditions
    pub fn try_pop(&self) -> Option<WriteCommand> {
        let current_writes = self.pending_writes.load(Ordering::Acquire);
        if current_writes == 0 {
            return None;
        }

        let read_idx = self.read_index.fetch_add(1, Ordering::AcqRel) % self.capacity;

        // This is unsafe but fast - we're assuming single reader per slot
        // Using proper memory ordering ensures we see writes from other cores
        unsafe {
            let slot = self.buffer.as_ptr().add(read_idx) as *mut Option<WriteCommand>;
            let command = (*slot).take();
            if command.is_some() {
                // Release ordering ensures the slot is cleared before decrementing pending count
                self.pending_writes.fetch_sub(1, Ordering::Release);
            }
            command
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending_writes.load(Ordering::Acquire)
    }

    /// Get buffer utilization as a percentage (0-100)
    pub fn utilization(&self) -> f32 {
        (self.pending_count() as f32 / self.capacity as f32) * 100.0
    }

    /// Check if buffer is nearly full (>90% capacity)
    pub fn is_nearly_full(&self) -> bool {
        self.pending_count() > (self.capacity * 9) / 10
    }
}

/// Vectored write command for zero-copy header + payload operations
#[derive(Debug)]
pub struct VectoredWriteCommand {
    header: bytes::Bytes,
    payload: bytes::Bytes,
}

/// Commands for streaming operations
#[derive(Debug)]
enum StreamingCommand {
    /// Direct write bytes for streaming
    WriteBytes(bytes::Bytes),
    /// Flush the writer
    Flush,
    /// Vectored write for header + payload (zero-copy)
    VectoredWrite(VectoredWriteCommand),
    /// Batch of owned chunks for streaming (zero-copy)
    OwnedChunks(Vec<bytes::Bytes>),
}

/// Truly lock-free streaming handle with dedicated background writer
#[derive(Clone)]
pub struct LockFreeStreamHandle {
    addr: SocketAddr,
    channel_id: ChannelId,
    sequence_counter: Arc<AtomicUsize>,
    frame_sequence: Arc<AtomicUsize>,
    bytes_written: Arc<AtomicUsize>, // This tracks actual TCP bytes written
    ring_buffer: Arc<LockFreeRingBuffer>,
    shutdown_signal: Arc<AtomicBool>,
    flush_pending: Arc<AtomicBool>,
    writer_idle: Arc<AtomicBool>,
    writer_notify: Arc<Notify>,
    ask_permits: Arc<Semaphore>,
    control_permits: Arc<Semaphore>,
    /// Atomic flag for coordinating streaming mode
    streaming_active: Arc<AtomicBool>,
    /// Channel to send streaming commands to background task
    streaming_tx: mpsc::UnboundedSender<StreamingCommand>,
    /// Buffer configuration that determines sizes and thresholds
    buffer_config: BufferConfig,
}

impl LockFreeStreamHandle {
    /// Create a new lock-free streaming handle with background writer task
    pub fn new<W>(
        tcp_writer: W,
        addr: SocketAddr,
        channel_id: ChannelId,
        buffer_config: BufferConfig,
    ) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let ring_buffer = Arc::new(LockFreeRingBuffer::new(buffer_config.ring_buffer_slots()));
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let streaming_active = Arc::new(AtomicBool::new(false));
        let flush_pending = Arc::new(AtomicBool::new(false));
        let writer_idle = Arc::new(AtomicBool::new(true));
        let writer_notify = Arc::new(Notify::new());

        let capacity = buffer_config.ring_buffer_slots();
        let reserved = control_reserved_slots(capacity);
        let available = capacity.saturating_sub(reserved);
        let ask_limit = if available == 0 {
            0
        } else {
            buffer_config
                .ask_inflight_limit()
                .min(available)
                .max(1)
        };
        let ask_permits = Arc::new(Semaphore::new(ask_limit));
        let control_permits = Arc::new(Semaphore::new(reserved));

        // Create shared counter for actual TCP bytes written
        let bytes_written = Arc::new(AtomicUsize::new(0));

        // Create channel for streaming commands
        let (streaming_tx, streaming_rx) = mpsc::unbounded_channel();

        // Spawn background writer task with exclusive TCP access - NO MUTEX!
        {
            let ring_buffer = ring_buffer.clone();
            let shutdown_signal = shutdown_signal.clone();
            let bytes_written_for_task = bytes_written.clone();
            let streaming_active_for_task = streaming_active.clone();
            let flush_pending_for_task = flush_pending.clone();
            let writer_idle_for_task = writer_idle.clone();
            let writer_notify_for_task = writer_notify.clone();

        tokio::spawn(async move {
            Self::background_writer_task(
                tcp_writer,
                ring_buffer,
                shutdown_signal,
                bytes_written_for_task,
                flush_pending_for_task,
                streaming_active_for_task,
                writer_idle_for_task,
                writer_notify_for_task,
                streaming_rx,
            )
            .await;
        });
        }

        Self {
            addr,
            channel_id,
            sequence_counter: Arc::new(AtomicUsize::new(0)),
            frame_sequence: Arc::new(AtomicUsize::new(0)),
            bytes_written, // This now tracks actual TCP bytes written
            ring_buffer,
            shutdown_signal,
            flush_pending,
            writer_idle,
            writer_notify,
            ask_permits,
            control_permits,
            streaming_active,
            streaming_tx,
            buffer_config,
        }
    }

    /// Background writer task - truly lock-free with exclusive TCP access
    /// OPTIMIZED FOR MAXIMUM THROUGHPUT - NO MUTEX NEEDED!
    async fn background_writer_task<W>(
        tcp_writer: W,
        ring_buffer: Arc<LockFreeRingBuffer>,
        shutdown_signal: Arc<AtomicBool>,
        bytes_written_counter: Arc<AtomicUsize>, // Track ALL bytes written to TCP
        flush_pending: Arc<AtomicBool>,
        streaming_active: Arc<AtomicBool>,
        writer_idle: Arc<AtomicBool>,
        writer_notify: Arc<Notify>,
        mut streaming_rx: mpsc::UnboundedReceiver<StreamingCommand>,
    ) where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        use std::io::IoSlice;
        use tokio::io::AsyncWriteExt;

        // Use direct tokio writer with vectored I/O for now.
        // Future optimization: wire in platform-specific writers (io_uring on Linux 5.1+).
        // This requires either:
        // 1. Modifying StreamWriter trait to work with OwnedWriteHalf
        // 2. Or using a different socket splitting approach that allows io_uring
        // Use full TCP buffer size for large messages (CRITICAL FIX!)
        // Need large buffer to handle 100KB+ BacktestSummary messages
        let mut writer = tokio::io::BufWriter::with_capacity(TCP_BUFFER_SIZE, tcp_writer);

        // Smaller batches for lower latency
        const RING_BATCH_SIZE: usize = 64; // Smaller batches for faster processing
        const FLUSH_THRESHOLD: usize = 4 * 1024; // 4KB before flush (much lower for ask/reply)

        let mut bytes_since_flush = 0;
        let mut last_flush = std::time::Instant::now();

        while !shutdown_signal.load(Ordering::Relaxed) {
            let mut total_bytes_written = 0;
            let mut did_work = false;

            while let Ok(cmd) = streaming_rx.try_recv() {
                did_work = true;
                match cmd {
                    StreamingCommand::WriteBytes(data) => {
                        match writer.write_all(&data).await {
                            Ok(_) => {
                                bytes_written_counter.fetch_add(data.len(), Ordering::Relaxed);
                                total_bytes_written += data.len();
                            }
                            Err(e) => {
                                error!("Streaming write error: {}", e);
                                return;
                            }
                        }
                    }
                    StreamingCommand::Flush => {
                        let _ = writer.flush().await;
                        flush_pending.store(false, Ordering::Release);
                        last_flush = std::time::Instant::now();
                        bytes_since_flush = 0;
                    }
                    StreamingCommand::VectoredWrite(cmd) => {
                        let header_slice = std::io::IoSlice::new(&cmd.header);
                        let payload_slice = std::io::IoSlice::new(&cmd.payload);
                        let bufs = &[header_slice, payload_slice];

                        match writer.write_vectored(bufs).await {
                            Ok(n) => {
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                            }
                            Err(e) => {
                                error!("Vectored write error: {}", e);
                                return;
                            }
                        }
                    }
                    StreamingCommand::OwnedChunks(chunks) => {
                        let slices: Vec<std::io::IoSlice> = chunks
                            .iter()
                            .map(|chunk| std::io::IoSlice::new(chunk))
                            .collect();

                        match writer.write_vectored(&slices).await {
                            Ok(n) => {
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                            }
                            Err(e) => {
                                error!("Chunk batch write error: {}", e);
                                return;
                            }
                        }
                    }
                }
            }

            if !streaming_active.load(Ordering::Acquire) {
                let mut write_chunks = Vec::with_capacity(RING_BATCH_SIZE * 2);
                let mut permits = Vec::with_capacity(RING_BATCH_SIZE);

                for _ in 0..RING_BATCH_SIZE {
                    if let Some(mut command) = ring_buffer.try_pop() {
                        did_work = true;
                        if let Some(permit) = command.permit.take() {
                            permits.push(permit);
                        }
                        match command.data {
                            WritePayload::Single(data) => write_chunks.push(data),
                            WritePayload::HeaderPayload { header, payload } => {
                                write_chunks.push(header);
                                write_chunks.push(payload);
                            }
                            WritePayload::HeaderInline {
                                header,
                                header_len,
                                payload,
                            } => {
                                if !write_chunks.is_empty() {
                                    const MAX_IOV: usize = 64;
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut idx = 0;
                                    let mut iov: [std::mem::MaybeUninit<IoSlice<'_>>; MAX_IOV] =
                                        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

                                    for chunk in &chunks {
                                        iov[idx].write(IoSlice::new(chunk));
                                        idx += 1;
                                        if idx == MAX_IOV {
                                            let slices = unsafe {
                                                std::slice::from_raw_parts(
                                                    iov.as_ptr() as *const IoSlice<'_>,
                                                    idx,
                                                )
                                            };
                                            match writer.write_vectored(slices).await {
                                                Ok(bytes_written) => {
                                                    bytes_written_counter.fetch_add(
                                                        bytes_written,
                                                        Ordering::Relaxed,
                                                    );
                                                    total_bytes_written += bytes_written;
                                                }
                                                Err(_) => return,
                                            }
                                            idx = 0;
                                        }
                                    }

                                    if idx > 0 {
                                        let slices = unsafe {
                                            std::slice::from_raw_parts(
                                                iov.as_ptr() as *const IoSlice<'_>,
                                                idx,
                                            )
                                        };
                                        match writer.write_vectored(slices).await {
                                            Ok(bytes_written) => {
                                                bytes_written_counter.fetch_add(
                                                    bytes_written,
                                                    Ordering::Relaxed,
                                                );
                                                total_bytes_written += bytes_written;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                let header_len = header_len as usize;
                                let mut header_off = 0usize;
                                let mut payload_off = 0usize;
                                let payload_len = payload.len();

                                while header_off < header_len || payload_off < payload_len {
                                    let h = &header[header_off..header_len];
                                    let p = &payload[payload_off..];
                                    let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                    let slice_count = if h.is_empty() {
                                        slices[0] = IoSlice::new(p);
                                        1
                                    } else if p.is_empty() {
                                        slices[0] = IoSlice::new(h);
                                        1
                                    } else {
                                        2
                                    };

                                    match writer.write_vectored(&slices[..slice_count]).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                            if header_off < header_len {
                                                let h_rem = header_len - header_off;
                                                if n < h_rem {
                                                    header_off += n;
                                                    continue;
                                                } else {
                                                    header_off = header_len;
                                                    payload_off += n - h_rem;
                                                }
                                            } else {
                                                payload_off += n;
                                            }
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderPooled {
                                header,
                                prefix,
                                mut payload,
                            } => {
                                if !write_chunks.is_empty() {
                                    const MAX_IOV: usize = 64;
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut idx = 0;
                                    let mut iov: [std::mem::MaybeUninit<IoSlice<'_>>; MAX_IOV] =
                                        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

                                    for chunk in &chunks {
                                        iov[idx].write(IoSlice::new(chunk));
                                        idx += 1;
                                        if idx == MAX_IOV {
                                            let slices = unsafe {
                                                std::slice::from_raw_parts(
                                                    iov.as_ptr() as *const IoSlice<'_>,
                                                    idx,
                                                )
                                            };
                                            match writer.write_vectored(slices).await {
                                                Ok(bytes_written) => {
                                                    bytes_written_counter.fetch_add(
                                                        bytes_written,
                                                        Ordering::Relaxed,
                                                    );
                                                    total_bytes_written += bytes_written;
                                                }
                                                Err(_) => return,
                                            }
                                            idx = 0;
                                        }
                                    }

                                    if idx > 0 {
                                        let slices = unsafe {
                                            std::slice::from_raw_parts(
                                                iov.as_ptr() as *const IoSlice<'_>,
                                                idx,
                                            )
                                        };
                                        match writer.write_vectored(slices).await {
                                            Ok(bytes_written) => {
                                                bytes_written_counter.fetch_add(
                                                    bytes_written,
                                                    Ordering::Relaxed,
                                                );
                                                total_bytes_written += bytes_written;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                if let Err(_) = writer.write_all(&header).await {
                                    return;
                                }
                                bytes_written_counter.fetch_add(header.len(), Ordering::Relaxed);
                                total_bytes_written += header.len();

                                if let Some(prefix) = prefix {
                                    if let Err(_) = writer.write_all(&prefix).await {
                                        return;
                                    }
                                    bytes_written_counter.fetch_add(prefix.len(), Ordering::Relaxed);
                                    total_bytes_written += prefix.len();
                                }

                                while payload.has_remaining() {
                                    match writer.write_buf(&mut payload).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInlinePooled {
                                header,
                                header_len,
                                prefix,
                                prefix_len,
                                mut payload,
                            } => {
                                if !write_chunks.is_empty() {
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut slices = Vec::with_capacity(chunks.len());
                                    for chunk in &chunks {
                                        slices.push(IoSlice::new(chunk));
                                    }
                                    match writer.write_vectored(&slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter.fetch_add(
                                                bytes_written,
                                                Ordering::Relaxed,
                                            );
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                let header_len = header_len as usize;
                                let prefix_len = prefix_len as usize;
                                let mut header_off = 0usize;
                                let mut prefix_off = 0usize;

                                if let Some(prefix) = prefix {
                                    while header_off < header_len || prefix_off < prefix_len {
                                        let h = &header[header_off..header_len];
                                        let p = &prefix[prefix_off..prefix_len];
                                        let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                        let slice_count = if h.is_empty() {
                                            slices[0] = IoSlice::new(p);
                                            1
                                        } else if p.is_empty() {
                                            slices[0] = IoSlice::new(h);
                                            1
                                        } else {
                                            2
                                        };

                                        match writer.write_vectored(&slices[..slice_count]).await {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                if header_off < header_len {
                                                    let h_rem = header_len - header_off;
                                                    if n < h_rem {
                                                        header_off += n;
                                                        continue;
                                                    } else {
                                                        header_off = header_len;
                                                        prefix_off += n - h_rem;
                                                    }
                                                } else {
                                                    prefix_off += n;
                                                }
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                } else {
                                    while header_off < header_len {
                                        let h = &header[header_off..header_len];
                                        match writer.write_vectored(&[IoSlice::new(h)]).await {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                header_off += n;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                while payload.has_remaining() {
                                    match writer.write_buf(&mut payload).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::Buf(mut buf) => {
                                if !write_chunks.is_empty() {
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut slices = Vec::with_capacity(chunks.len());
                                    for chunk in &chunks {
                                        slices.push(IoSlice::new(chunk));
                                    }
                                    match writer.write_vectored(&slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter.fetch_add(
                                                bytes_written,
                                                Ordering::Relaxed,
                                            );
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                while buf.has_remaining() {
                                    match writer.write_buf(&mut buf).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }

                if !write_chunks.is_empty() {
                    const MAX_IOV: usize = 64;
                    let chunks = std::mem::take(&mut write_chunks);
                    let mut idx = 0;
                    let mut iov: [std::mem::MaybeUninit<IoSlice<'_>>; MAX_IOV] =
                        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

                    for chunk in &chunks {
                        iov[idx].write(IoSlice::new(chunk));
                        idx += 1;
                        if idx == MAX_IOV {
                            let slices = unsafe {
                                std::slice::from_raw_parts(
                                    iov.as_ptr() as *const IoSlice<'_>,
                                    idx,
                                )
                            };
                            match writer.write_vectored(slices).await {
                                Ok(bytes_written) => {
                                    bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                Err(_) => return,
                            }
                            idx = 0;
                        }
                    }

                    if idx > 0 {
                        let slices = unsafe {
                            std::slice::from_raw_parts(
                                iov.as_ptr() as *const IoSlice<'_>,
                                idx,
                            )
                        };
                        match writer.write_vectored(slices).await {
                            Ok(bytes_written) => {
                                bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                                total_bytes_written += bytes_written;
                            }
                            Err(_) => return,
                        }
                    }
                }

                drop(permits);
            }

            bytes_since_flush += total_bytes_written;
            let ring_empty = ring_buffer.pending_count() == 0;
            let elapsed = last_flush.elapsed();

            if should_flush(
                bytes_since_flush,
                ring_empty,
                elapsed,
                FLUSH_THRESHOLD,
                WRITER_MAX_LATENCY,
            ) {
                let _ = writer.flush().await;
                bytes_since_flush = 0;
                last_flush = std::time::Instant::now();
                flush_pending.store(false, Ordering::Release);
            }

            if !did_work {
                writer_idle.store(true, Ordering::Release);

                if bytes_since_flush > 0 {
                    tokio::select! {
                        Some(cmd) = streaming_rx.recv() => {
                            writer_idle.store(false, Ordering::Release);
                            match cmd {
                                StreamingCommand::WriteBytes(data) => {
                                    if writer.write_all(&data).await.is_err() {
                                        return;
                                    }
                                    bytes_written_counter.fetch_add(data.len(), Ordering::Relaxed);
                                    bytes_since_flush += data.len();
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::Flush => {
                                    let _ = writer.flush().await;
                                    flush_pending.store(false, Ordering::Release);
                                    bytes_since_flush = 0;
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::VectoredWrite(cmd) => {
                                    let header_slice = std::io::IoSlice::new(&cmd.header);
                                    let payload_slice = std::io::IoSlice::new(&cmd.payload);
                                    let bufs = &[header_slice, payload_slice];
                                    if let Ok(n) = writer.write_vectored(bufs).await {
                                        bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                        bytes_since_flush += n;
                                        last_flush = std::time::Instant::now();
                                    } else {
                                        return;
                                    }
                                }
                                StreamingCommand::OwnedChunks(chunks) => {
                                    let slices: Vec<std::io::IoSlice> = chunks
                                        .iter()
                                        .map(|chunk| std::io::IoSlice::new(chunk))
                                        .collect();
                                    if let Ok(n) = writer.write_vectored(&slices).await {
                                        bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                        bytes_since_flush += n;
                                        last_flush = std::time::Instant::now();
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }
                        _ = writer_notify.notified() => {
                            writer_idle.store(false, Ordering::Release);
                        }
                        _ = tokio::time::sleep(WRITER_MAX_LATENCY) => {
                            writer_idle.store(false, Ordering::Release);
                        }
                    }
                } else {
                    tokio::select! {
                        Some(cmd) = streaming_rx.recv() => {
                            writer_idle.store(false, Ordering::Release);
                            match cmd {
                                StreamingCommand::WriteBytes(data) => {
                                    if writer.write_all(&data).await.is_err() {
                                        return;
                                    }
                                    bytes_written_counter.fetch_add(data.len(), Ordering::Relaxed);
                                    bytes_since_flush += data.len();
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::Flush => {
                                    let _ = writer.flush().await;
                                    flush_pending.store(false, Ordering::Release);
                                    bytes_since_flush = 0;
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::VectoredWrite(cmd) => {
                                    let header_slice = std::io::IoSlice::new(&cmd.header);
                                    let payload_slice = std::io::IoSlice::new(&cmd.payload);
                                    let bufs = &[header_slice, payload_slice];
                                    if let Ok(n) = writer.write_vectored(bufs).await {
                                        bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                        bytes_since_flush += n;
                                        last_flush = std::time::Instant::now();
                                    } else {
                                        return;
                                    }
                                }
                                StreamingCommand::OwnedChunks(chunks) => {
                                    let slices: Vec<std::io::IoSlice> = chunks
                                        .iter()
                                        .map(|chunk| std::io::IoSlice::new(chunk))
                                        .collect();
                                    if let Ok(n) = writer.write_vectored(&slices).await {
                                        bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                        bytes_since_flush += n;
                                        last_flush = std::time::Instant::now();
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }
                        _ = writer_notify.notified() => {
                            writer_idle.store(false, Ordering::Release);
                        }
                    }
                }
            }
        }
    }

    async fn acquire_ask_permit(&self) -> OwnedSemaphorePermit {
        self.ask_permits
            .clone()
            .acquire_owned()
            .await
            .expect("ask permit semaphore closed")
    }

    async fn acquire_control_permit(&self) -> OwnedSemaphorePermit {
        self.control_permits
            .clone()
            .acquire_owned()
            .await
            .expect("control permit semaphore closed")
    }

    #[cfg(test)]
    fn permit_counts(&self) -> (usize, usize) {
        (
            self.ask_permits.available_permits(),
            self.control_permits.available_permits(),
        )
    }

    #[cfg(test)]
    async fn acquire_ask_permit_for_test(&self) -> OwnedSemaphorePermit {
        self.acquire_ask_permit().await
    }

    #[cfg(test)]
    async fn acquire_control_permit_for_test(&self) -> OwnedSemaphorePermit {
        self.acquire_control_permit().await
    }

    fn enqueue_with_permit(&self, data: WritePayload, permit: OwnedSemaphorePermit) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        let command = WriteCommand {
            channel_id: self.channel_id,
            data,
            sequence: sequence as u64,
            permit: Some(permit),
        };

        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    fn wake_writer_if_idle(&self) {
        if self
            .writer_idle
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.writer_notify.notify_one();
        }
    }

    pub async fn write_bytes_ask(&self, data: bytes::Bytes) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(WritePayload::Single(data), permit)
    }

    pub async fn write_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        let permit = self.acquire_control_permit().await;
        self.enqueue_with_permit(WritePayload::Single(data), permit)
    }

    pub async fn write_header_and_payload_control(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderPayload { header, payload },
            permit,
        )
    }

    pub async fn write_header_and_payload_control_inline(
        &self,
        header: [u8; 8],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderInline {
                header,
                header_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_header_and_payload_ask(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(WritePayload::HeaderPayload { header, payload }, permit)
    }

    pub async fn write_header_and_payload_ask_inline(
        &self,
        header: [u8; 8],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderInline {
                header,
                header_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_control(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_control_inline(
        &self,
        header: [u8; 8],
        header_len: u8,
        prefix: Option<[u8; 8]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderInlinePooled {
                header,
                header_len,
                prefix,
                prefix_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_ask(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_ask_inline(
        &self,
        header: [u8; 8],
        header_len: u8,
        prefix: Option<[u8; 8]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderInlinePooled {
                header,
                header_len,
                prefix,
                prefix_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_buf_control<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let permit = self.acquire_control_permit().await;
        self.enqueue_with_permit(WritePayload::Buf(Box::new(buf)), permit)
    }

    pub async fn write_buf_ask<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(WritePayload::Buf(Box::new(buf)), permit)
    }

    /// Write Bytes to the lock-free ring buffer - NO BLOCKING, NO COPY
    pub fn write_bytes_nonblocking(&self, data: bytes::Bytes) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::Single(data),
            sequence: sequence as u64,
            permit: None,
        };

        let _ = self.ring_buffer.try_push(command);
        self.wake_writer_if_idle();
        // Don't increment bytes_written here - only increment when actually written to TCP
        Ok(())
    }

    /// Write header + payload without concatenating (single ring-buffer entry).
    pub fn write_header_and_payload_nonblocking(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::HeaderPayload { header, payload },
            sequence: sequence as u64,
            permit: None,
        };

        let _ = self.ring_buffer.try_push(command);
        self.wake_writer_if_idle();
        Ok(())
    }

    /// Write Bytes to the ring buffer; returns WouldBlock if full.
    pub fn write_bytes_nonblocking_checked(&self, data: bytes::Bytes) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::Single(data),
            sequence: sequence as u64,
            permit: None,
        };

        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    /// Write header + payload; returns WouldBlock if full.
    pub fn write_header_and_payload_nonblocking_checked(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::HeaderPayload { header, payload },
            sequence: sequence as u64,
            permit: None,
        };

        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    /// Flush the writer immediately - used for low-latency ask operations
    pub fn flush_immediately(&self) -> Result<()> {
        // Coalesce flush requests to avoid flooding the writer task.
        if self
            .flush_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.streaming_tx
                .send(StreamingCommand::Flush)
                .map_err(|_| {
                    self.flush_pending.store(false, Ordering::Release);
                    GossipError::Shutdown
                })
        } else {
            Ok(())
        }
    }

    /// Write data with vectored batching - still no blocking
    pub fn write_vectored_nonblocking(&self, data_chunks: &[&[u8]]) -> Result<()> {
        if data_chunks.is_empty() {
            return Ok(());
        }

        // Use BytesMut for efficient concatenation
        let total_len: usize = data_chunks.iter().map(|chunk| chunk.len()).sum();
        let mut combined_buffer = bytes::BytesMut::with_capacity(total_len);

        for chunk in data_chunks {
            combined_buffer.extend_from_slice(chunk);
        }

        self.write_bytes_nonblocking(combined_buffer.freeze())
    }

    /// Write large data in chunks to avoid blocking
    pub fn write_chunked_nonblocking(&self, data: &[u8], chunk_size: usize) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        for chunk in data.chunks(chunk_size) {
            let _ = self.write_bytes_nonblocking(bytes::Bytes::copy_from_slice(chunk));
        }

        Ok(())
    }

    /// Get ring buffer status
    pub fn buffer_status(&self) -> (usize, usize) {
        let pending = self.ring_buffer.pending_count();
        (pending, self.ring_buffer.capacity - pending)
    }

    /// Check if ring buffer is near capacity
    pub fn is_buffer_full(&self) -> bool {
        self.ring_buffer.pending_count() >= (self.ring_buffer.capacity * 9 / 10)
        // 90% full
    }

    /// Get channel ID
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Get total bytes written
    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get sequence counter
    pub fn sequence_number(&self) -> usize {
        self.sequence_counter.load(Ordering::Relaxed)
    }

    /// Get next stream frame sequence ID (wraps at u16::MAX)
    fn next_frame_sequence_id(&self) -> u16 {
        (self.frame_sequence.fetch_add(1, Ordering::Relaxed) & 0xFFFF) as u16
    }

    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shutdown the background writer task
    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }

    /// Get the streaming threshold for this connection
    ///
    /// Returns the threshold above which messages should be streamed rather than
    /// sent through the ring buffer. This is always derived from the buffer size.
    pub fn streaming_threshold(&self) -> usize {
        self.buffer_config.streaming_threshold()
    }

    /// Stream a large message directly to the socket, bypassing the ring buffer
    /// This provides maximum performance for large messages like PreBacktest
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        use crate::{current_timestamp, MessageType, StreamHeader};

        const CHUNK_SIZE: usize = STREAM_CHUNK_SIZE;

        // Acquire streaming mode atomically
        while self
            .streaming_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tokio::task::yield_now().await;
        }

        // Ensure we release streaming mode on exit
        let _guard = StreamingGuard {
            flag: self.streaming_active.clone(),
        };

        // Generate unique stream ID
        let stream_id = current_timestamp();

        // Helper to serialize message with type and header
        fn serialize_stream_message(msg_type: MessageType, header: &StreamHeader) -> Vec<u8> {
            // Message format: [length:4][type:1][correlation_id:2][reserved:5][header:36]
            let inner_size = 8 + StreamHeader::SERIALIZED_SIZE;
            let mut message = Vec::with_capacity(4 + inner_size);

            // Length prefix (required by protocol)
            message.extend_from_slice(&(inner_size as u32).to_be_bytes());

            // Header
            message.push(msg_type as u8);
            message.extend_from_slice(&[0, 0]); // correlation_id (not used for streaming)
            message.extend_from_slice(&[0, 0, 0, 0, 0]); // reserved
            message.extend_from_slice(&header.to_bytes());
            message
        }

        // Send StreamStart header
        let start_header = StreamHeader {
            stream_id,
            total_size: msg.len() as u64,
            chunk_size: 0,
            chunk_index: 0,
            type_hash,
            actor_id,
        };

        let start_msg = serialize_stream_message(MessageType::StreamStart, &start_header);
        self.streaming_tx
            .send(StreamingCommand::WriteBytes(start_msg.into()))
            .map_err(|_| GossipError::Shutdown)?;

        // Stream chunks directly
        for (idx, chunk) in msg.chunks(CHUNK_SIZE).enumerate() {
            let data_header = StreamHeader {
                stream_id,
                total_size: msg.len() as u64,
                chunk_size: chunk.len() as u32,
                chunk_index: idx as u32,
                type_hash,
                actor_id,
            };

            // Create combined message with proper length prefix
            // Message format: [length:4][type:1][correlation_id:2][reserved:5][header:36][chunk_data:N]
            let inner_size = 8 + StreamHeader::SERIALIZED_SIZE + chunk.len();
            let mut chunk_msg = Vec::with_capacity(4 + inner_size);

            // Length prefix (includes header + chunk data)
            chunk_msg.extend_from_slice(&(inner_size as u32).to_be_bytes());

            // Header
            chunk_msg.push(MessageType::StreamData as u8);
            chunk_msg.extend_from_slice(&[0, 0]); // correlation_id
            chunk_msg.extend_from_slice(&[0, 0, 0, 0, 0]); // reserved
            chunk_msg.extend_from_slice(&data_header.to_bytes());

            // Chunk data
            chunk_msg.extend_from_slice(chunk);

            self.streaming_tx
                .send(StreamingCommand::WriteBytes(chunk_msg.into()))
                .map_err(|_| GossipError::Shutdown)?;

            // Yield periodically to prevent blocking
            if idx % 10 == 0 {
                self.streaming_tx
                    .send(StreamingCommand::Flush)
                    .map_err(|_| GossipError::Shutdown)?;
                tokio::task::yield_now().await;
            }
        }

        // Send StreamEnd
        let end_msg = serialize_stream_message(MessageType::StreamEnd, &start_header);
        self.streaming_tx
            .send(StreamingCommand::WriteBytes(end_msg.into()))
            .map_err(|_| GossipError::Shutdown)?;
        self.streaming_tx
            .send(StreamingCommand::Flush)
            .map_err(|_| GossipError::Shutdown)?;

        debug!(
            "✅ STREAMING: Successfully streamed {} MB in {} chunks",
            msg.len() as f64 / 1_048_576.0,
            msg.len().div_ceil(CHUNK_SIZE)
        );

        Ok(())
    }

    /// Zero-copy vectored write for header + payload in single operation
    /// This eliminates copying payload data into frame buffer - optimal for streaming
    pub fn write_bytes_vectored(&self, header: bytes::Bytes, payload: bytes::Bytes) -> Result<()> {
        // Create vectored command that preserves both header and payload as separate Bytes
        let command = VectoredWriteCommand { header, payload };

        // Try to send via streaming channel first for vectored operations
        match self
            .streaming_tx
            .send(StreamingCommand::VectoredWrite(command))
        {
            Ok(_) => Ok(()),
            Err(send_error) => {
                // Fallback: combine into single write if streaming channel is closed
                let cmd = send_error.0;
                if let StreamingCommand::VectoredWrite(vectored_cmd) = cmd {
                    let total_len = vectored_cmd.header.len() + vectored_cmd.payload.len();
                    let mut combined = bytes::BytesMut::with_capacity(total_len);
                    combined.extend_from_slice(&vectored_cmd.header);
                    combined.extend_from_slice(&vectored_cmd.payload);
                    self.write_bytes_nonblocking(combined.freeze())
                } else {
                    Err(GossipError::Shutdown)
                }
            }
        }
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        // Send chunks as a batch via streaming channel for optimal vectored I/O
        let command = StreamingCommand::OwnedChunks(chunks);
        self.streaming_tx
            .send(command)
            .map_err(|_| GossipError::Shutdown)?;

        Ok(())
    }
}

/// Guard to ensure streaming_active is released on drop
struct StreamingGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for StreamingGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

impl Debug for LockFreeStreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockFreeStreamHandle")
            .field("addr", &self.addr)
            .field("channel_id", &self.channel_id)
            .field("bytes_written", &self.bytes_written.load(Ordering::Relaxed))
            .field("sequence", &self.sequence_counter.load(Ordering::Relaxed))
            .finish()
    }
}

/// Message types for seamless batching with tell()
pub enum TellMessage<'a> {
    Single(&'a [u8]),
    Batch(Vec<&'a [u8]>),
}

impl<'a> TellMessage<'a> {
    /// Create a single message
    pub fn single(data: &'a [u8]) -> Self {
        Self::Single(data)
    }

    /// Create a batch message
    pub fn batch(messages: Vec<&'a [u8]>) -> Self {
        Self::Batch(messages)
    }

    /// Create from a slice - automatically detects single vs batch
    pub fn from_slice(messages: &'a [&'a [u8]]) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::Batch(messages.to_vec())
        }
    }

    /// Send this message via the connection handle
    pub async fn send_via(self, handle: &ConnectionHandle) -> Result<()> {
        match self {
            TellMessage::Single(data) => handle.tell_raw(data).await,
            TellMessage::Batch(messages) => handle.tell_batch(&messages).await,
        }
    }
}

/// Implement From trait for automatic conversion
impl<'a> From<&'a [u8]> for TellMessage<'a> {
    fn from(data: &'a [u8]) -> Self {
        Self::Single(data)
    }
}

impl<'a> From<Vec<&'a [u8]>> for TellMessage<'a> {
    fn from(messages: Vec<&'a [u8]>) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::Batch(messages)
        }
    }
}

impl<'a> From<&'a [&'a [u8]]> for TellMessage<'a> {
    fn from(messages: &'a [&'a [u8]]) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::Batch(messages.to_vec())
        }
    }
}

impl<'a, const N: usize> From<&'a [u8; N]> for TellMessage<'a> {
    fn from(data: &'a [u8; N]) -> Self {
        Self::Single(data.as_slice())
    }
}

impl<'a> From<&'a Vec<u8>> for TellMessage<'a> {
    fn from(data: &'a Vec<u8>) -> Self {
        Self::Single(data.as_slice())
    }
}

/// Macro for ergonomic tell message creation
#[macro_export]
macro_rules! tell_msg {
    ($single:expr) => {
        TellMessage::single($single)
    };
    ($($msg:expr),+ $(,)?) => {
        TellMessage::batch(vec![$($msg),+])
    };
}

/// Connection pool for maintaining persistent TCP connections to peers
/// All connections are persistent - there is no checkout/checkin
/// Lock-free connection pool using atomic operations and lock-free data structures
pub struct ConnectionPool {
    /// PRIMARY: Mapping Peer ID -> LockFreeConnection
    /// This is the main storage - we identify connections by peer ID, not address
    connections_by_peer: dashmap::DashMap<crate::PeerId, Arc<LockFreeConnection>>,
    /// SECONDARY: Mapping SocketAddr -> Peer ID (for incoming connection identification)
    addr_to_peer_id: dashmap::DashMap<SocketAddr, crate::PeerId>,
    /// Configuration: Peer ID -> Expected SocketAddr (where to connect)
    pub peer_id_to_addr: dashmap::DashMap<crate::PeerId, SocketAddr>,
    /// Address-based connection index for fast lookup by SocketAddr
    connections_by_addr: dashmap::DashMap<SocketAddr, Arc<LockFreeConnection>>,
    /// Shared correlation trackers by peer ID - ensures ask/response works across bidirectional connections
    correlation_trackers: dashmap::DashMap<crate::PeerId, Arc<CorrelationTracker>>,
    max_connections: usize,
    connection_timeout: Duration,
    /// Registry reference for handling incoming messages
    registry: Option<std::sync::Weak<GossipRegistry>>,
    /// Shared message buffer pool for zero-allocation processing
    message_buffer_pool: Arc<MessageBufferPool>,
    /// Connection counter for load balancing
    connection_counter: AtomicUsize,
}

unsafe impl Send for ConnectionPool {}
unsafe impl Sync for ConnectionPool {}

/// Maximum number of pending responses (must be power of 2 for fast modulo)
const PENDING_RESPONSES_SIZE: usize = 1024;

/// Pending response slot
struct PendingResponseSlot {
    notify: tokio::sync::Notify,
    in_use: AtomicBool,
    response: parking_lot::Mutex<Option<bytes::Bytes>>,
}

/// Shared state for correlation tracking
pub(crate) struct CorrelationTracker {
    /// Next correlation ID to use
    next_id: AtomicU16,
    /// Fixed-size array of pending responses
    pending: [PendingResponseSlot; PENDING_RESPONSES_SIZE],
}

impl std::fmt::Debug for CorrelationTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrelationTracker")
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .finish()
    }
}

impl CorrelationTracker {
    fn new() -> Arc<Self> {
        let pending = std::array::from_fn(|_| PendingResponseSlot {
            notify: tokio::sync::Notify::new(),
            in_use: AtomicBool::new(false),
            response: parking_lot::Mutex::new(None),
        });
        Arc::new(Self {
            next_id: AtomicU16::new(1),
            pending,
        })
    }

    /// Allocate a correlation ID and reserve the response slot.
    fn allocate(&self) -> u16 {
        loop {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if id == 0 {
                continue; // Skip 0 as it's reserved
            }

            let slot = (id as usize) % PENDING_RESPONSES_SIZE;
            let slot_ref = &self.pending[slot];
            if !slot_ref.in_use.swap(true, Ordering::AcqRel) {
                let mut response = slot_ref.response.lock();
                *response = None;
                debug!(
                    "CorrelationTracker: Allocated correlation_id {} in slot {}",
                    id, slot
                );
                return id;
            }

            // Slot is occupied, try next ID
            debug!("CorrelationTracker: Slot {} occupied, trying next ID", slot);
        }
    }

    /// Check if a correlation ID has a pending request
    pub(crate) fn has_pending(&self, correlation_id: u16) -> bool {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        self.pending[slot].in_use.load(Ordering::Acquire)
    }

    /// Complete a pending request with a response
    pub(crate) fn complete(&self, correlation_id: u16, response: bytes::Bytes) {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let slot_ref = &self.pending[slot];
        if !slot_ref.in_use.load(Ordering::Acquire) {
            return;
        }
        let mut stored = slot_ref.response.lock();
        *stored = Some(response);
        slot_ref.notify.notify_waiters();
    }

    /// Cancel a pending request (used when send fails).
    pub(crate) fn cancel(&self, correlation_id: u16) {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let slot_ref = &self.pending[slot];
        slot_ref.in_use.store(false, Ordering::Release);
        let mut stored = slot_ref.response.lock();
        *stored = None;
        slot_ref.notify.notify_waiters();
    }

    async fn wait_for_response(
        &self,
        correlation_id: u16,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let slot_ref = &self.pending[slot];

        loop {
            if let Some(response) = slot_ref.response.lock().take() {
                slot_ref.in_use.store(false, Ordering::Release);
                return Ok(response);
            }

            let notified = tokio::time::timeout(timeout, slot_ref.notify.notified()).await;
            match notified {
                Ok(()) => continue,
                Err(_) => {
                    slot_ref.in_use.store(false, Ordering::Release);
                    return Err(crate::GossipError::Timeout);
                }
            }
        }
    }
}

/// Handle to send messages through a persistent connection - LOCK-FREE
#[derive(Clone)]
pub struct ConnectionHandle {
    pub addr: SocketAddr,
    // Direct lock-free stream handle - NO MUTEX!
    stream_handle: Arc<LockFreeStreamHandle>,
    // Correlation tracker for ask/response
    correlation: Arc<CorrelationTracker>,
}

impl std::fmt::Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle")
            .field("addr", &self.addr)
            .field("stream_handle", &self.stream_handle)
            .finish()
    }
}

/// Delegated reply sender for asynchronous response handling
/// Allows passing around the ability to reply to a request without blocking the original caller
pub struct DelegatedReplySender {
    sender: tokio::sync::oneshot::Sender<bytes::Bytes>,
    receiver: tokio::sync::oneshot::Receiver<bytes::Bytes>,
    request_len: usize,
    timeout: Option<Duration>,
    created_at: std::time::Instant,
}

impl DelegatedReplySender {
    /// Create a new delegated reply sender
    pub fn new(
        sender: tokio::sync::oneshot::Sender<bytes::Bytes>,
        receiver: tokio::sync::oneshot::Receiver<bytes::Bytes>,
        request_len: usize,
    ) -> Self {
        Self {
            sender,
            receiver,
            request_len,
            timeout: None,
            created_at: std::time::Instant::now(),
        }
    }

    /// Create a new delegated reply sender with timeout
    pub fn new_with_timeout(
        sender: tokio::sync::oneshot::Sender<bytes::Bytes>,
        receiver: tokio::sync::oneshot::Receiver<bytes::Bytes>,
        request_len: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            sender,
            receiver,
            request_len,
            timeout: Some(timeout),
            created_at: std::time::Instant::now(),
        }
    }

    /// Send a reply using this delegated sender
    /// This can be called from anywhere in the code to complete the request-response cycle
    pub fn reply(self, response: bytes::Bytes) -> Result<()> {
        match self.sender.send(response) {
            Ok(()) => Ok(()),
            Err(_) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Reply receiver was dropped",
            ))),
        }
    }

    /// Send a reply with error
    pub fn reply_error(self, error: &str) -> Result<()> {
        let error_response = bytes::Bytes::from(format!("ERROR:{}", error).into_bytes());
        self.reply(error_response)
    }

    /// Wait for the reply with optional timeout
    pub async fn wait_for_reply(self) -> Result<bytes::Bytes> {
        if let Some(timeout) = self.timeout {
            match tokio::time::timeout(timeout, self.receiver).await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(_)) => Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Reply sender was dropped",
                ))),
                Err(_) => Err(GossipError::Timeout),
            }
        } else {
            match self.receiver.await {
                Ok(response) => Ok(response),
                Err(_) => Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Reply sender was dropped",
                ))),
            }
        }
    }

    /// Wait for the reply with a custom timeout
    pub async fn wait_for_reply_with_timeout(self, timeout: Duration) -> Result<bytes::Bytes> {
        match tokio::time::timeout(timeout, self.receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Reply sender was dropped",
            ))),
            Err(_) => Err(GossipError::Timeout),
        }
    }

    /// Get the original request length (useful for creating mock responses)
    pub fn request_len(&self) -> usize {
        self.request_len
    }

    /// Get the elapsed time since the request was made
    pub fn elapsed(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Check if this reply sender has timed out
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.created_at.elapsed() > timeout
        } else {
            false
        }
    }

    /// Create a mock reply for testing (simulates the original ask() behavior)
    pub fn create_mock_reply(&self) -> bytes::Bytes {
        bytes::Bytes::from(format!("RESPONSE:{}", self.request_len).into_bytes())
    }
}

impl std::fmt::Debug for DelegatedReplySender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegatedReplySender")
            .field("request_len", &self.request_len)
            .field("timeout", &self.timeout)
            .field("elapsed", &self.elapsed())
            .field("is_timed_out", &self.is_timed_out())
            .finish()
    }
}

impl ConnectionHandle {
    /// Send pre-serialized data through this connection - LOCK-FREE
    pub async fn send_data(&self, data: Vec<u8>) -> Result<()> {
        self.stream_handle
            .write_bytes_control(bytes::Bytes::from(data))
            .await
    }

    /// Send raw bytes without any framing - used by ReplyTo
    pub async fn send_raw_bytes(&self, data: &[u8]) -> Result<()> {
        // Must copy here since we don't own the data
        self.stream_handle
            .write_bytes_control(bytes::Bytes::copy_from_slice(data))
            .await
    }

    /// Send a response payload with framing, without copying the payload.
    pub async fn send_response_bytes(
        &self,
        correlation_id: u16,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );

        self.stream_handle
            .write_header_and_payload_control_inline(header, 8, payload)
            .await
    }

    /// Send a response payload using a Buf without copying.
    pub async fn send_response_buf<B>(
        &self,
        correlation_id: u16,
        payload: B,
        payload_len: usize,
    ) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload_len,
        );
        let buf = bytes::Bytes::copy_from_slice(&header).chain(payload);
        self.stream_handle.write_buf_control(buf).await
    }

    /// Send a response payload using a pooled payload without dynamic dispatch.
    pub async fn send_response_pooled(
        &self,
        correlation_id: u16,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 8]>,
        payload_len: usize,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload_len,
        );
        let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
        self.stream_handle
            .write_pooled_control_inline(header, 8, prefix, prefix_len, payload)
            .await
    }

    /// Send bytes without copying - TRUE ZERO-COPY
    pub async fn send_bytes_zero_copy(&self, data: bytes::Bytes) -> Result<()> {
        self.stream_handle.write_bytes_control(data).await
    }

    /// Stream a large message directly - MAXIMUM PERFORMANCE
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        self.stream_handle
            .stream_large_message(msg, type_hash, actor_id)
            .await
    }

    /// Get the streaming threshold for this connection
    ///
    /// Messages larger than this threshold should be sent via streaming
    /// rather than through the ring buffer to prevent message loss.
    pub fn streaming_threshold(&self) -> usize {
        self.stream_handle.streaming_threshold()
    }

    /// Raw tell() - LOCK-FREE write (used internally)
    pub async fn tell_raw(&self, data: &[u8]) -> Result<()> {
        // Create message with length header using BytesMut for efficiency
        let mut message = bytes::BytesMut::with_capacity(4 + data.len());
        message.extend_from_slice(&(data.len() as u32).to_be_bytes());
        message.extend_from_slice(data);

        self.stream_handle
            .write_bytes_control(message.freeze())
            .await
    }

    /// Tell using owned bytes to avoid payload copies.
    pub async fn tell_bytes(&self, data: bytes::Bytes) -> Result<()> {
        let mut header = [0u8; 8];
        header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());

        self.stream_handle
            .write_header_and_payload_control_inline(header, 4, data)
            .await
    }

    /// Tell with typed payload (rkyv) and debug-only type hash verification.
    pub async fn tell_typed<T>(&self, value: &T) -> Result<()>
    where
        T: crate::typed::WireEncode,
    {
        let payload = crate::typed::encode_typed_pooled(value)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<T>(payload);
        let mut header = [0u8; 8];
        header[..4].copy_from_slice(&(payload_len as u32).to_be_bytes());
        let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
        self.stream_handle
            .write_pooled_control_inline(header, 4, prefix, prefix_len, payload)
            .await
    }

    /// Send a pre-formatted binary message (already has length prefix)
    pub async fn send_binary_message(&self, message: &[u8]) -> Result<()> {
        // Message already has length prefix, send as-is
        self.stream_handle
            .write_bytes_control(bytes::Bytes::copy_from_slice(message))
            .await
    }

    /// Smart tell() - accepts TellMessage with automatic batch detection
    pub async fn tell<'a, T: Into<TellMessage<'a>>>(&self, message: T) -> Result<()> {
        let tell_message = message.into();
        tell_message.send_via(self).await
    }

    /// Smart tell() - automatically uses batching for Vec<T>
    pub async fn tell_smart<T: AsRef<[u8]>>(&self, payload: &[T]) -> Result<()> {
        if payload.len() == 1 {
            // Single message - use regular tell
            self.tell(payload[0].as_ref()).await
        } else {
            // Multiple messages - use batch
            let batch: Vec<&[u8]> = payload.iter().map(|item| item.as_ref()).collect();
            self.tell_batch(&batch).await
        }
    }

    /// Tell with automatic batching detection
    pub async fn tell_auto(&self, data: &[u8]) -> Result<()> {
        // Single message path
        self.tell(data).await
    }

    /// Tell multiple messages with a single call
    pub async fn tell_many<T: AsRef<[u8]>>(&self, messages: &[T]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        if messages.len() == 1 {
            // Single message optimization
            self.tell(messages[0].as_ref()).await
        } else {
            // Batch multiple messages
            let batch: Vec<&[u8]> = messages.iter().map(|msg| msg.as_ref()).collect();
            self.tell_batch(&batch).await
        }
    }

    /// Send a TellMessage (single or batch) - same as tell() but explicit
    pub async fn send_tell_message(&self, message: TellMessage<'_>) -> Result<()> {
        message.send_via(self).await
    }

    /// Universal send() - detects single vs multiple messages automatically
    pub async fn send_messages(&self, messages: &[&[u8]]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        if messages.len() == 1 {
            self.tell_raw(messages[0]).await
        } else {
            self.tell_batch(messages).await
        }
    }

    /// Batch tell() for multiple messages - LOCK-FREE
    pub async fn tell_batch(&self, messages: &[&[u8]]) -> Result<()> {
        // OPTIMIZATION: Pre-calculate total size and write in one go
        let total_size: usize = messages.iter().map(|m| 4 + m.len()).sum();
        let mut batch_buffer = bytes::BytesMut::with_capacity(total_size);

        for msg in messages {
            batch_buffer.extend_from_slice(&(msg.len() as u32).to_be_bytes());
            batch_buffer.extend_from_slice(msg);
        }

        self.stream_handle
            .write_bytes_control(batch_buffer.freeze())
            .await
    }

    /// Direct access to try_send for maximum performance testing
    pub fn try_send_direct(&self, _data: &[u8]) -> Result<()> {
        // Direct TCP doesn't support try_send - would need try_lock
        Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "use tell() for direct TCP writes",
        )))
    }

    /// Send raw bytes through existing connection (zero-copy where possible)
    pub async fn send_raw(&self, data: &[u8]) -> Result<()> {
        // Direct TCP write
        self.tell_raw(data).await
    }

    /// Ask method for request-response (Note: This is a simplified implementation)
    /// For full request-response, you would need a proper protocol with correlation IDs
    pub async fn ask(&self, request: &[u8]) -> Result<Vec<u8>> {
        // Use default timeout of 30 seconds
        self.ask_with_timeout(request, Duration::from_secs(30))
            .await
    }

    /// Ask method with custom timeout for request-response
    pub async fn ask_with_timeout(&self, request: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        // Allocate correlation ID
        let correlation_id = self.correlation.allocate();

        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );

        if let Err(e) = self
            .stream_handle
            .write_header_and_payload_ask_inline(
                header,
                8,
                bytes::Bytes::copy_from_slice(request),
            )
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        let response = self
            .correlation
            .wait_for_response(correlation_id, timeout)
            .await?;
        Ok(response.to_vec())
    }

    /// Ask using owned bytes to avoid payload copies. Returns response as Bytes.
    pub async fn ask_bytes(&self, request: bytes::Bytes) -> Result<bytes::Bytes> {
        self.ask_with_timeout_bytes(request, Duration::from_secs(30))
            .await
    }

    /// Ask with typed request/response (rkyv) and debug-only type hash verification.
    pub async fn ask_typed<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            > + rkyv::Deserialize<Resp, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, Duration::from_secs(30))
            .await?;
        crate::typed::decode_typed::<Resp>(response.as_ref())
    }

    /// Ask with typed request/response and custom timeout.
    pub async fn ask_typed_with_timeout<Req, Resp>(
        &self,
        request: &Req,
        timeout: Duration,
    ) -> Result<Resp>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            > + rkyv::Deserialize<Resp, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, timeout)
            .await?;
        crate::typed::decode_typed::<Resp>(response.as_ref())
    }

    /// Ask with typed request and return a zero-copy archived response.
    pub async fn ask_typed_archived<Req, Resp>(
        &self,
        request: &Req,
    ) -> Result<crate::typed::ArchivedBytes<Resp>>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, Duration::from_secs(30))
            .await?;
        crate::typed::decode_typed_archived::<Resp>(response)
    }

    /// Ask with typed request and custom timeout, returning a zero-copy archived response.
    pub async fn ask_typed_archived_with_timeout<Req, Resp>(
        &self,
        request: &Req,
        timeout: Duration,
    ) -> Result<crate::typed::ArchivedBytes<Resp>>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let response = self
            .ask_with_timeout_pooled(payload, prefix, payload_len, timeout)
            .await?;
        crate::typed::decode_typed_archived::<Resp>(response)
    }

    async fn ask_with_timeout_pooled(
        &self,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 8]>,
        payload_len: usize,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation.allocate();
        let header =
            framing::write_ask_response_header(crate::MessageType::Ask, correlation_id, payload_len);
        let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;

        if let Err(e) = self
            .stream_handle
            .write_pooled_ask_inline(header, 8, prefix, prefix_len, payload)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response(correlation_id, timeout)
            .await
    }

    /// Ask using owned bytes and custom timeout. Returns response as Bytes.
    pub async fn ask_with_timeout_bytes(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation.allocate();

        let header =
            framing::write_ask_response_header(crate::MessageType::Ask, correlation_id, request.len());

        if let Err(e) = self
            .stream_handle
            .write_header_and_payload_ask_inline(header, 8, request)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response(correlation_id, timeout)
            .await
    }

    /// Ask method that returns a ReplyTo handle for delegated replies
    pub async fn ask_with_reply_to(&self, request: &[u8]) -> Result<crate::ReplyTo> {
        // Allocate correlation ID
        let correlation_id = self.correlation.allocate();

        // Create ask message with length prefix + 4-byte header + payload
        let mut message = bytes::BytesMut::with_capacity(
            framing::ASK_RESPONSE_FRAME_HEADER_LEN + request.len(),
        );
        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );
        message.extend_from_slice(&header);
        message.extend_from_slice(request);

        if let Err(e) = self.stream_handle.write_bytes_ask(message.freeze()).await {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        // Return ReplyTo handle
        Ok(crate::ReplyTo {
            correlation_id,
            connection: Arc::new(self.clone()),
        })
    }

    /// Ask method with delegated reply sender for asynchronous response handling
    /// Returns a DelegatedReplySender that can be passed around to handle the response elsewhere
    pub async fn ask_with_reply_sender(&self, request: &[u8]) -> Result<DelegatedReplySender> {
        // Create a oneshot channel for the response
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel::<bytes::Bytes>();

        // Send the request (in a real implementation, this would include correlation ID)
        self.tell(request).await?;

        // Return the delegated reply sender
        Ok(DelegatedReplySender::new(
            reply_sender,
            reply_receiver,
            request.len(),
        ))
    }

    /// Ask method with timeout and delegated reply
    pub async fn ask_with_timeout_and_reply(
        &self,
        request: &[u8],
        timeout: Duration,
    ) -> Result<DelegatedReplySender> {
        // Create a oneshot channel for the response
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel::<bytes::Bytes>();

        // Send the request
        self.tell(request).await?;

        // Return the delegated reply sender with timeout
        Ok(DelegatedReplySender::new_with_timeout(
            reply_sender,
            reply_receiver,
            request.len(),
            timeout,
        ))
    }

    /// Batch ask method for multiple requests in a single network round-trip
    /// Returns a vector of response futures that can be awaited independently
    pub async fn ask_batch(
        &self,
        requests: &[&[u8]],
    ) -> Result<Vec<tokio::sync::oneshot::Receiver<bytes::Bytes>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut receivers = Vec::with_capacity(requests.len());
        let mut correlation_ids = Vec::with_capacity(requests.len());
        let mut batch_message = Vec::new();

        // Process each request
        for request in requests {
            // Create oneshot channel for this response
            let (_tx, rx) = tokio::sync::oneshot::channel::<bytes::Bytes>();
            receivers.push(rx);

            // Allocate correlation ID
            let correlation_id = self.correlation.allocate();
            correlation_ids.push(correlation_id);

            // Build ask message: [type:1][correlation_id:2][pad:1] + payload
            let header = framing::write_ask_response_header(
                crate::MessageType::Ask,
                correlation_id,
                request.len(),
            );
            batch_message.extend_from_slice(&header);
            batch_message.extend_from_slice(request);
        }

        if let Err(e) = self
            .stream_handle
            .write_bytes_ask(bytes::Bytes::from(batch_message))
            .await
        {
            for correlation_id in correlation_ids {
                self.correlation.cancel(correlation_id);
            }
            return Err(e);
        }

        Ok(receivers)
    }

    /// Batch ask with timeout - returns Vec<Result<Vec<u8>>> with individual timeout handling
    pub async fn ask_batch_with_timeout(
        &self,
        requests: &[&[u8]],
        timeout: Duration,
    ) -> Result<Vec<Result<bytes::Bytes>>> {
        let receivers = self.ask_batch(requests).await?;

        // Create futures for all responses with timeout
        let mut response_futures = Vec::with_capacity(receivers.len());

        for receiver in receivers {
            let timeout_future = async move {
                match tokio::time::timeout(timeout, receiver).await {
                    Ok(Ok(response)) => Ok(response),
                    Ok(Err(_)) => Err(crate::GossipError::Network(std::io::Error::other(
                        "Response channel closed",
                    ))),
                    Err(_) => Err(crate::GossipError::Timeout),
                }
            };
            response_futures.push(timeout_future);
        }

        // Wait for all responses concurrently
        let results = futures::future::join_all(response_futures).await;
        Ok(results)
    }

    /// High-performance batch ask with pre-allocated buffers
    /// This version minimizes allocations for maximum throughput
    pub async fn ask_batch_optimized(
        &self,
        requests: &[&[u8]],
        response_buffer: &mut Vec<tokio::sync::oneshot::Receiver<bytes::Bytes>>,
    ) -> Result<()> {
        response_buffer.clear();
        response_buffer.reserve(requests.len());

        // Pre-calculate total message size
        let total_size: usize = requests
            .iter()
            .map(|req| framing::ASK_RESPONSE_FRAME_HEADER_LEN + req.len())
            .sum();

        let mut batch_message = bytes::BytesMut::with_capacity(total_size);
        let mut correlation_ids = Vec::with_capacity(requests.len());

        // Build all messages
        for request in requests {
            // Create oneshot channel for this response
            let (_tx, rx) = tokio::sync::oneshot::channel::<bytes::Bytes>();
            response_buffer.push(rx);

            // Allocate correlation ID
            let correlation_id = self.correlation.allocate();
            correlation_ids.push(correlation_id);

            // Build ask message
            let header = framing::write_ask_response_header(
                crate::MessageType::Ask,
                correlation_id,
                request.len(),
            );
            batch_message.extend_from_slice(&header);
            batch_message.extend_from_slice(request);
        }

        if let Err(e) = self
            .stream_handle
            .write_bytes_ask(batch_message.freeze())
            .await
        {
            for correlation_id in correlation_ids {
                self.correlation.cancel(correlation_id);
            }
            return Err(e);
        }

        Ok(())
    }

    /// High-performance streaming API - send structured data with custom framing - LOCK-FREE
    pub async fn stream_send<T>(&self, data: &T) -> Result<()>
    where
        T: for<'a> rkyv::Serialize<
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
        // Serialize the data using rkyv for maximum performance
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(data)
            .map_err(crate::GossipError::Serialization)?;

        // Create stream frame: [frame_type, channel_id, flags, seq_id[2], payload_len[4]]
        let frame_header = StreamFrameHeader {
            frame_type: StreamFrameType::Data as u8,
            channel_id: ChannelId::TellAsk as u8,
            flags: 0,
            sequence_id: self.stream_handle.next_frame_sequence_id(),
            payload_len: payload.len() as u32,
        };

        let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header)
            .map_err(crate::GossipError::Serialization)?;

        // Combine header and payload for single write
        let mut combined = bytes::BytesMut::with_capacity(header_bytes.len() + payload.len());
        combined.extend_from_slice(&header_bytes);
        combined.extend_from_slice(&payload);

        // Use lock-free ring buffer - NO MUTEX!
        self.stream_handle
            .write_bytes_nonblocking(combined.freeze())
    }

    /// High-performance streaming API - send batch of structured data - LOCK-FREE
    pub async fn stream_send_batch<T>(&self, batch: &[T]) -> Result<()>
    where
        T: for<'a> rkyv::Serialize<
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
        if batch.is_empty() {
            return Ok(());
        }

        // Pre-allocate buffer for entire batch
        let mut total_payload = Vec::new();

        for item in batch {
            let payload = rkyv::to_bytes::<rkyv::rancor::Error>(item)
                .map_err(crate::GossipError::Serialization)?;

            let frame_header = StreamFrameHeader {
                frame_type: StreamFrameType::Data as u8,
                channel_id: ChannelId::TellAsk as u8,
                flags: if std::ptr::eq(item, batch.last().unwrap()) {
                    0
                } else {
                    StreamFrameFlags::More as u8
                },
                sequence_id: self.stream_handle.next_frame_sequence_id(),
                payload_len: payload.len() as u32,
            };

            let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header)
                .map_err(crate::GossipError::Serialization)?;
            total_payload.extend_from_slice(&header_bytes);
            total_payload.extend_from_slice(&payload);
        }

        // Use lock-free ring buffer for entire batch - NO MUTEX!
        self.stream_handle
            .write_bytes_nonblocking(bytes::Bytes::from(total_payload))
    }

    /// Get truly lock-free streaming handle - direct access to the internal handle
    pub fn get_lock_free_stream(&self) -> &Arc<LockFreeStreamHandle> {
        &self.stream_handle
    }

    /// Zero-copy vectored write for header + payload in single syscall
    /// This eliminates the need to copy payload data into frame buffer
    pub fn write_bytes_vectored(&self, header: bytes::Bytes, payload: bytes::Bytes) -> Result<()> {
        self.stream_handle.write_bytes_vectored(header, payload)
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        self.stream_handle.write_owned_chunks(chunks)
    }
}

impl ConnectionPool {
    pub fn new(max_connections: usize, connection_timeout: Duration) -> Self {
        const POOL_SIZE: usize = 256;
        const BUFFER_SIZE: usize = TCP_BUFFER_SIZE / 128; // Smaller pool buffers (8KB default)

        let pool = Self {
            connections_by_peer: dashmap::DashMap::new(),
            addr_to_peer_id: dashmap::DashMap::new(),
            peer_id_to_addr: dashmap::DashMap::new(),
            connections_by_addr: dashmap::DashMap::new(),
            correlation_trackers: dashmap::DashMap::new(),
            max_connections,
            connection_timeout,
            registry: None,
            message_buffer_pool: Arc::new(MessageBufferPool::new(POOL_SIZE, BUFFER_SIZE)),
            connection_counter: AtomicUsize::new(0),
        };

        // Log the pool's address for debugging
        debug!(
            "CONNECTION POOL: Created new pool at {:p}",
            &pool as *const _
        );
        pool
    }

    /// Set the registry reference for handling incoming messages
    pub fn set_registry(&mut self, registry: std::sync::Arc<GossipRegistry>) {
        self.registry = Some(std::sync::Arc::downgrade(&registry));
    }

    fn clear_capabilities_for_addr(&self, addr: &SocketAddr) {
        if let Some(registry) = self.registry.as_ref().and_then(|w| w.upgrade()) {
            registry.clear_peer_capabilities(addr);
        }
    }

    /// Store or update the address for a peer
    /// Only updates if no address is already configured for this peer
    pub fn update_node_address(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        // Check if we already have a configured address for this node
        if let Some(existing_addr_entry) = self.peer_id_to_addr.get(peer_id) {
            let existing_addr = *existing_addr_entry.value();
            debug!("CONNECTION POOL: Node {} already has configured address {}, not updating to ephemeral port {}",
                   peer_id, existing_addr, addr);
            return;
        }

        // Only update if no address is configured
        self.peer_id_to_addr.insert(peer_id.clone(), addr);
        self.addr_to_peer_id.insert(addr, peer_id.clone());
        debug!(
            "CONNECTION POOL: Set initial address for node {} to {}",
            peer_id, addr
        );
    }

    /// Reindex an existing connection under a new logical address for the peer
    pub fn reindex_connection_addr(&self, peer_id: &crate::PeerId, new_addr: SocketAddr) {
        if let Some(entry) = self.connections_by_peer.get(peer_id) {
            let connection = entry.value().clone();
            let old_addr = connection.addr;

            if old_addr == new_addr {
                return;
            }

            self.connections_by_addr.insert(new_addr, connection);
            self.addr_to_peer_id.insert(new_addr, peer_id.clone());

            self.connections_by_addr.remove(&old_addr);
            self.addr_to_peer_id.remove(&old_addr);

            debug!(
                "CONNECTION POOL: Reindexed peer {} from {} to {}",
                peer_id, old_addr, new_addr
            );
        }
    }

    /// Get a connection by peer ID
    pub fn get_connection_by_peer_id(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        // PRIMARY: Look up connection directly by peer ID
        if let Some(conn_entry) = self.connections_by_peer.get(peer_id) {
            let conn = conn_entry.value().clone();
            if conn.is_connected() {
                debug!("CONNECTION POOL: Found connection for peer '{}'", peer_id);
                Some(conn)
            } else {
                warn!(
                    "CONNECTION POOL: Connection for peer '{}' is disconnected",
                    peer_id
                );
                None
            }
        } else {
            warn!(
                "CONNECTION POOL: No connection found for peer '{}'",
                peer_id
            );
            // Debug: show what nodes we do have connections for
            let connected_nodes: Vec<String> = self
                .connections_by_peer
                .iter()
                .map(|entry| entry.key().to_hex())
                .collect();
            warn!(
                "CONNECTION POOL: Available node connections: {:?}",
                connected_nodes
            );
            None
        }
    }

    /// Get a connection by socket address
    pub fn get_connection_by_addr(
        &self,
        addr: &SocketAddr,
    ) -> Option<Arc<LockFreeConnection>> {
        self.connections_by_addr.get(addr).and_then(|entry| {
            let conn = entry.value().clone();
            if conn.is_connected() {
                Some(conn)
            } else {
                None
            }
        })
    }

    /// Get or create a correlation tracker for a peer
    pub(crate) fn get_or_create_correlation_tracker(
        &self,
        peer_id: &crate::PeerId,
    ) -> Arc<CorrelationTracker> {
        let tracker = self
            .correlation_trackers
            .entry(peer_id.clone())
            .or_insert_with(|| {
                debug!(
                    "CONNECTION POOL: Creating new correlation tracker for peer {}",
                    peer_id
                );
                CorrelationTracker::new()
            })
            .clone();
        debug!(
            "CONNECTION POOL: Got correlation tracker for peer {} (total trackers: {})",
            peer_id,
            self.correlation_trackers.len()
        );
        tracker
    }

    /// Add a connection indexed by peer ID
    pub fn add_connection_by_peer_id(
        &self,
        peer_id: crate::PeerId,
        addr: SocketAddr,
        mut connection: Arc<LockFreeConnection>,
    ) -> bool {
        // Only set correlation tracker if the connection doesn't already have one
        if connection.correlation.is_none() {
            // Get or create shared correlation tracker for this peer
            let correlation_tracker = self.get_or_create_correlation_tracker(&peer_id);

            // Set the correlation tracker on the connection
            // We need to make the connection mutable
            if let Some(conn_mut) = Arc::get_mut(&mut connection) {
                conn_mut.correlation = Some(correlation_tracker);
            } else {
                warn!(
                    "CONNECTION POOL: Cannot set correlation tracker - Arc has multiple references"
                );
            }
        } else {
            // Connection already has a correlation tracker - ensure it's registered
            if let Some(ref correlation) = connection.correlation {
                self.correlation_trackers
                    .insert(peer_id.clone(), correlation.clone());
                debug!(
                    "CONNECTION POOL: Registered existing correlation tracker for peer '{}'",
                    peer_id
                );
            }
        }

        // Update the address mappings
        self.addr_to_peer_id.insert(addr, peer_id.clone());

        debug!(
            "CONNECTION POOL: Added connection for peer '{}' (address: {})",
            peer_id, addr
        );

        // PRIMARY: Store the connection by peer ID
        self.connections_by_peer.insert(peer_id, connection.clone());

        // Also index by address for direct lookups
        self.connections_by_addr.insert(addr, connection);

        self.connection_counter.fetch_add(1, Ordering::AcqRel);
        true
    }

    /// Send data to a peer by ID
    pub fn send_to_peer_id(&self, peer_id: &crate::PeerId, data: &[u8]) -> Result<()> {
        debug!(
            "CONNECTION POOL: send_to_peer_id called for peer '{}', pool has {} peer connections",
            peer_id,
            self.connections_by_peer.len()
        );
        if let Some(connection) = self.get_connection_by_peer_id(peer_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                debug!(
                    "CONNECTION POOL: Sending {} bytes to peer '{}'",
                    data.len(),
                    peer_id
                );
                return stream_handle.write_bytes_nonblocking(bytes::Bytes::copy_from_slice(data));
            } else {
                warn!(peer_id = %peer_id, "Connection found but no stream handle");
            }
        } else {
            warn!(peer_id = %peer_id, "No connection found for peer");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Connection not found for peer {}", peer_id),
        )))
    }

    /// Send bytes to a peer by its ID (zero-copy version)
    pub fn send_bytes_to_peer_id(&self, peer_id: &crate::PeerId, data: bytes::Bytes) -> Result<()> {
        debug!(
            "CONNECTION POOL: send_bytes_to_peer_id called for peer '{}', pool has {} peer connections",
            peer_id,
            self.connections_by_peer.len()
        );
        if let Some(connection) = self.get_connection_by_peer_id(peer_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                debug!(
                    "CONNECTION POOL: Sending {} bytes to peer '{}'",
                    data.len(),
                    peer_id
                );
                return stream_handle.write_bytes_nonblocking(data);
            } else {
                warn!(peer_id = %peer_id, "Connection found but no stream handle");
            }
        } else {
            warn!(peer_id = %peer_id, "No connection found for peer");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Connection not found for peer {}", peer_id),
        )))
    }

    /// Get or create a lock-free connection - NO MUTEX NEEDED
    pub fn get_lock_free_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        self.connections_by_addr
            .get(&addr)
            .map(|entry| entry.value().clone())
    }

    /// Add a new lock-free connection - completely lock-free operation
    pub fn add_lock_free_connection(
        &self,
        addr: SocketAddr,
        tcp_stream: TcpStream,
    ) -> Result<Arc<LockFreeConnection>> {
        let connection_count = self.connection_counter.fetch_add(1, Ordering::AcqRel);

        if connection_count >= self.max_connections {
            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            return Err(crate::GossipError::Network(std::io::Error::other(format!(
                "Max connections ({}) reached",
                self.max_connections
            ))));
        }

        // Split the stream for reading and writing
        let (reader, writer) = tcp_stream.into_split();

        // Create lock-free streaming handle with exclusive socket ownership
        let buffer_config = self
            .registry
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|registry| {
                BufferConfig::default().with_ask_inflight_limit(registry.config.ask_inflight_limit)
            })
            .unwrap_or_else(BufferConfig::default);

        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer, // Pass writer half
            addr,
            ChannelId::Global,
            buffer_config,
        ));

        let mut connection = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        connection.stream_handle = Some(stream_handle);
        connection.set_state(ConnectionState::Connected);
        connection.update_last_used();

        let connection_arc = Arc::new(connection);

        // Spawn reader task for this connection
        // This reader needs to process incoming messages on outgoing connections
        let reader_connection = connection_arc.clone();
        let registry_weak = self.registry.clone();
        tokio::spawn(async move {
            info!(peer = %addr, "Starting reader task for outgoing connection");
            Self::handle_persistent_connection_reader(reader, None, addr, registry_weak).await;
            reader_connection.set_state(ConnectionState::Disconnected);
            info!(peer = %addr, "Reader task for outgoing connection ended");
        });

        // Insert into lock-free hash map
        self.connections_by_addr
            .insert(addr, connection_arc.clone());
        debug!(
            "CONNECTION POOL: Added lock-free connection to {} - pool now has {} connections",
            addr,
            self.connections_by_addr.len()
        );

        Ok(connection_arc)
    }

    /// Send data through lock-free connection - NO BLOCKING
    pub fn send_lock_free(&self, addr: SocketAddr, data: &[u8]) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                return stream_handle.write_bytes_nonblocking(bytes::Bytes::copy_from_slice(data));
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
            warn!(
                "Available connections: {:?}",
                self.connections_by_addr
                    .iter()
                    .map(|e| *e.key())
                    .collect::<Vec<_>>()
            );
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found",
        )))
    }

    /// Send header + payload without copying the payload.
    pub fn send_lock_free_parts(
        &self,
        addr: SocketAddr,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                stream_handle.write_header_and_payload_nonblocking_checked(header, payload)?;
                return Ok(());
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found",
        )))
    }

    /// Try to send data through any available connection for a node
    /// This handles cases where we might have multiple connections (incoming/outgoing)
    pub fn send_to_node(
        &self,
        node_addr: SocketAddr,
        data: &[u8],
        _registry: &GossipRegistry,
    ) -> Result<()> {
        // First try direct lookup
        if let Ok(()) = self.send_lock_free(node_addr, data) {
            return Ok(());
        }

        // If that fails, look for any connection that could reach this node
        // This could be enhanced with a node ID -> connections mapping
        debug!(node_addr = %node_addr, "Direct send failed, looking for alternative connections");

        // For now, we'll rely on the caller to handle fallback strategies
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No connection found for node {}", node_addr),
        )))
    }

    /// Remove a connection from the pool - lock-free operation
    pub fn remove_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        // First remove from address-based map
        if let Some((_, connection)) = self.connections_by_addr.remove(&addr) {
            debug!(
                "CONNECTION POOL: Removed connection to {} - pool now has {} connections",
                addr,
                self.connections_by_addr.len()
            );

            // Also remove from node ID mapping
            if let Some(node_id_entry) = self.addr_to_peer_id.remove(&addr) {
                let (_, node_id) = node_id_entry;
                if let Some((_, _)) = self.connections_by_peer.remove(&node_id) {
                    debug!(
                        "CONNECTION POOL: Also removed connection by node ID '{}'",
                        node_id
                    );
                }
            }

            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            self.clear_capabilities_for_addr(&addr);
            Some(connection)
        } else {
            None
        }
    }

    /// Disconnect and remove a connection by peer ID
    pub fn disconnect_connection_by_peer_id(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        if let Some((_, connection)) = self.connections_by_peer.remove(peer_id) {
            let addr = connection.addr;
            self.connections_by_addr.remove(&addr);
            self.addr_to_peer_id.remove(&addr);
            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            self.clear_capabilities_for_addr(&addr);
            Some(connection)
        } else {
            None
        }
    }

    /// Get connection count - lock-free operation
    pub fn connection_count(&self) -> usize {
        self.connections_by_peer
            .iter()
            .filter(|entry| entry.value().is_connected())
            .count()
    }

    /// Get all connected peers - lock-free operation
    pub fn get_connected_peers(&self) -> Vec<SocketAddr> {
        self.connections_by_addr
            .iter()
            .filter_map(|entry| {
                if entry.value().is_connected() {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get all connections (including disconnected) - for debugging
    pub fn get_all_connections(&self) -> Vec<SocketAddr> {
        self.connections_by_addr
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }

    /// Get a buffer from the pool or create a new one
    pub fn get_buffer(&mut self, min_capacity: usize) -> Vec<u8> {
        // Use the message buffer pool for lock-free buffer management
        if let Some(buffer) = self.message_buffer_pool.get_buffer() {
            if buffer.capacity() >= min_capacity {
                return buffer;
            }
            // Buffer too small, return it and create new one
            self.message_buffer_pool.return_buffer(buffer);
        }
        Vec::with_capacity(min_capacity.max(1024)) // Minimum 1KB buffers
    }

    /// Return a buffer to the pool for reuse
    pub fn return_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= TCP_BUFFER_SIZE {
            // Return to the lock-free message buffer pool (up to TCP_BUFFER_SIZE)
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Get a message buffer from the pool for zero-copy processing
    pub fn get_message_buffer(&mut self) -> Vec<u8> {
        self.message_buffer_pool
            .get_buffer()
            .unwrap_or_else(|| Vec::with_capacity(TCP_BUFFER_SIZE / 256)) // Default small buffer
    }

    /// Return a message buffer to the pool
    pub fn return_message_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= TCP_BUFFER_SIZE {
            // Keep buffers with reasonable size (up to TCP_BUFFER_SIZE)
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Create a gossip message buffer with length header (optimized for reuse)
    pub fn create_message_buffer(&mut self, data: &[u8]) -> Vec<u8> {
        let header = framing::write_gossip_frame_prefix(data.len());
        let mut buffer = self.get_buffer(header.len() + data.len());
        buffer.extend_from_slice(&header);
        buffer.extend_from_slice(data);
        buffer
    }

    /// Get or create a persistent connection to a peer
    /// Fast path: Check for existing connection without creating new ones
    pub fn get_existing_connection(&mut self, addr: SocketAddr) -> Option<ConnectionHandle> {
        let _current_time = current_timestamp();

        if let Some(conn) = self.connections_by_addr.get_mut(&addr) {
            // The connection here is a mutable reference to Arc<LockFreeConnection>
            if conn.value().is_connected() {
                conn.value().update_last_used();
                debug!(addr = %addr, "using existing persistent connection (fast path)");
                if let Some(ref stream_handle) = conn.value().stream_handle {
                    return Some(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn
                            .value()
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                    });
                }

                debug!(addr = %addr, "existing connection missing stream handle");
                return None;
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                self.connections_by_addr.remove(&addr);
            }
        }
        None
    }

    /// Get or create a connection to a peer by its ID
    pub async fn get_connection_to_peer(
        &mut self,
        peer_id: &crate::PeerId,
    ) -> Result<ConnectionHandle> {
        debug!(
            "CONNECTION POOL: get_connection_to_peer called for peer '{}'",
            peer_id
        );

        // First check if we already have a connection to this node
        if let Some(conn_entry) = self.connections_by_peer.get(peer_id) {
            let conn = conn_entry.value();
            if conn.is_connected() {
                conn.update_last_used();
                debug!(
                    "CONNECTION POOL: Found existing connection to peer '{}'",
                    peer_id
                );

                if let Some(ref stream_handle) = conn.stream_handle {
                    // Need to get the address for ConnectionHandle
                    let addr = conn.addr;
                    return Ok(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                    });
                } else {
                    return Err(crate::GossipError::Network(std::io::Error::other(
                        "Connection exists but no stream handle",
                    )));
                }
            } else {
                // Remove disconnected connection
                debug!(
                    "CONNECTION POOL: Removing disconnected connection to peer '{}'",
                    peer_id
                );
                drop(conn_entry);
                self.connections_by_peer.remove(peer_id);
            }
        }

        // Look up the address for this node
        let addr = if let Some(addr_entry) = self.peer_id_to_addr.get(peer_id) {
            *addr_entry.value()
        } else {
            return Err(crate::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No address configured for peer '{}'", peer_id),
            )));
        };

        debug!(
            "CONNECTION POOL: Creating new connection to peer '{}' at {}",
            peer_id, addr
        );

        // Convert PeerId to NodeId for TLS
        let node_id_for_tls = Some(peer_id.to_node_id());

        // Create the connection and store it by node ID
        // Pass the NodeId so TLS can work even if gossip state doesn't have it yet
        let handle = self
            .get_connection_with_node_id(addr, node_id_for_tls)
            .await?;

        // After successful connection, ensure it's indexed by node ID
        if let Some(conn) = self.connections_by_addr.get(&addr) {
            self.connections_by_peer
                .insert(peer_id.clone(), conn.value().clone());
            self.addr_to_peer_id.insert(addr, peer_id.clone());
            debug!(
                "CONNECTION POOL: Indexed new connection under peer ID '{}'",
                peer_id
            );
        }

        Ok(handle)
    }

    pub async fn get_connection(&mut self, addr: SocketAddr) -> Result<ConnectionHandle> {
        self.get_connection_with_node_id(addr, None).await
    }

    pub async fn get_connection_with_node_id(
        &mut self,
        addr: SocketAddr,
        node_id: Option<crate::NodeId>,
    ) -> Result<ConnectionHandle> {
        let _current_time = current_timestamp();
        // Debug logging removed for performance - these logs were too verbose
        // debug!("CONNECTION POOL: get_connection called on pool at {:p} for {}", self as *const _, addr);
        // debug!("CONNECTION POOL: This pool instance has {} connections stored", self.connections_by_addr.len());

        // Use address index to reuse existing connections when available

        // Check if we already have a lock-free connection
        if let Some(entry) = self.connections_by_addr.get(&addr) {
            let conn = entry.value();
            if conn.is_connected() {
                conn.update_last_used();
                debug!(addr = %addr, "found existing lock-free connection, reusing handle");

                // Return the existing lock-free connection handle
                if let Some(ref stream_handle) = conn.stream_handle {
                    return Ok(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                    });
                } else {
                    // Connection exists but no stream handle - this shouldn't happen
                    return Err(crate::GossipError::Network(std::io::Error::other(
                        "Connection exists but no stream handle",
                    )));
                }
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                drop(entry);
                self.connections_by_addr.remove(&addr);
            }
        }

        // Extract what we need before any await points to avoid Send issues
        let max_connections = self.max_connections;
        let connection_timeout = self.connection_timeout;
        let registry_weak = self.registry.clone();
        let resolved_node_id = match node_id {
            Some(node_id) => Some(node_id),
            None => {
                if let Some(registry_arc) = registry_weak.as_ref().and_then(|w| w.upgrade()) {
                    registry_arc
                        .lookup_node_id(&addr)
                        .await
                        .or_else(|| {
                            registry_arc
                                .peer_capability_addr_to_node
                                .get(&addr)
                                .map(|entry| *entry.value())
                        })
                } else {
                    None
                }
            }
        };

        // Make room if necessary - operate on self.connections_by_addr directly!
        if self.connections_by_addr.len() >= max_connections {
            let oldest_addr = self
                .connections_by_addr
                .iter()
                .min_by_key(|entry| entry.value().last_used.load(Ordering::Acquire))
                .map(|entry| *entry.key());

            if let Some(oldest) = oldest_addr {
                self.connections_by_addr.remove(&oldest);
                warn!(addr = %oldest, "removed oldest connection to make room");
            }
        }

        // Duplicate connection tie-breaker: decide whether to reuse an existing link
        if let (Some(registry_arc), Some(node_id_value)) = (
            registry_weak.as_ref().and_then(|w| w.upgrade()),
            resolved_node_id.as_ref(),
        ) {
            let remote_peer_id = crate::PeerId::from(node_id_value);
            if let Some(existing_conn) = self.get_connection_by_peer_id(&remote_peer_id) {
                if !registry_arc.should_keep_connection(&remote_peer_id, true) {
                    debug!(
                        remote = %remote_peer_id,
                        "tie-breaker: reusing existing connection instead of dialing outbound"
                    );
                    if let Some(ref stream_handle) = existing_conn.stream_handle {
                        return Ok(ConnectionHandle {
                            addr: existing_conn.addr,
                            stream_handle: stream_handle.clone(),
                            correlation: existing_conn
                                .correlation
                                .clone()
                                .unwrap_or_else(CorrelationTracker::new),
                        });
                    } else {
                        return Err(GossipError::Network(std::io::Error::other(
                            "Existing connection missing stream handle",
                        )));
                    }
                } else {
                    debug!(
                        remote = %remote_peer_id,
                        "tie-breaker: replacing existing connection with outbound dial"
                    );
                    if let Some(removed) = self.disconnect_connection_by_peer_id(&remote_peer_id) {
                        if let Some(handle) = removed.stream_handle.as_ref() {
                            handle.shutdown();
                        }
                    }
                }
            }
        }

        // Connect with timeout
        debug!("CONNECTION POOL: Attempting to connect to {}", addr);
        let stream = tokio::time::timeout(connection_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                debug!("CONNECTION POOL: Connection to {} timed out after {:?}", addr, connection_timeout);
                GossipError::Timeout
            })?
            .map_err(|e| {
                debug!("CONNECTION POOL: Connection to {} failed: {} (will retry in {}s if this is a gossip peer)",
                      addr, e, 5); // 5s is the default retry interval
                GossipError::Network(e)
            })?;
        debug!("CONNECTION POOL: Successfully connected to {}", addr);

        // Configure socket
        stream.set_nodelay(true).map_err(GossipError::Network)?;

        // Check if TLS is enabled
        let (mut reader, writer) = if let Some(registry_arc) =
            registry_weak.as_ref().and_then(|w| w.upgrade())
        {
            if let Some(tls_config) = &registry_arc.tls_config {
                // TLS is enabled - perform handshake as client
                debug!("CONNECTION POOL: TLS enabled, checking for peer NodeId");

                // Use provided NodeId if available, otherwise look it up from gossip state
                let peer_node_id = resolved_node_id.or_else(|| {
                    registry_arc
                        .peer_capability_addr_to_node
                        .get(&addr)
                        .map(|entry| *entry.value())
                });

                let mut discovered_node_id = peer_node_id;
                let (server_name, server_name_label) = if let Some(node_id) = discovered_node_id {
                    debug!(
                        "CONNECTION POOL: Found NodeId for peer {}, performing TLS handshake",
                        addr
                    );
                    let dns_name = crate::tls::name::encode(&node_id);
                    let server_name = rustls::pki_types::ServerName::try_from(dns_name)
                        .map_err(|e| GossipError::TlsError(format!("Invalid DNS name: {}", e)))?;
                    (server_name, format!("NodeId {}", node_id.fmt_short()))
                } else {
                    // Use placeholder DNS name so TLS can still negotiate; verifier will extract NodeId from cert
                    let placeholder = format!("peer-{}.kameo.invalid", addr.port());
                    let server_name = rustls::pki_types::ServerName::try_from(placeholder.clone())
                        .map_err(|e| {
                            GossipError::TlsError(format!("Invalid fallback DNS name: {}", e))
                        })?;
                    (
                        server_name,
                        format!("placeholder SNI {} (NodeId unknown)", placeholder),
                    )
                };

                info!(
                    "🔐 TLS ENABLED: Initiating TLS connection to {} using {}",
                    addr, server_name_label
                );
                let connector = tokio_rustls::TlsConnector::from(tls_config.client_config.clone());

                match tokio::time::timeout(
                    Duration::from_secs(10),
                    connector.connect(server_name, stream),
                )
                .await
                {
                    Ok(Ok(mut tls_stream)) => {
                        if discovered_node_id.is_none() {
                            if let Some(certs) = tls_stream.get_ref().1.peer_certificates() {
                                if let Some(cert) = certs.first() {
                                    match crate::tls::extract_node_id_from_cert(cert) {
                                        Ok(node_id) => {
                                            debug!(
                                                addr = %addr,
                                                "Extracted NodeId {} from peer certificate",
                                                node_id.fmt_short()
                                            );
                                            if registry_arc.lookup_node_id(&addr).await.is_none() {
                                                registry_arc
                                                    .add_peer_with_node_id(addr, Some(node_id))
                                                    .await;
                                            }
                                            discovered_node_id = Some(node_id);
                                        }
                                        Err(err) => {
                                            warn!(
                                                addr = %addr,
                                                error = %err,
                                                "Failed to extract NodeId from peer certificate"
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        if let Some(node_id) = discovered_node_id {
                            info!(
                                "✅ TLS handshake successful with {} (NodeId: {})",
                                addr,
                                node_id.fmt_short()
                            );
                        } else {
                            info!("✅ TLS handshake successful with {} (NodeId unknown)", addr);
                        }

                        let negotiated_alpn = tls_stream
                            .get_ref()
                            .1
                            .alpn_protocol()
                            .map(|proto| proto.to_vec());

                        let enable_peer_discovery = registry_arc.config.enable_peer_discovery;
                        let peer_caps = match crate::handshake::perform_hello_handshake(
                            &mut tls_stream,
                            negotiated_alpn.as_deref(),
                            enable_peer_discovery,
                        )
                        .await
                        {
                            Ok(caps) => {
                                eprintln!(
                                    "outbound hello capabilities to {} can_send={}",
                                    addr,
                                    caps.can_send_peer_list()
                                );
                                caps
                            }
                            Err(err) => {
                                warn!(
                                    addr = %addr,
                                    error = %err,
                                    "Hello handshake failed after TLS session establishment"
                                );
                                return Err(err);
                            }
                        };
                        registry_arc.set_peer_capabilities(addr, peer_caps.clone());

                        if let Some(node_id) = discovered_node_id
                            .or_else(|| registry_arc.lookup_node_id(&addr).now_or_never().flatten())
                        {
                            registry_arc
                                .associate_peer_capabilities_with_node(addr, node_id)
                                .await;
                        }

                        let (read_half, write_half) = tokio::io::split(tls_stream);
                        (
                            Box::pin(read_half) as Pin<Box<dyn AsyncRead + Send>>,
                            Box::pin(write_half) as Pin<Box<dyn AsyncWrite + Send>>,
                        )
                    }
                    Ok(Err(e)) => {
                        error!(addr = %addr, error = %e, "❌ TLS handshake failed");
                        return Err(GossipError::TlsError(format!(
                            "TLS handshake failed: {}",
                            e
                        )));
                    }
                    Err(_) => {
                        error!(
                            addr = %addr,
                            "⏱️ TLS handshake timed out (NodeId hint: {:?})",
                            discovered_node_id.as_ref().map(|id| id.fmt_short())
                        );
                        return Err(GossipError::Timeout);
                    }
                }
            } else {
                // No TLS configured - panic to enforce TLS-only
                panic!("🔥 TLS is NOT configured but is required! Cannot establish plain TCP connection to {}", addr);
            }
        } else {
            // No registry reference - panic to enforce TLS requirement
            panic!("🔥 No registry reference available - TLS cannot be verified! Cannot establish connection to {}", addr);
        };

        // Create lock-free connection for receiving
        let buffer_config = self
            .registry
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|registry| {
                BufferConfig::default().with_ask_inflight_limit(registry.config.ask_inflight_limit)
            })
            .unwrap_or_else(BufferConfig::default);
        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer,
            addr,
            ChannelId::Global,
            buffer_config,
        ));

        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        conn.stream_handle = Some(stream_handle.clone());
        conn.set_state(ConnectionState::Connected);
        conn.update_last_used();

        // For outgoing connections, we might know the peer ID from configuration
        let peer_id_opt = self
            .addr_to_peer_id
            .get(&addr)
            .map(|entry| entry.clone())
            .or_else(|| {
                // Try reverse lookup: find peer ID that maps to this address
                self.peer_id_to_addr
                    .iter()
                    .find(|entry| entry.value() == &addr)
                    .map(|entry| entry.key().clone())
            });

        if let Some(peer_id) = peer_id_opt {
            // Use shared correlation tracker for this peer
            conn.correlation = Some(self.get_or_create_correlation_tracker(&peer_id));
            debug!(
                "CONNECTION POOL: Using shared correlation tracker for peer {:?} at {}",
                peer_id, addr
            );
        } else {
            // No peer ID yet, create a new correlation tracker
            // This will be replaced when we learn the peer ID from their FullSync message
            conn.correlation = Some(CorrelationTracker::new());
            debug!(
                "CONNECTION POOL: Created new correlation tracker for unknown peer at {}",
                addr
            );
        }

        let connection_arc = Arc::new(conn);

        // Insert into lock-free map before spawning
        self.connections_by_addr
            .insert(addr, connection_arc.clone());
        debug!("CONNECTION POOL: Added connection via get_connection to {} - pool now has {} connections",
              addr, self.connections_by_addr.len());
        // Double check it's really there
        assert!(
            self.connections_by_addr.contains_key(&addr),
            "Connection was not added to pool!"
        );
        debug!("CONNECTION POOL: Verified connection exists for {}", addr);

        // Send initial FullSync message to identify ourselves
        if let Some(registry_arc) = registry_weak.as_ref().and_then(|w| w.upgrade()) {
            let initial_msg = {
                let actor_state = registry_arc.actor_state.read().await;
                let gossip_state = registry_arc.gossip_state.lock().await;

                RegistryMessage::FullSync {
                    local_actors: actor_state.local_actors.clone().into_iter().collect(),
                    known_actors: actor_state.known_actors.clone().into_iter().collect(),
                    sender_peer_id: registry_arc.peer_id.clone(),
                    sequence: gossip_state.gossip_sequence,
                    wall_clock_time: crate::current_timestamp(),
                }
            };

            // Serialize and send the initial message with Gossip type prefix
            match rkyv::to_bytes::<rkyv::rancor::Error>(&initial_msg) {
                Ok(data) => {
                    let header = framing::write_gossip_frame_prefix(data.len());
                    let mut msg_buffer = Vec::with_capacity(header.len() + data.len());
                    msg_buffer.extend_from_slice(&header);
                    msg_buffer.extend_from_slice(&data);

                    // Create a connection handle to send the message
                    let conn_handle = ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: connection_arc
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                    };
                    if let Err(e) = conn_handle.send_data(msg_buffer).await {
                        warn!(peer = %addr, error = %e, "Failed to send initial FullSync message");
                    } else {
                        info!(peer = %addr, "Sent initial FullSync message to identify ourselves");
                    }
                }
                Err(e) => {
                    warn!(peer = %addr, error = %e, "Failed to serialize initial FullSync message");
                }
            }
        }

        // Note: actor_message_handler is fetched from registry on each message to handle
        // cases where the handler is registered after connection establishment

        // Spawn reader task for outgoing connection
        // This MUST process incoming messages to receive responses!
        let reader_connection = connection_arc.clone();
        let registry_weak_for_reader = registry_weak.clone();
        tokio::spawn(async move {
            info!(peer = %addr, "Starting outgoing connection reader with message processing");

            let max_message_size = registry_weak_for_reader
                .as_ref()
                .and_then(|w| w.upgrade())
                .map(|registry| registry.config.max_message_size)
                .unwrap_or(10 * 1024 * 1024);

            loop {
                match crate::handle::read_message_from_tls_reader(&mut reader, max_message_size).await {
                    Ok(crate::handle::MessageReadResult::Gossip(msg, correlation_id)) => {
                        let msg_to_handle = if let RegistryMessage::ActorMessage {
                            actor_id,
                            type_hash,
                            payload,
                            correlation_id: _,
                        } = msg
                        {
                            RegistryMessage::ActorMessage {
                                actor_id,
                                type_hash,
                                payload,
                                correlation_id,
                            }
                        } else {
                            msg
                        };

                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            if let Err(e) =
                                handle_incoming_message(registry, addr, msg_to_handle).await
                            {
                                warn!(peer = %addr, error = %e, "Failed to handle gossip message");
                            }
                        }
                    }
                    Ok(crate::handle::MessageReadResult::AskRaw {
                        correlation_id,
                        payload,
                    }) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            crate::handle::handle_raw_ask_request(
                                &registry,
                                addr,
                                correlation_id,
                                &payload,
                            )
                            .await;
                        }
                    }
                    Ok(crate::handle::MessageReadResult::Response {
                        correlation_id,
                        payload,
                    }) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            crate::handle::handle_response_message(
                                &registry,
                                addr,
                                correlation_id,
                                payload,
                            )
                            .await;
                        }
                    }
                    Ok(crate::handle::MessageReadResult::Actor {
                        msg_type,
                        correlation_id,
                        actor_id,
                        type_hash,
                        payload,
                    }) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            if let Some(handler) = &*registry.actor_message_handler.lock().await {
                                let actor_id_str = actor_id.to_string();
                                let correlation =
                                    if msg_type == crate::MessageType::ActorAsk as u8 {
                                        Some(correlation_id)
                                    } else {
                                        None
                                    };
                                let _ = handler
                                    .handle_actor_message(
                                        &actor_id_str,
                                        type_hash,
                                        &payload,
                                        correlation,
                                    )
                                    .await;
                            }
                        }
                    }
                    Ok(crate::handle::MessageReadResult::Streaming { .. }) => {
                        // Streaming messages are not handled on outgoing readers yet.
                    }
                    Ok(crate::handle::MessageReadResult::Raw(payload)) => {
                        #[cfg(any(test, feature = "test-helpers", debug_assertions))]
                        {
                            if std::env::var("KAMEO_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
                                crate::test_helpers::record_raw_payload(payload.clone());
                            }
                        }
                        // Raw tell payloads are ignored on outgoing readers.
                    }
                    Err(e) => {
                        reader_connection.set_state(ConnectionState::Disconnected);
                        warn!(peer = %addr, error = %e, "Outgoing connection reader error");
                        break;
                    }
                }
            }

            info!(peer = %addr, "Outgoing connection reader exited");
        });

        // Reset failure state for this peer since we successfully connected
        if let Some(ref registry_weak) = registry_weak {
            if let Some(registry) = registry_weak.upgrade() {
                let registry_clone = registry.clone();
                let peer_addr = addr;
                tokio::spawn(async move {
                    let mut gossip_state = registry_clone.gossip_state.lock().await;

                    // Check if we need to reset failures and clear pending
                    let need_to_clear_pending = if let Some(peer_info) =
                        gossip_state.peers.get_mut(&peer_addr)
                    {
                        let had_failures = peer_info.failures > 0;
                        if had_failures {
                            info!(peer = %peer_addr,
                                  prev_failures = peer_info.failures,
                                  "✅ Successfully established outgoing connection - resetting failure state");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        had_failures
                    } else {
                        false
                    };

                    // Clear pending failure record if needed
                    if need_to_clear_pending {
                        gossip_state.pending_peer_failures.remove(&peer_addr);
                    }
                });
            }
        }

        info!(peer = %addr, "successfully created new persistent connection");

        // Verify the connection is in the pool
        debug!(
            "CONNECTION POOL: After get_connection, pool has {} connections",
            self.connections_by_addr.len()
        );
        debug!(
            "CONNECTION POOL: Pool contains connection to {}? {}",
            addr,
            self.connections_by_addr.contains_key(&addr)
        );

        // Return a lock-free ConnectionHandle
        Ok(ConnectionHandle {
            addr,
            stream_handle,
            correlation: connection_arc
                .correlation
                .clone()
                .unwrap_or_else(CorrelationTracker::new),
        })
    }

    /// Mark a connection as disconnected
    pub fn mark_disconnected(&mut self, addr: SocketAddr) {
        if let Some(entry) = self.connections_by_addr.get(&addr) {
            entry.value().set_state(ConnectionState::Disconnected);
            info!(peer = %addr, "marked connection as disconnected");
        }
    }

    /// Remove a connection from the pool by address
    pub fn remove_connection_mut(&mut self, addr: SocketAddr) {
        if let Some(_conn) = self.connections_by_addr.remove(&addr) {
            info!(addr = %addr, "removed connection from pool");
            // Dropping the sender will cause the receiver to return None,
            // signaling the connection handler to shut down
            // No need to drop writer
            self.clear_capabilities_for_addr(&addr);
        }
    }

    /// Check if we have a connection to a peer by address
    pub fn has_connection(&self, addr: &SocketAddr) -> bool {
        self.connections_by_addr
            .get(addr)
            .map(|entry| entry.value().is_connected())
            .unwrap_or(false)
    }

    /// Check if we have a connection to a peer by peer ID
    pub fn has_connection_by_peer_id(&self, peer_id: &crate::PeerId) -> bool {
        self.connections_by_peer
            .get(peer_id)
            .map(|entry| entry.value().is_connected())
            .unwrap_or(false)
    }

    /// Check health of all connections
    pub async fn check_connection_health(&mut self) -> Vec<SocketAddr> {
        // Health checking is now done by the persistent connection handlers
        Vec::new()
    }

    /// Clean up stale connections
    pub fn cleanup_stale_connections(&mut self) {
        let to_remove: Vec<_> = self
            .connections_by_addr
            .iter()
            .filter(|entry| !entry.value().is_connected())
            .map(|entry| *entry.key())
            .collect();

        for addr in to_remove {
            self.connections_by_addr.remove(&addr);
            debug!(addr = %addr, "cleaned up disconnected connection");
        }
    }

    /// Close all connections (for shutdown)
    pub fn close_all_connections(&mut self) {
        let addrs: Vec<_> = self
            .connections_by_addr
            .iter()
            .map(|entry| *entry.key())
            .collect();
        let count = addrs.len();
        for addr in addrs {
            self.remove_connection(addr);
        }
        info!("closed all {} connections", count);
    }
/// Handle persistent connection reader - only reads messages, no channels
pub(crate) async fn handle_persistent_connection_reader(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    writer: Option<tokio::net::tcp::OwnedWriteHalf>,
    peer_addr: SocketAddr,
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    use tokio::io::AsyncReadExt;

    let mut partial_msg_buf = Vec::new();
    // Increase buffer size to handle large messages more efficiently
    let mut read_buf = vec![0u8; 1024 * 1024]; // 1MB read buffer

    // For incoming connections with a writer, create a stream handle
    // For outgoing connections, we'll use the existing handle from the pool
    let response_handle = writer.map(|writer| {
        let buffer_config = registry_weak
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|registry| {
                BufferConfig::default().with_ask_inflight_limit(registry.config.ask_inflight_limit)
            })
            .unwrap_or_else(BufferConfig::default);
        Arc::new(LockFreeStreamHandle::new(
            writer,
            peer_addr,
            ChannelId::Global,
            buffer_config,
        ))
    });

    loop {
        match reader.read(&mut read_buf).await {
            Ok(0) => {
                info!(peer = %peer_addr, "Connection closed by peer");
                break;
            }
            Ok(n) => {
                partial_msg_buf.extend_from_slice(&read_buf[..n]);

                // Process complete messages
                while partial_msg_buf.len() >= 4 {
                    let len = u32::from_be_bytes([
                        partial_msg_buf[0],
                        partial_msg_buf[1],
                        partial_msg_buf[2],
                        partial_msg_buf[3],
                    ]) as usize;
                    debug!(
                        peer = %peer_addr,
                        frame_len = len,
                        "🔍 TLS reader got frame length prefix (server stream)"
                    );

                    if len > 100 * 1024 * 1024 {
                        warn!(peer = %peer_addr, len = len, "Message too large - possible buffer corruption");
                        // This usually indicates we're reading from the wrong position in the buffer
                        // Clear the buffer completely and restart message processing
                        partial_msg_buf.clear();
                        break;
                    }

                    // Debug log for large messages (>1MB)
                    if len > 1024 * 1024 {
                        // eprintln!("📦 SERVER: LARGE MESSAGE detected! len={}, from peer={}", len, peer_addr);
                        // info!(peer = %peer_addr, len = len, buf_len = partial_msg_buf.len(), needed = 4 + len,
                        //       "📦 LARGE MESSAGE: Receiving large message (have {} of {} bytes)",
                        //       partial_msg_buf.len(), 4 + len);

                        // Pre-allocate buffer capacity for large messages to avoid repeated allocations
                        let total_needed = 4 + len;
                        if partial_msg_buf.capacity() < total_needed {
                            partial_msg_buf.reserve(total_needed - partial_msg_buf.len());
                            info!(peer = %peer_addr, "📦 LARGE MESSAGE: Reserved {} bytes for message", total_needed);
                        }
                    }

                    let total_len = 4 + len;
                    if partial_msg_buf.len() < total_len {
                        // if len > 1024 * 1024 {
                        //     eprintln!("📦 SERVER: Accumulating large message... have {}/{} bytes", partial_msg_buf.len(), total_len);
                        // }
                        // Not enough data yet, break inner loop to read more
                        break;
                    }
                    if partial_msg_buf.len() >= total_len {
                        let msg_data =
                            &partial_msg_buf[crate::framing::LENGTH_PREFIX_LEN..total_len];

                        // Log when we have the complete large message
                        // if len > 1024 * 1024 {
                        //     eprintln!("📦 SERVER: LARGE MESSAGE COMPLETE! len={}, processing...", len);
                        //     // info!(peer = %peer_addr, len = len, "📦 LARGE MESSAGE: Complete message received, processing...");
                        // }

                        // Debug: Log first few bytes of every message
                        if msg_data.len() >= crate::framing::ASK_RESPONSE_HEADER_LEN {
                            info!(peer = %peer_addr,
                                  "🔍 SERVER RECV: msg_len={}, first_bytes=[{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}]",
                                  msg_data.len(),
                                  msg_data[0], msg_data[1], msg_data[2], msg_data[3],
                                  msg_data[4], msg_data[5], msg_data[6], msg_data[7]);
                        }

                        // Check if this is an Ask/Response message by looking at first byte
                        if !msg_data.is_empty() {
                            let msg_type_byte = msg_data[0];
                            debug!(
                                peer = %peer_addr,
                                msg_type_byte,
                                payload_bytes = msg_data.len(),
                                "🔍 TLS reader inspecting message type (server stream)"
                            );
                            // Debug log for large messages
                            // if len > 1024 * 1024 {
                            //     info!(peer = %peer_addr, first_byte = msg_data[0], "📦 LARGE MESSAGE first byte: {} (0=Gossip, 3=ActorTell)", msg_data[0]);
                            // }
                            if let Some(msg_type) = crate::MessageType::from_byte(msg_type_byte) {
                                // This is an Ask/Response message
                                if msg_data.len() < crate::framing::ASK_RESPONSE_HEADER_LEN {
                                    warn!(peer = %peer_addr, "Ask/Response message too small");
                                    partial_msg_buf.drain(..total_len);
                                    continue;
                                }

                                let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                                let payload = &msg_data[crate::framing::ASK_RESPONSE_HEADER_LEN..];

                                match msg_type {
                                    crate::MessageType::Ask => {
                                        info!(peer = %peer_addr, correlation_id = correlation_id, payload_len = payload.len(),
                                              "📨 ASK DEBUG: Received Ask message on bidirectional connection");

                                        // Try to deserialize the payload as a RegistryMessage
                                        // Note: payload might not be aligned, so we need to copy it to an aligned buffer
                                        let aligned_payload = payload.to_vec();
                                        match rkyv::from_bytes::<
                                            crate::registry::RegistryMessage,
                                            rkyv::rancor::Error,
                                        >(
                                            &aligned_payload
                                        ) {
                                            Ok(mut registry_msg) => {
                                                info!(peer = %peer_addr, correlation_id = correlation_id,
                                                      "✅ ASK DEBUG: Successfully deserialized RegistryMessage");
                                                debug!(peer = %peer_addr, correlation_id = correlation_id, "Received Ask with RegistryMessage payload");

                                                // If it's an ActorMessage, ensure it has the correlation_id from the Ask envelope
                                                if let crate::registry::RegistryMessage::ActorMessage {
                                                ref actor_id,
                                                ref type_hash,
                                                payload: _,
                                                correlation_id: ref mut inner_correlation_id,
                                            } = registry_msg {
                                                if inner_correlation_id.is_none() {
                                                    // Use the Ask envelope's correlation_id
                                                    *inner_correlation_id = Some(correlation_id);
                                                    debug!(
                                                        peer = %peer_addr,
                                                        correlation_id = correlation_id,
                                                        actor_id = %actor_id,
                                                        type_hash = %format!("{:08x}", type_hash),
                                                        "Set ActorMessage correlation_id from Ask envelope"
                                                    );
                                                }
                                            }

                                                // Handle the registry message and get the response
                                                // For Ask messages, we need to handle the reply ourselves
                                                if let Some(ref registry_weak) = registry_weak {
                                                    if let Some(registry) = registry_weak.upgrade()
                                                    {
                                                        // Special handling for ActorMessage with correlation_id
                                                        if let crate::registry::RegistryMessage::ActorMessage {
                                                        ref actor_id,
                                                        ref type_hash,
                                                        ref payload,
                                                        correlation_id: Some(corr_id),
                                                    } = registry_msg {
                                                        info!(peer = %peer_addr, actor_id = %actor_id, type_hash = %format!("{:08x}", type_hash),
                                                              payload_len = payload.len(), correlation_id = corr_id,
                                                              "🎯 ASK DEBUG: Processing ActorMessage ask");
                                                        // Handle the actor message directly
                                                        match registry.handle_actor_message(actor_id, *type_hash, payload, Some(corr_id)).await {
                                                            Ok(Some(reply_payload)) => {
                                                                debug!(peer = %peer_addr, correlation_id = corr_id, reply_len = reply_payload.len(),
                                                                       "Got reply from actor, sending response back");

                                                                let header = framing::write_ask_response_header(
                                                                    crate::MessageType::Response,
                                                                    corr_id,
                                                                    reply_payload.len(),
                                                                );
                                                                let payload = bytes::Bytes::from(reply_payload);

                                                                // For both incoming and outgoing connections, find the stream handle from the pool
                                                                let pool = registry.connection_pool.lock().await;
                                                                if let Some(conn) = pool.connections_by_addr.get(&peer_addr).map(|c| c.value().clone()) {
                                                                    if let Some(ref stream_handle) = conn.stream_handle {
                                                                        if let Err(e) = stream_handle
                                                                            .write_header_and_payload_control(
                                                                                bytes::Bytes::copy_from_slice(&header),
                                                                                payload,
                                                                            )
                                                                            .await
                                                                        {
                                                                            warn!(peer = %peer_addr, error = %e, "Failed to send ask reply");
                                                                        } else {
                                                                            debug!(peer = %peer_addr, correlation_id = corr_id, "Sent ask reply through connection pool");
                                                                        }
                                                                    } else {
                                                                        warn!(peer = %peer_addr, "Connection has no stream handle");
                                                                    }
                                                                } else {
                                                                    warn!(peer = %peer_addr, "No connection found in pool to send ask reply");
                                                                }
                                                            }
                                                            Ok(None) => {
                                                                debug!(peer = %peer_addr, correlation_id = corr_id, "No reply from actor");
                                                            }
                                                            Err(e) => {
                                                                warn!(peer = %peer_addr, error = %e, correlation_id = corr_id, "Failed to handle actor message");
                                                            }
                                                        }
                                                    } else {
                                                        // For other messages, use the normal handler
                                                        match handle_incoming_message(registry.clone(), peer_addr, registry_msg).await {
                                                        Ok(()) => {
                                                            debug!(peer = %peer_addr, correlation_id = correlation_id, "Ask message processed");
                                                        }
                                                        Err(e) => {
                                                            warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id, "Failed to handle Ask message");
                                                        }
                                                    }
                                                    }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                warn!(peer = %peer_addr, correlation_id = correlation_id, error = %e,
                                                       payload_len = payload.len(),
                                                       "HANDLE ASK: Failed to deserialize as RegistryMessage, trying MessageWrapper");

                                                // Not a RegistryMessage, handle as before for test helpers
                                                #[cfg(any(test, feature = "test-helpers", debug_assertions))]
                                                {
                                                    if let Some(ref registry_weak) = registry_weak {
                                                        if let Some(registry) =
                                                            registry_weak.upgrade()
                                                        {
                                                            let conn = {
                                                                let pool = registry
                                                                    .connection_pool
                                                                    .lock()
                                                                    .await;
                                                                pool.connections_by_addr
                                                                    .get(&peer_addr)
                                                                    .map(|conn_ref| {
                                                                        conn_ref.value().clone()
                                                                    })
                                                            };

                                                            if let Some(conn) = conn {
                                                                // Process the request and generate response
                                                                let response_data =
                                                                    process_mock_request_payload(
                                                                        payload,
                                                                    );

                                                                // Build response message
                                                                let mut msg =
                                                                    bytes::BytesMut::with_capacity(
                                                                        framing::ASK_RESPONSE_FRAME_HEADER_LEN
                                                                            + response_data.len(),
                                                                    );

                                                                // Header: [type:1][corr_id:2][pad:1]
                                                                let header =
                                                                    framing::write_ask_response_header(
                                                                    crate::MessageType::Response,
                                                                    correlation_id,
                                                                    response_data.len(),
                                                                );
                                                                msg.extend_from_slice(&header);
                                                                msg.extend_from_slice(
                                                                    &response_data,
                                                                );

                                                                // Send response back through stream handle
                                                                if let Some(ref stream_handle) =
                                                                    conn.stream_handle
                                                                {
                                                                    if let Err(e) = stream_handle
                                                                        .write_bytes_nonblocking(
                                                                            msg.freeze(),
                                                                        )
                                                                    {
                                                                        warn!("Failed to send mock response: {}", e);
                                                                    } else {
                                                                        debug!("Sent mock response for correlation_id {}", correlation_id);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
                                                {
                                                    // This might be a kameo AskWrapper - try to handle it
                                                    debug!(peer = %peer_addr, correlation_id = correlation_id, payload_len = payload.len(),
                                                      "Received non-RegistryMessage Ask request, checking if it's from kameo");

                                                    // Try to parse binary format from kameo: [actor_id:8][type_hash:4][payload_len:4][payload:N]
                                                    if payload.len() >= 16 {
                                                        let actor_id = u64::from_be_bytes([
                                                            payload[0], payload[1], payload[2],
                                                            payload[3], payload[4], payload[5],
                                                            payload[6], payload[7],
                                                        ]);
                                                        let type_hash = u32::from_be_bytes([
                                                            payload[8],
                                                            payload[9],
                                                            payload[10],
                                                            payload[11],
                                                        ]);
                                                        let payload_len = u32::from_be_bytes([
                                                            payload[12],
                                                            payload[13],
                                                            payload[14],
                                                            payload[15],
                                                        ])
                                                            as usize;

                                                        if payload.len() >= 16 + payload_len {
                                                            let inner_payload =
                                                                &payload[16..16 + payload_len];

                                                            debug!(peer = %peer_addr, correlation_id = correlation_id,
                                                               actor_id = actor_id, type_hash = type_hash,
                                                               "Successfully decoded kameo binary message format");

                                                            if let Some(ref registry_weak) =
                                                                registry_weak
                                                            {
                                                                if let Some(registry) =
                                                                    registry_weak.upgrade()
                                                                {
                                                                    // Handle the actor message
                                                                    let actor_id_str =
                                                                        actor_id.to_string();
                                                                    match registry
                                                                        .handle_actor_message(
                                                                            &actor_id_str,
                                                                            type_hash,
                                                                            inner_payload,
                                                                            Some(correlation_id),
                                                                        )
                                                                        .await
                                                                    {
                                                                        Ok(Some(reply_payload)) => {
                                                                            debug!(peer = %peer_addr, correlation_id = correlation_id, reply_len = reply_payload.len(),
                                                                           "Got reply from kameo actor, sending response back");

                                                                            let header =
                                                                                framing::write_ask_response_header(
                                                                                    crate::MessageType::Response,
                                                                                    correlation_id,
                                                                                    reply_payload.len(),
                                                                                );
                                                                            let payload =
                                                                                bytes::Bytes::from(reply_payload);

                                                                            // Send response back through the response handle we saved
                                                                            if let Some(ref handle) =
                                                                                response_handle
                                                                            {
                                                                                if let Err(e) = handle
                                                                                    .write_header_and_payload_control(
                                                                                        bytes::Bytes::copy_from_slice(&header),
                                                                                        payload.clone(),
                                                                                    )
                                                                                    .await
                                                                                {
                                                                                    warn!(peer = %peer_addr, error = %e, "Failed to send ask reply");
                                                                                } else {
                                                                                    debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ask reply directly through writer");
                                                                                }
                                                                            } else {
                                                                                // Fall back to finding in pool
                                                                                warn!(peer = %peer_addr, "No response_handle, falling back to pool lookup");
                                                                                let pool = registry
                                                                                    .connection_pool
                                                                                    .lock()
                                                                                    .await;
                                                                                if let Some(conn) = pool.connections_by_addr.get(&peer_addr).map(|c| c.value().clone()) {
                                                                                    if let Some(ref stream_handle) = conn.stream_handle {
                                                                                        if let Err(e) = stream_handle
                                                                                            .write_header_and_payload_control(
                                                                                                bytes::Bytes::copy_from_slice(&header),
                                                                                                payload.clone(),
                                                                                            )
                                                                                            .await
                                                                                        {
                                                                                            warn!(peer = %peer_addr, error = %e, "Failed to send ask reply");
                                                                                        } else {
                                                                                            debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ask reply through connection pool");
                                                                                        }
                                                                                    } else {
                                                                                        warn!(peer = %peer_addr, "Connection has no stream handle");
                                                                                    }
                                                                                } else {
                                                                                    warn!(peer = %peer_addr, "No connection found in pool for reply");
                                                                                }
                                                                            }
                                                                        }
                                                                        Ok(None) => {
                                                                            debug!(peer = %peer_addr, correlation_id = correlation_id, "No reply from kameo actor");
                                                                        }
                                                                        Err(e) => {
                                                                            warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id, "Failed to handle kameo actor message");
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        } else {
                                                            debug!(peer = %peer_addr, correlation_id = correlation_id,
                                                               "Binary message payload too short: expected {} bytes but got {}",
                                                               16 + payload_len, payload.len());
                                                        }
                                                    } else {
                                                        debug!(peer = %peer_addr, correlation_id = correlation_id,
                                                           "Ask payload too short for binary format: {} bytes (need at least 16)",
                                                           payload.len());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    crate::MessageType::Response => {
                                        // Handle incoming response
                                        if let Some(ref registry_weak) = registry_weak {
                                            if let Some(registry) = registry_weak.upgrade() {
                                                let pool = registry.connection_pool.lock().await;
                                                let mut delivered = false;

                                                // Look up peer ID for this address
                                                if let Some(peer_id) = pool
                                                    .addr_to_peer_id
                                                    .get(&peer_addr)
                                                    .map(|e| e.clone())
                                                {
                                                    // Use shared correlation tracker
                                                    if let Some(correlation) =
                                                        pool.correlation_trackers.get(&peer_id)
                                                    {
                                                        if correlation.has_pending(correlation_id) {
                                                            correlation.complete(
                                                                correlation_id,
                                                                bytes::Bytes::copy_from_slice(
                                                                    payload,
                                                                ),
                                                            );
                                                            debug!(peer = %peer_addr, correlation_id = correlation_id, "Delivered response to shared correlation tracker");
                                                            delivered = true;
                                                        }
                                                    }
                                                }

                                                if !delivered {
                                                    debug!(peer = %peer_addr, correlation_id = correlation_id, "Could not find pending request for correlation_id");
                                                }
                                            }
                                        }
                                    }
                                    crate::MessageType::Gossip => {
                                        // Gossip messages can arrive here, just ignore them
                                    }
                                    crate::MessageType::ActorTell => {
                                        // Direct actor tell message format:
                                        // Already parsed: [type:1][correlation_id:2][pad:1]
                                        // Payload: [actor_id:8][type_hash:4][payload_len:4][payload:N]
                                        if payload.len() >= 16 {
                                            let actor_id = u64::from_be_bytes(
                                                payload[0..8].try_into().unwrap(),
                                            );
                                            let type_hash = u32::from_be_bytes(
                                                payload[8..12].try_into().unwrap(),
                                            );
                                            let payload_len = u32::from_be_bytes(
                                                payload[12..16].try_into().unwrap(),
                                            )
                                                as usize;

                                            if payload.len() >= 16 + payload_len {
                                                let actor_payload = &payload[16..16 + payload_len];

                                                // Log large ActorTell messages
                                                if payload_len > 1024 * 1024 {
                                                    info!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                          payload_len = payload_len, "📨 LARGE ActorTell message - will call handler");
                                                }

                                                // Call actor message handler if available
                                                if let Some(ref registry_weak) = registry_weak {
                                                    if let Some(registry) = registry_weak.upgrade()
                                                    {
                                                        if let Some(ref handler) = &*registry
                                                            .actor_message_handler
                                                            .lock()
                                                            .await
                                                        {
                                                            if payload_len > 1024 * 1024 {
                                                                info!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                       "🚀 Calling actor message handler for LARGE ActorTell");
                                                            } else {
                                                                debug!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                       "Calling actor message handler for ActorTell");
                                                            }
                                                            match handler
                                                                .handle_actor_message(
                                                                    &actor_id.to_string(),
                                                                    type_hash,
                                                                    actor_payload,
                                                                    None, // No correlation for tell
                                                                )
                                                                .await
                                                            {
                                                                Ok(_) => {
                                                                    if payload_len > 1024 * 1024 {
                                                                        info!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                               "✅ Successfully handled LARGE ActorTell");
                                                                    } else {
                                                                        debug!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                               "Successfully handled ActorTell");
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!(peer = %peer_addr, error = %e, "Failed to handle ActorTell on incoming connection")
                                                                }
                                                            }
                                                        } else {
                                                            warn!(peer = %peer_addr, "No actor message handler registered in registry");
                                                        }
                                                    } else {
                                                        warn!(peer = %peer_addr, "Registry weak reference could not be upgraded");
                                                    }
                                                } else {
                                                    warn!(peer = %peer_addr, "No registry weak reference available");
                                                }
                                            } else {
                                                warn!(peer = %peer_addr, expected = 16 + payload_len, actual = payload.len(),
                                                      "ActorTell payload too short");
                                            }
                                        } else {
                                            warn!(peer = %peer_addr, payload_len = payload.len(), "ActorTell header too short");
                                        }
                                    }
                                    crate::MessageType::ActorAsk => {
                                        // Direct actor ask message format:
                                        // Already parsed: [type:1][correlation_id:2][pad:1]
                                        // Payload: [actor_id:8][type_hash:4][payload_len:4][payload:N]
                                        if payload.len() >= 16 {
                                            let actor_id = u64::from_be_bytes(
                                                payload[0..8].try_into().unwrap(),
                                            );
                                            let type_hash = u32::from_be_bytes(
                                                payload[8..12].try_into().unwrap(),
                                            );
                                            let payload_len = u32::from_be_bytes(
                                                payload[12..16].try_into().unwrap(),
                                            )
                                                as usize;

                                            if payload.len() >= 16 + payload_len {
                                                let actor_payload = &payload[16..16 + payload_len];

                                                if let Some(ref registry_weak) = registry_weak {
                                                    if let Some(registry) = registry_weak.upgrade()
                                                    {
                                                        let actor_id_str = actor_id.to_string();
                                                        match registry
                                                            .handle_actor_message(
                                                                &actor_id_str,
                                                                type_hash,
                                                                actor_payload,
                                                                Some(correlation_id),
                                                            )
                                                            .await
                                                        {
                                                            Ok(Some(reply_payload)) => {
                                                                let header =
                                                                    framing::write_ask_response_header(
                                                                        crate::MessageType::Response,
                                                                        correlation_id,
                                                                        reply_payload.len(),
                                                                    );
                                                                let payload =
                                                                    bytes::Bytes::from(reply_payload);

                                                                if let Some(ref handle) =
                                                                    response_handle
                                                                {
                                                                    if let Err(e) = handle
                                                                        .write_header_and_payload_control(
                                                                            bytes::Bytes::copy_from_slice(&header),
                                                                            payload.clone(),
                                                                        )
                                                                        .await
                                                                    {
                                                                        warn!(peer = %peer_addr, error = %e, "Failed to send ActorAsk response");
                                                                    } else {
                                                                        debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ActorAsk response");
                                                                    }
                                                                } else {
                                                                    let pool = registry
                                                                        .connection_pool
                                                                        .lock()
                                                                        .await;
                                                                    if let Some(conn) = pool
                                                                        .connections_by_addr
                                                                        .get(&peer_addr)
                                                                        .map(|c| c.value().clone())
                                                                    {
                                                                        if let Some(
                                                                            ref stream_handle,
                                                                        ) = conn.stream_handle
                                                                        {
                                                                            if let Err(e) = stream_handle
                                                                                .write_header_and_payload_control(
                                                                                    bytes::Bytes::copy_from_slice(&header),
                                                                                    payload.clone(),
                                                                                )
                                                                                .await
                                                                            {
                                                                                warn!(peer = %peer_addr, error = %e, "Failed to send ActorAsk response");
                                                                            } else {
                                                                                debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ActorAsk response through connection pool");
                                                                            }
                                                                        } else {
                                                                            warn!(peer = %peer_addr, "Connection has no stream handle");
                                                                        }
                                                                    } else {
                                                                        warn!(peer = %peer_addr, "No connection found in pool for ActorAsk reply");
                                                                    }
                                                                }
                                                            }
                                                            Ok(None) => {
                                                                debug!(peer = %peer_addr, correlation_id = correlation_id, "ActorAsk handled with no reply");
                                                            }
                                                            Err(e) => {
                                                                warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id, "Failed to handle ActorAsk");
                                                            }
                                                        }
                                                    } else {
                                                        warn!(peer = %peer_addr, "Registry weak reference could not be upgraded");
                                                    }
                                                } else {
                                                    warn!(peer = %peer_addr, "No registry weak reference available");
                                                }
                                            } else {
                                                warn!(peer = %peer_addr, expected = 16 + payload_len, actual = payload.len(),
                                                      "ActorAsk payload too short");
                                            }
                                        } else {
                                            warn!(peer = %peer_addr, payload_len = payload.len(), "ActorAsk header too short");
                                        }
                                    }
                                    crate::MessageType::StreamStart => {
                                        // Parse stream header from payload
                                        if payload.len() >= crate::StreamHeader::SERIALIZED_SIZE {
                                            if let Some(header) =
                                                crate::StreamHeader::from_bytes(payload)
                                            {
                                                info!(peer = %peer_addr, stream_id = header.stream_id, total_size = header.total_size,
                                                      type_hash = %format!("{:08x}", header.type_hash), actor_id = header.actor_id,
                                                      "📥 StreamStart: Beginning streaming transfer");

                                                // Initialize stream assembly
                                                if let Some(ref registry_weak) = registry_weak {
                                                    if let Some(registry) = registry_weak.upgrade()
                                                    {
                                                        registry
                                                            .start_stream_assembly(header)
                                                            .await;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    crate::MessageType::StreamData => {
                                        // Parse stream header and data
                                        if payload.len() >= crate::StreamHeader::SERIALIZED_SIZE {
                                            if let Some(header) =
                                                crate::StreamHeader::from_bytes(payload)
                                            {
                                                let data_start =
                                                    crate::StreamHeader::SERIALIZED_SIZE;
                                                if payload.len()
                                                    >= data_start + header.chunk_size as usize
                                                {
                                                    let chunk_data = &payload[data_start
                                                        ..data_start + header.chunk_size as usize];

                                                    // debug!(peer = %peer_addr, stream_id = header.stream_id, chunk_index = header.chunk_index,
                                                    //        chunk_size = header.chunk_size, "📦 StreamData: Received chunk");

                                                    // Add chunk to stream assembly
                                                    if let Some(ref registry_weak) = registry_weak {
                                                        if let Some(registry) =
                                                            registry_weak.upgrade()
                                                        {
                                                            registry
                                                                .add_stream_chunk(
                                                                    header,
                                                                    chunk_data.to_vec(),
                                                                )
                                                                .await;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    crate::MessageType::StreamEnd => {
                                        // Parse stream header and complete assembly
                                        if payload.len() >= crate::StreamHeader::SERIALIZED_SIZE {
                                            if let Some(header) =
                                                crate::StreamHeader::from_bytes(payload)
                                            {
                                                info!(peer = %peer_addr, stream_id = header.stream_id,
                                                      "📤 StreamEnd: Completing streaming transfer");

                                                // Complete stream assembly and deliver to actor
                                                if let Some(ref registry_weak) = registry_weak {
                                                    if let Some(registry) = registry_weak.upgrade()
                                                    {
                                                        if let Some(complete_msg) = registry
                                                            .complete_stream_assembly(
                                                                header.stream_id,
                                                            )
                                                            .await
                                                        {
                                                            // Deliver to actor
                                                            if let Some(ref handler) = &*registry
                                                                .actor_message_handler
                                                                .lock()
                                                                .await
                                                            {
                                                                match handler
                                                                    .handle_actor_message(
                                                                        &header
                                                                            .actor_id
                                                                            .to_string(),
                                                                        header.type_hash,
                                                                        &complete_msg,
                                                                        None,
                                                                    )
                                                                    .await
                                                                {
                                                                    Ok(_) => {
                                                                        info!(peer = %peer_addr, stream_id = header.stream_id,
                                                                                  "✅ Successfully delivered streamed message to actor")
                                                                    }
                                                                    Err(e) => {
                                                                        error!(peer = %peer_addr, stream_id = header.stream_id,
                                                                                    error = %e, "❌ Failed to deliver streamed message")
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                // if len > 1024 * 1024 {
                                //     eprintln!("📦 SERVER: Drained {} bytes from buffer after processing large message", total_len);
                                // }
                                partial_msg_buf.drain(..total_len);
                                continue;
                            }
                        }

                        // This is a gossip protocol message

                        // Add timing right after TCP read - BEFORE any deserialization
                        let tcp_read_timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos();

                        // Process message
                        if let Some(ref registry_weak) = registry_weak {
                            if let Some(registry) = registry_weak.upgrade() {
                                if let Ok(msg) = rkyv::from_bytes::<
                                    crate::registry::RegistryMessage,
                                    rkyv::rancor::Error,
                                >(msg_data)
                                {
                                    // Debug: Show timing right after TCP read, before any processing
                                    if let crate::registry::RegistryMessage::DeltaGossip { delta } =
                                        &msg
                                    {
                                        let tcp_transmission_nanos =
                                            tcp_read_timestamp - delta.precise_timing_nanos as u128;
                                        let _tcp_transmission_ms =
                                            tcp_transmission_nanos as f64 / 1_000_000.0;
                                        // eprintln!("🔍 TCP_TRANSMISSION_TIME: {}ms ({}ns)", _tcp_transmission_ms, tcp_transmission_nanos);
                                    }

                                    if let Err(e) =
                                        handle_incoming_message(registry, peer_addr, msg).await
                                    {
                                        warn!(peer = %peer_addr, error = %e, "Failed to handle message");
                                    }
                                }
                            }
                        }

                        // Drain after processing to avoid invalidating msg_data
                        partial_msg_buf.drain(..total_len);
                    } else {
                        break;
                    }
                }
            }
            Err(e) => {
                warn!(peer = %peer_addr, error = %e, "Read error");
                break;
            }
        }
    }

    // Handle peer failure when connection is lost
    info!(peer = %peer_addr, "CONNECTION_POOL: Triggering peer failure handling");
    if let Some(ref registry_weak) = registry_weak {
        if let Some(registry) = registry_weak.upgrade() {
            if let Err(e) = registry.handle_peer_connection_failure(peer_addr).await {
                warn!(error = %e, peer = %peer_addr, "CONNECTION_POOL: Failed to handle peer connection failure");
            }
        }
    }
}
}

/// Resolve the peer state address for a sender.
async fn resolve_peer_state_addr(
    registry: &GossipRegistry,
    sender_peer_id: Option<&crate::PeerId>,
    socket_addr: SocketAddr,
) -> SocketAddr {
    if let Some(peer_id) = sender_peer_id {
        if let Some(addr) = {
            let pool = registry.connection_pool.lock().await;
            pool.peer_id_to_addr
                .get(peer_id)
                .map(|entry| *entry.value())
                .filter(|addr| addr.port() != 0)
        } {
            return addr;
        }

        if let Some(addr) = registry.lookup_advertised_addr(&peer_id.to_node_id()).await {
            return addr;
        }
    }

    if let Some(node_id) = registry
        .peer_capability_addr_to_node
        .get(&socket_addr)
        .map(|entry| *entry.value())
    {
        if let Some(addr) = registry.lookup_advertised_addr(&node_id).await {
            return addr;
        }
    }

    socket_addr
}

/// Handle an incoming message on a bidirectional connection
pub(crate) fn handle_incoming_message(
    registry: Arc<GossipRegistry>,
    _peer_addr: SocketAddr,
    msg: RegistryMessage,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async move {
        match msg {
            RegistryMessage::DeltaGossip { delta } => {
                debug!(
                    sender = %delta.sender_peer_id,
                    since_sequence = delta.since_sequence,
                    changes = delta.changes.len(),
                    "received delta gossip message on bidirectional connection"
                );

                let sender_socket_addr = resolve_peer_state_addr(
                    &registry,
                    Some(&delta.sender_peer_id),
                    _peer_addr,
                )
                .await;

                // OPTIMIZATION: Do all peer management in one lock acquisition
                {
                    let mut gossip_state = registry.gossip_state.lock().await;

                    // Add the sender as a peer (inlined to avoid separate lock)
                    if delta.sender_peer_id != registry.peer_id {
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            gossip_state.peers.entry(sender_socket_addr)
                        {
                            let current_time = crate::current_timestamp();
                            e.insert(crate::registry::PeerInfo {
                                address: sender_socket_addr,
                                peer_address: None,
                                node_id: None,
                                failures: 0,
                                last_attempt: current_time,
                                last_success: current_time,
                                last_sequence: 0,
                                last_sent_sequence: 0,
                                consecutive_deltas: 0,
                                last_failure_time: None,
                            });
                        }
                    }

                    // Check if this is a previously failed peer
                    let was_failed = gossip_state
                        .peers
                        .get(&sender_socket_addr)
                        .map(|info| info.failures >= registry.config.max_peer_failures)
                        .unwrap_or(false);

                    if was_failed {
                        info!(
                            peer = %delta.sender_peer_id,
                            "✅ Received delta from previously failed peer - connection restored!"
                        );

                        // Clear the pending failure record
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }

                    // Update peer info and check if we need to clear pending failures
                    let need_to_clear_pending =
                        if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                            // Always reset failure state when we receive messages from the peer
                            // This proves the peer is alive and communicating
                            let had_failures = peer_info.failures > 0;
                            if had_failures {
                                info!(peer = %delta.sender_peer_id,
                              prev_failures = peer_info.failures,
                              "🔄 Resetting failure state after receiving DeltaGossip");
                                peer_info.failures = 0;
                                peer_info.last_failure_time = None;
                            }
                            peer_info.last_success = crate::current_timestamp();

                            peer_info.last_sequence =
                                std::cmp::max(peer_info.last_sequence, delta.current_sequence);
                            peer_info.consecutive_deltas += 1;

                            had_failures
                        } else {
                            false
                        };

                    // Clear pending failure record if needed
                    if need_to_clear_pending {
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }
                    gossip_state.delta_exchanges += 1;
                }

                // CRITICAL OPTIMIZATION: Inline apply_delta to eliminate function call overhead
                // Apply the delta directly here to minimize async scheduling delays
                {
                    let total_changes = delta.changes.len();
                    let sender_addr = sender_socket_addr;

                    // Pre-compute priority flags to avoid redundant checks
                    let has_immediate = delta.changes.iter().any(|change| match change {
                        crate::registry::RegistryChange::ActorAdded { priority, .. } => {
                            priority.should_trigger_immediate_gossip()
                        }
                        crate::registry::RegistryChange::ActorRemoved { priority, .. } => {
                            priority.should_trigger_immediate_gossip()
                        }
                    });

                    if has_immediate {
                        info!(
                            "🎯 RECEIVING IMMEDIATE CHANGES: {} total changes from {}",
                            total_changes, sender_addr
                        );
                    }

                    // Pre-capture timing info outside lock for better performance - use high resolution timing
                    let _received_instant = std::time::Instant::now();
                    let received_timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos();

                    // eprintln!("🔍 RECEIVED_TIMESTAMP: {}ns", received_timestamp);

                    // Collect immediate actors for ACK before consuming changes
                    let mut immediate_actors = Vec::new();
                    for change in &delta.changes {
                        if let crate::registry::RegistryChange::ActorAdded {
                            name, priority, ..
                        } = change
                        {
                            if priority.should_trigger_immediate_gossip() {
                                immediate_actors.push(name.clone());
                            }
                        }
                    }

                    // OPTIMIZATION: Fast-path state updates with try_lock to avoid blocking
                    let (applied_count, _pending_updates) = {
                        // Try non-blocking access first
                        if let Ok(mut actor_state) = registry.actor_state.try_write() {
                            // Fast path - direct update without batching overhead
                            let mut applied = 0;
                            for change in delta.changes {
                                match change {
                                    crate::registry::RegistryChange::ActorAdded {
                                        name,
                                        location,
                                        priority: _,
                                    } => {
                                        // Don't override local actors
                                        if actor_state.local_actors.contains_key(name.as_str()) {
                                            continue;
                                        }

                                        // Quick conflict check
                                        let should_apply =
                                            match actor_state.known_actors.get(name.as_str()) {
                                                Some(existing) => {
                                                    location.wall_clock_time
                                                        > existing.wall_clock_time
                                                        || (location.wall_clock_time
                                                            == existing.wall_clock_time
                                                            && location.address > existing.address)
                                                }
                                                None => true,
                                            };

                                        if should_apply {
                                            actor_state
                                                .known_actors
                                                .insert(name.clone(), location.clone());
                                            applied += 1;

                                            // Inline timing calculation for immediate priority
                                            if location.priority.should_trigger_immediate_gossip() {
                                                let network_time_nanos = received_timestamp
                                                    - delta.precise_timing_nanos as u128;
                                                let network_time_ms =
                                                    network_time_nanos as f64 / 1_000_000.0;
                                                let propagation_time_ms = network_time_ms; // Same as network time for now
                                                let processing_only_time_ms = 0.0; // No additional processing time beyond network

                                                eprintln!("🔍 FAST_PATH: Processing immediate priority actor: {} propagation_time_ms={:.3}ms", name, propagation_time_ms);

                                                info!(
                                                    actor_name = %name,
                                                    priority = ?location.priority,
                                                    propagation_time_ms = propagation_time_ms,
                                                    network_processing_time_ms = network_time_ms,
                                                    processing_only_time_ms = processing_only_time_ms,
                                                    "RECEIVED_ACTOR"
                                                );
                                            }
                                        }
                                    }
                                    crate::registry::RegistryChange::ActorRemoved {
                                        name,
                                        vector_clock,
                                        removing_node_id,
                                        priority: _,
                                    } => {
                                        // Check vector clock ordering before applying removal
                                        let should_remove =
                                            match actor_state.known_actors.get(name.as_str()) {
                                                Some(existing_location) => {
                                                    match vector_clock
                                                        .compare(&existing_location.vector_clock)
                                                    {
                                                        crate::ClockOrdering::After => true,
                                                        crate::ClockOrdering::Concurrent => {
                                                            // For concurrent removals, use node_id as deterministic tiebreaker
                                                            // This ensures all nodes make the same decision
                                                            removing_node_id
                                                                > existing_location.node_id
                                                        }
                                                        _ => false, // Ignore outdated removals (Before or Equal)
                                                    }
                                                }
                                                None => false, // Actor doesn't exist
                                            };

                                        if should_remove
                                            && actor_state
                                                .known_actors
                                                .remove(name.as_str())
                                                .is_some()
                                        {
                                            applied += 1;
                                        }
                                    }
                                }
                            }
                            (applied, Vec::new())
                        } else {
                            // Fallback to batched approach if lock is contended
                            let actor_state = registry.actor_state.read().await;
                            let mut pending_updates = Vec::new();
                            let mut pending_removals = Vec::new();

                            for change in &delta.changes {
                                match change {
                                    crate::registry::RegistryChange::ActorAdded {
                                        name,
                                        location,
                                        priority: _,
                                    } => {
                                        // Don't override local actors - early exit
                                        if actor_state.local_actors.contains_key(name.as_str()) {
                                            debug!(
                                                actor_name = %name,
                                                "skipping remote actor update - actor is local"
                                            );
                                            continue;
                                        }

                                        // Check if we already know about this actor
                                        let should_apply =
                                            match actor_state.known_actors.get(name.as_str()) {
                                                Some(existing_location) => {
                                                    // Use vector clock for causal ordering
                                                    match location
                                                        .vector_clock
                                                        .compare(&existing_location.vector_clock)
                                                    {
                                                        crate::ClockOrdering::After => true,
                                                        crate::ClockOrdering::Concurrent => {
                                                            // For concurrent updates, use node_id as tiebreaker
                                                            location.node_id
                                                                > existing_location.node_id
                                                        }
                                                        _ => false, // Keep existing for Before or Equal
                                                    }
                                                }
                                                None => {
                                                    debug!(
                                                        actor_name = %name,
                                                        "applying new actor"
                                                    );
                                                    true // New actor
                                                }
                                            };

                                        if should_apply {
                                            pending_updates.push((name.clone(), location.clone()));
                                        }
                                    }
                                    crate::registry::RegistryChange::ActorRemoved {
                                        name,
                                        vector_clock,
                                        removing_node_id,
                                        priority: _,
                                    } => {
                                        // Check vector clock ordering before queueing removal
                                        if let Some(existing_location) =
                                            actor_state.known_actors.get(name.as_str())
                                        {
                                            match vector_clock
                                                .compare(&existing_location.vector_clock)
                                            {
                                                crate::ClockOrdering::After => {
                                                    // Queue removal if it's after
                                                    pending_removals.push((
                                                        name.clone(),
                                                        vector_clock.clone(),
                                                        *removing_node_id,
                                                    ));
                                                }
                                                crate::ClockOrdering::Concurrent => {
                                                    // For concurrent removals, use node_id as tiebreaker
                                                    if removing_node_id > &existing_location.node_id
                                                    {
                                                        pending_removals.push((
                                                            name.clone(),
                                                            vector_clock.clone(),
                                                            *removing_node_id,
                                                        ));
                                                    }
                                                }
                                                _ => {} // Ignore outdated removals (Before or Equal)
                                            }
                                        }
                                    }
                                }
                            }

                            // Drop read lock before acquiring write lock
                            drop(actor_state);

                            // Second pass: apply all updates under write lock with re-validation
                            let mut actor_state = registry.actor_state.write().await;
                            let mut applied = 0;

                            // Re-validate and apply actor additions
                            for (name, location) in &pending_updates {
                                // Re-check if this is still valid (state may have changed)
                                let still_valid = match actor_state.known_actors.get(name.as_str())
                                {
                                    Some(existing_location) => {
                                        match location
                                            .vector_clock
                                            .compare(&existing_location.vector_clock)
                                        {
                                            crate::ClockOrdering::After => true,
                                            crate::ClockOrdering::Concurrent => {
                                                location.node_id > existing_location.node_id
                                            }
                                            _ => false,
                                        }
                                    }
                                    None => !actor_state.local_actors.contains_key(name.as_str()),
                                };

                                if still_valid {
                                    actor_state
                                        .known_actors
                                        .insert(name.clone(), location.clone());
                                    applied += 1;

                                    // Log the timing information for immediate priority changes
                                    if location.priority.should_trigger_immediate_gossip() {
                                        // Calculate time from when delta was sent (network + processing)
                                        let network_processing_time_nanos =
                                            received_timestamp - delta.precise_timing_nanos as u128;
                                        let network_processing_time_ms =
                                            network_processing_time_nanos as f64 / 1_000_000.0;
                                        let propagation_time_ms = network_processing_time_ms; // Same as network time for now
                                        let processing_only_time_ms = 0.0; // No additional processing time beyond network

                                        // Debug: Break down where the time is spent
                                        eprintln!("🔍 TIMING_BREAKDOWN: sent={}, received={}, delta={}ns ({}ms)",
                                     delta.precise_timing_nanos, received_timestamp,
                                     network_processing_time_nanos, network_processing_time_ms);

                                        info!(
                                            actor_name = %name,
                                            priority = ?location.priority,
                                            propagation_time_ms = propagation_time_ms,
                                            network_processing_time_ms = network_processing_time_ms,
                                            processing_only_time_ms = processing_only_time_ms,
                                            "RECEIVED_ACTOR"
                                        );
                                    }
                                }
                            }

                            // Re-validate and apply actor removals
                            for (name, removal_clock, removing_node_id) in &pending_removals {
                                // Re-check if removal is still valid (state may have changed)
                                if let Some(existing_location) =
                                    actor_state.known_actors.get(name.as_str())
                                {
                                    let still_valid = match removal_clock
                                        .compare(&existing_location.vector_clock)
                                    {
                                        crate::ClockOrdering::After => true,
                                        crate::ClockOrdering::Concurrent => {
                                            // Use same deterministic tiebreaker for re-validation
                                            removing_node_id > &existing_location.node_id
                                        }
                                        _ => false,
                                    };

                                    if still_valid
                                        && actor_state.known_actors.remove(name.as_str()).is_some()
                                    {
                                        applied += 1;
                                    }
                                }
                            }

                            (applied, pending_updates)
                        }
                    };

                    if applied_count > 0 {
                        debug!(
                            applied_count = applied_count,
                            sender = %sender_addr,
                            "applied delta changes in batched update"
                        );
                    }

                    // NEW: Send ACK back for immediate registrations
                    if !immediate_actors.is_empty() {
                        // Send ACKs for immediate priority actor additions
                        // Use lock-free send since we're responding on the same connection
                        for actor_name in immediate_actors {
                            // Send lightweight ACK immediately
                            let ack = crate::registry::RegistryMessage::ImmediateAck {
                                actor_name: actor_name.clone(),
                                success: true,
                            };

                            // Serialize and send
                            if let Ok(serialized) = rkyv::to_bytes::<rkyv::rancor::Error>(&ack) {
                                let mut pool = registry.connection_pool.lock().await;
                                let buffer = pool.create_message_buffer(&serialized);
                                // Use send_lock_free to send directly without needing a connection handle
                                if let Err(e) = pool.send_lock_free(sender_socket_addr, &buffer) {
                                    warn!("Failed to send ImmediateAck: {}", e);
                                } else {
                                    info!("Sent ImmediateAck for actor '{}'", actor_name);
                                }
                            }
                        }
                    }
                }

                // Note: Response will be sent during regular gossip rounds
                Ok(())
            }
            RegistryMessage::FullSync {
                local_actors,
                known_actors,
                sender_peer_id,
                sequence,
                wall_clock_time,
            } => {
                let sender_socket_addr = resolve_peer_state_addr(
                    &registry,
                    Some(&sender_peer_id),
                    _peer_addr,
                )
                .await;

                // Note: sender_peer_id is now a PeerId (e.g., "node_a"), not an address
                debug!(
                    "Received FullSync from node '{}' at address {}",
                    sender_peer_id, sender_socket_addr
                );

                // OPTIMIZATION: Do all peer management in one lock acquisition
                {
                    let mut gossip_state = registry.gossip_state.lock().await;

                    // Add the sender as a peer (inlined to avoid separate lock)
                    if sender_socket_addr != registry.bind_addr {
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            gossip_state.peers.entry(sender_socket_addr)
                        {
                            info!(peer = %sender_socket_addr, "Adding new peer from FullSync");
                            let current_time = crate::current_timestamp();
                            e.insert(crate::registry::PeerInfo {
                                address: sender_socket_addr,
                                peer_address: None,
                                node_id: None,
                                failures: 0,
                                last_attempt: current_time,
                                last_success: current_time,
                                last_sequence: 0,
                                last_sent_sequence: 0,
                                consecutive_deltas: 0,
                                last_failure_time: None,
                            });
                        }
                    }

                    // Update peer info and reset failure state
                    let had_failures = gossip_state
                        .peers
                        .get(&sender_socket_addr)
                        .map(|info| info.failures > 0)
                        .unwrap_or(false);

                    if had_failures {
                        // Clear the pending failure record
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }

                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        let prev_failures = peer_info.failures;
                        // Always reset failure state when we receive a FullSync from the peer
                        // This proves the peer is alive and communicating
                        if peer_info.failures > 0 {
                            info!(peer = %sender_socket_addr,
                              prev_failures = prev_failures,
                              "🔄 Resetting failure state after receiving FullSync");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        peer_info.consecutive_deltas = 0;
                    } else {
                        warn!(peer = %sender_socket_addr, "Peer not found in peer list when trying to reset failure state");
                    }
                    gossip_state.full_sync_exchanges += 1;
                }

                debug!(
                    sender = %sender_peer_id,
                    sequence = sequence,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "📨 INCOMING: Received full sync message on bidirectional connection"
                );

                // IMPORTANT: Register the incoming connection with the peer_id mapping
                // This allows bidirectional communication to work properly
                {
                    let pool = registry.connection_pool.lock().await;
                    pool.peer_id_to_addr
                        .insert(sender_peer_id.clone(), sender_socket_addr);
                    pool.addr_to_peer_id
                        .insert(sender_socket_addr, sender_peer_id.clone());
                    debug!(
                        "BIDIRECTIONAL: Registered incoming connection - peer_id={} addr={}",
                        sender_peer_id, sender_socket_addr
                    );
                }

                // Only remaining async operation
                registry
                    .merge_full_sync(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_socket_addr,
                        sequence,
                        wall_clock_time,
                    )
                    .await;

                // Send back our state as a response so the sender can receive our actors
                // This is critical for late-joining nodes (like Node C) to get existing state
                {
                    // Get our current state
                    let (our_local_actors, our_known_actors, our_sequence) = {
                        let actor_state = registry.actor_state.read().await;
                        let gossip_state = registry.gossip_state.lock().await;
                        (
                            actor_state.local_actors.clone(),
                            actor_state.known_actors.clone(),
                            gossip_state.gossip_sequence,
                        )
                    };

                    // Calculate sizes before moving
                    let local_actors_count = our_local_actors.len();
                    let known_actors_count = our_known_actors.len();

                    // Create a FullSyncResponse message
                    let response = RegistryMessage::FullSyncResponse {
                        local_actors: our_local_actors.into_iter().collect(),
                        known_actors: our_known_actors.into_iter().collect(),
                        sender_peer_id: registry.peer_id.clone(), // Use peer ID
                        sequence: our_sequence,
                        wall_clock_time: crate::current_timestamp(),
                    };

                    // Send the response back through existing connection
                    // We'll use send_lock_free which doesn't create new connections
                    let response_data = match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
                        Ok(data) => data,
                        Err(e) => {
                            warn!(error = %e, "Failed to serialize FullSync response");
                            return Ok(());
                        }
                    };

                    // Try to send immediately on existing connection
                    {
                        debug!(
                            "FULLSYNC RESPONSE: Node {} is about to acquire connection pool lock",
                            registry.bind_addr
                        );
                        let mut pool = registry.connection_pool.lock().await;
                        debug!(
                            "FULLSYNC RESPONSE: Node {} got pool lock, pool has {} total entries",
                            registry.bind_addr,
                            pool.connection_count()
                        );
                        debug!("FULLSYNC RESPONSE: Pool instance address: {:p}", &*pool);

                        // Log details about each connection
                        for entry in pool.connections_by_addr.iter() {
                            let addr = entry.key();
                            let conn = entry.value();
                            debug!(
                                "FULLSYNC RESPONSE: Connection to {} - state={:?}",
                                addr,
                                conn.get_state()
                            );
                        }

                        // Create message with length + type prefix
                        let buffer = pool.create_message_buffer(&response_data);

                        // Debug: Log what connections we have
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Available connections by addr: {:?}",
                            pool.connections_by_addr
                                .iter()
                                .map(|entry| *entry.key())
                                .collect::<Vec<_>>()
                        );
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Available node mappings: {:?}",
                            pool.peer_id_to_addr
                                .iter()
                                .map(|entry| (entry.key().clone(), *entry.value()))
                                .collect::<Vec<_>>()
                        );
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Looking for connection to sender_peer_id: {}",
                            sender_peer_id
                        );
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: sender_socket_addr={}",
                            sender_socket_addr
                        );

                        // Try to send using peer ID
                        let frozen_buffer = bytes::Bytes::from(buffer);
                        let send_result = match pool
                            .send_bytes_to_peer_id(&sender_peer_id, frozen_buffer.clone())
                        {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                warn!("Failed to send via peer ID {}: {}", sender_peer_id, e);
                                // Fall back to socket address
                                pool.send_lock_free(sender_socket_addr, &frozen_buffer)
                            }
                        };

                        if send_result.is_err() {
                            warn!(
                                "Primary send failed for peer {}, no fallback available",
                                sender_peer_id
                            );
                        }

                        match send_result {
                            Ok(()) => {
                                debug!(peer = %sender_socket_addr,
                                  peer_id = %sender_peer_id,
                                  local_actors = local_actors_count,
                                  known_actors = known_actors_count,
                                  bind_addr = %registry.bind_addr,
                                  "📤 RESPONSE: Successfully sent FullSync response with our state");
                            }
                            Err(e) => {
                                // If we can't send immediately, queue it for the next gossip round
                                warn!(peer = %sender_socket_addr,
                                  peer_id = %sender_peer_id,
                                  error = %e,
                                  "Could not send FullSync response immediately - will be sent in next gossip round");

                                // Store in gossip state to be sent during next gossip round
                                drop(pool); // Release the pool lock first
                                let mut gossip_state = registry.gossip_state.lock().await;

                                // Mark that we need to send a full sync to this peer
                                if let Some(peer_info) =
                                    gossip_state.peers.get_mut(&sender_socket_addr)
                                {
                                    // Force a full sync on the next gossip round
                                    peer_info.consecutive_deltas =
                                        registry.config.max_delta_history as u64;
                                    info!(peer = %sender_socket_addr,
                                      "Marked peer for full sync in next gossip round");
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
            RegistryMessage::FullSyncRequest {
                sender_peer_id,
                sequence: _,
                wall_clock_time: _,
            } => {
                debug!(
                    sender = %sender_peer_id,
                    "received full sync request on bidirectional connection"
                );

                {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    gossip_state.full_sync_exchanges += 1;
                }

                // Note: Response will be sent during regular gossip rounds
                Ok(())
            }
            // Handle response messages (these can arrive on incoming connections too)
            RegistryMessage::DeltaGossipResponse { delta } => {
                debug!(
                    sender = %delta.sender_peer_id,
                    changes = delta.changes.len(),
                    "received delta gossip response on bidirectional connection"
                );

                if let Err(err) = registry.apply_delta(delta).await {
                    warn!(error = %err, "failed to apply delta from response");
                } else {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    gossip_state.delta_exchanges += 1;
                }
                Ok(())
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_peer_id,
                sequence,
                wall_clock_time,
            } => {
                debug!(
                    sender = %sender_peer_id,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "RECEIVED: FullSyncResponse from peer"
                );

                let sender_socket_addr = resolve_peer_state_addr(
                    &registry,
                    Some(&sender_peer_id),
                    _peer_addr,
                )
                .await;

                registry
                    .merge_full_sync(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_socket_addr,
                        sequence,
                        wall_clock_time,
                    )
                    .await;

                // Reset failure state when receiving response
                let mut gossip_state = registry.gossip_state.lock().await;

                // Reset failure state for responding peer
                let need_to_clear_pending =
                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        let had_failures = peer_info.failures > 0;
                        if had_failures {
                            info!(peer = %sender_socket_addr,
                          prev_failures = peer_info.failures,
                          "🔄 Resetting failure state after receiving FullSyncResponse");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        had_failures
                    } else {
                        false
                    };

                // Clear pending failure record if needed
                if need_to_clear_pending {
                    gossip_state
                        .pending_peer_failures
                        .remove(&sender_socket_addr);
                }

                gossip_state.full_sync_exchanges += 1;
                Ok(())
            }
            RegistryMessage::PeerHealthQuery {
                sender,
                target_peer,
                timestamp: _,
            } => {
                let sender_socket_addr =
                    resolve_peer_state_addr(&registry, Some(&sender), _peer_addr).await;
                debug!(
                    sender = %sender,
                    target = %target_peer,
                    "received peer health query"
                );

                // Check our connection status to the target peer
                let target_addr = match target_peer.parse::<SocketAddr>() {
                    Ok(addr) => addr,
                    Err(_) => {
                        warn!(
                            "Invalid target peer address in health query: {}",
                            target_peer
                        );
                        return Ok(());
                    }
                };

                let is_alive = {
                    let pool = registry.connection_pool.lock().await;
                    pool.has_connection(&target_addr)
                };

                let last_contact = if is_alive {
                    crate::current_timestamp()
                } else {
                    // Check when we last had successful contact
                    let gossip_state = registry.gossip_state.lock().await;
                    gossip_state
                        .peers
                        .get(&target_addr)
                        .map(|info| info.last_success)
                        .unwrap_or(0)
                };

                // Send our health report back
                let mut peer_statuses = HashMap::new();

                // Get actual failure count from gossip state
                let failure_count = {
                    let gossip_state = registry.gossip_state.lock().await;
                    gossip_state
                        .peers
                        .get(&target_addr)
                        .map(|info| info.failures as u32)
                        .unwrap_or(0)
                };

                peer_statuses.insert(
                    target_peer,
                    crate::registry::PeerHealthStatus {
                        is_alive,
                        last_contact,
                        failure_count,
                    },
                );

                let report = RegistryMessage::PeerHealthReport {
                    reporter: registry.peer_id.clone(),
                    peer_statuses: peer_statuses.into_iter().collect(),
                    timestamp: crate::current_timestamp(),
                };

                // Send report back to the querying peer
                if let Ok(data) = rkyv::to_bytes::<rkyv::rancor::Error>(&report) {
                    // Use the actual peer address we received from
                    let sender_addr = sender_socket_addr;

                    // Create message with length + type prefix
                    let mut pool = registry.connection_pool.lock().await;
                    let buffer = pool.create_message_buffer(&data);

                    // Use send_lock_free which doesn't create new connections
                    if let Err(e) = pool.send_lock_free(sender_addr, &buffer) {
                        warn!(peer = %sender_addr, error = %e, "Failed to send peer health report");
                    }
                }

                Ok(())
            }
            RegistryMessage::PeerHealthReport {
                reporter,
                peer_statuses,
                timestamp: _,
            } => {
                let reporter_addr =
                    resolve_peer_state_addr(&registry, Some(&reporter), _peer_addr).await;
                debug!(
                    reporter = %reporter,
                    peers = peer_statuses.len(),
                    "received peer health report"
                );

                // Store the health reports
                {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    for (peer, status) in peer_statuses {
                        if let Ok(peer_addr) = peer.parse::<SocketAddr>() {
                            // For now, use the reporter's peer address from the connection
                            gossip_state
                                .peer_health_reports
                                .entry(peer_addr)
                                .or_insert_with(HashMap::new)
                                .insert(reporter_addr, status);
                        }
                    }
                }

                // Check if we have enough reports to make a decision
                registry.check_peer_consensus().await;

                Ok(())
            }
            RegistryMessage::ActorMessage {
                actor_id,
                type_hash,
                payload,
                correlation_id,
            } => {
                let peer_state_addr =
                    resolve_peer_state_addr(&registry, None, _peer_addr).await;
                debug!(
                    actor_id = %actor_id,
                    type_hash = %format!("{:08x}", type_hash),
                    payload_len = payload.len(),
                    correlation_id = ?correlation_id,
                    "received actor message"
                );

                // Forward to the registry's actor message handler
                match registry
                    .handle_actor_message(&actor_id, type_hash, &payload, correlation_id)
                    .await
                {
                    Ok(Some(reply_payload)) => {
                        // If there's a correlation_id, this is an ask message and we need to send the reply back
                        if let Some(corr_id) = correlation_id {
                            debug!(
                                actor_id = %actor_id,
                                type_hash = %format!("{:08x}", type_hash),
                                correlation_id = corr_id,
                                reply_len = reply_payload.len(),
                                "sending ask reply back to sender"
                            );

                            let header = framing::write_ask_response_header(
                                crate::MessageType::Response,
                                corr_id,
                                reply_payload.len(),
                            );
                            let payload = bytes::Bytes::from(reply_payload);

                            // Send response back through the same connection
                            let pool = registry.connection_pool.lock().await;
                            if let Err(e) = pool.send_lock_free_parts(
                                peer_state_addr,
                                bytes::Bytes::copy_from_slice(&header),
                                payload,
                            ) {
                                warn!(peer = %peer_state_addr, error = %e, "Failed to send ask reply");
                            }
                        }

                        debug!(
                            actor_id = %actor_id,
                            type_hash = %format!("{:08x}", type_hash),
                            "actor message processed successfully with reply"
                        );
                    }
                    Ok(None) => {
                        debug!(
                            actor_id = %actor_id,
                            type_hash = %format!("{:08x}", type_hash),
                            "actor message processed successfully"
                        );
                    }
                    Err(e) => {
                        warn!(
                            actor_id = %actor_id,
                            type_hash = %format!("{:08x}", type_hash),
                            error = %e,
                            "failed to process actor message"
                        );
                    }
                }

                Ok(())
            }

            RegistryMessage::ImmediateAck {
                actor_name,
                success,
            } => {
                debug!(
                    actor_name = %actor_name,
                    success = success,
                    "received immediate ACK for synchronous registration"
                );

                // Look up the pending ACK sender for this actor
                let mut pending_acks = registry.pending_acks.lock().await;
                if let Some(sender) = pending_acks.remove(&actor_name) {
                    // Send the success status through the oneshot channel
                    if sender.send(success).is_err() {
                        warn!(
                            actor_name = %actor_name,
                            "Failed to send ACK to waiting registration - receiver dropped"
                        );
                    } else {
                        info!(
                            actor_name = %actor_name,
                            success = success,
                            "✅ Sent ACK to waiting synchronous registration"
                        );
                    }
                } else {
                    debug!(
                        actor_name = %actor_name,
                        "Received ACK but no pending registration found (may have timed out)"
                    );
                }

                Ok(())
            }

            RegistryMessage::PeerListGossip {
                peers,
                timestamp,
                sender_addr,
            } => {
                let peer_state_addr =
                    resolve_peer_state_addr(&registry, None, _peer_addr).await;
                debug!(
                    peer_count = peers.len(),
                    timestamp = timestamp,
                    sender = %sender_addr,
                    "received peer list gossip message"
                );

                // Accept peer list only from connected peers
                if !registry.has_active_connection(&peer_state_addr).await {
                    debug!(
                        peer = %peer_state_addr,
                        "ignoring peer list gossip from non-connected peer"
                    );
                    return Ok(());
                }

                if !registry.peer_supports_peer_list(&peer_state_addr).await {
                    debug!(
                        peer = %peer_state_addr,
                        "ignoring peer list gossip from peer without capability"
                    );
                    return Ok(());
                }

                let candidates = registry
                    .on_peer_list_gossip(peers, &sender_addr, timestamp)
                    .await;

                if candidates.is_empty() {
                    return Ok(());
                }

                let registry_clone = registry.clone();
                tokio::spawn(async move {
                    for addr in candidates {
                        let node_id = registry_clone.lookup_node_id(&addr).await;
                        registry_clone.add_peer_with_node_id(addr, node_id).await;

                        match registry_clone.get_connection(addr).await {
                            Ok(_) => {
                                registry_clone.mark_peer_connected(addr).await;
                                debug!(peer = %addr, "connected to discovered peer");
                            }
                            Err(e) => {
                                registry_clone.mark_peer_failed(addr).await;
                                warn!(peer = %addr, error = %e, "failed to connect to discovered peer");
                            }
                        }
                    }
                });

                Ok(())
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::time::sleep;

    async fn create_test_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept connections in background
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    // Simple echo server - just keep connection open
                    let mut buf = vec![0; 1024];
                    loop {
                        use tokio::io::AsyncReadExt;
                        match stream.read(&mut buf).await {
                            Ok(0) => break, // Connection closed
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                });
            }
        });

        addr
    }

    #[test]
    fn test_connection_handle_debug() {
        // Compile-time test to ensure Debug is implemented
        use std::fmt::Debug;
        fn assert_debug<T: Debug>() {}
        assert_debug::<ConnectionHandle>();
    }

    #[test]
    fn test_buffer_config_validation() {
        // Should reject buffers < 256KB
        let result = BufferConfig::new(100 * 1024);
        assert!(result.is_err());

        // Should accept valid sizes
        let config = BufferConfig::new(512 * 1024).unwrap();
        assert_eq!(config.tcp_buffer_size(), 512 * 1024);
        assert_eq!(config.ring_buffer_slots(), RING_BUFFER_SLOTS);

        // Streaming threshold should be buffer_size - 1KB
        assert_eq!(config.streaming_threshold(), 511 * 1024);
    }

    #[test]
    fn test_streaming_threshold_calculation() {
        let config = BufferConfig::new(1024 * 1024).unwrap();

        // 1MB buffer should have ~1MB-1KB threshold
        let threshold = config.streaming_threshold();
        assert!(threshold < config.tcp_buffer_size());
        assert!(threshold > 1020 * 1024); // At least 1020KB
        assert_eq!(threshold, 1023 * 1024); // Exactly 1023KB
    }

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert_eq!(config.tcp_buffer_size(), 1024 * 1024); // 1MB
        assert_eq!(config.ring_buffer_slots(), RING_BUFFER_SLOTS); // 1024 slots
        assert_eq!(config.streaming_threshold(), 1023 * 1024); // 1MB - 1KB
        assert_eq!(
            config.ask_inflight_limit(),
            crate::config::DEFAULT_ASK_INFLIGHT_LIMIT
        );
    }

    #[test]
    fn test_buffer_config_minimum_size() {
        // Test exactly at minimum boundary
        let config = BufferConfig::new(256 * 1024).unwrap();
        assert_eq!(config.tcp_buffer_size(), 256 * 1024);
        assert_eq!(config.ring_buffer_slots(), RING_BUFFER_SLOTS);
        assert_eq!(config.streaming_threshold(), 255 * 1024);

        // Test just below minimum (should fail)
        let result = BufferConfig::new(256 * 1024 - 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_streaming_threshold_saturation() {
        // Test that streaming_threshold handles edge cases properly
        let config = BufferConfig::new(256 * 1024).unwrap(); // Minimum buffer (256KB)
                                                             // Should be 255KB (256KB - 1KB)
        assert_eq!(config.streaming_threshold(), 255 * 1024);

        // Test with exactly 1KB buffer would be rejected by validation,
        // but we can verify saturating_sub behavior directly
        let large_config = BufferConfig::new(2 * 1024 * 1024).unwrap(); // 2MB
        assert_eq!(large_config.streaming_threshold(), 2 * 1024 * 1024 - 1024);
    }

    #[test]
    fn test_control_reserved_slots_bounds() {
        assert_eq!(control_reserved_slots(1), 1);
        assert_eq!(control_reserved_slots(2), 1);
        assert_eq!(control_reserved_slots(CONTROL_RESERVED_SLOTS), CONTROL_RESERVED_SLOTS);
        assert_eq!(control_reserved_slots(CONTROL_RESERVED_SLOTS * 2), CONTROL_RESERVED_SLOTS);
    }

    #[test]
    fn test_should_flush_rules() {
        assert!(!should_flush(0, true, Duration::from_millis(1), 4 * 1024, WRITER_MAX_LATENCY));
        assert!(should_flush(
            4096,
            false,
            Duration::from_millis(1),
            4 * 1024,
            WRITER_MAX_LATENCY
        ));
        assert!(should_flush(
            1,
            true,
            Duration::from_millis(1),
            4 * 1024,
            WRITER_MAX_LATENCY
        ));
        assert!(should_flush(
            1,
            false,
            WRITER_MAX_LATENCY + Duration::from_micros(1),
            4 * 1024,
            WRITER_MAX_LATENCY
        ));
    }

    #[tokio::test]
    async fn test_permit_lanes_allow_control_when_ask_exhausted() {
        let (writer, _reader) = tokio::io::duplex(1024);
        let handle = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:0".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
        );

        let (ask_available, control_available) = handle.permit_counts();
        assert!(ask_available > 0);
        assert!(control_available > 0);

        let mut ask_permits = Vec::with_capacity(ask_available);
        for _ in 0..ask_available {
            ask_permits.push(handle.acquire_ask_permit_for_test().await);
        }

        let control = tokio::time::timeout(
            Duration::from_millis(50),
            handle.acquire_control_permit_for_test(),
        )
        .await;
        assert!(control.is_ok());

        let blocked = tokio::time::timeout(
            Duration::from_millis(10),
            handle.acquire_ask_permit_for_test(),
        )
        .await;
        assert!(blocked.is_err());

        drop(control.unwrap());
        drop(ask_permits.pop());

        let recovered = tokio::time::timeout(
            Duration::from_millis(50),
            handle.acquire_ask_permit_for_test(),
        )
        .await;
        assert!(recovered.is_ok());
    }

    #[tokio::test]
    async fn test_connection_pool_new() {
        let pool = ConnectionPool::new(10, Duration::from_secs(5));
        assert_eq!(pool.connection_count(), 0);
        assert_eq!(pool.max_connections, 10);
        assert_eq!(pool.connection_timeout, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_set_registry() {
        use crate::{registry::GossipRegistry, GossipConfig, KeyPair};
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let registry = Arc::new(GossipRegistry::new(
            "127.0.0.1:8080".parse().unwrap(),
            GossipConfig {
                key_pair: Some(KeyPair::new_for_testing("conn_pool_registry")),
                ..Default::default()
            },
        ));

        pool.set_registry(registry.clone());
        assert!(pool.registry.is_some());
    }

    #[tokio::test]
    async fn test_connection_handle_send_data() {
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, writer) = stream.into_split();

        // Create a LockFreeStreamHandle
        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
        ));

        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            correlation: CorrelationTracker::new(),
        };

        let data = vec![1, 2, 3, 4];
        // Just test that send_data doesn't error - the data goes to the mock stream
        handle.send_data(data.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_handle_send_data_closed() {
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, mut writer) = stream.into_split();
        use tokio::io::AsyncWriteExt;
        let _ = writer.shutdown().await; // Close the writer

        // Create a LockFreeStreamHandle
        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
        ));

        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            correlation: CorrelationTracker::new(),
        };

        // The lock-free design uses fire-and-forget semantics, so send_data
        // will succeed even if the writer is closed. The error is detected
        // asynchronously in the background writer task.
        //
        // For this test, we'll just verify that send_data doesn't panic
        // and returns some result (which should be Ok due to fire-and-forget).
        let result = handle.send_data(vec![1, 2, 3]).await;
        // With fire-and-forget semantics, this should return Ok even with a closed writer
        assert!(result.is_ok());
    }
}
