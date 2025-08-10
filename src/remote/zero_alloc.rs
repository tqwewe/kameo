//! Zero-allocation optimizations for remote messaging
//! 
//! This module provides allocation-free alternatives for high-performance scenarios

use std::mem::MaybeUninit;
use bytes::{Bytes, BytesMut};

/// Pre-allocated message buffer pool
pub struct MessageBufferPool {
    buffers: Vec<BytesMut>,
}

impl MessageBufferPool {
    /// Create a new buffer pool with pre-allocated buffers
    pub fn new(count: usize, capacity: usize) -> Self {
        let buffers = (0..count)
            .map(|_| BytesMut::with_capacity(capacity))
            .collect();
        
        Self { buffers }
    }
    
    /// Get a buffer from the pool
    pub fn get(&mut self) -> Option<BytesMut> {
        self.buffers.pop()
    }
    
    /// Return a buffer to the pool
    pub fn return_buffer(&mut self, mut buffer: BytesMut) {
        buffer.clear();
        self.buffers.push(buffer);
    }
}

/// Thread-local buffer pool for zero-allocation message sending
thread_local! {
    static MESSAGE_BUFFER_POOL: std::cell::RefCell<MessageBufferPool> = 
        std::cell::RefCell::new(MessageBufferPool::new(16, 4096));
}

/// Stack-allocated message header (28 bytes)
#[repr(C, packed)]
pub struct MessageHeader {
    pub length: [u8; 4],
    pub msg_type: u8,
    pub correlation_id: [u8; 2],
    pub reserved: [u8; 5],
    pub actor_id: [u8; 8],
    pub type_hash: [u8; 4],
    pub payload_len: [u8; 4],
}

impl MessageHeader {
    /// Create a new message header on the stack
    #[inline(always)]
    pub fn new_tell(actor_id: u64, type_hash: u32, payload_len: u32) -> Self {
        let inner_size = 8 + 16 + payload_len; // header + actor fields + payload
        
        Self {
            length: (inner_size as u32).to_be_bytes(),
            msg_type: 3, // ActorTell
            correlation_id: 0u16.to_be_bytes(),
            reserved: [0u8; 5],
            actor_id: actor_id.to_be_bytes(),
            type_hash: type_hash.to_be_bytes(),
            payload_len: payload_len.to_be_bytes(),
        }
    }
    
    /// Get header as byte slice
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>()
            )
        }
    }
}

/// Zero-allocation message builder using stack buffer
pub struct StackMessageBuilder<const N: usize> {
    buffer: [u8; N],
    pos: usize,
}

impl<const N: usize> StackMessageBuilder<N> {
    /// Create a new stack-based message builder
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            buffer: unsafe { MaybeUninit::uninit().assume_init() },
            pos: 0,
        }
    }
    
    /// Write bytes to the buffer
    #[inline(always)]
    pub fn write(&mut self, data: &[u8]) -> Result<(), &'static str> {
        let end = self.pos + data.len();
        if end > N {
            return Err("Buffer overflow");
        }
        
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.buffer.as_mut_ptr().add(self.pos),
                data.len()
            );
        }
        
        self.pos = end;
        Ok(())
    }
    
    /// Get the written data as a slice
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[..self.pos]
    }
}

/// Optimized serialization directly into a provided buffer
pub trait SerializeInto {
    /// Serialize directly into the provided buffer, returning bytes written
    fn serialize_into(&self, buffer: &mut [u8]) -> Result<usize, rkyv::rancor::Error>;
}

// We'd implement this for message types to avoid AlignedVec allocation
impl<T> SerializeInto for T 
where
    T: rkyv::Serialize<rkyv::rancor::Strategy<rkyv::ser::Serializer<&mut [u8], rkyv::ser::allocator::ArenaHandle<'_>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>>
{
    fn serialize_into(&self, buffer: &mut [u8]) -> Result<usize, rkyv::rancor::Error> {
        // This would need a custom rkyv serializer that writes to the provided buffer
        // For now, this is a placeholder
        todo!("Implement direct buffer serialization")
    }
}