use crate::{GossipError, Result};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Handle for replying to a remote ask() request
/// This can be passed between local actors to delegate the reply
#[derive(Clone)]
pub struct ReplyTo {
    /// Correlation ID to match request with response
    pub(crate) correlation_id: u16,
    /// Connection back to the requesting node
    pub(crate) connection: Arc<crate::connection_pool::ConnectionHandle>,
}

impl ReplyTo {
    /// Send reply back to the original requester
    pub async fn reply(self, response: &[u8]) -> Result<()> {
        let result = self
            .connection
            .send_response_bytes(self.correlation_id, Bytes::copy_from_slice(response))
            .await;

        match &result {
            Ok(_) => tracing::info!("ReplyTo::reply: successfully sent response"),
            Err(e) => tracing::error!("ReplyTo::reply: failed to send response: {}", e),
        }

        result
    }

    /// Reply using owned bytes without copying the payload.
    pub async fn reply_bytes(self, response: Bytes) -> Result<()> {
        self.connection
            .send_response_bytes(self.correlation_id, response)
            .await
    }

    /// Reply with a typed payload (rkyv) and debug-only type hash verification.
    pub async fn reply_typed<T>(self, value: &T) -> Result<()>
    where
        T: crate::typed::WireEncode,
    {
        let payload = crate::typed::encode_typed_pooled(value)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<T>(payload);
        self.connection
            .send_response_pooled(self.correlation_id, payload, prefix, payload_len)
            .await
    }

    /// Reply with a serializable type using rkyv
    pub async fn reply_with<T>(self, value: &T) -> Result<()>
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
        let response =
            rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(GossipError::Serialization)?;
        self.reply_bytes(Bytes::copy_from_slice(response.as_ref()))
            .await
    }

    /// Create a reply handle with timeout
    pub fn with_timeout(self, timeout: Duration) -> TimeoutReplyTo {
        TimeoutReplyTo {
            inner: self,
            deadline: Instant::now() + timeout,
        }
    }
}

/// ReplyTo handle with timeout enforcement
pub struct TimeoutReplyTo {
    inner: ReplyTo,
    deadline: Instant,
}

impl TimeoutReplyTo {
    /// Reply if not timed out
    pub async fn reply(self, response: &[u8]) -> Result<()> {
        if Instant::now() > self.deadline {
            return Err(GossipError::Timeout);
        }
        self.inner.reply(response).await
    }

    /// Reply with a serializable type if not timed out
    pub async fn reply_with<T>(self, value: &T) -> Result<()>
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
        if Instant::now() > self.deadline {
            return Err(GossipError::Timeout);
        }
        self.inner.reply_with(value).await
    }

    /// Check if timed out
    pub fn is_timed_out(&self) -> bool {
        Instant::now() > self.deadline
    }

    /// Get remaining time before timeout
    pub fn time_remaining(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }
}

impl std::fmt::Debug for ReplyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyTo")
            .field("correlation_id", &self.correlation_id)
            .field("connection", &self.connection.addr)
            .finish()
    }
}

impl std::fmt::Debug for TimeoutReplyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeoutReplyTo")
            .field("correlation_id", &self.inner.correlation_id)
            .field("deadline", &self.deadline)
            .field("time_remaining", &self.time_remaining())
            .finish()
    }
}
