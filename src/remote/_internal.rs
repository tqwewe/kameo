//! Internal types for remote message handling
//!
//! This module only contains the RemoteMessageFns type used by the type hash registry

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use kameo_remote::AlignedBytes;

use crate::actor::ActorId;
use crate::error::RemoteSendError;

/// Function pointers for remote message handling
pub struct RemoteMessageFns {
    pub ask: Arc<RemoteAskFn>,
    pub try_ask: Arc<RemoteTryAskFn>,
    pub tell: Arc<RemoteTellFn>,
    pub try_tell: Arc<RemoteTryTellFn>,
}

impl std::fmt::Debug for RemoteMessageFns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteMessageFns").finish_non_exhaustive()
    }
}

pub type RemoteAskFn = dyn Fn(
        ActorId,
        AlignedBytes,
        Option<Duration>,
        Option<Duration>,
    ) -> BoxFuture<'static, Result<Bytes, RemoteSendError<Bytes>>>
    + Send
    + Sync;

pub type RemoteTryAskFn = dyn Fn(
        ActorId,
        AlignedBytes,
        Option<Duration>,
    ) -> BoxFuture<'static, Result<Bytes, RemoteSendError<Bytes>>>
    + Send
    + Sync;

pub type RemoteTellFn = dyn Fn(ActorId, AlignedBytes, Option<Duration>) -> BoxFuture<'static, Result<(), RemoteSendError>>
    + Send
    + Sync;

pub type RemoteTryTellFn =
    dyn Fn(ActorId, AlignedBytes) -> BoxFuture<'static, Result<(), RemoteSendError>> + Send + Sync;
