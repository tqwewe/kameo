//! Internal types for remote message handling
//! 
//! This module only contains the RemoteMessageFns type used by the type hash registry

use std::time::Duration;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::actor::ActorId;
use crate::error::RemoteSendError;

/// Function pointers for remote message handling
pub struct RemoteMessageFns {
    pub ask: Arc<RemoteAskFn>,
    pub try_ask: Arc<RemoteTryAskFn>,
    pub tell: Arc<RemoteTellFn>,
    pub try_tell: Arc<RemoteTryTellFn>,
}

pub type RemoteAskFn = dyn Fn(
    ActorId,
    Vec<u8>,
    Option<Duration>,
    Option<Duration>,
) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>> + Send + Sync;

pub type RemoteTryAskFn = dyn Fn(
    ActorId,
    Vec<u8>,
    Option<Duration>,
) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>> + Send + Sync;

pub type RemoteTellFn = dyn Fn(
    ActorId,
    Vec<u8>,
    Option<Duration>,
) -> BoxFuture<'static, Result<(), RemoteSendError>> + Send + Sync;

pub type RemoteTryTellFn =
    dyn Fn(ActorId, Vec<u8>) -> BoxFuture<'static, Result<(), RemoteSendError>> + Send + Sync;

