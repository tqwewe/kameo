use std::{marker::PhantomData, mem, time::Duration};

use futures::TryFutureExt;
use tokio::{task::JoinHandle, time::timeout};

use crate::{
    actor::{ActorRef, BoundedMailbox, Signal, UnboundedMailbox},
    error::SendError,
    message::Message,
    Actor, Reply,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor without any reply.
///
/// This can be thought of as "fire and forget".
#[allow(missing_debug_implementations)]
pub struct TellRequest<A, Mb, M, T>
where
    A: Actor<Mailbox = Mb>,
{
    mailbox: Mb,
    signal: Signal<A>,
    timeout: T,
    phantom: PhantomData<M>,
}

impl<A, M> TellRequest<A, A::Mailbox, M, WithoutRequestTimeout>
where
    A: Actor,
{
    pub(crate) fn new(actor_ref: &ActorRef<A>, msg: M) -> Self
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest {
            mailbox: actor_ref.mailbox().clone(),
            signal: Signal::Message {
                message: Box::new(msg),
                actor_ref: actor_ref.clone(),
                reply: None,
                sent_within_actor: actor_ref.is_current(),
            },
            timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<A, M, T> TellRequest<A, BoundedMailbox<A>, M, T>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for a reply from the actor.
    pub fn timeout(
        self,
        duration: Duration,
    ) -> TellRequest<A, BoundedMailbox<A>, M, WithRequestTimeout> {
        TellRequest {
            mailbox: self.mailbox,
            signal: self.signal,
            timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

impl<A, M> TellRequest<A, BoundedMailbox<A>, M, WithoutRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message.
    pub async fn send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.send(self.signal).await?;
        Ok(())
    }

    /// Sends the message from outside the async runtime.
    pub fn blocking_send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.blocking_send(self.signal)?;
        Ok(())
    }

    /// Tries to send the message if the mailbox is not full.
    pub fn try_send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.try_send(self.signal)?;
        Ok(())
    }

    /// Sends the message after the given delay.
    ///
    /// If reserve is true, then a permit will be reserved in
    /// the actors mailbox before waiting for the delay.
    pub fn delayed_send(
        mut self,
        delay: Duration,
        reserve: bool,
    ) -> JoinHandle<Result<(), SendError<M, <A::Reply as Reply>::Error>>>
    where
        A: 'static,
        M: Send,
    {
        tokio::spawn(async move {
            let permit = match reserve {
                true => Some(self.mailbox.0.reserve().await.map_err(|_| {
                    SendError::ActorNotRunning(
                        mem::replace(&mut self.signal, Signal::Stop) // Replace signal with a dummy value
                            .downcast_message::<M>()
                            .unwrap(),
                    )
                })?),
                false => None,
            };

            tokio::time::sleep(delay).await;

            match permit {
                Some(permit) => permit.send(self.signal),
                None => self.mailbox.0.send(self.signal).await?,
            }

            Ok(())
        })
    }
}

impl<A, M> TellRequest<A, BoundedMailbox<A>, M, WithRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message with the timeout set.
    pub async fn send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox
            .0
            .send_timeout(self.signal, self.timeout.0)
            .await?;
        Ok(())
    }

    /// Sends the message after the given delay with the timeout set.
    ///
    /// If reserve is true, then a permit will be reserved in
    /// the actors mailbox before waiting for the delay.
    pub fn delayed_send(
        mut self,
        delay: Duration,
        reserve: bool,
    ) -> JoinHandle<Result<(), SendError<M, <A::Reply as Reply>::Error>>>
    where
        A: 'static,
        M: Send,
    {
        tokio::spawn(async move {
            let permit = match reserve {
                true => {
                    let permit = timeout(
                        self.timeout.0,
                        self.mailbox.0.reserve().map_err(|_| {
                            SendError::ActorNotRunning(
                                mem::replace(&mut self.signal, Signal::Stop) // Replace signal with a dummy value
                                    .downcast_message::<M>()
                                    .unwrap(),
                            )
                        }),
                    )
                    .await??;
                    Some(permit)
                }
                false => None,
            };

            tokio::time::sleep(delay).await;

            match permit {
                Some(permit) => permit.send(self.signal),
                None => {
                    self.mailbox
                        .0
                        .send_timeout(self.signal, self.timeout.0)
                        .await?
                }
            }

            Ok(())
        })
    }
}

impl<A, M> TellRequest<A, UnboundedMailbox<A>, M, WithoutRequestTimeout>
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message.
    pub fn send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.send(self.signal)?;
        Ok(())
    }
}
