use std::{fmt, marker::PhantomData};

use tokio::sync::oneshot;

use crate::{
    actor_ref::ActorRef,
    error::BoxSendError,
    message::{BoxDebug, BoxReply},
    reply::Reply,
};

/// A context provided to message and query handlers providing access
/// to the current actor ref, and reply channel.
#[derive(Debug)]
pub struct Context<'r, A: ?Sized, R: ?Sized>
where
    R: Reply,
{
    actor_ref: ActorRef<A>,
    reply: &'r mut Option<ReplySender<R::Value>>,
}

impl<'r, A, R> Context<'r, A, R>
where
    R: Reply,
{
    pub(crate) fn new(
        actor_ref: ActorRef<A>,
        reply: &'r mut Option<ReplySender<R::Value>>,
    ) -> Self {
        Context { actor_ref, reply }
    }

    /// Returns the current actor's ref, allowing messages to be sent to itself.
    pub fn actor_ref(&self) -> ActorRef<A> {
        self.actor_ref.clone()
    }

    /// Extracts the reply sender, providing a mechanism for delegated responses and an optional reply sender.
    ///
    /// This method is designed for scenarios where the response to a message is not immediate and needs to be
    /// handled by another actor or elsewhere. Upon calling this method, if the reply sender exists (is `Some`),
    /// it must be utilized through [ReplySender::send] to send the response back to the original requester.
    ///
    /// This method returns a tuple consisting of [DelegatedReply] and an optional [ReplySender]. The [DelegatedReply]
    /// is a marker type indicating that the message handler will delegate the task of replying to another part of the
    /// system. It should be returned by the message handler to signify this intention. The [ReplySender], if present,
    /// should be used to actually send the response back to the caller. The [ReplySender] will not be present if the
    /// message was sent as async (no repsonse is needed by the caller).
    ///
    /// # Usage
    ///
    /// - The [DelegatedReply] marker should be returned by the handler to indicate that the response will be delegated.
    /// - The [ReplySender], if not `None`, should be used by the delegated responder to send the actual reply.
    ///
    /// It is important to ensure that [ReplySender::send] is called to complete the transaction and send the response
    /// back to the requester. Failure to do so could result in the requester waiting indefinitely for a response.
    pub fn reply_sender(&mut self) -> (DelegatedReply<R::Value>, Option<ReplySender<R::Value>>) {
        (
            DelegatedReply {
                phantom: PhantomData,
            },
            self.reply.take(),
        )
    }
}

/// A marker type indicating that the reply to a message will be handled elsewhere.
///
/// `DelegatedReply` is used in scenarios where the immediate receiver of a message
/// delegates the task of replying to another actor or component. It serves as a
/// signal to the actor system that the current handler will not directly respond to
/// the message, and that the responsibility for sending the reply has been transferred.
///
/// This type is returned by message handlers that invoke [Context::reply_sender]
/// to delegate response duties. It is crucial in maintaining the clarity of message
/// flow and ensuring that the system accurately tracks the delegation of response
/// responsibilities.
///
/// # Examples
///
/// A typical usage might involve a handler for a request that requires information
/// from another part of the system. The handler would call `Context::reply_sender` to
/// obtain a `DelegatedReply` and an optional `ReplySender`. The `DelegatedReply`
/// would be returned by the handler to indicate that the reply will be delegated,
/// and the `ReplySender`, if present, would be passed to the actual responder.
///
/// ```
/// impl Message<Request> for MyActor {
///     type Reply = DelegatedReply<MyResponseType>;
///
///     fn handle(&mut self, msg: Request, ctx: &mut Context<'_, Self, Self::Reply>) -> Self::Reply {
///         let (delegated_reply, reply_sender) = ctx.reply_sender();
///    
///         // Logic to delegate the response, potentially involving other actors
///         if let Some(sender) = reply_sender {
///             another_actor.send(SomeMessage { sender });
///         }
///    
///         delegated_reply
///     }   
/// }
/// ```
///
/// It is essential that the delegated entity completes the response process by
/// utilizing the provided [ReplySender]. Failing to send a response may lead to
/// the requester indefinitely awaiting a reply.

#[must_use = "the deligated reply should be returned by the handler"]
#[derive(Clone, Copy, Debug)]
pub struct DelegatedReply<R> {
    phantom: PhantomData<R>,
}

impl<R> Reply for DelegatedReply<R>
where
    R: Reply,
{
    type Ok = R::Ok;
    type Error = R::Error;
    type Value = R::Value;

    fn to_result(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("a DeligatedReply cannot be converted to a result and is only a marker type")
    }

    fn into_boxed_err(self) -> Option<BoxDebug> {
        None
    }

    fn into_value(self) -> Self::Value {
        unimplemented!("a DeligatedReply cannot be converted to a value and is only a marker type")
    }
}

/// A mechanism for sending replies back to the original requester in a message exchange.
///
/// `ReplySender` encapsulates the functionality to send a response back to whereever
/// a request or query was initiated. It is typically used in scenarios where the
/// processing of a request is delegated to another actor within the system.
/// Upon completion of the request handling, `ReplySender` is used to send the result back,
/// ensuring that the flow of communication is maintained and the requester receives the
/// necessary response.
///
/// This type is designed to be used once per message received; it consumes itself upon sending
/// a reply to enforce a single use and prevent multiple replies to a single message.
///
/// # Usage
///
/// A `ReplySender` is obtained as part of the delegation process when handling a message. It should
/// be used to send a reply once the requested data is available or the operation is complete.
///
/// # Example
///
/// Imagine a scenario in an actor system where an actor needs to query another actor for information
/// and then reply to the original sender:
///
/// ```
/// fn handle_query(sender: ReplySender<QueryResult>, query: Query) {
///     let result = process_query(query);
///     sender.send(result); // Completes the query by sending the result back
/// }
/// ```
///
/// The `ReplySender` provides a clear and straightforward interface for completing the message handling cycle,
/// facilitating efficient and organized communication within the system.

#[must_use = "the receiver expects a reply to be sent"]
pub struct ReplySender<R: ?Sized> {
    tx: oneshot::Sender<Result<BoxReply, BoxSendError>>,
    phantom: PhantomData<R>,
}

impl<R> ReplySender<R> {
    pub(crate) fn new(tx: oneshot::Sender<Result<BoxReply, BoxSendError>>) -> Self {
        ReplySender {
            tx,
            phantom: PhantomData,
        }
    }

    /// Sends a reply using the current `ReplySender`.
    ///
    /// Consumes the `ReplySender`, sending the specified reply to the original
    /// requester. This method is the final step in the response process for
    /// delegated replies, ensuring that the message's intended recipient receives
    /// the necessary data or acknowledgment.
    ///
    /// The method takes ownership of the `ReplySender` to prevent multiple uses,
    /// aligning with the one-time use pattern typical in actor-based messaging for
    /// reply mechanisms. Once called, the `ReplySender` cannot be used again,
    /// enforcing a single-reply guarantee for each message received.
    ///
    /// # Note
    ///
    /// It is crucial to send a reply for every received message to avoid leaving the
    /// requester in a state of indefinite waiting. Failure to do so can lead to deadlocks
    /// or wasted resources in waiting for a response that will never arrive.
    pub fn send(self, reply: R)
    where
        R: Reply,
    {
        let _ = self.tx.send(
            reply
                .to_result()
                .map(|value| Box::new(value) as BoxReply)
                .map_err(|err| BoxSendError::HandlerError(Box::new(err))),
        );
    }
}

impl<R: ?Sized> fmt::Debug for ReplySender<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplySender")
            .field("tx", &self.tx)
            .field("phantom", &self.phantom)
            .finish()
    }
}
