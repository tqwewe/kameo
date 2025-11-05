use std::time::Duration;

use flume::{Receiver, Sender, WeakSender};
use futures::{future::BoxFuture, FutureExt, TryFutureExt};

use crate::Actor;

use super::{
    BoxMailboxSender, BoxWeakMailboxSender, MailboxReceiver, MailboxSendError,
    MailboxSendTimeoutError, MailboxSender, MailboxTryRecvError, MailboxTrySendError, Signal,
    WeakMailboxSender,
};

impl<A: Actor> MailboxSender<A> for Sender<Signal<A>> {
    fn send(&self, signal: Signal<A>) -> BoxFuture<'_, Result<(), MailboxSendError<A>>> {
        self.send_async(signal)
            .map_err(|err| MailboxSendError(err.0))
            .boxed()
    }

    fn try_send(&self, signal: Signal<A>) -> Result<(), MailboxTrySendError<A>> {
        self.try_send(signal).map_err(|err| match err {
            flume::TrySendError::Full(signal) => MailboxTrySendError::Full(signal),
            flume::TrySendError::Disconnected(signal) => MailboxTrySendError::Closed(signal),
        })
    }

    fn send_timeout(
        &self,
        signal: Signal<A>,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<(), MailboxSendTimeoutError<A>>> {
        async move {
            match tokio::time::timeout(timeout, self.send_async(signal)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(MailboxSendTimeoutError::Closed(err.0)),
                Err(_) => Err(MailboxSendTimeoutError::Timeout(None)),
            }
        }
        .boxed()
    }

    fn blocking_send(&self, signal: Signal<A>) -> Result<(), MailboxSendError<A>> {
        self.send(signal).map_err(|err| MailboxSendError(err.0))
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        unimplemented!("flume doesn't support closed")
    }

    fn is_closed(&self) -> bool {
        self.is_disconnected()
    }

    fn capacity(&self) -> Option<usize> {
        self.capacity()
    }

    fn downgrade(&self) -> BoxWeakMailboxSender<A> {
        BoxWeakMailboxSender::new(self.downgrade())
    }

    fn strong_count(&self) -> usize {
        self.sender_count()
    }

    fn weak_count(&self) -> usize {
        unimplemented!("flume doesn't support weak_count")
    }
}

impl<A: Actor> WeakMailboxSender<A> for WeakSender<Signal<A>> {
    fn upgrade(&self) -> Option<BoxMailboxSender<A>> {
        self.upgrade().map(BoxMailboxSender::new)
    }

    fn strong_count(&self) -> usize {
        unimplemented!("flume doesn't support strong_count")
    }

    fn weak_count(&self) -> usize {
        unimplemented!("flume doesn't support weak_count")
    }
}

impl<A: Actor> MailboxReceiver<A> for Receiver<Signal<A>> {
    fn recv(&mut self) -> BoxFuture<'_, Option<Signal<A>>> {
        async move { (Receiver::recv_async(self).await).ok() }.boxed()
    }

    fn try_recv(&mut self) -> Result<Signal<A>, MailboxTryRecvError> {
        Receiver::try_recv(self).map_err(|err| match err {
            flume::TryRecvError::Empty => MailboxTryRecvError::Empty,
            flume::TryRecvError::Disconnected => MailboxTryRecvError::Closed,
        })
    }

    fn blocking_recv(&mut self) -> Option<Signal<A>> {
        Receiver::recv(self).ok()
    }

    fn close(&mut self) {
        unimplemented!("flume doesn't support close")
    }

    fn is_closed(&self) -> bool {
        Receiver::is_disconnected(self)
    }

    fn is_empty(&self) -> bool {
        Receiver::is_empty(self)
    }

    fn len(&self) -> usize {
        Receiver::len(self)
    }

    fn sender_strong_count(&self) -> usize {
        Receiver::sender_count(self)
    }

    fn sender_weak_count(&self) -> usize {
        unimplemented!("flume doesn't support sender_weak_count")
    }
}
