use std::time::Duration;

use futures::{
    future::{ready, BoxFuture},
    FutureExt, TryFutureExt,
};
use tokio::sync::mpsc;

use crate::Actor;

use super::{
    BoxMailboxSender, BoxWeakMailboxSender, MailboxReceiver, MailboxSendError,
    MailboxSendTimeoutError, MailboxSender, MailboxTryRecvError, MailboxTrySendError, Signal,
    WeakMailboxSender,
};

impl<A: Actor> From<mpsc::error::SendError<Signal<A>>> for MailboxSendError<A> {
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self {
        MailboxSendError(err.0)
    }
}

impl<A: Actor> From<mpsc::error::SendError<Signal<A>>> for MailboxTrySendError<A> {
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self {
        MailboxTrySendError::Closed(err.0)
    }
}

impl<A: Actor> From<mpsc::error::SendError<Signal<A>>> for MailboxSendTimeoutError<A> {
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self {
        MailboxSendTimeoutError::Closed(err.0)
    }
}

impl<A: Actor> From<mpsc::error::TrySendError<Signal<A>>> for MailboxTrySendError<A> {
    fn from(err: mpsc::error::TrySendError<Signal<A>>) -> Self {
        match err {
            mpsc::error::TrySendError::Full(signal) => MailboxTrySendError::Full(signal),
            mpsc::error::TrySendError::Closed(signal) => MailboxTrySendError::Closed(signal),
        }
    }
}

impl<A: Actor> From<mpsc::error::SendTimeoutError<Signal<A>>> for MailboxSendTimeoutError<A> {
    fn from(err: mpsc::error::SendTimeoutError<Signal<A>>) -> Self {
        match err {
            mpsc::error::SendTimeoutError::Timeout(signal) => {
                MailboxSendTimeoutError::Timeout(Some(signal))
            }
            mpsc::error::SendTimeoutError::Closed(signal) => {
                MailboxSendTimeoutError::Closed(signal)
            }
        }
    }
}

impl From<mpsc::error::TryRecvError> for MailboxTryRecvError {
    fn from(err: mpsc::error::TryRecvError) -> Self {
        match err {
            mpsc::error::TryRecvError::Empty => MailboxTryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => MailboxTryRecvError::Closed,
        }
    }
}

impl<A: Actor> MailboxSender<A> for mpsc::Sender<Signal<A>> {
    #[inline(always)]
    fn send(&self, signal: Signal<A>) -> BoxFuture<'_, Result<(), MailboxSendError<A>>> {
        self.send(signal).map_err(From::from).boxed()
    }

    #[inline(always)]
    fn try_send(&self, signal: Signal<A>) -> Result<(), MailboxTrySendError<A>> {
        self.try_send(signal).map_err(From::from)
    }

    #[inline(always)]
    fn send_timeout(
        &self,
        signal: Signal<A>,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<(), MailboxSendTimeoutError<A>>> {
        self.send_timeout(signal, timeout)
            .map_err(From::from)
            .boxed()
    }

    #[inline(always)]
    fn blocking_send(&self, signal: Signal<A>) -> Result<(), MailboxSendError<A>> {
        self.blocking_send(signal).map_err(From::from)
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        self.closed().boxed()
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn capacity(&self) -> Option<usize> {
        Some(self.capacity())
    }

    fn downgrade(&self) -> BoxWeakMailboxSender<A> {
        BoxWeakMailboxSender::new(self.downgrade())
    }

    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    fn weak_count(&self) -> usize {
        self.weak_count()
    }
}

impl<A: Actor> WeakMailboxSender<A> for mpsc::WeakSender<Signal<A>> {
    fn upgrade(&self) -> Option<BoxMailboxSender<A>> {
        self.upgrade().map(BoxMailboxSender::new)
    }

    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    fn weak_count(&self) -> usize {
        self.weak_count()
    }
}

impl<A: Actor> MailboxReceiver<A> for mpsc::Receiver<Signal<A>> {
    #[inline(always)]
    fn recv(&mut self) -> BoxFuture<'_, Option<Signal<A>>> {
        self.recv().boxed()
    }

    #[inline(always)]
    fn try_recv(&mut self) -> Result<Signal<A>, MailboxTryRecvError> {
        self.try_recv().map_err(From::from)
    }

    #[inline(always)]
    fn blocking_recv(&mut self) -> Option<Signal<A>> {
        self.blocking_recv()
    }

    fn close(&mut self) {
        self.close()
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn sender_strong_count(&self) -> usize {
        self.sender_strong_count()
    }

    fn sender_weak_count(&self) -> usize {
        self.sender_weak_count()
    }
}

impl<A: Actor> MailboxSender<A> for mpsc::UnboundedSender<Signal<A>> {
    #[inline(always)]
    fn send(&self, signal: Signal<A>) -> BoxFuture<'_, Result<(), MailboxSendError<A>>> {
        ready(self.send(signal).map_err(From::from)).boxed()
    }

    #[inline(always)]
    fn try_send(&self, signal: Signal<A>) -> Result<(), MailboxTrySendError<A>> {
        self.send(signal).map_err(From::from)
    }

    #[inline(always)]
    fn send_timeout(
        &self,
        signal: Signal<A>,
        _timeout: Duration,
    ) -> BoxFuture<'_, Result<(), MailboxSendTimeoutError<A>>> {
        ready(self.send(signal).map_err(From::from)).boxed()
    }

    #[inline(always)]
    fn blocking_send(&self, signal: Signal<A>) -> Result<(), MailboxSendError<A>> {
        self.send(signal).map_err(From::from)
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        self.closed().boxed()
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn capacity(&self) -> Option<usize> {
        None
    }

    fn downgrade(&self) -> BoxWeakMailboxSender<A> {
        BoxWeakMailboxSender::new(self.downgrade())
    }

    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    fn weak_count(&self) -> usize {
        self.weak_count()
    }
}

impl<A: Actor> WeakMailboxSender<A> for mpsc::WeakUnboundedSender<Signal<A>> {
    fn upgrade(&self) -> Option<BoxMailboxSender<A>> {
        self.upgrade().map(BoxMailboxSender::new)
    }

    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    fn weak_count(&self) -> usize {
        self.weak_count()
    }
}

impl<A: Actor> MailboxReceiver<A> for mpsc::UnboundedReceiver<Signal<A>> {
    #[inline(always)]
    fn recv(&mut self) -> BoxFuture<'_, Option<Signal<A>>> {
        self.recv().boxed()
    }

    #[inline(always)]
    fn try_recv(&mut self) -> Result<Signal<A>, MailboxTryRecvError> {
        self.try_recv().map_err(From::from)
    }

    #[inline(always)]
    fn blocking_recv(&mut self) -> Option<Signal<A>> {
        self.blocking_recv()
    }

    fn close(&mut self) {
        self.close()
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn sender_strong_count(&self) -> usize {
        self.sender_strong_count()
    }

    fn sender_weak_count(&self) -> usize {
        self.sender_weak_count()
    }
}
