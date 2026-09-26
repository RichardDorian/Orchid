use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::sync::mpsc;

use crate::{KeyValue, Result, Revision};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    Put,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchEvent {
    pub kind: EventKind,
    /// For a put, the new value. For a delete, only `key` and `mod_revision`
    /// (the revision of the deletion) are set.
    pub kv: KeyValue,
    /// The value before the event, if the key existed.
    pub prev_kv: Option<KeyValue>,
}

impl WatchEvent {
    /// Revision of the write that produced the event.
    pub fn revision(&self) -> Revision {
        self.kv.mod_revision
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchResponse {
    /// Events of one or more writes, in revision order. The events of a single
    /// transaction are always delivered in the same response.
    Events(Vec<WatchEvent>),
    /// Answer to [`Watch::request_progress`]: every event up to this revision
    /// has been delivered, the watch can be resumed from `revision + 1`.
    Progress(Revision),
}

/// A stream of [`WatchResponse`]s.
///
/// The stream ends after yielding an error ([`StoreError::Compacted`] if the
/// start revision has been compacted, [`StoreError::WatchClosed`] if the store
/// closed it). Dropping the watch cancels it.
///
/// [`StoreError::Compacted`]: crate::StoreError::Compacted
/// [`StoreError::WatchClosed`]: crate::StoreError::WatchClosed
pub struct Watch {
    responses: mpsc::UnboundedReceiver<Result<WatchResponse>>,
    request_progress: Box<dyn Fn() + Send + Sync>,
}

impl Watch {
    pub(crate) fn new(
        responses: mpsc::UnboundedReceiver<Result<WatchResponse>>,
        request_progress: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            responses,
            request_progress: Box::new(request_progress),
        }
    }

    /// Receives the next response. `None` once the watch ended.
    pub async fn recv(&mut self) -> Option<Result<WatchResponse>> {
        self.responses.recv().await
    }

    /// Asks the store to send a [`WatchResponse::Progress`] as soon as possible.
    pub fn request_progress(&self) {
        (self.request_progress)();
    }
}

impl Stream for Watch {
    type Item = Result<WatchResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.responses.poll_recv(cx)
    }
}

impl fmt::Debug for Watch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Watch").finish_non_exhaustive()
    }
}
