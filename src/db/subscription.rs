use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::nostr::Filter;

pub(crate) const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 32_768;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamItem {
    Event(String),
    Eose,
}

#[derive(Clone)]
pub(crate) struct SubscriptionManager {
    inner: Arc<SubscriptionInner>,
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new(DEFAULT_SUBSCRIPTION_CAPACITY)
    }
}

struct SubscriptionInner {
    next_id: AtomicUsize,
    entries: Mutex<Vec<SubscriptionEntry>>,
    capacity: usize,
}

struct SubscriptionEntry {
    id: usize,
    filters: Vec<Filter>,
    sender: Sender<StreamItem>,
}

impl fmt::Debug for SubscriptionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionManager").finish()
    }
}

impl SubscriptionManager {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner: Arc::new(SubscriptionInner {
                next_id: AtomicUsize::new(1),
                entries: Mutex::new(Vec::new()),
                capacity,
            }),
        }
    }

    pub(crate) fn register(
        &self,
        filters: Vec<Filter>,
    ) -> (usize, Receiver<StreamItem>, Sender<StreamItem>) {
        let capacity = self.inner.capacity;
        let (sender, receiver) = mpsc::channel(capacity);
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = SubscriptionEntry {
            id,
            filters,
            sender: sender.clone(),
        };
        let mut guard = self.inner.entries.lock().expect("subscriptions poisoned");
        guard.push(entry);
        (id, receiver, sender)
    }

    pub(crate) fn unregister(&self, id: usize) {
        let mut guard = self.inner.entries.lock().expect("subscriptions poisoned");
        guard.retain(|entry| entry.id != id);
    }

    pub(crate) fn collect_matching_senders(
        &self,
        mut matches_filter: impl FnMut(&Filter) -> bool,
    ) -> Vec<(usize, Sender<StreamItem>)> {
        let guard = self.inner.entries.lock().expect("subscriptions poisoned");
        guard
            .iter()
            .filter(|entry| {
                entry.filters.is_empty() || entry.filters.iter().any(&mut matches_filter)
            })
            .map(|entry| (entry.id, entry.sender.clone()))
            .collect()
    }
}

pub struct Subscription {
    id: usize,
    receiver: Receiver<StreamItem>,
    manager: SubscriptionManager,
}

impl Subscription {
    pub(crate) fn new(
        id: usize,
        receiver: Receiver<StreamItem>,
        manager: SubscriptionManager,
    ) -> Self {
        Self {
            id,
            receiver,
            manager,
        }
    }

    pub fn try_next(&mut self) -> Option<StreamItem> {
        match self.receiver.try_recv() {
            Ok(item) => Some(item),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => None,
        }
    }

    pub async fn next(&mut self) -> Option<StreamItem> {
        self.receiver.recv().await
    }
}

impl Stream for Subscription {
    type Item = StreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.manager.unregister(self.id);
    }
}
