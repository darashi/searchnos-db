use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::nostr::{Filter, extract_note_expiration};
use futures_core::Stream;
use ndb::{MatchEventOptions, NdbNote};
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::db::SearchnosDB;
use crate::text::normalize_query_terms;

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

    pub(crate) fn collect_matching_senders<'note>(
        &self,
        note: &NdbNote<'note>,
        normalized_content: &[u8],
    ) -> Vec<(usize, Sender<StreamItem>)> {
        let guard = self.inner.entries.lock().expect("subscriptions poisoned");
        guard
            .iter()
            .filter(|entry| {
                if entry.filters.is_empty() {
                    return true;
                }
                entry.filters.iter().any(|filter| {
                    SearchnosDB::filter_matches_note(filter, note, normalized_content)
                })
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

impl SearchnosDB {
    pub(crate) fn filter_matches_note(
        filter: &Filter,
        note: &NdbNote<'_>,
        normalized_content: &[u8],
    ) -> bool {
        if matches!(filter.limit, Some(0)) {
            return false;
        }
        if filter
            .search
            .as_ref()
            .is_some_and(|search| normalize_query_terms(search).is_empty())
        {
            return false;
        }

        let ndb_filter = Self::to_ndb_filter(filter, true);
        let options = MatchEventOptions::new();
        if !note.matches_filter(&ndb_filter, options, normalized_content) {
            return false;
        }

        if let Some(expiration) = extract_note_expiration(note)
            && !Self::note_is_ephemeral(note)
            && Self::is_expired(expiration)
        {
            return false;
        }

        true
    }

    pub(crate) fn broadcast_note(
        &self,
        note: &NdbNote<'_>,
        normalized_content: &[u8],
        event_json: &str,
    ) {
        let targets = self
            .subscriptions
            .collect_matching_senders(note, normalized_content);
        if targets.is_empty() {
            return;
        }
        let payload = event_json.to_owned();
        for (id, sender) in targets {
            let item = StreamItem::Event(payload.clone());
            if let Err(err) = sender.try_send(item)
                && matches!(err, TrySendError::Closed(_))
            {
                self.subscriptions.unregister(id);
            }
        }
    }
}
