use std::cell::RefCell;
use std::error::Error;
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use crate::ndb_ext::from_ndb_note;
use crate::nostr::Filter;
use crate::storage::Storage;

use super::subscription::{StreamItem, SubscriptionManager};

const SNAPSHOT_BATCH_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) struct SnapshotRequest {
    pub(crate) id: usize,
    pub(crate) filters: Vec<Filter>,
    pub(crate) sender: Sender<StreamItem>,
}

pub(crate) struct SnapshotBatcher {
    sender: Option<mpsc::Sender<SnapshotRequest>>,
    worker: Option<JoinHandle<()>>,
}

impl SnapshotBatcher {
    pub(crate) fn start(storage: Arc<Storage>, subscriptions: SubscriptionManager) -> Self {
        let (sender, receiver) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("searchnos-query-batch".to_owned())
            .spawn(move || run(storage, subscriptions, receiver))
            .expect("spawn subscription snapshot batch worker");
        Self {
            sender: Some(sender),
            worker: Some(worker),
        }
    }

    pub(crate) fn enqueue(&self, request: SnapshotRequest) -> Result<(), SnapshotRequest> {
        self.sender
            .as_ref()
            .expect("snapshot batch sender is available before drop")
            .send(request)
            .map_err(|err| err.0)
    }
}

impl Drop for SnapshotBatcher {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn run(
    storage: Arc<Storage>,
    subscriptions: SubscriptionManager,
    receiver: mpsc::Receiver<SnapshotRequest>,
) {
    while let Ok(first) = receiver.recv() {
        let started_at = Instant::now();
        let deadline = started_at + SNAPSHOT_BATCH_INTERVAL;
        let mut requests = vec![first];
        let mut disconnected = false;

        while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            match receiver.recv_timeout(remaining) {
                Ok(request) => requests.push(request),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        process_batch(&storage, &subscriptions, &requests, started_at);
        if disconnected {
            break;
        }
    }
}

fn process_batch(
    storage: &Storage,
    subscriptions: &SubscriptionManager,
    requests: &[SnapshotRequest],
    started_at: Instant,
) {
    let active = RefCell::new(
        requests
            .iter()
            .map(|request| !request.sender.is_closed())
            .collect::<Vec<_>>(),
    );
    let queries = requests
        .iter()
        .map(|request| request.filters.as_slice())
        .collect::<Vec<_>>();
    let result = storage.query_streaming_batch(
        &queries,
        |query_index| active.borrow()[query_index] && !requests[query_index].sender.is_closed(),
        |packet, query_indexes| {
            let event_json =
                from_ndb_note(&packet).map_err(|err| Box::new(err) as Box<dyn Error>)?;
            let mut failed = Vec::new();
            for query_index in query_indexes {
                let request = &requests[*query_index];
                if request
                    .sender
                    .try_send(StreamItem::Event(event_json.clone()))
                    .is_err()
                {
                    active.borrow_mut()[*query_index] = false;
                    subscriptions.unregister(request.id);
                    failed.push(*query_index);
                }
            }
            Ok(failed)
        },
    );

    match result {
        Ok(()) => {
            for (query_index, request) in requests.iter().enumerate() {
                if active.borrow()[query_index]
                    && request.sender.try_send(StreamItem::Eose).is_err()
                {
                    active.borrow_mut()[query_index] = false;
                    subscriptions.unregister(request.id);
                }
            }
            info!(
                queries = requests.len(),
                elapsed_ms = started_at.elapsed().as_millis(),
                "completed subscription snapshot batch"
            );
        }
        Err(err) => {
            for request in requests {
                subscriptions.unregister(request.id);
            }
            warn!(
                queries = requests.len(),
                elapsed_ms = started_at.elapsed().as_millis(),
                error = %err,
                "failed subscription snapshot batch"
            );
        }
    }
}
