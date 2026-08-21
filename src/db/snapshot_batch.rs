use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Instant;

use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use crate::ndb_ext::from_ndb_note;
use crate::nostr::Filter;
use crate::storage::Storage;

use super::subscription::{StreamItem, SubscriptionManager};

pub(crate) struct SnapshotRequest {
    pub(crate) id: usize,
    pub(crate) filters: Vec<Filter>,
    pub(crate) sender: Sender<StreamItem>,
}

enum DispatcherMessage {
    Enqueue(SnapshotRequest),
    WorkerCompleted(usize),
    Shutdown,
}

pub(crate) struct SnapshotBatcher {
    sender: Option<mpsc::Sender<DispatcherMessage>>,
    dispatcher: Option<JoinHandle<()>>,
}

impl SnapshotBatcher {
    pub(crate) fn start(
        storage: Arc<Storage>,
        subscriptions: SubscriptionManager,
        worker_count: NonZeroUsize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel();
        let mut worker_senders = Vec::with_capacity(worker_count.get());
        let mut worker_handles = Vec::with_capacity(worker_count.get());

        for worker_index in 0..worker_count.get() {
            let (worker_sender, worker_receiver) = mpsc::channel();
            worker_senders.push(worker_sender);
            let storage = storage.clone();
            let subscriptions = subscriptions.clone();
            let completion_sender = sender.clone();
            let handle = std::thread::Builder::new()
                .name(format!("searchnos-query-batch-{}", worker_index + 1))
                .spawn(move || {
                    run_worker(
                        worker_index,
                        storage,
                        subscriptions,
                        completion_sender,
                        worker_receiver,
                    )
                })
                .expect("spawn subscription snapshot worker");
            worker_handles.push(handle);
        }

        let dispatcher_subscriptions = subscriptions.clone();
        let dispatcher = std::thread::Builder::new()
            .name("searchnos-query-dispatch".to_owned())
            .spawn(move || {
                run_dispatcher(
                    receiver,
                    worker_senders,
                    worker_handles,
                    dispatcher_subscriptions,
                )
            })
            .expect("spawn subscription snapshot dispatcher");

        Self {
            sender: Some(sender),
            dispatcher: Some(dispatcher),
        }
    }

    pub(crate) fn enqueue(&self, request: SnapshotRequest) -> Result<(), SnapshotRequest> {
        self.sender
            .as_ref()
            .expect("snapshot dispatcher sender is available before drop")
            .send(DispatcherMessage::Enqueue(request))
            .map_err(|err| match err.0 {
                DispatcherMessage::Enqueue(request) => request,
                DispatcherMessage::WorkerCompleted(_) | DispatcherMessage::Shutdown => {
                    unreachable!("enqueue only sends snapshot requests")
                }
            })
    }
}

impl Drop for SnapshotBatcher {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(DispatcherMessage::Shutdown);
        }
        if let Some(dispatcher) = self.dispatcher.take() {
            let _ = dispatcher.join();
        }
    }
}

fn run_dispatcher(
    receiver: mpsc::Receiver<DispatcherMessage>,
    worker_senders: Vec<mpsc::Sender<Vec<SnapshotRequest>>>,
    worker_handles: Vec<JoinHandle<()>>,
    subscriptions: SubscriptionManager,
) {
    let worker_count = worker_senders.len();
    let mut idle_workers = (0..worker_count).rev().collect::<Vec<_>>();
    let mut pending = VecDeque::new();
    let mut ready_batches = VecDeque::new();

    while let Ok(message) = receiver.recv() {
        if handle_dispatcher_message(message, &mut pending, &mut idle_workers) {
            break;
        }

        if let Err(failed_batch) = dispatch_available(
            &mut pending,
            &mut ready_batches,
            &mut idle_workers,
            &worker_senders,
            worker_count,
        ) {
            warn!("subscription snapshot worker stopped unexpectedly");
            unregister_requests(failed_batch, &subscriptions);
            break;
        }
    }

    unregister_requests(pending, &subscriptions);
    for batch in ready_batches {
        unregister_requests(batch, &subscriptions);
    }
    drop(worker_senders);
    for worker in worker_handles {
        let _ = worker.join();
    }
}

fn handle_dispatcher_message(
    message: DispatcherMessage,
    pending: &mut VecDeque<SnapshotRequest>,
    idle_workers: &mut Vec<usize>,
) -> bool {
    match message {
        DispatcherMessage::Enqueue(request) => {
            pending.push_back(request);
            false
        }
        DispatcherMessage::WorkerCompleted(worker_index) => {
            debug_assert!(!idle_workers.contains(&worker_index));
            idle_workers.push(worker_index);
            false
        }
        DispatcherMessage::Shutdown => true,
    }
}

fn split_pending<T>(pending: &mut VecDeque<T>, max_batches: usize) -> VecDeque<Vec<T>> {
    let batch_count = pending.len().min(max_batches);
    let mut batches = VecDeque::with_capacity(batch_count);
    for batch_index in 0..batch_count {
        let remaining_batches = batch_count - batch_index;
        let batch_size = pending.len().div_ceil(remaining_batches);
        let batch = pending.drain(..batch_size).collect();
        batches.push_back(batch);
    }
    batches
}

fn dispatch_available<T>(
    pending: &mut VecDeque<T>,
    ready_batches: &mut VecDeque<Vec<T>>,
    idle_workers: &mut Vec<usize>,
    worker_senders: &[mpsc::Sender<Vec<T>>],
    worker_count: usize,
) -> Result<(), Vec<T>> {
    dispatch_ready_batches(idle_workers, ready_batches, worker_senders)?;
    if !idle_workers.is_empty() && ready_batches.is_empty() {
        ready_batches.extend(split_pending(pending, worker_count));
        dispatch_ready_batches(idle_workers, ready_batches, worker_senders)?;
    }
    Ok(())
}

fn dispatch_ready_batches<T>(
    idle_workers: &mut Vec<usize>,
    ready_batches: &mut VecDeque<Vec<T>>,
    worker_senders: &[mpsc::Sender<Vec<T>>],
) -> Result<(), Vec<T>> {
    while !idle_workers.is_empty() && !ready_batches.is_empty() {
        let worker_index = idle_workers.pop().expect("idle worker exists");
        let batch = ready_batches.pop_front().expect("ready batch exists");
        if let Err(err) = worker_senders[worker_index].send(batch) {
            idle_workers.push(worker_index);
            return Err(err.0);
        }
    }
    Ok(())
}

fn unregister_requests(
    requests: impl IntoIterator<Item = SnapshotRequest>,
    subscriptions: &SubscriptionManager,
) {
    for request in requests {
        subscriptions.unregister(request.id);
    }
}

fn run_worker(
    worker_index: usize,
    storage: Arc<Storage>,
    subscriptions: SubscriptionManager,
    completion_sender: mpsc::Sender<DispatcherMessage>,
    receiver: mpsc::Receiver<Vec<SnapshotRequest>>,
) {
    while let Ok(mut requests) = receiver.recv() {
        requests.retain(|request| !request.sender.is_closed());
        if !requests.is_empty() {
            process_batch(
                worker_index,
                &storage,
                &subscriptions,
                &requests,
                Instant::now(),
            );
        }
        if completion_sender
            .send(DispatcherMessage::WorkerCompleted(worker_index))
            .is_err()
        {
            break;
        }
    }
}

fn process_batch(
    worker_index: usize,
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
                worker = worker_index + 1,
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
                worker = worker_index + 1,
                queries = requests.len(),
                elapsed_ms = started_at.elapsed().as_millis(),
                error = %err,
                "failed subscription snapshot batch"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::mpsc;

    use super::{dispatch_available, split_pending};

    #[test]
    fn pending_requests_are_balanced_across_available_workers() {
        let mut pending = (0..10).collect::<VecDeque<_>>();

        let batches = split_pending(&mut pending, 4);

        assert!(pending.is_empty());
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![3, 3, 2, 2]
        );
        assert_eq!(
            batches.into_iter().flatten().collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn backlog_is_dispatched_to_all_idle_workers() {
        let (first_sender, first_receiver) = mpsc::channel();
        let (second_sender, second_receiver) = mpsc::channel();
        let worker_senders = vec![first_sender, second_sender];
        let mut idle_workers = vec![1, 0];
        let mut pending = (0..5).collect::<VecDeque<_>>();
        let mut batches = VecDeque::new();

        dispatch_available(
            &mut pending,
            &mut batches,
            &mut idle_workers,
            &worker_senders,
            2,
        )
        .unwrap();

        assert!(idle_workers.is_empty());
        assert!(batches.is_empty());
        assert_eq!(first_receiver.try_recv().unwrap(), vec![0, 1, 2]);
        assert_eq!(second_receiver.try_recv().unwrap(), vec![3, 4]);
    }

    #[test]
    fn first_request_starts_immediately_when_a_worker_is_idle() {
        let (first_sender, first_receiver) = mpsc::channel();
        let (second_sender, second_receiver) = mpsc::channel();
        let worker_senders = vec![first_sender, second_sender];
        let mut idle_workers = vec![1, 0];
        let mut pending = VecDeque::from([1]);
        let mut batches = VecDeque::new();

        dispatch_available(
            &mut pending,
            &mut batches,
            &mut idle_workers,
            &worker_senders,
            2,
        )
        .unwrap();

        assert_eq!(first_receiver.try_recv().unwrap(), vec![1]);
        assert_eq!(second_receiver.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert_eq!(idle_workers, vec![1]);
    }

    #[test]
    fn requests_wait_and_batch_while_every_worker_is_busy() {
        let (sender, receiver) = mpsc::channel();
        let worker_senders = vec![sender];
        let mut idle_workers = Vec::new();
        let mut pending = VecDeque::from([1, 2, 3]);
        let mut batches = VecDeque::new();

        dispatch_available(
            &mut pending,
            &mut batches,
            &mut idle_workers,
            &worker_senders,
            1,
        )
        .unwrap();

        assert_eq!(pending, VecDeque::from([1, 2, 3]));
        assert!(batches.is_empty());
        assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty));

        idle_workers.push(0);
        dispatch_available(
            &mut pending,
            &mut batches,
            &mut idle_workers,
            &worker_senders,
            1,
        )
        .unwrap();

        assert!(pending.is_empty());
        assert_eq!(receiver.try_recv().unwrap(), vec![1, 2, 3]);
    }
}
