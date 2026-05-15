use crate::ndb_ext::{from_ndb_note, to_ndb_note_buf, verify_note};
use crate::nostr::{EventError, Filter};
pub use crate::storage::{CompactStats, ReindexProgress, ReindexProgressPhase, ReindexStats};
use crate::storage::{DEFAULT_HOT_MAX_BYTES, Storage};
use serde_json::{Map, Value};
use std::error::Error;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod purge_policy;
mod subscription;

pub use purge_policy::{PurgePolicy, PurgeSpecError};
pub use subscription::{StreamItem, Subscription};

const DUMP_LENGTH_PREFIX_BYTES: u64 = std::mem::size_of::<u32>() as u64;
const LOAD_BATCH_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct SearchnosDBOptions {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub purge_policy: Option<PurgePolicy>,
    pub subscription_capacity: usize,
    pub default_limit: Option<usize>,
    pub max_limit: Option<usize>,
    pub hot_max_bytes: u64,
    /// None means all event kinds are searchable.
    pub searchable_kinds: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertOptions {
    pub notify_subscribers: bool,
}

impl Default for InsertOptions {
    fn default() -> Self {
        Self {
            notify_subscribers: true,
        }
    }
}

impl Default for SearchnosDBOptions {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            flush_interval: Duration::from_millis(100),
            purge_policy: None,
            subscription_capacity: subscription::DEFAULT_SUBSCRIPTION_CAPACITY,
            default_limit: None,
            max_limit: None,
            hot_max_bytes: DEFAULT_HOT_MAX_BYTES,
            searchable_kinds: None,
        }
    }
}

pub struct SearchnosDB {
    storage: Storage,
    subscriptions: subscription::SubscriptionManager,
    default_limit: Option<usize>,
    max_limit: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum SearchnosDBError {
    #[error("failed to access storage: {0}")]
    Storage(String),
    #[error("failed to read or write data: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse nostr event JSON: {0}")]
    ParseEvent(#[from] EventError),
    #[error("invalid event signature: {0}")]
    InvalidSignature(EventError),
    #[error("failed to convert event to ndb bytes: {0}")]
    EncodeEvent(#[from] ndb::Error),
    #[error("failed to decode ndb event: {0}")]
    DecodeEvent(ndb::Error),
    #[error("failed to parse filters JSON: {0}")]
    ParseFilters(#[from] serde_json::Error),
    #[error("invalid tag query '{tag}': tag identifier must be a single character")]
    InvalidTagQuery { tag: String },
    #[error("event payload length exceeds u32: {0} bytes")]
    EventPayloadTooLarge(usize),
    #[error("batch state is poisoned")]
    BatchStatePoisoned,
}

#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub name: String,
    pub count: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub total_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DumpProgress {
    pub events_written: u64,
    pub total_events: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadProgress {
    pub events_loaded: u64,
    pub bytes_read: u64,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub events: Vec<String>,
    pub stats: QueryStats,
}

#[derive(Debug, Clone)]
pub struct QueryStats {
    pub total_elapsed: Duration,
    pub index_scan_duration: Duration,
    pub post_processing_duration: Duration,
    pub filters: Vec<FilterStats>,
}

#[derive(Debug, Clone)]
pub struct FilterStats {
    pub index_scan_duration: Duration,
    pub post_processing_duration: Duration,
    pub matched_event_count: usize,
    pub candidate_count: usize,
}

struct LoadedDumpRecord {
    payload: Vec<u8>,
    progress: LoadProgress,
    payload_offset: u64,
}

impl SearchnosDB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SearchnosDBError> {
        Self::open_with_options(path, SearchnosDBOptions::default())
    }

    pub fn open_with_options<P: AsRef<Path>>(
        path: P,
        options: SearchnosDBOptions,
    ) -> Result<Self, SearchnosDBError> {
        let root = path.as_ref();
        let storage = Storage::open_at_with_searchable_kinds(
            root,
            options.hot_max_bytes,
            options.searchable_kinds.as_deref(),
        )
        .map_err(Self::storage_error)?;

        Ok(Self {
            storage,
            subscriptions: subscription::SubscriptionManager::new(options.subscription_capacity),
            default_limit: options.default_limit,
            max_limit: options.max_limit,
        })
    }

    pub fn subscribe(
        self: Arc<Self>,
        filters_json: &str,
    ) -> Result<subscription::Subscription, SearchnosDBError> {
        let filters = self.normalized_storage_filters_from_json(filters_json)?;
        let (id, receiver, sender) = self.subscriptions.register(filters);
        let subscription =
            subscription::Subscription::new(id, receiver, self.subscriptions.clone());
        let filters_json = filters_json.to_owned();
        let db = self.clone();

        std::thread::spawn(move || {
            let mut receiver_open = true;
            let result = db.stream_query(&filters_json, |event_json| {
                if sender
                    .blocking_send(subscription::StreamItem::Event(event_json))
                    .is_err()
                {
                    receiver_open = false;
                    return false;
                }
                true
            });

            match result {
                Ok(()) if receiver_open => {
                    if sender
                        .blocking_send(subscription::StreamItem::Eose)
                        .is_err()
                    {
                        db.subscriptions.unregister(id);
                    }
                }
                Ok(()) => db.subscriptions.unregister(id),
                Err(err) => {
                    db.subscriptions.unregister(id);
                    eprintln!("failed to stream subscription snapshot: {err}");
                }
            }
        });

        Ok(subscription)
    }

    pub fn insert_event_json(
        &self,
        event_json: &str,
        options: InsertOptions,
    ) -> Result<(), SearchnosDBError> {
        self.insert_event_json_owned(event_json.to_owned(), options)
    }

    pub fn insert_event_json_owned(
        &self,
        event_json: String,
        options: InsertOptions,
    ) -> Result<(), SearchnosDBError> {
        let note = to_ndb_note_buf(&event_json).map_err(Self::note_json_error_to_db_error)?;
        let note_ref = note.as_ndb_note();
        verify_note(&note_ref).map_err(SearchnosDBError::InvalidSignature)?;
        let bytes = note.into_bytes();
        self.storage
            .append_packet(&bytes)
            .map_err(Self::storage_error)?;
        if options.notify_subscribers {
            self.broadcast_packet(&bytes, &event_json);
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), SearchnosDBError> {
        Ok(())
    }

    pub fn compact(&self) -> Result<CompactStats, SearchnosDBError> {
        self.storage.compact().map_err(Self::storage_error)
    }

    pub fn reindex(&self) -> Result<ReindexStats, SearchnosDBError> {
        self.reindex_with_progress(false, |_| {})
    }

    pub fn reindex_all(&self) -> Result<ReindexStats, SearchnosDBError> {
        self.reindex_with_progress(true, |_| {})
    }

    pub fn reindex_with_progress<F>(
        &self,
        force: bool,
        on_progress: F,
    ) -> Result<ReindexStats, SearchnosDBError>
    where
        F: FnMut(ReindexProgress),
    {
        self.storage
            .reindex_with_progress(force, on_progress)
            .map_err(Self::storage_error)
    }

    pub fn query(&self, filters_json: &str) -> Result<Vec<String>, SearchnosDBError> {
        self.query_with_stats(filters_json)
            .map(|result| result.events)
    }

    pub fn query_with_stats(&self, filters_json: &str) -> Result<QueryResult, SearchnosDBError> {
        let total_start = Instant::now();
        let mut events = Vec::new();
        let stats = self.stream_query_with_stats(filters_json, |event| {
            events.push(event);
            true
        })?;
        let stats = QueryStats {
            total_elapsed: total_start.elapsed(),
            ..stats
        };
        Ok(QueryResult { events, stats })
    }

    pub fn stream_query<F>(&self, filters_json: &str, on_event: F) -> Result<(), SearchnosDBError>
    where
        F: FnMut(String) -> bool,
    {
        self.stream_query_with_stats(filters_json, on_event)
            .map(|_| ())
    }

    pub fn stream_query_with_stats<F>(
        &self,
        filters_json: &str,
        mut on_event: F,
    ) -> Result<QueryStats, SearchnosDBError>
    where
        F: FnMut(String) -> bool,
    {
        let total_start = Instant::now();
        let filters = self.normalized_storage_filters_from_json(filters_json)?;
        let mut matched_event_count = 0usize;
        let mut completed = true;
        let index_start = Instant::now();

        self.storage
            .query_streaming(&filters, |packet| {
                matched_event_count += 1;
                let event_json =
                    from_ndb_note(&packet).map_err(|err| Box::new(err) as Box<dyn Error>)?;
                if on_event(event_json) {
                    Ok(())
                } else {
                    completed = false;
                    Err(Box::new(StopStreaming))
                }
            })
            .map_err(|err| {
                if err.downcast_ref::<StopStreaming>().is_some() {
                    SearchnosDBError::Storage(String::new())
                } else {
                    Self::storage_error(err)
                }
            })
            .or_else(|err| match err {
                SearchnosDBError::Storage(message) if message.is_empty() => Ok(()),
                err => Err(err),
            })?;

        let index_scan_duration = index_start.elapsed();
        let filter_stats = filters
            .iter()
            .map(|_| FilterStats {
                index_scan_duration,
                post_processing_duration: Duration::default(),
                matched_event_count,
                candidate_count: matched_event_count,
            })
            .collect();

        Ok(QueryStats {
            total_elapsed: total_start.elapsed(),
            index_scan_duration,
            post_processing_duration: if completed {
                total_start.elapsed().saturating_sub(index_scan_duration)
            } else {
                Duration::default()
            },
            filters: filter_stats,
        })
    }

    pub fn dump_events<W: Write>(&self, writer: W) -> Result<u64, SearchnosDBError> {
        self.dump_events_with_progress(writer, |_| {})
    }

    pub fn dump_events_with_progress<W, F>(
        &self,
        mut writer: W,
        mut on_progress: F,
    ) -> Result<u64, SearchnosDBError>
    where
        W: Write,
        F: FnMut(DumpProgress),
    {
        let packets = self
            .storage
            .query(&[Filter::new()])
            .map_err(Self::storage_error)?;
        let total_events = packets.len() as u64;
        let mut bytes_written = 0u64;

        for (index, payload) in packets.iter().enumerate() {
            let len = u32::try_from(payload.len())
                .map_err(|_| SearchnosDBError::EventPayloadTooLarge(payload.len()))?;
            writer.write_all(&len.to_be_bytes())?;
            writer.write_all(payload)?;
            bytes_written += DUMP_LENGTH_PREFIX_BYTES + u64::from(len);
            on_progress(DumpProgress {
                events_written: index as u64 + 1,
                total_events,
                bytes_written,
            });
        }

        Ok(total_events)
    }

    pub fn load_events<R: Read>(&self, reader: R) -> Result<u64, SearchnosDBError> {
        self.load_events_with_progress(reader, |_| {})
    }

    pub fn load_events_with_progress<R, F>(
        &self,
        mut reader: R,
        mut on_progress: F,
    ) -> Result<u64, SearchnosDBError>
    where
        R: Read,
        F: FnMut(LoadProgress),
    {
        let mut count = 0u64;
        let mut bytes_read = 0u64;

        loop {
            let mut batch = Vec::with_capacity(LOAD_BATCH_SIZE);
            for _ in 0..LOAD_BATCH_SIZE {
                let Some(len) = Self::read_dump_record_length(&mut reader)? else {
                    break;
                };
                bytes_read += DUMP_LENGTH_PREFIX_BYTES;

                let payload_offset = bytes_read;
                let mut payload = vec![0u8; len as usize];
                reader.read_exact(&mut payload)?;
                bytes_read += u64::from(len);
                count += 1;

                batch.push(LoadedDumpRecord {
                    payload,
                    progress: LoadProgress {
                        events_loaded: count,
                        bytes_read,
                    },
                    payload_offset,
                });
            }

            if batch.is_empty() {
                break;
            }

            for record in batch {
                match self.insert_loaded_dump_record(&record.payload) {
                    Ok(()) => on_progress(record.progress),
                    Err(err) if Self::is_load_record_error(&err) => {
                        eprintln!(
                            "warning: skipping dump record {} at payload offset {}: {}",
                            record.progress.events_loaded, record.payload_offset, err
                        );
                        on_progress(record.progress);
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        Ok(count)
    }

    pub fn database_stats(&self) -> Result<Vec<DatabaseStats>, SearchnosDBError> {
        let packets = self
            .storage
            .query(&[Filter::new()])
            .map_err(Self::storage_error)?;
        let value_bytes = packets.iter().map(Vec::len).sum();
        Ok(vec![DatabaseStats {
            name: "storage".to_string(),
            count: packets.len(),
            key_bytes: 0,
            value_bytes,
            total_bytes: value_bytes,
        }])
    }

    pub fn purge_stale_events(&self, _max_events: usize) -> Result<usize, SearchnosDBError> {
        Ok(0)
    }

    fn insert_loaded_dump_record(&self, payload: &[u8]) -> Result<(), SearchnosDBError> {
        let note = ndb::NdbNote::from_bytes(payload).map_err(SearchnosDBError::DecodeEvent)?;
        verify_note(&note).map_err(SearchnosDBError::InvalidSignature)?;
        let event_json = note
            .to_json_string()
            .map_err(SearchnosDBError::DecodeEvent)?;
        self.storage
            .append_packet(payload)
            .map_err(Self::storage_error)?;
        self.broadcast_packet(payload, &event_json);
        Ok(())
    }

    fn broadcast_packet(&self, packet: &[u8], event_json: &str) {
        let targets = self.subscriptions.collect_matching_senders(|filter| {
            self.storage
                .packet_matches_filter(packet, filter)
                .unwrap_or(false)
        });
        for (id, sender) in targets {
            if sender
                .try_send(subscription::StreamItem::Event(event_json.to_owned()))
                .is_err()
            {
                self.subscriptions.unregister(id);
            }
        }
    }

    fn normalized_storage_filters_from_json(
        &self,
        filters_json: &str,
    ) -> Result<Vec<Filter>, SearchnosDBError> {
        let filters = Self::parse_filters_json(filters_json)?;
        Ok(self.normalized_filters_or_default(&filters))
    }

    fn effective_limit(&self, provided: Option<usize>) -> Option<usize> {
        let mut limit = provided.or(self.default_limit);
        if let Some(max_limit) = self.max_limit {
            limit = Some(limit.unwrap_or(max_limit).min(max_limit));
        }
        limit
    }

    fn normalize_filter(&self, filter: &Filter) -> Filter {
        let mut normalized = filter.clone();
        normalized.limit = self.effective_limit(filter.limit);
        normalized
    }

    fn normalized_filters_or_default(&self, filters: &[Filter]) -> Vec<Filter> {
        if filters.is_empty() {
            vec![self.normalize_filter(&Filter::new())]
        } else {
            filters
                .iter()
                .map(|filter| self.normalize_filter(filter))
                .collect()
        }
    }

    fn parse_filters_json(filters_json: &str) -> Result<Vec<Filter>, SearchnosDBError> {
        let value: Value = serde_json::from_str(filters_json)?;
        Self::validate_tag_queries(&value)?;
        Filter::from_value(value).map_err(SearchnosDBError::from)
    }

    fn validate_tag_queries(value: &Value) -> Result<(), SearchnosDBError> {
        match value {
            Value::Array(filters) => {
                for filter in filters {
                    if let Value::Object(map) = filter {
                        Self::validate_tag_map(map)?;
                    }
                }
            }
            Value::Object(map) => Self::validate_tag_map(map)?,
            _ => {}
        }
        Ok(())
    }

    fn validate_tag_map(map: &Map<String, Value>) -> Result<(), SearchnosDBError> {
        for key in map.keys() {
            if let Some(tag_key) = key.strip_prefix('#')
                && tag_key.len() != 1
            {
                return Err(SearchnosDBError::InvalidTagQuery { tag: key.clone() });
            }
        }
        Ok(())
    }

    fn read_dump_record_length<R: Read>(reader: &mut R) -> Result<Option<u32>, SearchnosDBError> {
        let mut prefix = [0u8; std::mem::size_of::<u32>()];
        match reader.read(&mut prefix[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => {}
            Ok(_) => unreachable!("single-byte buffer cannot read more than one byte"),
            Err(err) => return Err(err.into()),
        }

        match reader.read_exact(&mut prefix[1..]) {
            Ok(()) => Ok(Some(u32::from_be_bytes(prefix))),
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "dump record length prefix is truncated",
            )
            .into()),
            Err(err) => Err(err.into()),
        }
    }

    fn note_json_error_to_db_error(err: ndb::Error) -> SearchnosDBError {
        match err {
            ndb::Error::Json(_)
            | ndb::Error::MissingField(_)
            | ndb::Error::InvalidJsonShape(_)
            | ndb::Error::InvalidHex
            | ndb::Error::InvalidUtf8 => {
                SearchnosDBError::ParseEvent(EventError::InvalidJson(err.to_string()))
            }
            err => SearchnosDBError::EncodeEvent(err),
        }
    }

    fn is_load_record_error(err: &SearchnosDBError) -> bool {
        matches!(
            err,
            SearchnosDBError::ParseEvent(_)
                | SearchnosDBError::InvalidSignature(_)
                | SearchnosDBError::DecodeEvent(_)
        )
    }

    fn storage_error(err: impl std::fmt::Display) -> SearchnosDBError {
        SearchnosDBError::Storage(err.to_string())
    }
}

#[derive(Debug)]
struct StopStreaming;

impl std::fmt::Display for StopStreaming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("streaming stopped")
    }
}

impl Error for StopStreaming {}

#[cfg(test)]
mod tests {
    use super::{InsertOptions, SearchnosDB, StreamItem, Subscription};
    use crate::nostr::{
        Event, Filter, JsonUtil,
        test_utils::{EventBuilder, Keys},
    };
    use serde_json::to_string as to_json_string;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before unix epoch")
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("searchnos-db-test-{}-{unique}", std::process::id()));
            fs::create_dir(&path).expect("failed to create temp test directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn open_test_db() -> (TestDir, Arc<SearchnosDB>) {
        let dir = TestDir::new();
        let db = SearchnosDB::open(dir.path()).expect("failed to open test db");
        (dir, Arc::new(db))
    }

    fn subscribe(db: &Arc<SearchnosDB>, filters: &[Filter]) -> Subscription {
        let json = to_json_string(filters).expect("failed to encode filters");
        db.clone().subscribe(&json).expect("subscribe failed")
    }

    fn query(db: &SearchnosDB, filters: &[Filter]) -> Vec<String> {
        let json = to_json_string(filters).expect("failed to encode filters");
        db.query(&json).expect("query failed")
    }

    fn wait_for_next(subscription: &mut Subscription) -> Option<StreamItem> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(item) = subscription.try_next() {
                return Some(item);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_eose(subscription: &mut Subscription) {
        match wait_for_next(subscription) {
            Some(StreamItem::Eose) => {}
            Some(StreamItem::Event(event)) => {
                panic!("unexpected snapshot event before EOSE: {event}")
            }
            None => panic!("timed out waiting for EOSE"),
        }
    }

    fn assert_no_live_item(subscription: &mut Subscription) {
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if let Some(item) = subscription.try_next() {
                panic!("unexpected live subscription item: {item:?}");
            }
            if Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn assert_next_event(subscription: &mut Subscription, expected: &Event) {
        assert_eq!(
            wait_for_next(subscription),
            Some(StreamItem::Event(expected.as_json()))
        );
    }

    #[test]
    fn insert_event_json_can_store_without_live_notification() {
        let (_dir, db) = open_test_db();
        let keys = Keys::generate();
        let event = EventBuilder::text_note("backfilled event")
            .sign_with_keys(&keys)
            .expect("failed to sign event");
        let filters = vec![Filter::new().author(keys.public_key())];
        let mut subscription = subscribe(&db, &filters);
        wait_for_eose(&mut subscription);

        db.insert_event_json(
            &event.as_json(),
            InsertOptions {
                notify_subscribers: false,
            },
        )
        .expect("insert failed");

        assert_no_live_item(&mut subscription);
        assert_eq!(query(&db, &filters), vec![event.as_json()]);

        let mut new_subscription = subscribe(&db, &filters);
        assert_next_event(&mut new_subscription, &event);
        assert_eq!(wait_for_next(&mut new_subscription), Some(StreamItem::Eose));
    }

    #[test]
    fn suppressed_insert_does_not_unregister_non_matching_subscriptions() {
        let (_dir, db) = open_test_db();
        let matching_keys = Keys::generate();
        let other_keys = Keys::generate();
        let suppressed_event = EventBuilder::text_note("suppressed")
            .sign_with_keys(&matching_keys)
            .expect("failed to sign suppressed event");
        let other_event = EventBuilder::text_note("other")
            .sign_with_keys(&other_keys)
            .expect("failed to sign other event");
        let other_filters = vec![Filter::new().author(other_keys.public_key())];
        let mut other_subscription = subscribe(&db, &other_filters);
        wait_for_eose(&mut other_subscription);

        db.insert_event_json(
            &suppressed_event.as_json(),
            InsertOptions {
                notify_subscribers: false,
            },
        )
        .expect("suppressed insert failed");
        assert_no_live_item(&mut other_subscription);

        db.insert_event_json(&other_event.as_json(), InsertOptions::default())
            .expect("other insert failed");
        assert_next_event(&mut other_subscription, &other_event);
    }
}
