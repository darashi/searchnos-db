use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
};

use crate::ndb_ext::from_ndb_note;
use crate::nostr::{EventError, Filter};
use serde_json::{Map, Value};
use std::io::{ErrorKind, Read, Write};
use std::{mem, sync::Arc};
use std::{path::Path, sync::Mutex, time::Duration};

use tokio::task::JoinError;

mod batch;
mod index;
mod normalize;
mod purge;
mod purge_policy;
pub mod query;
mod stat;
mod subscription;
#[cfg(test)]
pub(crate) mod test_support;
mod write;

pub use purge_policy::{PurgePolicy, PurgeSpecError};

pub use subscription::{StreamItem, Subscription};

#[derive(Debug, Clone)]
pub struct SearchnosDBOptions {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub purge_policy: Option<PurgePolicy>,
    pub subscription_capacity: usize,
    pub default_limit: Option<usize>,
    pub max_limit: Option<usize>,
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
        }
    }
}

use batch::BatchState;

const KEY_BYTES: usize = std::mem::size_of::<u64>();
const EVENTS_DB_NAME: &str = "events";
const DEFAULT_MAP_SIZE: usize = 1usize << 40; // 1 TiB
const SEQ_BYTES: usize = std::mem::size_of::<u64>();
const CREATED_AT_BYTES: usize = std::mem::size_of::<u64>();
const EXPIRATION_BYTES: usize = std::mem::size_of::<u64>();
const DUMP_LENGTH_PREFIX_BYTES: u64 = std::mem::size_of::<u32>() as u64;

use index::{
    ContentsStore, DeletionIndex, EventIdIndex, ExpirationIndex, ReplacableIndex,
    ReplaceDeletionIndex,
};

#[derive(Debug)]
pub struct SearchnosDB {
    env: Environment,
    events: Database,
    event_id_index: EventIdIndex,
    deletions: DeletionIndex,
    replacables: ReplacableIndex,
    replace_deletions: ReplaceDeletionIndex,
    contents: ContentsStore,
    expiration_index: ExpirationIndex,
    batch: Mutex<BatchState>,
    purge_policy: Option<PurgePolicy>,
    subscriptions: subscription::SubscriptionManager,
    default_limit: Option<usize>,
    max_limit: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum SearchnosDBError {
    #[error("failed to create LMDB directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to open LMDB environment: {0}")]
    Lmdb(#[from] lmdb::Error),
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
    #[error("invalid deletion a-tag: {0}")]
    InvalidDeletionATag(String),
    #[error("unexpected key length: expected {KEY_BYTES} bytes, got {0}")]
    InvalidKeyLength(usize),
    #[error("u64 key space exhausted")]
    KeyspaceExhausted,
    #[error("unexpected seq length: expected {SEQ_BYTES} bytes, got {0}")]
    InvalidSeqLength(usize),
    #[error("unexpected created_at length: expected {CREATED_AT_BYTES} bytes, got {0}")]
    InvalidCreatedAtLength(usize),
    #[error("unexpected expiration length: expected {EXPIRATION_BYTES} bytes, got {0}")]
    InvalidExpirationLength(usize),
    #[error("event payload length exceeds u32: {0} bytes")]
    EventPayloadTooLarge(usize),
    #[error("normalized content is not valid UTF-8: {0}")]
    InvalidUtf8Content(#[from] std::str::Utf8Error),
    #[error("batch state is poisoned")]
    BatchStatePoisoned,
    #[error("async task join error: {0}")]
    AsyncJoin(#[from] JoinError),
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
    pub filters: Vec<FilterPlanStats>,
}

#[derive(Debug, Clone)]
pub struct FilterPlanStats {
    pub plan: query::QueryPlan,
    pub index_scan_duration: Duration,
    pub post_processing_duration: Duration,
    pub matched_event_count: usize,
    pub candidate_count: usize,
}

pub struct SubscriptionWithStats {
    pub subscription: Subscription,
    pub initial_query: QueryStats,
}

impl SearchnosDB {
    /// Open a database at `path` using default options, creating it if necessary.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SearchnosDBError> {
        Self::open_with_options(path, SearchnosDBOptions::default())
    }

    /// Open a database at `path` with custom configuration values.
    pub fn open_with_options<P: AsRef<Path>>(
        path: P,
        options: SearchnosDBOptions,
    ) -> Result<Self, SearchnosDBError> {
        let path_ref = path.as_ref();
        std::fs::create_dir_all(path_ref)?;

        let env = Environment::new()
            .set_max_dbs(14)
            .set_map_size(DEFAULT_MAP_SIZE)
            .open(path_ref)?;

        let events = env.create_db(Some(EVENTS_DB_NAME), DatabaseFlags::INTEGER_KEY)?;
        let event_id_index = EventIdIndex::open(&env)?;
        let deletions = DeletionIndex::open(&env)?;
        let replacables = ReplacableIndex::open(&env)?;
        let replace_deletions = ReplaceDeletionIndex::open(&env)?;
        let contents = ContentsStore::open(&env)?;
        let expiration_index = ExpirationIndex::open(&env)?;
        let batch = Mutex::new(BatchState::new(options.batch_size, options.flush_interval));
        let purge_policy = options.purge_policy.clone();
        let subscriptions = subscription::SubscriptionManager::new(options.subscription_capacity);

        Ok(Self {
            env,
            events,
            event_id_index,
            deletions,
            replacables,
            replace_deletions,
            contents,
            expiration_index,
            batch,
            purge_policy,
            subscriptions,
            default_limit: options.default_limit,
            max_limit: options.max_limit,
        })
    }

    /// Begin a read-only LMDB transaction scoped to this environment.
    pub(crate) fn begin_ro_txn(&self) -> Result<RoTransaction<'_>, SearchnosDBError> {
        self.env.begin_ro_txn().map_err(SearchnosDBError::from)
    }

    /// Begin a read-write LMDB transaction scoped to this environment.
    pub(crate) fn begin_rw_txn(&self) -> Result<RwTransaction<'_>, SearchnosDBError> {
        self.env.begin_rw_txn().map_err(SearchnosDBError::from)
    }

    /// Subscribe to filters, delivering snapshot events followed by an EOSE marker before live updates.
    pub fn subscribe_with_stats(
        &self,
        filters_json: &str,
    ) -> Result<SubscriptionWithStats, SearchnosDBError> {
        let filters = Self::parse_filters_json(filters_json)?;
        let filter_set = if filters.is_empty() {
            Vec::new()
        } else {
            self.normalized_filters(&filters)
        };
        let (id, receiver, sender) = self.subscriptions.register(filter_set);
        let subscription =
            subscription::Subscription::new(id, receiver, self.subscriptions.clone());

        match self.query_with_stats(filters_json) {
            Ok(QueryResult { events, stats }) => {
                for event_json in events {
                    if sender
                        .try_send(subscription::StreamItem::Event(event_json))
                        .is_err()
                    {
                        // Channel closed or full, stop sending
                        self.subscriptions.unregister(id);
                        return Ok(SubscriptionWithStats {
                            subscription,
                            initial_query: stats,
                        });
                    }
                }

                if sender.try_send(subscription::StreamItem::Eose).is_err() {
                    // Channel closed or full, stop sending
                    self.subscriptions.unregister(id);
                }

                Ok(SubscriptionWithStats {
                    subscription,
                    initial_query: stats,
                })
            }
            Err(err) => {
                self.subscriptions.unregister(id);
                Err(err)
            }
        }
    }

    pub fn subscribe(
        &self,
        filters_json: &str,
    ) -> Result<subscription::Subscription, SearchnosDBError> {
        self.subscribe_with_stats(filters_json)
            .map(|result| result.subscription)
    }

    /// Async wrapper around `subscribe` that offloads blocking work.
    pub async fn subscribe_async_with_stats(
        self: Arc<Self>,
        filters_json: &str,
    ) -> Result<SubscriptionWithStats, SearchnosDBError> {
        let filters_json = filters_json.to_owned();
        tokio::task::spawn_blocking(move || self.subscribe_with_stats(&filters_json)).await?
    }

    pub async fn subscribe_async(
        self: Arc<Self>,
        filters_json: &str,
    ) -> Result<subscription::Subscription, SearchnosDBError> {
        self.subscribe_async_with_stats(filters_json)
            .await
            .map(|result| result.subscription)
    }

    fn effective_limit(&self, provided: Option<usize>) -> Option<usize> {
        let mut limit = provided.or(self.default_limit);
        if let Some(max_limit) = self.max_limit {
            limit = Some(limit.unwrap_or(max_limit).min(max_limit));
        }
        limit
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
            Value::Object(map) => {
                Self::validate_tag_map(map)?;
            }
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

    fn normalize_filter(&self, filter: &Filter) -> Filter {
        let mut normalized = filter.clone();
        normalized.limit = self.effective_limit(filter.limit);
        normalized
    }

    fn normalized_filters(&self, filters: &[Filter]) -> Vec<Filter> {
        filters
            .iter()
            .map(|filter| self.normalize_filter(filter))
            .collect()
    }

    fn normalized_filters_or_default(&self, filters: &[Filter]) -> Vec<Filter> {
        if filters.is_empty() {
            let default_filter = Filter::new();
            let normalized = self.normalize_filter(&default_filter);
            vec![normalized]
        } else {
            self.normalized_filters(filters)
        }
    }

    /// Write stored ndb notes as repeated `(u32 length, payload)` records.
    ///
    /// The length prefix is encoded as big-endian bytes. Payload bytes are the
    /// ndb note bytes stored in the `events` database.
    pub fn dump_events<W: Write>(&self, writer: W) -> Result<u64, SearchnosDBError> {
        self.dump_events_with_progress(writer, |_| {})
    }

    /// Write stored ndb notes and report progress after each record is written.
    ///
    /// `total_events` is the number of events visible in the read transaction
    /// used for this dump.
    pub fn dump_events_with_progress<W, F>(
        &self,
        mut writer: W,
        mut on_progress: F,
    ) -> Result<u64, SearchnosDBError>
    where
        W: Write,
        F: FnMut(DumpProgress),
    {
        self.flush()?;

        let txn = self.begin_ro_txn()?;
        let total_events = Self::database_entry_count(&txn, self.events)?;
        let mut cursor = txn.open_ro_cursor(self.events)?;
        let mut count = 0u64;
        let mut bytes_written = 0u64;

        for (_, payload) in cursor.iter() {
            let len = u32::try_from(payload.len())
                .map_err(|_| SearchnosDBError::EventPayloadTooLarge(payload.len()))?;
            writer.write_all(&len.to_be_bytes())?;
            writer.write_all(payload)?;
            count += 1;
            bytes_written += DUMP_LENGTH_PREFIX_BYTES + u64::from(len);
            on_progress(DumpProgress {
                events_written: count,
                total_events,
                bytes_written,
            });
        }

        Ok(count)
    }

    /// Load ndb notes from repeated `(u32 length, payload)` records.
    ///
    /// The length prefix must be encoded as big-endian bytes. Payload bytes are
    /// decoded as `ndb_note` records and inserted through the normal event
    /// insertion path.
    pub fn load_events<R: Read>(&self, reader: R) -> Result<u64, SearchnosDBError> {
        self.load_events_with_progress(reader, |_| {})
    }

    /// Load ndb notes and report progress after each record is read.
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

        self.flush()?;

        loop {
            let Some(len) = Self::read_dump_record_length(&mut reader)? else {
                break;
            };
            bytes_read += DUMP_LENGTH_PREFIX_BYTES;

            let mut payload = vec![0u8; len as usize];
            reader.read_exact(&mut payload)?;
            bytes_read += u64::from(len);

            let event_json = from_ndb_note(&payload).map_err(SearchnosDBError::DecodeEvent)?;
            self.load_event_json_immediate_owned(event_json)?;
            count += 1;
            on_progress(LoadProgress {
                events_loaded: count,
                bytes_read,
            });
        }

        Ok(count)
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

    fn database_entry_count<T>(txn: &T, database: Database) -> Result<u64, SearchnosDBError>
    where
        T: Transaction,
    {
        let mut stat = unsafe { mem::zeroed::<lmdb_sys::MDB_stat>() };
        let rc = unsafe { lmdb_sys::mdb_stat(txn.txn(), database.dbi(), &mut stat) };
        if rc != 0 {
            return Err(lmdb::Error::from_err_code(rc).into());
        }
        Ok(stat.ms_entries as u64)
    }

    /// Enqueue an event for insertion from borrowed JSON data.
    pub fn insert_event_json(&self, event_json: &str) -> Result<(), SearchnosDBError> {
        self.insert_event_json_owned(event_json.to_owned())
    }

    /// Enqueue an event for insertion, taking ownership of the JSON string.
    pub fn insert_event_json_owned(&self, event_json: String) -> Result<(), SearchnosDBError> {
        let mut batch = self
            .batch
            .lock()
            .map_err(|_| SearchnosDBError::BatchStatePoisoned)?;
        batch.push(self, event_json)
    }

    /// Flush any buffered events to LMDB.
    pub fn flush(&self) -> Result<(), SearchnosDBError> {
        let mut batch = self
            .batch
            .lock()
            .map_err(|_| SearchnosDBError::BatchStatePoisoned)?;
        batch.flush(self)
    }
}

impl Drop for SearchnosDB {
    fn drop(&mut self) {
        if let Ok(mut batch) = self.batch.lock()
            && let Err(err) = batch.flush(self)
        {
            eprintln!("failed to flush pending events on drop: {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use crate::SearchnosDB;
    use crate::db::{
        PurgePolicy, SearchnosDBError, SearchnosDBOptions, StreamItem, Subscription,
        test_support::TestDatabase,
    };
    use crate::ndb_ext::to_ndb_note;
    use crate::nostr::{
        Event, EventDeletionRequest, Filter, JsonUtil, Kind, Metadata, PublicKey, Timestamp,
        test_utils::{EventBuilder, Keys},
    };

    fn expiration_tag(timestamp: Timestamp) -> Vec<String> {
        vec!["expiration".to_string(), timestamp.as_u64().to_string()]
    }

    const SAMPLE_ID: &str = "1726bf37195e345ddc4bba9560d9499c918a544afbf72057a595d68fbe908ee5";
    const SAMPLE_PUBKEY: &str = "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

    #[test]
    fn parse_filters_accepts_single_character_tag_queries() {
        let json = "[{\"#t\":[\"hello\"]}]";
        let parsed = SearchnosDB::parse_filters_json(json);
        assert!(
            parsed.is_ok(),
            "expected single-character tag query to pass"
        );
    }

    #[test]
    fn parse_filters_rejects_multi_character_tag_queries() {
        let json = "[{\"#ab\":[\"hello\"]}]";
        let err = SearchnosDB::parse_filters_json(json)
            .expect_err("expected multi-character tag query to fail");
        match err {
            SearchnosDBError::InvalidTagQuery { tag } => {
                assert_eq!(tag, "#ab");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn parse_filters_accepts_valid_hex_fields() {
        let json = format!(
            "[{{\"ids\":[\"{}\"],\"authors\":[\"{}\"]}}]",
            SAMPLE_ID, SAMPLE_PUBKEY
        );
        let parsed = SearchnosDB::parse_filters_json(&json);
        assert!(parsed.is_ok(), "expected valid hex fields to pass");
    }

    #[test]
    fn parse_filters_rejects_invalid_hex_length() {
        let json = "[{\"ids\":[\"short\"],\"authors\":[]}]";
        let err =
            SearchnosDB::parse_filters_json(json).expect_err("expected invalid hex length to fail");
        assert!(
            matches!(err, SearchnosDBError::ParseFilters(_)),
            "unexpected error variant: {err:?}"
        );
    }

    #[test]
    fn parse_filters_rejects_invalid_hex_character() {
        let invalid = "z".repeat(64);
        let json = format!("[{{\"ids\":[],\"authors\":[\"{invalid}\"]}}]");
        let err = SearchnosDB::parse_filters_json(&json)
            .expect_err("expected invalid hex character to fail");
        assert!(
            matches!(err, SearchnosDBError::ParseFilters(_)),
            "unexpected error variant: {err:?}"
        );
    }

    #[test]
    fn insert_event_persists_storage_and_required_indexes() {
        let db = TestDatabase::new();
        let keys = Keys::generate();
        let event = EventBuilder::text_note("hello searchnos")
            .sign_with_keys(&keys)
            .expect("failed to build event");
        let seq = db.insert(&event);

        assert_eq!(seq, 0);

        let txn = db.ro_txn();
        db.assert_event_stored(&txn, seq, &event);
    }

    #[test]
    fn dump_events_writes_length_prefixed_ndb_notes() {
        let db = TestDatabase::new();
        let keys = Keys::generate();
        let first = EventBuilder::text_note("first dumped note")
            .sign_with_keys(&keys)
            .expect("failed to build first event");
        let second = EventBuilder::text_note("second dumped note")
            .sign_with_keys(&keys)
            .expect("failed to build second event");

        db.insert(&first);
        db.insert(&second);

        let plain_dumped = db.dump_events();
        let (dumped, progress) = db.dump_events_with_progress();
        assert_eq!(plain_dumped, dumped);

        let mut remaining = dumped.as_slice();
        let mut dumped_events = Vec::new();

        while !remaining.is_empty() {
            let (len_bytes, rest) = remaining
                .split_first_chunk::<4>()
                .expect("dump record missing length");
            let len = u32::from_be_bytes(*len_bytes) as usize;
            assert!(
                rest.len() >= len,
                "dump record payload shorter than length prefix"
            );
            let (payload, rest) = rest.split_at(len);
            let event_json = crate::ndb_ext::from_ndb_note(payload).expect("invalid ndb note");
            dumped_events.push(
                Event::from_json(&event_json).expect("dumped ndb note contained invalid event"),
            );
            remaining = rest;
        }

        assert_eq!(dumped_events.len(), 2);
        assert_eq!(dumped_events[0].id, first.id);
        assert_eq!(dumped_events[1].id, second.id);
        assert_eq!(progress.len(), 2);
        assert_eq!(progress[0].events_written, 1);
        assert_eq!(progress[0].total_events, 2);
        assert_eq!(progress[1].events_written, 2);
        assert_eq!(progress[1].total_events, 2);
        assert_eq!(progress[1].bytes_written as usize, dumped.len());
    }

    #[test]
    fn load_events_reads_length_prefixed_ndb_notes() {
        let source = TestDatabase::new();
        let keys = Keys::generate();
        let first = EventBuilder::text_note("first loaded note")
            .sign_with_keys(&keys)
            .expect("failed to build first event");
        let second = EventBuilder::text_note("second loaded note")
            .sign_with_keys(&keys)
            .expect("failed to build second event");

        source.insert(&first);
        source.insert(&second);
        let dumped = source.dump_events();

        let destination = TestDatabase::new();
        let progress = destination.load_events_with_progress(&dumped);

        let loaded_events = destination.query(&[Filter::new().search("loaded")]);
        assert_eq!(loaded_events.len(), 2);
        assert_eq!(progress.len(), 2);
        assert_eq!(progress[0].events_loaded, 1);
        assert_eq!(progress[1].events_loaded, 2);
        assert_eq!(progress[1].bytes_read as usize, dumped.len());
    }

    #[test]
    fn load_events_stores_invalid_a_tag_deletions_without_applying_them() {
        let keys = Keys::generate();
        let other_keys = Keys::generate();
        let valid = EventBuilder::text_note("valid loaded note")
            .sign_with_keys(&keys)
            .expect("failed to build valid event");
        let invalid_deletion = EventBuilder::new(Kind::EventDeletion, "invalid")
            .tag(addressable_tag(
                Kind::LongFormTextNote,
                other_keys.public_key(),
                "slot",
            ))
            .custom_created_at(Timestamp::from_secs(6_000))
            .sign_with_keys(&keys)
            .expect("failed to build invalid deletion event");
        assert!(matches!(
            TestDatabase::new().insert_error(&invalid_deletion),
            SearchnosDBError::InvalidDeletionATag(_)
        ));

        let mut dumped = Vec::new();
        append_dump_record(&mut dumped, &valid);
        append_dump_record(&mut dumped, &invalid_deletion);

        let destination = TestDatabase::new();
        let progress = destination.load_events_with_progress(&dumped);

        let loaded_events = destination.query(&[Filter::new().search("loaded")]);
        assert_eq!(loaded_events, vec![valid.as_json()]);
        let loaded_deletions = destination.query(&[Filter::new().id(invalid_deletion.id)]);
        assert_eq!(loaded_deletions, vec![invalid_deletion.as_json()]);
        assert_eq!(progress.len(), 2);
        assert_eq!(progress[0].events_loaded, 1);
        assert_eq!(progress[1].events_loaded, 2);
        assert_eq!(progress[1].bytes_read as usize, dumped.len());
    }

    #[test]
    fn expired_event_is_dropped_on_insert() {
        let db = TestDatabase::new();
        let keys = Keys::generate();
        let event = EventBuilder::text_note("expired message")
            .tag(expiration_tag(Timestamp::from(1)))
            .sign_with_keys(&keys)
            .expect("failed to build event");

        let result = db.insert_allow_drop(&event);
        assert!(result.is_none(), "expired event should be dropped");

        let events = db.query(&[Filter::new().id(event.id)]);
        assert!(events.is_empty(), "expired event should not be stored");
    }

    #[test]
    fn query_skips_events_past_expiration() {
        let db = TestDatabase::new();
        let keys = Keys::generate();
        let expiration = Timestamp::from(Timestamp::now().as_u64().saturating_add(1));
        let event = EventBuilder::text_note("short lived")
            .tag(expiration_tag(expiration))
            .sign_with_keys(&keys)
            .expect("failed to build event");

        let seq = db.insert(&event);
        assert_eq!(seq, 0);

        thread::sleep(Duration::from_secs(2));

        let events = db.query(&[Filter::new().id(event.id)]);
        assert!(
            events.is_empty(),
            "expired event should be filtered from queries"
        );
    }

    #[test]
    fn ephemeral_event_is_not_persisted() {
        let db = TestDatabase::new();
        let keys = Keys::generate();
        let filters = vec![Filter::new().author(keys.public_key())];
        let mut subscription = db.subscribe(&filters);

        match subscription.try_next() {
            Some(StreamItem::Eose) => {}
            other => panic!("expected initial EOSE, got {:?}", other),
        }

        let event = EventBuilder::new(Kind::from_u16(20_000), "transient")
            .tag(expiration_tag(Timestamp::from(1)))
            .sign_with_keys(&keys)
            .expect("failed to build event");

        let result = db.insert_allow_drop(&event);
        assert!(result.is_none(), "ephemeral events should not persist");

        match subscription.try_next() {
            Some(StreamItem::Event(json)) => assert_eq!(json, event.as_json()),
            other => panic!("expected live ephemeral event, got {:?}", other),
        }

        let events = db.query(&[Filter::new().id(event.id)]);
        assert!(events.is_empty(), "ephemeral events should not be stored");
    }

    #[test]
    fn purge_policy_drops_events_on_insert() {
        let options = SearchnosDBOptions {
            purge_policy: Some(PurgePolicy::from_specs(["1:purge"]).unwrap()),
            ..Default::default()
        };

        let db = TestDatabase::with_options(options);
        let keys = Keys::generate();

        let text_note = EventBuilder::text_note("discard me")
            .sign_with_keys(&keys)
            .expect("failed to build event");
        let metadata = EventBuilder::new(Kind::Metadata, "{}")
            .sign_with_keys(&keys)
            .expect("failed to build metadata");

        let dropped = db.insert_allow_drop(&text_note);
        assert!(dropped.is_none(), "purge kinds must be dropped");

        let kept_seq = db.insert(&metadata);
        assert_eq!(kept_seq, 0);

        let events = db.query(&[Filter::new().id(text_note.id)]);
        assert!(events.is_empty(), "purged event should not be stored");
    }

    #[test]
    fn delete_event_removes_storage_and_required_indexes() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let original_event = EventBuilder::text_note("note to delete")
            .sign_with_keys(&keys)
            .expect("failed to build event");
        let original_seq = db.insert(&original_event);
        assert_eq!(original_seq, 0);

        let deletion_event =
            EventBuilder::delete(EventDeletionRequest::new().id(original_event.id))
                .sign_with_keys(&keys)
                .expect("failed to build deletion event");
        let deletion_seq = db.insert(&deletion_event);
        assert_eq!(deletion_seq, 1);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, original_seq, &original_event);
        db.assert_deletion_marker_eq(
            &txn,
            &original_event.id,
            &original_event.pubkey,
            deletion_seq,
        );
        db.assert_event_stored(&txn, deletion_seq, &deletion_event);
    }

    #[test]
    fn delete_event_from_other_author_preserves_original_event() {
        let db = TestDatabase::new();
        let author_keys = Keys::generate();
        let other_keys = Keys::generate();

        let original_event = EventBuilder::text_note("note to keep")
            .sign_with_keys(&author_keys)
            .expect("failed to build event");
        let original_seq = db.insert(&original_event);
        assert_eq!(original_seq, 0);

        let deletion_event =
            EventBuilder::delete(EventDeletionRequest::new().id(original_event.id))
                .sign_with_keys(&other_keys)
                .expect("failed to build deletion event");
        let deletion_seq = db.insert(&deletion_event);
        assert_eq!(deletion_seq, 1);

        let txn = db.ro_txn();
        db.assert_event_stored(&txn, original_seq, &original_event);
        db.assert_no_deletion_marker(&txn, &original_event.id, &author_keys.public_key());
        db.assert_deletion_marker_eq(
            &txn,
            &original_event.id,
            &other_keys.public_key(),
            deletion_seq,
        );
        db.assert_event_stored(&txn, deletion_seq, &deletion_event);
    }

    #[test]
    fn deletion_before_event_blocks_late_insert() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let original_event = EventBuilder::text_note("note arrives late")
            .sign_with_keys(&keys)
            .expect("failed to build event");
        let deletion_event =
            EventBuilder::delete(EventDeletionRequest::new().id(original_event.id))
                .sign_with_keys(&keys)
                .expect("failed to build deletion event");

        let deletion_seq = db.insert(&deletion_event);
        assert_eq!(deletion_seq, 0);

        let seq_after_late_insert = db.insert(&original_event);
        assert_eq!(seq_after_late_insert, deletion_seq);

        let txn = db.ro_txn();
        db.assert_event_absent(&txn, &original_event);
        db.assert_deletion_marker_eq(&txn, &original_event.id, &keys.public_key(), deletion_seq);
        db.assert_event_stored(&txn, deletion_seq, &deletion_event);
    }

    #[test]
    fn deletion_from_other_author_does_not_block_late_insert() {
        let db = TestDatabase::new();
        let original_keys = Keys::generate();
        let deleter_keys = Keys::generate();

        let original_event = EventBuilder::text_note("late note from author")
            .sign_with_keys(&original_keys)
            .expect("failed to build event");
        let deletion_event =
            EventBuilder::delete(EventDeletionRequest::new().id(original_event.id))
                .sign_with_keys(&deleter_keys)
                .expect("failed to build deletion event");

        let deletion_seq = db.insert(&deletion_event);
        assert_eq!(deletion_seq, 0);

        let seq_after_late_insert = db.insert(&original_event);
        assert_eq!(seq_after_late_insert, 1);

        let txn = db.ro_txn();
        db.assert_event_stored(&txn, seq_after_late_insert, &original_event);
        db.assert_no_deletion_marker(&txn, &original_event.id, &original_keys.public_key());
        db.assert_deletion_marker_eq(
            &txn,
            &original_event.id,
            &deleter_keys.public_key(),
            deletion_seq,
        );
        db.assert_event_stored(&txn, deletion_seq, &deletion_event);
    }

    #[test]
    fn deletion_with_multiple_e_tags_removes_all_targets() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let first_event = EventBuilder::text_note("first note")
            .sign_with_keys(&keys)
            .expect("failed to build first event");
        let first_seq = db.insert(&first_event);
        assert_eq!(first_seq, 0);

        let second_event = EventBuilder::text_note("second note")
            .sign_with_keys(&keys)
            .expect("failed to build second event");
        let second_seq = db.insert(&second_event);
        assert_eq!(second_seq, 1);

        let deletion_request = EventDeletionRequest::new().ids([first_event.id, second_event.id]);
        let deletion_event = EventBuilder::delete(deletion_request)
            .sign_with_keys(&keys)
            .expect("failed to build deletion event");
        let deletion_seq = db.insert(&deletion_event);
        assert_eq!(deletion_seq, 2);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, first_seq, &first_event);
        db.assert_event_removed(&txn, second_seq, &second_event);
        db.assert_deletion_marker_eq(&txn, &first_event.id, &first_event.pubkey, deletion_seq);
        db.assert_deletion_marker_eq(&txn, &second_event.id, &second_event.pubkey, deletion_seq);
        db.assert_event_stored(&txn, deletion_seq, &deletion_event);
    }

    #[test]
    fn deletion_with_mixed_pubkeys_only_removes_matching_author() {
        let db = TestDatabase::new();
        let author_keys = Keys::generate();
        let other_keys = Keys::generate();

        let author_event = EventBuilder::text_note("author note")
            .sign_with_keys(&author_keys)
            .expect("failed to build author event");
        let author_seq = db.insert(&author_event);
        assert_eq!(author_seq, 0);

        let other_event = EventBuilder::text_note("other note")
            .sign_with_keys(&other_keys)
            .expect("failed to build other event");
        let other_seq = db.insert(&other_event);
        assert_eq!(other_seq, 1);

        let deletion_request = EventDeletionRequest::new().ids([author_event.id, other_event.id]);
        let deletion_event = EventBuilder::delete(deletion_request)
            .sign_with_keys(&author_keys)
            .expect("failed to build deletion event");
        let deletion_seq = db.insert(&deletion_event);
        assert_eq!(deletion_seq, 2);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, author_seq, &author_event);
        db.assert_event_stored(&txn, other_seq, &other_event);
        db.assert_deletion_marker_eq(&txn, &author_event.id, &author_event.pubkey, deletion_seq);
        db.assert_no_deletion_marker(&txn, &other_event.id, &other_event.pubkey);
        db.assert_event_stored(&txn, deletion_seq, &deletion_event);
    }

    #[test]
    fn replaceable_event_overwrites_previous_version() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let first_metadata = Metadata::new().display_name("first");
        let first_event = EventBuilder::metadata(&first_metadata)
            .custom_created_at(Timestamp::from_secs(1))
            .sign_with_keys(&keys)
            .expect("failed to build first metadata event");
        let first_seq = db.insert(&first_event);
        assert_eq!(first_seq, 0);

        let txn_after_first = db.ro_txn();
        db.assert_event_stored(&txn_after_first, first_seq, &first_event);
        drop(txn_after_first);

        let second_metadata = Metadata::new().display_name("second");
        let second_event = EventBuilder::metadata(&second_metadata)
            .custom_created_at(Timestamp::from_secs(2))
            .sign_with_keys(&keys)
            .expect("failed to build second metadata event");
        let second_seq = db.insert(&second_event);
        assert_eq!(second_seq, 1);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, first_seq, &first_event);
        db.assert_event_stored(&txn, second_seq, &second_event);
    }

    #[test]
    fn replaceable_events_are_scoped_by_pubkey() {
        let db = TestDatabase::new();
        let author_keys = Keys::generate();
        let other_keys = Keys::generate();

        let author_metadata = Metadata::new().display_name("author");
        let author_event = EventBuilder::metadata(&author_metadata)
            .custom_created_at(Timestamp::from_secs(5))
            .sign_with_keys(&author_keys)
            .expect("failed to build author metadata event");
        let author_seq = db.insert(&author_event);
        assert_eq!(author_seq, 0);

        let other_metadata = Metadata::new().display_name("other");
        let other_event = EventBuilder::metadata(&other_metadata)
            .custom_created_at(Timestamp::from_secs(6))
            .sign_with_keys(&other_keys)
            .expect("failed to build other metadata event");
        let other_seq = db.insert(&other_event);
        assert_eq!(other_seq, 1);

        let txn = db.ro_txn();
        db.assert_event_stored(&txn, author_seq, &author_event);
        db.assert_event_stored(&txn, other_seq, &other_event);
    }

    #[test]
    fn addressable_event_overwrites_same_slot() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let first_event = TestDatabase::build_addressable_event(&keys, "slot", 10, "first");
        let first_seq = db.insert(&first_event);
        assert_eq!(first_seq, 0);

        let second_event = TestDatabase::build_addressable_event(&keys, "slot", 20, "second");
        let second_seq = db.insert(&second_event);
        assert_eq!(second_seq, 1);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, first_seq, &first_event);
        db.assert_event_stored(&txn, second_seq, &second_event);
    }

    #[test]
    fn addressable_events_are_scoped_by_slot() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let first_event = TestDatabase::build_addressable_event(&keys, "slot-a", 10, "first");
        let first_seq = db.insert(&first_event);
        assert_eq!(first_seq, 0);

        let second_event = TestDatabase::build_addressable_event(&keys, "slot-b", 20, "second");
        let second_seq = db.insert(&second_event);
        assert_eq!(second_seq, 1);

        let txn = db.ro_txn();
        db.assert_event_stored(&txn, first_seq, &first_event);
        db.assert_event_stored(&txn, second_seq, &second_event);
    }

    #[test]
    fn addressable_events_are_scoped_by_pubkey() {
        let db = TestDatabase::new();
        let author_keys = Keys::generate();
        let other_keys = Keys::generate();

        let author_event =
            TestDatabase::build_addressable_event(&author_keys, "slot", 10, "author");
        let author_seq = db.insert(&author_event);
        assert_eq!(author_seq, 0);

        let other_event = TestDatabase::build_addressable_event(&other_keys, "slot", 20, "other");
        let other_seq = db.insert(&other_event);
        assert_eq!(other_seq, 1);

        let txn = db.ro_txn();
        db.assert_event_stored(&txn, author_seq, &author_event);
        db.assert_event_stored(&txn, other_seq, &other_event);
    }

    #[test]
    fn deletion_by_a_tag_removes_addressable_event_and_blocks_late_insert() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let original = TestDatabase::build_addressable_event(&keys, "slot", 5_000, "original");
        let original_seq = db.insert(&original);
        assert_eq!(original_seq, 0);

        let deletion = EventBuilder::new(Kind::EventDeletion, "delete addressable")
            .tag(addressable_tag(
                Kind::LongFormTextNote,
                keys.public_key(),
                "slot",
            ))
            .custom_created_at(Timestamp::from_secs(6_000))
            .sign_with_keys(&keys)
            .expect("failed to build a-tag deletion");
        let deletion_seq = db.insert(&deletion);
        assert_eq!(deletion_seq, 1);

        let late = TestDatabase::build_addressable_event(&keys, "slot", 5_500, "late");
        assert_eq!(db.insert(&late), deletion_seq);

        let newer = TestDatabase::build_addressable_event(&keys, "slot", 6_500, "newer");
        let newer_seq = db.insert(&newer);
        assert_eq!(newer_seq, 2);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, original_seq, &original);
        db.assert_event_absent(&txn, &late);
        db.assert_event_stored(&txn, deletion_seq, &deletion);
        db.assert_event_stored(&txn, newer_seq, &newer);
    }

    #[test]
    fn deletion_by_noncanonical_a_tag_blocks_late_addressable_insert() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let original = TestDatabase::build_addressable_event(&keys, "slot", 5_000, "original");
        let original_seq = db.insert(&original);
        assert_eq!(original_seq, 0);

        let deletion = EventBuilder::new(Kind::EventDeletion, "delete addressable")
            .tag(vec![
                "a".to_string(),
                format!(
                    "030023:{}:slot",
                    keys.public_key().to_hex().to_ascii_uppercase()
                ),
            ])
            .custom_created_at(Timestamp::from_secs(6_000))
            .sign_with_keys(&keys)
            .expect("failed to build a-tag deletion");
        let deletion_seq = db.insert(&deletion);
        assert_eq!(deletion_seq, 1);

        let late = TestDatabase::build_addressable_event(&keys, "slot", 5_500, "late");
        assert_eq!(db.insert(&late), deletion_seq);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, original_seq, &original);
        db.assert_event_absent(&txn, &late);
        db.assert_event_stored(&txn, deletion_seq, &deletion);
    }

    #[test]
    fn deleting_a_tag_deletion_removes_replace_deletion_marker() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let original = TestDatabase::build_addressable_event(&keys, "slot", 5_000, "original");
        let original_seq = db.insert(&original);
        assert_eq!(original_seq, 0);

        let deletion = EventBuilder::new(Kind::EventDeletion, "delete addressable")
            .tag(addressable_tag(
                Kind::LongFormTextNote,
                keys.public_key(),
                "slot",
            ))
            .custom_created_at(Timestamp::from_secs(6_000))
            .sign_with_keys(&keys)
            .expect("failed to build a-tag deletion");
        let deletion_seq = db.insert(&deletion);
        assert_eq!(deletion_seq, 1);
        assert_eq!(db.insert(&original), deletion_seq);

        let deletion_removal = EventBuilder::delete(EventDeletionRequest::new().id(deletion.id))
            .custom_created_at(Timestamp::from_secs(7_000))
            .sign_with_keys(&keys)
            .expect("failed to build deletion removal");
        let deletion_removal_seq = db.insert(&deletion_removal);
        assert_eq!(deletion_removal_seq, 2);

        let readded_seq = db.insert(&original);
        assert_eq!(readded_seq, 3);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, deletion_seq, &deletion);
        db.assert_event_stored(&txn, deletion_removal_seq, &deletion_removal);
        db.assert_event_stored(&txn, readded_seq, &original);
    }

    #[test]
    fn deletion_by_a_tag_rejects_other_pubkey() {
        let db = TestDatabase::new();
        let author_keys = Keys::generate();
        let other_keys = Keys::generate();

        let deletion = EventBuilder::new(Kind::EventDeletion, "invalid")
            .tag(addressable_tag(
                Kind::LongFormTextNote,
                author_keys.public_key(),
                "slot",
            ))
            .custom_created_at(Timestamp::from_secs(6_000))
            .sign_with_keys(&other_keys)
            .expect("failed to build invalid a-tag deletion");

        assert!(matches!(
            db.insert_error(&deletion),
            SearchnosDBError::InvalidDeletionATag(_)
        ));
    }

    #[test]
    fn subscribe_streams_events() {
        let db = TestDatabase::new();
        let keys = Keys::generate();
        let first = EventBuilder::text_note("first")
            .sign_with_keys(&keys)
            .expect("failed to build event");
        let second = EventBuilder::text_note("second")
            .sign_with_keys(&keys)
            .expect("failed to build event");

        db.insert(&first);

        let filters = vec![Filter::new().author(keys.public_key())];
        let mut subscription = db.subscribe(&filters);

        match subscription.try_next() {
            Some(StreamItem::Event(event)) => assert_eq!(event, first.as_json()),
            other => panic!("expected snapshot event, got {:?}", other),
        }
        match subscription.try_next() {
            Some(StreamItem::Eose) => {}
            other => panic!("expected EOSE, got {:?}", other),
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("failed to build runtime");

        db.insert(&second);

        let received = rt.block_on(async { subscription.next().await });
        assert_eq!(received, Some(StreamItem::Event(second.as_json())));
    }

    fn addressable_tag(kind: Kind, pubkey: PublicKey, d_tag: &str) -> Vec<String> {
        vec![
            "a".to_string(),
            format!("{}:{}:{d_tag}", kind.as_u32(), pubkey.to_hex()),
        ]
    }

    fn append_dump_record(out: &mut Vec<u8>, event: &Event) {
        let payload = to_ndb_note(&event.as_json()).expect("failed to encode ndb note");
        let len = u32::try_from(payload.len()).expect("payload too large");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&payload);
    }

    /// Helper function for polling subscription with timeout
    fn wait_for_event(subscription: &mut Subscription, timeout_ms: u64) -> Result<String, String> {
        let iterations = timeout_ms / 10;
        for _ in 0..iterations {
            if let Some(item) = subscription.try_next() {
                match item {
                    StreamItem::Event(json) => return Ok(json),
                    other => return Err(format!("unexpected stream item: {:?}", other)),
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        Err("timed out waiting for matching event".to_string())
    }

    #[test]
    fn subscribe_limit_zero_streams_future_events() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let snapshot_event = EventBuilder::text_note("snapshot")
            .sign_with_keys(&keys)
            .expect("failed to build snapshot event");
        db.insert(&snapshot_event);

        let filters = vec![Filter::new().limit(0)];
        let mut subscription = db.subscribe(&filters);

        match subscription.try_next() {
            Some(StreamItem::Eose) => {}
            other => panic!("expected EOSE, got {:?}", other),
        }

        match subscription.try_next() {
            None => {}
            Some(item) => panic!("unexpected snapshot item: {:?}", item),
        }

        let future_event = EventBuilder::text_note("future")
            .sign_with_keys(&keys)
            .expect("failed to build future event");
        db.insert(&future_event);

        match wait_for_event(&mut subscription, 1000) {
            Ok(json) => assert_eq!(json, future_event.as_json()),
            Err(err) => panic!("{}", err),
        }
    }

    /// Test helper for search queries after EOSE
    fn test_search_after_eose(search_query: &str, content: &str) {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let filters = vec![Filter::new().search(search_query)];
        let mut subscription = db.subscribe(&filters);

        match subscription.try_next() {
            Some(StreamItem::Eose) => {}
            other => panic!("expected EOSE, got {:?}", other),
        }

        let note = EventBuilder::text_note(content)
            .sign_with_keys(&keys)
            .expect("failed to build event");

        db.insert(&note);

        match wait_for_event(&mut subscription, 1000) {
            Ok(json) => assert_eq!(json, note.as_json()),
            Err(err) => panic!("{}", err),
        }
    }

    #[test]
    fn subscribe_search_matches_ascii_exclamation_after_eose() {
        test_search_after_eose("!", "wow!");
    }

    #[test]
    fn subscribe_search_matches_fullwidth_exclamation_after_eose() {
        test_search_after_eose("！", "wow！");
    }

    #[test]
    fn subscribe_search_normalizes_fullwidth_characters() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let filters = vec![Filter::new().search("！")];
        let mut subscription = db.subscribe(&filters);

        match subscription.try_next() {
            Some(StreamItem::Eose) => {}
            other => panic!("expected EOSE, got {:?}", other),
        }

        // Insert event with fullwidth exclamation
        let fullwidth_event = EventBuilder::text_note("こんにちは！世界")
            .sign_with_keys(&keys)
            .expect("failed to build fullwidth event");
        db.insert(&fullwidth_event);

        // Insert event with halfwidth exclamation
        let halfwidth_event = EventBuilder::text_note("hello! world")
            .sign_with_keys(&keys)
            .expect("failed to build halfwidth event");
        db.insert(&halfwidth_event);

        // Both events should match the fullwidth search query
        match wait_for_event(&mut subscription, 1000) {
            Ok(json) => {
                assert!(
                    json == fullwidth_event.as_json() || json == halfwidth_event.as_json(),
                    "first event should be one of the inserted events"
                );
            }
            Err(err) => panic!("{}", err),
        }

        match wait_for_event(&mut subscription, 1000) {
            Ok(json) => {
                assert!(
                    json == fullwidth_event.as_json() || json == halfwidth_event.as_json(),
                    "second event should be one of the inserted events"
                );
            }
            Err(err) => panic!("{}", err),
        }
    }

    #[test]
    fn subscribe_search_treats_space_as_and_condition() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let filters = vec![Filter::new().search("rust programming")];
        let mut subscription = db.subscribe(&filters);

        match subscription.try_next() {
            Some(StreamItem::Eose) => {}
            other => panic!("expected EOSE, got {:?}", other),
        }

        // Insert event with both terms
        let both_terms = EventBuilder::text_note("rust programming language")
            .sign_with_keys(&keys)
            .expect("failed to build event with both terms");
        db.insert(&both_terms);

        // Insert event with only one term
        let one_term = EventBuilder::text_note("rust is awesome")
            .sign_with_keys(&keys)
            .expect("failed to build event with one term");
        db.insert(&one_term);

        // Only the event with both terms should match
        match wait_for_event(&mut subscription, 1000) {
            Ok(json) => {
                assert_eq!(
                    json,
                    both_terms.as_json(),
                    "only event with both terms should match"
                );
            }
            Err(err) => panic!("{}", err),
        }

        // No more events should arrive
        thread::sleep(Duration::from_millis(100));
        match subscription.try_next() {
            None => {} // Expected
            Some(item) => panic!("unexpected item: {:?}", item),
        }
    }

    #[test]
    fn subscribe_search_normalizes_and_applies_and_condition() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        // Search for "hello world" with space (AND condition)
        let filters = vec![Filter::new().search("hello world")];
        let mut subscription = db.subscribe(&filters);

        match subscription.try_next() {
            Some(StreamItem::Eose) => {}
            other => panic!("expected EOSE, got {:?}", other),
        }

        // Insert event with both terms
        let both = EventBuilder::text_note("hello world everyone")
            .sign_with_keys(&keys)
            .expect("failed to build event with both terms");
        db.insert(&both);

        // Insert event with only one term
        let partial = EventBuilder::text_note("hello everyone")
            .sign_with_keys(&keys)
            .expect("failed to build partial event");
        db.insert(&partial);

        // Only event with both terms should match
        match wait_for_event(&mut subscription, 1000) {
            Ok(json) => {
                assert_eq!(
                    json,
                    both.as_json(),
                    "only event with both terms should match"
                );
            }
            Err(err) => panic!("{}", err),
        }

        // No more events should arrive
        thread::sleep(Duration::from_millis(100));
        match subscription.try_next() {
            None => {} // Expected
            Some(item) => panic!("unexpected item: {:?}", item),
        }
    }

    #[test]
    fn query_search_normalizes_fullwidth_characters() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        // Insert events with fullwidth and halfwidth characters
        let fullwidth_event = EventBuilder::text_note("こんにちは！世界")
            .sign_with_keys(&keys)
            .expect("failed to build fullwidth event");
        let halfwidth_event = EventBuilder::text_note("hello! world")
            .sign_with_keys(&keys)
            .expect("failed to build halfwidth event");

        db.insert(&fullwidth_event);
        db.insert(&halfwidth_event);

        // Query with fullwidth exclamation should match both (normalized to halfwidth)
        let results = db.query(&[Filter::new().search("！")]);
        assert_eq!(
            results.len(),
            2,
            "both events should match normalized search"
        );

        // Query with halfwidth exclamation should also match both
        let results = db.query(&[Filter::new().search("!")]);
        assert_eq!(
            results.len(),
            2,
            "both events should match halfwidth search"
        );
    }

    #[test]
    fn query_search_treats_space_as_and_condition() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let first = EventBuilder::text_note("rust programming language")
            .sign_with_keys(&keys)
            .expect("failed to build first event");
        let second = EventBuilder::text_note("rust is awesome")
            .sign_with_keys(&keys)
            .expect("failed to build second event");
        let third = EventBuilder::text_note("programming in python")
            .sign_with_keys(&keys)
            .expect("failed to build third event");

        db.insert(&first);
        db.insert(&second);
        db.insert(&third);

        // Search with space should require both terms
        let results = db.query(&[Filter::new().search("rust programming")]);
        assert_eq!(results.len(), 1, "only one event contains both terms");

        let event = &results[0];
        assert!(
            event.contains("rust") && event.contains("programming"),
            "result should contain both search terms"
        );
    }

    #[test]
    fn query_search_matches_non_adjacent_terms() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let target = EventBuilder::text_note("foo bar baz")
            .sign_with_keys(&keys)
            .expect("failed to build target event");
        let non_matching = EventBuilder::text_note("foo only")
            .sign_with_keys(&keys)
            .expect("failed to build non-matching event");

        db.insert(&target);
        db.insert(&non_matching);

        // Search for non-adjacent terms should match if both present
        let results = db.query(&[Filter::new().search("foo baz")]);
        assert_eq!(
            results.len(),
            1,
            "should match event containing both terms even if not adjacent"
        );

        let result_id: String = serde_json::from_str::<serde_json::Value>(&results[0])
            .ok()
            .and_then(|v| v["id"].as_str().map(String::from))
            .expect("failed to parse result");

        assert_eq!(
            result_id,
            target.id.to_string(),
            "should match the target event with both terms"
        );
    }

    #[test]
    fn query_search_normalizes_and_applies_and_condition() {
        let db = TestDatabase::new();
        let keys = Keys::generate();

        let fullwidth = EventBuilder::text_note("Ｒｕｓｔ　プログラミング！")
            .sign_with_keys(&keys)
            .expect("failed to build fullwidth event");
        let halfwidth = EventBuilder::text_note("rust programming!")
            .sign_with_keys(&keys)
            .expect("failed to build halfwidth event");
        let partial = EventBuilder::text_note("rust only")
            .sign_with_keys(&keys)
            .expect("failed to build partial event");

        db.insert(&fullwidth);
        db.insert(&halfwidth);
        db.insert(&partial);

        // Search with fullwidth characters and space (AND condition)
        let results = db.query(&[Filter::new().search("Ｒｕｓｔ　！")]);
        assert_eq!(
            results.len(),
            2,
            "both fullwidth and halfwidth events should match after normalization"
        );

        // Verify that the partial match doesn't appear in results
        let result_ids: Vec<String> = results
            .iter()
            .filter_map(|json| {
                serde_json::from_str::<serde_json::Value>(json)
                    .ok()
                    .and_then(|v| v["id"].as_str().map(String::from))
            })
            .collect();

        assert!(
            !result_ids.contains(&partial.id.to_string()),
            "partial match should not be included"
        );
        assert!(
            result_ids.contains(&fullwidth.id.to_string()),
            "fullwidth event should be included"
        );
        assert!(
            result_ids.contains(&halfwidth.id.to_string()),
            "halfwidth event should be included"
        );
    }
}
