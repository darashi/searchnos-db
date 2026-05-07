use std::convert::TryInto;

use crate::text::{MAX_NGRAM_SIZE, MIN_NGRAM_SIZE, char_ngrams, extract_text, normalize_text};

use super::{
    QueryResult, SEQ_BYTES, SearchnosDB, SearchnosDBOptions, Subscription, write::InsertResult,
};

use crate::ndb_ext::from_ndb_note;
use crate::nostr::{
    Event, EventId, Filter, JsonUtil, Kind, PublicKey, TagExt, TagKind, Timestamp,
    test_utils::{EventBuilder, Keys},
};
use lmdb::{Cursor, RoTransaction, Transaction};
use serde_json::to_string as to_json_string;

pub(crate) struct TestDatabase {
    _dir: tempfile::TempDir,
    db: SearchnosDB,
}

impl TestDatabase {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().expect("failed to create tempdir");
        let db = SearchnosDB::open(dir.path()).expect("failed to open db");
        Self { _dir: dir, db }
    }

    pub(crate) fn with_options(options: SearchnosDBOptions) -> Self {
        let dir = tempfile::tempdir().expect("failed to create tempdir");
        let db = SearchnosDB::open_with_options(dir.path(), options).expect("failed to open db");
        Self { _dir: dir, db }
    }

    pub(crate) fn insert(&self, event: &Event) -> u64 {
        let mut txn = self.db.begin_rw_txn().expect("failed to begin transaction");
        let seq = match self
            .db
            .insert(&mut txn, &event.as_json())
            .expect("insert failed")
        {
            InsertResult::Inserted(seq) | InsertResult::AlreadyExists(seq) => seq,
            InsertResult::Dropped => panic!("event unexpectedly dropped"),
        };
        txn.commit().expect("commit failed");
        seq
    }

    pub(crate) fn insert_allow_drop(&self, event: &Event) -> Option<u64> {
        let mut txn = self.db.begin_rw_txn().expect("failed to begin transaction");
        let result = self
            .db
            .insert(&mut txn, &event.as_json())
            .expect("insert failed");
        txn.commit().expect("commit failed");
        match result {
            InsertResult::Inserted(seq) | InsertResult::AlreadyExists(seq) => Some(seq),
            InsertResult::Dropped => None,
        }
    }

    pub(crate) fn purge_with_time(&self, limit: usize, now: u64) -> usize {
        self.db
            .purge_internal(limit, now, None, None)
            .expect("purge failed")
    }

    pub(crate) fn query(&self, filters: &[Filter]) -> Vec<String> {
        let json = to_json_string(filters).expect("failed to encode filters");
        self.db.query(&json).expect("query failed")
    }

    pub(crate) fn query_with_stats(&self, filters: &[Filter]) -> QueryResult {
        let json = to_json_string(filters).expect("failed to encode filters");
        self.db.query_with_stats(&json).expect("query failed")
    }

    pub(crate) fn subscribe(&self, filters: &[Filter]) -> Subscription {
        let json = to_json_string(filters).expect("failed to encode filters");
        self.db.subscribe(&json).expect("subscribe failed")
    }

    pub(crate) fn ro_txn(&self) -> RoTransaction<'_> {
        self.db
            .begin_ro_txn()
            .expect("failed to open read transaction")
    }

    pub(crate) fn assert_event_stored(&self, txn: &RoTransaction<'_>, seq: u64, event: &Event) {
        let seq_bytes = seq.to_ne_bytes();

        let stored_event_bytes = txn
            .get(self.db.events, &seq_bytes)
            .expect("event not stored");
        let stored_event_json = from_ndb_note(stored_event_bytes).expect("failed to decode event");
        let stored_event = Event::from_json(&stored_event_json).expect("invalid event json");
        assert_eq!(stored_event.id, event.id);
        assert_eq!(stored_event.pubkey, event.pubkey);

        let stored_content_bytes = txn
            .get(self.db.contents.database(), &seq_bytes)
            .expect("content not stored");
        let stored_content = std::str::from_utf8(stored_content_bytes).expect("content not utf8");
        let extracted_content = extract_text(event);
        assert_eq!(stored_content, normalize_text(&extracted_content));

        let index_key = Self::event_index_key(&event.id, event.created_at.as_u64());
        let index_seq = txn
            .get(self.db.event_id_index.database(), &index_key)
            .expect("event id index missing");
        assert_eq!(index_seq, seq_bytes);

        assert!(
            self.created_at_contains(txn, event.created_at.as_u64(), seq),
            "created_at index missing seq {seq}"
        );

        assert!(
            self.author_contains(txn, &event.pubkey, event.created_at.as_u64(), seq),
            "author index missing seq {seq}"
        );

        assert!(
            self.kind_contains(txn, event.kind.as_u16(), event.created_at.as_u64(), seq),
            "kind index missing seq {seq}"
        );
        assert!(
            self.pubkey_kind_contains(
                txn,
                &event.pubkey,
                event.kind.as_u16(),
                event.created_at.as_u64(),
                seq,
            ),
            "pubkey_kind index missing seq {seq}"
        );

        for gram in char_ngrams(stored_content, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE) {
            assert!(
                self.ngram_contains(txn, &gram, event.created_at.as_u64(), seq),
                "ngram index missing seq {seq} for gram '{gram}'"
            );
        }
        for tag in event.tags.iter() {
            let TagKind::SingleLetter(single) = tag.kind() else {
                continue;
            };
            let Some(content) = tag.content() else {
                continue;
            };
            if content.is_empty() {
                continue;
            }
            assert!(
                self.tag_contains(txn, single, content, event.created_at.as_u64(), seq),
                "tag index missing seq {seq} for tag {}:{content}",
                single
            );
        }
    }

    fn tag_contains(
        &self,
        txn: &RoTransaction<'_>,
        tag: char,
        value: &str,
        _created_at: u64,
        seq: u64,
    ) -> bool {
        self.db
            .tag_index
            .iter_candidates(txn, &[(tag, vec![value.to_string()])], None, None)
            .expect("tag iterator failed")
            .any(|candidate| candidate == seq.to_ne_bytes())
    }

    pub(crate) fn assert_event_removed(&self, txn: &RoTransaction<'_>, seq: u64, event: &Event) {
        let seq_bytes = seq.to_ne_bytes();
        let index_key = Self::event_index_key(&event.id, event.created_at.as_u64());

        assert!(matches!(
            txn.get(self.db.events, &seq_bytes),
            Err(lmdb::Error::NotFound)
        ));
        assert!(matches!(
            txn.get(self.db.contents.database(), &seq_bytes),
            Err(lmdb::Error::NotFound)
        ));
        assert!(matches!(
            txn.get(self.db.event_id_index.database(), &index_key),
            Err(lmdb::Error::NotFound)
        ));
        assert!(
            !self.created_at_contains(txn, event.created_at.as_u64(), seq),
            "created_at index retained seq {seq}"
        );

        assert!(
            !self.author_contains(txn, &event.pubkey, event.created_at.as_u64(), seq),
            "author index retained seq {seq}"
        );

        assert!(
            !self.kind_contains(txn, event.kind.as_u16(), event.created_at.as_u64(), seq),
            "kind index retained seq {seq}"
        );
        assert!(
            !self.pubkey_kind_contains(
                txn,
                &event.pubkey,
                event.kind.as_u16(),
                event.created_at.as_u64(),
                seq,
            ),
            "pubkey_kind index retained seq {seq}"
        );

        let normalized = normalize_text(&extract_text(event));
        for gram in char_ngrams(&normalized, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE) {
            assert!(
                !self.ngram_contains(txn, &gram, event.created_at.as_u64(), seq),
                "ngram index retained seq {seq} for gram '{gram}'"
            );
        }
        for tag in event.tags.iter() {
            let TagKind::SingleLetter(single) = tag.kind() else {
                continue;
            };
            let Some(content) = tag.content() else {
                continue;
            };
            if content.is_empty() {
                continue;
            }
            assert!(
                !self.tag_contains(txn, single, content, event.created_at.as_u64(), seq),
                "tag index retained seq {seq} for tag {}:{content}",
                single
            );
        }
    }

    pub(crate) fn assert_deletion_marker_eq(
        &self,
        txn: &RoTransaction<'_>,
        event_id: &EventId,
        pubkey: &PublicKey,
        expected_seq: u64,
    ) {
        let key = Self::deletion_marker_key(event_id, pubkey);
        let marker = txn
            .get(self.db.deletions.database(), &key)
            .expect("deletion marker missing");
        let marker_bytes: [u8; SEQ_BYTES] = marker
            .try_into()
            .expect("unexpected deletion marker length");
        assert_eq!(marker_bytes, expected_seq.to_ne_bytes());
    }

    #[allow(dead_code)]
    pub(crate) fn assert_no_deletion_marker(
        &self,
        txn: &RoTransaction<'_>,
        event_id: &EventId,
        pubkey: &PublicKey,
    ) {
        let key = Self::deletion_marker_key(event_id, pubkey);
        assert!(matches!(
            txn.get(self.db.deletions.database(), &key),
            Err(lmdb::Error::NotFound)
        ));
    }

    pub(crate) fn assert_event_absent(&self, txn: &RoTransaction<'_>, event: &Event) {
        let index_key = Self::event_index_key(&event.id, event.created_at.as_u64());
        assert!(matches!(
            txn.get(self.db.event_id_index.database(), &index_key),
            Err(lmdb::Error::NotFound)
        ));
    }

    pub(crate) fn created_at_contains(
        &self,
        txn: &RoTransaction<'_>,
        created_at: u64,
        seq: u64,
    ) -> bool {
        let mut cursor = txn
            .open_ro_cursor(self.db.created_at_index.database())
            .expect("failed to open created_at cursor");
        let created_at_key = created_at.to_ne_bytes();
        let seq_bytes = seq.to_ne_bytes();

        match cursor.iter_dup_of(&created_at_key) {
            Ok(mut iter) => iter.any(|(_, value)| value == seq_bytes),
            Err(lmdb::Error::NotFound) => false,
            Err(err) => panic!("unexpected cursor error: {err}"),
        }
    }

    pub(crate) fn kind_contains(
        &self,
        txn: &RoTransaction<'_>,
        kind: u16,
        _created_at: u64,
        seq: u64,
    ) -> bool {
        self.db
            .kind_index
            .iter_candidates(txn, &[kind], None, None)
            .expect("kind iterator failed")
            .any(|candidate| candidate == seq.to_ne_bytes())
    }

    pub(crate) fn pubkey_kind_contains(
        &self,
        txn: &RoTransaction<'_>,
        pubkey: &PublicKey,
        kind: u16,
        _created_at: u64,
        seq: u64,
    ) -> bool {
        self.db
            .pubkey_kind_index
            .iter_candidates(txn, &[pubkey.as_bytes()], &[kind], None, None)
            .expect("pubkey_kind iterator failed")
            .any(|candidate| candidate == seq.to_ne_bytes())
    }

    fn ngram_contains(
        &self,
        txn: &RoTransaction<'_>,
        gram: &str,
        created_at: u64,
        seq: u64,
    ) -> bool {
        self.db.ngram_index.contains(txn, gram, created_at, seq)
    }

    fn author_contains(
        &self,
        txn: &RoTransaction<'_>,
        pubkey: &PublicKey,
        _created_at: u64,
        seq: u64,
    ) -> bool {
        self.db
            .pubkey_index
            .iter_candidates(txn, &[pubkey.as_bytes()], None, None)
            .expect("author iterator failed")
            .any(|candidate| candidate == seq.to_ne_bytes())
    }

    pub(crate) fn event_index_key(event_id: &EventId, created_at: u64) -> Vec<u8> {
        let mut key = event_id.as_bytes().to_vec();
        key.extend_from_slice(&created_at.to_be_bytes());
        key
    }

    fn deletion_marker_key(event_id: &EventId, pubkey: &PublicKey) -> Vec<u8> {
        let mut key = event_id.as_bytes().to_vec();
        key.extend_from_slice(pubkey.as_bytes());
        key
    }

    pub(crate) fn build_addressable_event(
        keys: &Keys,
        slot: &str,
        created_at: u64,
        content: &str,
    ) -> Event {
        EventBuilder::new(Kind::LongFormTextNote, content)
            .tag(identifier_tag(slot))
            .custom_created_at(Timestamp::from_secs(created_at))
            .sign_with_keys(keys)
            .expect("failed to build addressable event")
    }
}

fn identifier_tag(value: impl Into<String>) -> Vec<String> {
    vec!["d".to_string(), value.into()]
}
