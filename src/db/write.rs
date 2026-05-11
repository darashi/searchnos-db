use std::{
    cmp::Ordering,
    convert::TryInto,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lmdb::{Cursor, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::MDB_LAST;
use ndb::NdbNote;

use crate::nostr::{
    Event, EventId, JsonUtil, Kind, PublicKey, TagExt, TagKind, Timestamp, extract_event_expiration,
};

use crate::ndb_ext::{from_ndb_note, to_ndb_note};

use super::{
    KEY_BYTES, SEQ_BYTES, SearchnosDB, SearchnosDBError,
    normalize::{EventIndexData, build_event_index_key},
};

#[derive(Debug)]
pub struct PreparedInsert {
    event: Event,
    index_data: EventIndexData,
    deletion_ids: Vec<EventId>,
    note_bytes: Vec<u8>,
    expiration: Option<u64>,
}

/// Result of an event insertion attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertResult {
    /// Event was newly inserted with the given sequence number
    Inserted(u64),
    /// Event already exists with the given sequence number
    AlreadyExists(u64),
    /// Event was dropped (expired, deleted, or filtered by purge policy)
    Dropped,
}

#[derive(Debug)]
enum ValidationResult {
    Insert,
    Skip,
}

#[derive(Debug)]
struct ReplacementPlan {
    slot_key: Vec<u8>,
    old_seq: Option<u64>,
}

#[derive(Debug)]
enum ExistingEventResult {
    AlreadyExists(u64),
    ShouldInsert {
        replacement: Option<ReplacementPlan>,
    },
    ShouldReplace {
        replacement: ReplacementPlan,
    },
}

impl SearchnosDB {
    pub(crate) fn prepare_insert(raw: &str) -> Result<PreparedInsert, SearchnosDBError> {
        let event = Event::from_json(raw)?;
        event.verify().map_err(SearchnosDBError::InvalidSignature)?;
        let index_data = EventIndexData::from_event(&event);
        let expiration = index_data.expiration;
        let is_deletion = event.kind == Kind::EventDeletion;
        let deletion_ids = if is_deletion {
            Self::collect_deletion_ids(&event)
        } else {
            Vec::new()
        };
        let note_bytes = to_ndb_note(raw)?;

        Ok(PreparedInsert {
            event,
            index_data,
            deletion_ids,
            note_bytes,
            expiration,
        })
    }

    pub(crate) fn is_expired(expiration: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        expiration <= now
    }

    #[cfg(test)]
    pub(crate) fn insert<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        raw: &str,
    ) -> Result<InsertResult, SearchnosDBError> {
        let prepared = Self::prepare_insert(raw)?;
        self.insert_prepared(txn, prepared)
    }

    pub(crate) fn insert_prepared<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        prepared: PreparedInsert,
    ) -> Result<InsertResult, SearchnosDBError> {
        let current_time = Timestamp::now();
        if matches!(
            self.validate_event_for_insertion(txn, &prepared.event, &prepared, current_time)?,
            ValidationResult::Skip
        ) {
            return Ok(InsertResult::Dropped);
        }

        let existing_result = self.check_existing_event(txn, &prepared.event)?;
        let replacement_plan = match existing_result {
            ExistingEventResult::AlreadyExists(seq) => return Ok(InsertResult::AlreadyExists(seq)),
            ExistingEventResult::ShouldInsert { replacement } => replacement,
            ExistingEventResult::ShouldReplace { replacement } => Some(replacement),
        };

        if prepared.event.kind.is_ephemeral() {
            let note =
                NdbNote::from_bytes(&prepared.note_bytes).map_err(SearchnosDBError::DecodeEvent)?;
            let event_json = prepared.event.as_json();
            self.broadcast_note(&note, &prepared.index_data.normalized_content, &event_json);
            return Ok(InsertResult::Dropped);
        }

        let seq = self.perform_event_insertion(txn, &prepared.event, &prepared)?;

        if let Some(plan) = &replacement_plan {
            self.replacables.put(txn, &plan.slot_key, seq)?;
            if let Some(existing_seq) = plan.old_seq
                && existing_seq != seq
            {
                self.remove_event_by_seq(txn, existing_seq)?;
            }
        }

        if prepared.event.kind == Kind::EventDeletion {
            self.handle_deletion_event(txn, &prepared.event.pubkey, &prepared.deletion_ids, seq)?;
        }

        let note =
            NdbNote::from_bytes(&prepared.note_bytes).map_err(SearchnosDBError::DecodeEvent)?;
        let event_json = prepared.event.as_json();
        self.broadcast_note(&note, &prepared.index_data.normalized_content, &event_json);

        Ok(InsertResult::Inserted(seq))
    }

    fn validate_event_for_insertion(
        &self,
        _txn: &lmdb::RwTransaction,
        event: &Event,
        prepared: &PreparedInsert,
        current_time: Timestamp,
    ) -> Result<ValidationResult, SearchnosDBError> {
        if let Some(expiration_ts) = prepared.expiration
            && !event.kind.is_ephemeral()
            && expiration_ts <= current_time.as_u64()
        {
            return Ok(ValidationResult::Skip);
        }

        if let Some(policy) = &self.purge_policy
            && policy.should_purge_immediately(event.kind.as_u16())
        {
            return Ok(ValidationResult::Skip);
        }

        Ok(ValidationResult::Insert)
    }

    fn check_existing_event<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        event: &Event,
    ) -> Result<ExistingEventResult, SearchnosDBError> {
        let event_index_key = build_event_index_key(event);
        let is_deletion = event.kind == Kind::EventDeletion;

        if !is_deletion {
            let deletion_key = Self::deletion_marker_key(&event.id, &event.pubkey);
            if let Some(existing_seq) = self.deletions.get_marker(txn, &deletion_key)? {
                return Ok(ExistingEventResult::AlreadyExists(existing_seq));
            }
        }

        if let Some(existing_seq) = self.event_id_index.get_seq(txn, &event_index_key)? {
            return Ok(ExistingEventResult::AlreadyExists(existing_seq));
        }

        let Some(slot_key) = Self::replacable_key(event) else {
            return Ok(ExistingEventResult::ShouldInsert { replacement: None });
        };

        let Some(existing_seq) = self.replacables.get_seq(txn, &slot_key)? else {
            return Ok(ExistingEventResult::ShouldInsert {
                replacement: Some(ReplacementPlan {
                    slot_key,
                    old_seq: None,
                }),
            });
        };

        let event_key = existing_seq.to_ne_bytes();
        let existing_bytes = match txn.get(self.events, &event_key) {
            Ok(bytes) => bytes,
            Err(lmdb::Error::NotFound) => {
                self.replacables.delete(txn, &slot_key)?;
                return Ok(ExistingEventResult::ShouldInsert {
                    replacement: Some(ReplacementPlan {
                        slot_key,
                        old_seq: None,
                    }),
                });
            }
            Err(err) => return Err(err.into()),
        };

        let existing_note =
            NdbNote::from_bytes(existing_bytes).map_err(SearchnosDBError::DecodeEvent)?;
        let existing_created = existing_note.created_at();
        let this_created = event.created_at.as_u64();

        let new_wins = this_created > existing_created
            || (this_created == existing_created
                && event.id.as_bytes().cmp(existing_note.id()) == Ordering::Less);

        if new_wins {
            return Ok(ExistingEventResult::ShouldReplace {
                replacement: ReplacementPlan {
                    slot_key,
                    old_seq: Some(existing_seq),
                },
            });
        }

        Ok(ExistingEventResult::AlreadyExists(existing_seq))
    }

    fn perform_event_insertion<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        event: &Event,
        prepared: &PreparedInsert,
    ) -> Result<u64, SearchnosDBError> {
        let seq = self.next_seq(txn)?;
        self.insert_event_data(txn, seq, &prepared.note_bytes, &prepared.index_data)?;
        self.update_indexes(txn, seq, event, &prepared.index_data, prepared.expiration)?;
        Ok(seq)
    }

    /// Insert event data into primary storage
    fn insert_event_data<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        seq: u64,
        note_bytes: &[u8],
        index_data: &EventIndexData,
    ) -> Result<(), SearchnosDBError> {
        let key_bytes = seq.to_ne_bytes();
        txn.put(self.events, &key_bytes, &note_bytes, WriteFlags::empty())?;
        self.contents.put(
            txn,
            index_data.created_at,
            seq,
            &index_data.normalized_content,
        )?;

        Ok(())
    }

    /// Update required indexes for the inserted event.
    fn update_indexes<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        seq: u64,
        event: &Event,
        index_data: &EventIndexData,
        expiration: Option<u64>,
    ) -> Result<(), SearchnosDBError> {
        let created_at = index_data.created_at;

        self.kind_index
            .put(txn, event.kind.as_u16(), created_at, seq)?;
        self.event_id_index
            .put(txn, &index_data.event_index_key, seq)?;

        if let Some(expiration_ts) = expiration {
            self.expiration_index.put(txn, expiration_ts, seq)?;
        }

        Ok(())
    }

    /// Handle deletion event - mark events as deleted and remove them
    fn handle_deletion_event<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        pubkey: &PublicKey,
        deletion_ids: &[EventId],
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        self.apply_deletions(txn, pubkey, deletion_ids, seq)
    }

    fn next_seq<'env>(&self, txn: &mut RwTransaction<'env>) -> Result<u64, SearchnosDBError> {
        let cursor = txn.open_rw_cursor(self.events)?;
        match cursor.get(None, None, MDB_LAST) {
            Ok((Some(key_bytes), _)) => {
                let key_bytes: [u8; KEY_BYTES] = key_bytes
                    .try_into()
                    .map_err(|_| SearchnosDBError::InvalidKeyLength(key_bytes.len()))?;
                let last = u64::from_ne_bytes(key_bytes);
                last.checked_add(1)
                    .ok_or(SearchnosDBError::KeyspaceExhausted)
            }
            Ok((None, _)) => Err(SearchnosDBError::InvalidKeyLength(0)),
            Err(lmdb::Error::NotFound) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    fn first_d_tag_bytes(event: &Event) -> Option<Vec<u8>> {
        for tag in event.tags.iter() {
            let TagKind::SingleLetter(single) = tag.kind() else {
                continue;
            };
            if single != 'd' {
                continue;
            }
            let value = tag.content().unwrap_or_default();
            return Some(value.as_bytes().to_vec());
        }
        None
    }

    pub(crate) fn replacable_key(event: &Event) -> Option<Vec<u8>> {
        let is_replaceable = event.kind.is_replaceable();
        let is_addressable = event.kind.is_addressable();

        if !is_replaceable && !is_addressable {
            return None;
        }

        let slot = if is_addressable {
            Self::first_d_tag_bytes(event).unwrap_or_default()
        } else {
            Vec::new()
        };

        let kind = event.kind.as_u16();
        let mut key = Vec::with_capacity(event.pubkey.as_bytes().len() + 2 + 4 + slot.len());
        key.extend_from_slice(event.pubkey.as_bytes());
        key.extend_from_slice(&kind.to_be_bytes());
        key.extend_from_slice(&(slot.len() as u32).to_be_bytes());
        key.extend_from_slice(&slot);
        Some(key)
    }

    pub(crate) fn collect_deletion_ids(event: &Event) -> Vec<EventId> {
        let mut ids = Vec::new();
        for tag in event.tags.iter() {
            let TagKind::SingleLetter(single) = tag.kind() else {
                continue;
            };
            if single != 'e' {
                continue;
            }
            let Some(content) = tag.content() else {
                continue;
            };
            let Ok(event_id) = EventId::from_str(content) else {
                continue;
            };
            ids.push(event_id);
        }
        ids
    }

    fn deletion_marker_key(event_id: &EventId, pubkey: &PublicKey) -> Vec<u8> {
        let mut key = Vec::with_capacity(event_id.as_bytes().len() + pubkey.as_bytes().len());
        key.extend_from_slice(event_id.as_bytes());
        key.extend_from_slice(pubkey.as_bytes());
        key
    }

    fn apply_deletions<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        pubkey: &PublicKey,
        deletion_ids: &[EventId],
        deletion_seq: u64,
    ) -> Result<(), SearchnosDBError> {
        if deletion_ids.is_empty() {
            return Ok(());
        }

        for event_id in deletion_ids {
            let id_bytes = event_id.as_bytes();
            let marker_key = Self::deletion_marker_key(event_id, pubkey);
            self.deletions.put_marker(txn, &marker_key, deletion_seq)?;

            let id_refs: [&[u8]; 1] = [id_bytes];
            let seqs: Vec<[u8; SEQ_BYTES]> = self
                .event_id_index
                .iter_candidates(txn, &id_refs)?
                .collect();
            for seq_bytes in seqs {
                let event_seq = u64::from_ne_bytes(seq_bytes);
                let event_key = event_seq.to_ne_bytes();
                let event_bytes = match txn.get(self.events, &event_key) {
                    Ok(bytes) => bytes,
                    Err(lmdb::Error::NotFound) => continue,
                    Err(err) => return Err(err.into()),
                };
                let event_json =
                    from_ndb_note(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
                let event = Event::from_json(&event_json)?;
                if event.pubkey != *pubkey {
                    continue;
                }
                self.remove_event_by_seq(txn, event_seq)?;
            }
        }

        Ok(())
    }

    pub(crate) fn remove_event_by_seq<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        self.remove_event_by_seq_internal(txn, seq, false)?;
        Ok(())
    }

    pub(crate) fn remove_event_by_seq_internal<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        seq: u64,
        remove_deletion_markers: bool,
    ) -> Result<bool, SearchnosDBError> {
        let event_key = seq.to_ne_bytes();

        let event_bytes = match txn.get(self.events, &event_key) {
            Ok(bytes) => bytes,
            Err(lmdb::Error::NotFound) => return Ok(false),
            Err(err) => return Err(err.into()),
        };

        let event_json = from_ndb_note(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
        let event = Event::from_json(&event_json)?;

        let event_index_key = build_event_index_key(&event);

        if let Some(expiration) = extract_event_expiration(&event) {
            self.expiration_index.delete_entry(txn, expiration, seq)?;
        }

        if remove_deletion_markers {
            if event.kind == Kind::EventDeletion {
                for id in Self::collect_deletion_ids(&event) {
                    let key = Self::deletion_marker_key(&id, &event.pubkey);
                    self.deletions.delete_marker(txn, &key)?;
                }
            }

            let key = Self::deletion_marker_key(&event.id, &event.pubkey);
            self.deletions.delete_marker(txn, &key)?;
        }

        self.event_id_index.delete(txn, &event_index_key)?;

        let created_at = event.created_at.as_u64();
        self.kind_index
            .delete_value(txn, event.kind.as_u16(), created_at, seq)?;

        if let Some(replacable_key) = Self::replacable_key(&event) {
            self.replacables.delete_entry(txn, &replacable_key, seq)?;
        }

        match txn.del(self.events, &event_key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => {}
            Err(err) => return Err(err.into()),
        }

        self.contents.delete(txn, created_at, seq)?;

        Ok(true)
    }
}
