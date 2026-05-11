use std::{
    cmp::Ordering,
    convert::TryInto,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lmdb::{Cursor, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::MDB_LAST;
use ndb::{Event as NdbEvent, NdbNote};

use crate::nostr::{
    Event, EventId, JsonUtil, Kind, PublicKey, Signature, TagExt, TagKind, Tags, Timestamp,
    extract_note_expiration,
};

use crate::ndb_ext::{
    note_deletion_ids, note_event_index_key, note_replacable_key, note_replace_deletion_hashes,
    parse_a_tag, replace_deletion_hash_from_parts, to_ndb_note,
};

use super::{
    KEY_BYTES, SEQ_BYTES, SearchnosDB, SearchnosDBError,
    normalize::{EventIndexData, build_event_index_key},
};

#[derive(Debug)]
pub struct PreparedInsert {
    event: Event,
    index_data: EventIndexData,
    deletion_ids: Vec<EventId>,
    replace_deletions: Vec<ReplaceDeletionTarget>,
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
struct ReplaceDeletionTarget {
    hash: [u8; 32],
    slot_key: Vec<u8>,
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
        Self::prepare_insert_with_options(raw, true)
    }

    pub(crate) fn prepare_loaded_note_insert(
        note_event: NdbEvent,
        note_bytes: Vec<u8>,
    ) -> Result<PreparedInsert, SearchnosDBError> {
        Self::prepare_insert_with_note_bytes(
            Self::event_from_ndb_event(note_event),
            false,
            note_bytes,
        )
    }

    fn prepare_insert_with_options(
        raw: &str,
        reject_invalid_replace_deletions: bool,
    ) -> Result<PreparedInsert, SearchnosDBError> {
        let event = Event::from_json(raw)?;
        let note_bytes = to_ndb_note(raw)?;
        Self::prepare_insert_with_note_bytes(event, reject_invalid_replace_deletions, note_bytes)
    }

    fn prepare_insert_with_note_bytes(
        event: Event,
        reject_invalid_replace_deletions: bool,
        note_bytes: Vec<u8>,
    ) -> Result<PreparedInsert, SearchnosDBError> {
        event.verify().map_err(SearchnosDBError::InvalidSignature)?;
        let index_data = EventIndexData::from_event(&event);
        let expiration = index_data.expiration;
        let is_deletion = event.kind == Kind::EventDeletion;
        let deletion_ids = if is_deletion {
            Self::collect_deletion_ids(&event)
        } else {
            Vec::new()
        };
        let replace_deletions = if is_deletion {
            Self::collect_replace_deletions(&event, reject_invalid_replace_deletions)?
        } else {
            Vec::new()
        };
        Ok(PreparedInsert {
            event,
            index_data,
            deletion_ids,
            replace_deletions,
            note_bytes,
            expiration,
        })
    }

    fn event_from_ndb_event(event: NdbEvent) -> Event {
        Event {
            id: EventId::from(event.id),
            pubkey: PublicKey::from_bytes(event.pubkey),
            created_at: Timestamp::from(event.created_at),
            kind: Kind::from_u32(event.kind),
            tags: Tags::new(event.tags),
            content: event.content,
            sig: Signature::from_bytes(event.sig),
        }
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

        let seq = self.perform_event_insertion(txn, &prepared)?;

        if let Some(plan) = &replacement_plan {
            self.replacables.put(txn, &plan.slot_key, seq)?;
            if let Some(existing_seq) = plan.old_seq
                && existing_seq != seq
            {
                self.remove_event_by_seq(txn, existing_seq)?;
            }
        }

        if prepared.event.kind == Kind::EventDeletion {
            self.handle_deletion_event(txn, &prepared, seq)?;
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

        if event.kind.is_addressable()
            && let Some(hash) = Self::replace_deletion_hash_for_event(event)
            && let Some(deletion_seq) =
                self.replace_deletions
                    .get_blocking_seq(txn, &hash, event.created_at.as_u64())?
        {
            return Ok(ExistingEventResult::AlreadyExists(deletion_seq));
        }

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
        prepared: &PreparedInsert,
    ) -> Result<u64, SearchnosDBError> {
        let seq = self.next_seq(txn)?;
        self.insert_event_data(txn, seq, &prepared.note_bytes, &prepared.index_data)?;
        self.update_indexes(txn, seq, &prepared.index_data, prepared.expiration)?;
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
        index_data: &EventIndexData,
        expiration: Option<u64>,
    ) -> Result<(), SearchnosDBError> {
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
        prepared: &PreparedInsert,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        self.apply_deletions(txn, &prepared.event.pubkey, &prepared.deletion_ids, seq)?;
        self.apply_replace_deletions(
            txn,
            &prepared.replace_deletions,
            prepared.event.created_at.as_u64(),
            seq,
        )
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

    fn collect_replace_deletions(
        event: &Event,
        reject_invalid: bool,
    ) -> Result<Vec<ReplaceDeletionTarget>, SearchnosDBError> {
        let mut targets = Vec::new();
        for tag in event.tags.iter() {
            let TagKind::SingleLetter(single) = tag.kind() else {
                continue;
            };
            if single != 'a' {
                continue;
            }
            let Some(a_tag) = tag.content() else {
                if !reject_invalid {
                    continue;
                }
                return Err(SearchnosDBError::InvalidDeletionATag(
                    "missing coordinate".to_string(),
                ));
            };
            let Some((kind, pubkey, d_tag)) = parse_a_tag(a_tag) else {
                if !reject_invalid {
                    continue;
                }
                return Err(SearchnosDBError::InvalidDeletionATag(a_tag.to_string()));
            };
            if pubkey != *event.pubkey.as_bytes() {
                if !reject_invalid {
                    continue;
                }
                return Err(SearchnosDBError::InvalidDeletionATag(
                    "cannot delete another pubkey's addressable event".to_string(),
                ));
            }
            if !(30_000..40_000).contains(&kind) {
                continue;
            }
            let kind = Kind::from_u16(kind as u16);

            targets.push(ReplaceDeletionTarget {
                hash: replace_deletion_hash_from_parts(kind.as_u32(), &pubkey, d_tag),
                slot_key: Self::replacable_key_from_parts(event.pubkey.as_bytes(), kind, d_tag),
            });
        }

        Ok(targets)
    }

    fn replace_deletion_hash_for_event(event: &Event) -> Option<[u8; 32]> {
        if !event.kind.is_addressable() {
            return None;
        }

        let d_tag = Self::first_d_tag_bytes(event).unwrap_or_default();
        Some(replace_deletion_hash_from_parts(
            event.kind.as_u32(),
            event.pubkey.as_bytes(),
            &String::from_utf8_lossy(&d_tag),
        ))
    }

    fn replacable_key_from_parts(pubkey: &[u8; 32], kind: Kind, slot: &str) -> Vec<u8> {
        let slot = slot.as_bytes();
        let mut key = Vec::with_capacity(pubkey.len() + 2 + 4 + slot.len());
        key.extend_from_slice(pubkey);
        key.extend_from_slice(&kind.as_u16().to_be_bytes());
        key.extend_from_slice(&(slot.len() as u32).to_be_bytes());
        key.extend_from_slice(slot);
        key
    }

    fn deletion_marker_key(event_id: &EventId, pubkey: &PublicKey) -> Vec<u8> {
        Self::deletion_marker_key_bytes(event_id.as_bytes(), pubkey.as_bytes())
    }

    fn deletion_marker_key_bytes(event_id: &[u8; 32], pubkey: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(event_id.len() + pubkey.len());
        key.extend_from_slice(event_id);
        key.extend_from_slice(pubkey);
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
                let note =
                    NdbNote::from_bytes(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
                if note.pubkey() != pubkey.as_bytes() {
                    continue;
                }
                let removes_markers = note.kind() == Kind::EventDeletion.as_u32();
                self.remove_event_by_seq_internal(txn, event_seq, removes_markers)?;
            }
        }

        Ok(())
    }

    fn apply_replace_deletions<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        targets: &[ReplaceDeletionTarget],
        deletion_created_at: u64,
        deletion_seq: u64,
    ) -> Result<(), SearchnosDBError> {
        for target in targets {
            self.replace_deletions
                .put(txn, &target.hash, deletion_created_at, deletion_seq)?;

            let Some(existing_seq) = self.replacables.get_seq(txn, &target.slot_key)? else {
                continue;
            };
            let event_key = existing_seq.to_ne_bytes();
            let event_bytes = match txn.get(self.events, &event_key) {
                Ok(bytes) => bytes,
                Err(lmdb::Error::NotFound) => {
                    self.replacables.delete(txn, &target.slot_key)?;
                    continue;
                }
                Err(err) => return Err(err.into()),
            };
            let should_delete = {
                let note =
                    NdbNote::from_bytes(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
                note.created_at() <= deletion_created_at
            };
            if should_delete {
                self.remove_event_by_seq(txn, existing_seq)?;
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

        let (
            event_index_key,
            expiration,
            deletion_marker_keys,
            replace_deletion_hashes,
            replacable_key,
            created_at,
        ) = {
            let note = NdbNote::from_bytes(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
            let event_index_key = note_event_index_key(&note);
            let expiration = extract_note_expiration(&note);
            let deletion_marker_keys = if remove_deletion_markers {
                let mut keys = Vec::new();
                if note.kind() == Kind::EventDeletion.as_u32() {
                    keys.extend(
                        note_deletion_ids(&note)
                            .into_iter()
                            .map(|id| Self::deletion_marker_key_bytes(&id, note.pubkey())),
                    );
                }
                keys.push(Self::deletion_marker_key_bytes(note.id(), note.pubkey()));
                keys
            } else {
                Vec::new()
            };
            let replace_deletion_hashes = if remove_deletion_markers {
                note_replace_deletion_hashes(&note)
            } else {
                Vec::new()
            };
            let replacable_key = note_replacable_key(&note);
            let created_at = note.created_at();

            (
                event_index_key,
                expiration,
                deletion_marker_keys,
                replace_deletion_hashes,
                replacable_key,
                created_at,
            )
        };

        if let Some(expiration) = expiration {
            self.expiration_index.delete_entry(txn, expiration, seq)?;
        }

        for key in deletion_marker_keys {
            self.deletions.delete_marker(txn, &key)?;
        }

        for hash in replace_deletion_hashes {
            self.replace_deletions
                .delete_entry(txn, &hash, created_at, seq)?;
        }

        self.event_id_index.delete(txn, &event_index_key)?;

        if let Some(replacable_key) = replacable_key {
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
