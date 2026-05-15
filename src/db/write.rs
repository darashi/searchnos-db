use std::{
    cmp::Ordering,
    convert::TryInto,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lmdb::{Cursor, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::MDB_LAST;
use ndb::{NdbNote, NdbNoteBuf, TagElement};

use crate::nostr::{EventError, Kind, Timestamp, extract_note_expiration};

use crate::ndb_ext::{
    note_deletion_ids, note_event_index_key, note_replacable_key, note_replace_deletion_hashes,
    parse_a_tag, replace_deletion_hash_from_parts, tag_text, to_ndb_note_buf, verify_note,
};

use super::{KEY_BYTES, SEQ_BYTES, SearchnosDB, SearchnosDBError, normalize::EventIndexData};

#[derive(Debug)]
pub struct PreparedInsert {
    index_data: EventIndexData,
    deletion_ids: Vec<[u8; 32]>,
    replace_deletions: Vec<ReplaceDeletionTarget>,
    note: NdbNoteBuf,
    expiration: Option<u64>,
    id: [u8; 32],
    pubkey: [u8; 32],
    created_at: u64,
    kind: u32,
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
        note_bytes: Vec<u8>,
    ) -> Result<PreparedInsert, SearchnosDBError> {
        let note = NdbNoteBuf::from_bytes(note_bytes).map_err(SearchnosDBError::DecodeEvent)?;
        Self::prepare_insert_with_note(note, false)
    }

    fn prepare_insert_with_options(
        raw: &str,
        reject_invalid_replace_deletions: bool,
    ) -> Result<PreparedInsert, SearchnosDBError> {
        let note = to_ndb_note_buf(raw).map_err(Self::note_json_error_to_db_error)?;
        Self::prepare_insert_with_note(note, reject_invalid_replace_deletions)
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

    fn prepare_insert_with_note(
        note: NdbNoteBuf,
        reject_invalid_replace_deletions: bool,
    ) -> Result<PreparedInsert, SearchnosDBError> {
        let note_ref = note.as_ndb_note();
        verify_note(&note_ref).map_err(SearchnosDBError::InvalidSignature)?;
        let index_data =
            EventIndexData::from_note(&note_ref).map_err(SearchnosDBError::DecodeEvent)?;
        let expiration = index_data.expiration;
        let id = *note_ref.id();
        let pubkey = *note_ref.pubkey();
        let created_at = note_ref.created_at();
        let kind = note_ref.kind();
        let is_deletion = kind == Kind::EventDeletion.as_u32();
        let deletion_ids = if is_deletion {
            note_deletion_ids(&note_ref)
        } else {
            Vec::new()
        };
        let replace_deletions = if is_deletion {
            Self::collect_note_replace_deletions(&note_ref, reject_invalid_replace_deletions)?
        } else {
            Vec::new()
        };
        Ok(PreparedInsert {
            index_data,
            deletion_ids,
            replace_deletions,
            note,
            expiration,
            id,
            pubkey,
            created_at,
            kind,
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
            self.validate_event_for_insertion(txn, &prepared, current_time)?,
            ValidationResult::Skip
        ) {
            return Ok(InsertResult::Dropped);
        }

        let existing_result = self.check_existing_event(txn, &prepared)?;
        let replacement_plan = match existing_result {
            ExistingEventResult::AlreadyExists(seq) => return Ok(InsertResult::AlreadyExists(seq)),
            ExistingEventResult::ShouldInsert { replacement } => replacement,
            ExistingEventResult::ShouldReplace { replacement } => Some(replacement),
        };

        if Self::note_kind_is_ephemeral(prepared.kind) {
            let note = prepared.note.as_ndb_note();
            let event_json = note
                .to_json_string()
                .map_err(SearchnosDBError::DecodeEvent)?;
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

        if prepared.kind == Kind::EventDeletion.as_u32() {
            self.handle_deletion_event(txn, &prepared, seq)?;
        }

        let note = prepared.note.as_ndb_note();
        let event_json = note
            .to_json_string()
            .map_err(SearchnosDBError::DecodeEvent)?;
        self.broadcast_note(&note, &prepared.index_data.normalized_content, &event_json);

        Ok(InsertResult::Inserted(seq))
    }

    fn validate_event_for_insertion(
        &self,
        _txn: &lmdb::RwTransaction,
        prepared: &PreparedInsert,
        current_time: Timestamp,
    ) -> Result<ValidationResult, SearchnosDBError> {
        if let Some(expiration_ts) = prepared.expiration
            && !Self::note_kind_is_ephemeral(prepared.kind)
            && expiration_ts <= current_time.as_u64()
        {
            return Ok(ValidationResult::Skip);
        }

        if let Some(policy) = &self.purge_policy
            && prepared.kind <= u16::MAX as u32
            && policy.should_purge_immediately(prepared.kind as u16)
        {
            return Ok(ValidationResult::Skip);
        }

        Ok(ValidationResult::Insert)
    }

    fn check_existing_event<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        prepared: &PreparedInsert,
    ) -> Result<ExistingEventResult, SearchnosDBError> {
        let event_index_key = &prepared.index_data.event_index_key;
        let is_deletion = prepared.kind == Kind::EventDeletion.as_u32();

        if !is_deletion {
            let deletion_key = Self::deletion_marker_key_bytes(&prepared.id, &prepared.pubkey);
            if let Some(existing_seq) = self.deletions.get_marker(txn, &deletion_key)? {
                return Ok(ExistingEventResult::AlreadyExists(existing_seq));
            }
        }

        if let Some(existing_seq) = self.event_id_index.get_seq(txn, &event_index_key)? {
            return Ok(ExistingEventResult::AlreadyExists(existing_seq));
        }

        let note = prepared.note.as_ndb_note();
        let Some(slot_key) = note_replacable_key(&note) else {
            return Ok(ExistingEventResult::ShouldInsert { replacement: None });
        };

        if Self::note_kind_is_addressable(prepared.kind)
            && let Some(hash) = Self::replace_deletion_hash_for_note(&note)
            && let Some(deletion_seq) =
                self.replace_deletions
                    .get_blocking_seq(txn, &hash, prepared.created_at)?
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
        let this_created = prepared.created_at;

        let new_wins = this_created > existing_created
            || (this_created == existing_created
                && prepared.id.as_slice().cmp(existing_note.id()) == Ordering::Less);

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
        self.insert_event_data(txn, seq, prepared.note.as_bytes(), &prepared.index_data)?;
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
        self.apply_deletions(txn, &prepared.pubkey, &prepared.deletion_ids, seq)?;
        self.apply_replace_deletions(txn, &prepared.replace_deletions, prepared.created_at, seq)
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

    fn note_kind_is_ephemeral(kind: u32) -> bool {
        kind <= u16::MAX as u32 && Kind::from_u16(kind as u16).is_ephemeral()
    }

    fn note_kind_is_addressable(kind: u32) -> bool {
        kind <= u16::MAX as u32 && Kind::from_u16(kind as u16).is_addressable()
    }

    fn first_note_d_tag(note: &NdbNote<'_>) -> Option<String> {
        for tag in note.tags() {
            let Ok(tag) = tag else {
                continue;
            };
            let mut elements = tag.elements();
            let Some(Ok(identifier)) = elements.next() else {
                continue;
            };
            if tag_text(identifier) != Some("d") {
                continue;
            }
            let Some(Ok(value)) = elements.next() else {
                return Some(String::new());
            };
            return match value {
                TagElement::Text(value) => Some(value.to_owned()),
                TagElement::Id(_) => Some(String::new()),
            };
        }

        None
    }

    fn collect_note_replace_deletions(
        note: &NdbNote<'_>,
        reject_invalid: bool,
    ) -> Result<Vec<ReplaceDeletionTarget>, SearchnosDBError> {
        let mut targets = Vec::new();
        for tag in note.tags() {
            let tag = tag.map_err(SearchnosDBError::DecodeEvent)?;
            let mut elements = tag.elements();
            let Some(identifier) = elements
                .next()
                .transpose()
                .map_err(SearchnosDBError::DecodeEvent)?
            else {
                continue;
            };
            if tag_text(identifier) != Some("a") {
                continue;
            }
            let Some(value) = elements
                .next()
                .transpose()
                .map_err(SearchnosDBError::DecodeEvent)?
            else {
                if !reject_invalid {
                    continue;
                }
                return Err(SearchnosDBError::InvalidDeletionATag(
                    "missing coordinate".to_string(),
                ));
            };
            let Some(a_tag) = tag_text(value) else {
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
            if pubkey != *note.pubkey() {
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
                slot_key: Self::replacable_key_from_parts(note.pubkey(), kind, d_tag),
            });
        }

        Ok(targets)
    }

    fn replace_deletion_hash_for_note(note: &NdbNote<'_>) -> Option<[u8; 32]> {
        if !Self::note_kind_is_addressable(note.kind()) {
            return None;
        }

        let d_tag = Self::first_note_d_tag(note).unwrap_or_default();
        Some(replace_deletion_hash_from_parts(
            note.kind(),
            note.pubkey(),
            &d_tag,
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

    fn deletion_marker_key_bytes(event_id: &[u8; 32], pubkey: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(event_id.len() + pubkey.len());
        key.extend_from_slice(event_id);
        key.extend_from_slice(pubkey);
        key
    }

    fn apply_deletions<'env>(
        &self,
        txn: &mut RwTransaction<'env>,
        pubkey: &[u8; 32],
        deletion_ids: &[[u8; 32]],
        deletion_seq: u64,
    ) -> Result<(), SearchnosDBError> {
        if deletion_ids.is_empty() {
            return Ok(());
        }

        for event_id in deletion_ids {
            let marker_key = Self::deletion_marker_key_bytes(event_id, pubkey);
            self.deletions.put_marker(txn, &marker_key, deletion_seq)?;

            let id_refs: [&[u8]; 1] = [event_id];
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
                if note.pubkey() != pubkey {
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
