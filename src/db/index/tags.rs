use std::cmp::Ordering;
use std::collections::HashMap;

use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
    WriteFlags,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_PREV};

use super::common::{
    append_created_at, position_cursor_at_prefix_end, put_keyed_seq, seq_from_value,
    split_created_at_from_key,
};
use crate::db::{CREATED_AT_BYTES, SEQ_BYTES, SearchnosDBError};

/// Index events by single-letter tag value.
#[derive(Debug)]
pub struct TagIndex {
    db: Database,
}

impl TagIndex {
    pub const NAME: &'static str = "tags_to_events";
    pub const MAX_VALUE_BYTES: usize = 255;

    /// Open (or create) the tag value index database.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::DUP_SORT)?;
        Ok(Self { db })
    }

    /// Access the LMDB database backing the tag index.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Build an index key combining the tag letter with a value.
    pub fn key(tag: char, value: &str) -> Option<Vec<u8>> {
        if !tag.is_ascii_alphabetic() {
            return None;
        }
        let value_bytes = value.as_bytes();
        if value_bytes.len() > Self::MAX_VALUE_BYTES {
            return None;
        }
        let mut key = Vec::with_capacity(1 + value_bytes.len());
        key.push(tag as u8);
        key.extend_from_slice(value_bytes);
        Some(key)
    }

    /// Insert a `(created_at, seq)` tuple for a tag entry.
    pub fn put<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        key: &K,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        let entry_key = Self::entry_key(key, created_at);
        put_keyed_seq(self.db, txn, &entry_key, seq)
    }

    /// Remove a stored tuple for a tag entry.
    pub fn delete_value<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        key: &K,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        let entry_key = Self::entry_key(key, created_at);
        let value = seq.to_ne_bytes();
        let mut cursor = txn.open_rw_cursor(self.db)?;
        match cursor.get(Some(&entry_key), Some(&value), lmdb_sys::MDB_GET_BOTH) {
            Ok(_) => match cursor.del(WriteFlags::CURRENT) {
                Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
                Err(err) => Err(err.into()),
            },
            Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn entry_key<K>(key: &K, created_at: u64) -> Vec<u8>
    where
        K: AsRef<[u8]>,
    {
        let mut buf = key.as_ref().to_vec();
        append_created_at(&mut buf, created_at);
        buf
    }

    /// Iterate over all seq_bytes that match the given tag entries (OR within tag, AND across tags)
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        entries: &[(char, Vec<String>)],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'env, SearchnosDBError> {
        if entries.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let mut combined: Option<HashMap<[u8; SEQ_BYTES], u64>> = None;

        for (tag, values) in entries {
            let mut tag_candidates = HashMap::new();

            for value in values {
                if value.is_empty() {
                    continue;
                }
                let Some(tag_key) = Self::key(*tag, value) else {
                    continue;
                };
                for (created_at, seq_bytes) in self.entries_for_key(txn, &tag_key, since, until)? {
                    tag_candidates.entry(seq_bytes).or_insert(created_at);
                }
            }

            if tag_candidates.is_empty() {
                return Ok(Vec::new().into_iter());
            }

            combined = Some(match combined.take() {
                None => tag_candidates,
                Some(mut existing) => {
                    existing.retain(|seq, created_at| {
                        if let Some(new_created) = tag_candidates.get(seq) {
                            *created_at = *new_created;
                            true
                        } else {
                            false
                        }
                    });
                    existing
                }
            });

            if combined.as_ref().is_some_and(|map| map.is_empty()) {
                return Ok(Vec::new().into_iter());
            }
        }

        let mut ordered: Vec<(u64, [u8; SEQ_BYTES])> = combined
            .unwrap_or_default()
            .into_iter()
            .map(|(seq, created_at)| (created_at, seq))
            .collect();

        ordered.sort_unstable_by(|a, b| match b.0.cmp(&a.0) {
            Ordering::Equal => b.1.cmp(&a.1),
            other => other,
        });

        let candidates: Vec<[u8; SEQ_BYTES]> = ordered.into_iter().map(|(_, seq)| seq).collect();
        Ok(candidates.into_iter())
    }

    fn entries_for_key<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        key: &[u8],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<Vec<(u64, [u8; SEQ_BYTES])>, SearchnosDBError> {
        let mut cursor = txn.open_ro_cursor(self.db)?;
        if !position_cursor_at_prefix_end(&mut cursor, key)? {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        loop {
            let (key_bytes, value_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                Ok((Some(current_key), value)) => (current_key, value),
                Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                Err(err) => return Err(err.into()),
            };

            if !key_bytes.starts_with(key) {
                break;
            }

            if key_bytes.len() != key.len() + CREATED_AT_BYTES {
                match cursor.get(None, None, MDB_PREV) {
                    Ok(_) => continue,
                    Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            }

            let created_at = split_created_at_from_key(key_bytes)?;
            let seq_bytes = seq_from_value(value_bytes)?;

            if let Some(until_bound) = until
                && created_at > until_bound
            {
                match cursor.get(None, None, MDB_PREV) {
                    Ok(_) => continue,
                    Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            }

            if let Some(since_bound) = since
                && created_at < since_bound
            {
                break;
            }
            entries.push((created_at, seq_bytes));

            match cursor.get(None, None, MDB_PREV) {
                Ok(_) => continue,
                Err(lmdb::Error::NotFound) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(entries)
    }
}
