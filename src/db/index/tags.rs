use std::collections::HashSet;

use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_NEXT, MDB_SET_RANGE};

use super::common::{decode_created_at_seq_value, delete_dup_value, put_no_dup};
use crate::db::{AUTHOR_INDEX_VALUE_BYTES, SEQ_BYTES, SearchnosDBError};

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
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        put_no_dup(self.db, txn, key, value)
    }

    /// Remove a stored tuple for a tag entry.
    pub fn delete_value<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        key: &K,
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        let mut cursor = txn.open_rw_cursor(self.db)?;
        delete_dup_value(&mut cursor, key.as_ref(), value)
    }

    /// Iterate over all seq_bytes that match the given tag entries (OR within tag, AND across tags)
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        entries: &[(char, Vec<String>)],
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'env, SearchnosDBError> {
        if entries.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let mut combined: Option<HashSet<[u8; SEQ_BYTES]>> = None;

        for (tag, values) in entries {
            let mut tag_candidates = HashSet::new();

            for value in values {
                if value.is_empty() {
                    continue;
                }
                let Some(tag_key) = Self::key(*tag, value) else {
                    continue;
                };
                let candidates = self.candidates_for_key(txn, &tag_key)?;
                if tag_candidates.is_empty() {
                    tag_candidates = candidates;
                } else {
                    tag_candidates.extend(candidates.into_iter());
                }
            }

            if tag_candidates.is_empty() {
                return Ok(Vec::new().into_iter());
            }

            combined = Some(match combined.take() {
                None => tag_candidates,
                Some(mut existing) => {
                    existing.retain(|seq| tag_candidates.contains(seq));
                    existing
                }
            });

            if combined.as_ref().is_some_and(|set| set.is_empty()) {
                return Ok(Vec::new().into_iter());
            }
        }

        let candidates: Vec<[u8; SEQ_BYTES]> = combined.unwrap_or_default().into_iter().collect();
        Ok(candidates.into_iter())
    }

    fn candidates_for_key<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        key: &[u8],
    ) -> Result<HashSet<[u8; SEQ_BYTES]>, SearchnosDBError> {
        let cursor = txn.open_ro_cursor(self.db)?;
        let mut candidates = HashSet::new();

        match cursor.get(Some(key), None, MDB_SET_RANGE) {
            Ok(_) => loop {
                let (key_bytes, value_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                    Ok((Some(key), value)) => (key, value),
                    Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                };

                if key_bytes != key {
                    break;
                }

                let (_, seq_bytes) = decode_created_at_seq_value(value_bytes)?;
                candidates.insert(seq_bytes);

                match cursor.get(None, None, MDB_NEXT) {
                    Ok(_) => continue,
                    Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            },
            Err(lmdb::Error::NotFound) => {}
            Err(err) => return Err(err.into()),
        }

        Ok(candidates)
    }
}
