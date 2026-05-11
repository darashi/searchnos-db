use lmdb::{Cursor, Database, DatabaseFlags, Environment, RwTransaction, WriteFlags};
#[cfg(test)]
use lmdb::{RoTransaction, Transaction};
#[cfg(test)]
use lmdb_sys::{MDB_GET_CURRENT, MDB_PREV};

use super::common::{append_created_at, put_keyed_seq};
#[cfg(test)]
use super::common::{position_cursor_at_prefix_end, seq_from_value, split_created_at_from_key};
#[cfg(test)]
use crate::db::SEQ_BYTES;
use crate::db::SearchnosDBError;

const KIND_BYTES: usize = std::mem::size_of::<u16>();

#[derive(Debug)]
pub struct KindsIndex {
    db: Database,
}

impl KindsIndex {
    pub const NAME: &'static str = "kind_to_events";

    /// Open (or create) the kind-to-events index.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::DUP_SORT)?;
        Ok(Self { db })
    }

    /// Access the LMDB database used for kind lookups.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Encode a kind value as a big-endian key.
    pub fn kind_key(kind: u16) -> [u8; KIND_BYTES] {
        kind.to_be_bytes()
    }

    /// Decode a big-endian kind key.
    pub fn decode_key(bytes: &[u8]) -> Result<u16, SearchnosDBError> {
        if bytes.len() != KIND_BYTES {
            return Err(SearchnosDBError::InvalidKeyLength(bytes.len()));
        }
        let mut key = [0u8; KIND_BYTES];
        key.copy_from_slice(bytes);
        Ok(u16::from_be_bytes(key))
    }

    /// Insert a kind mapping to a `(created_at, seq)` tuple.
    pub fn put(
        &self,
        txn: &mut RwTransaction<'_>,
        kind: u16,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = Self::key_with_suffix(kind, created_at);
        put_keyed_seq(self.db, txn, &key, seq)
    }

    /// Remove a specific `(created_at, seq)` tuple for a kind.
    pub fn delete_value(
        &self,
        txn: &mut RwTransaction<'_>,
        kind: u16,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = Self::key_with_suffix(kind, created_at);
        let value = seq.to_ne_bytes();
        let mut cursor = txn.open_rw_cursor(self.db)?;
        match cursor.get(Some(&key), Some(&value), lmdb_sys::MDB_GET_BOTH) {
            Ok(_) => match cursor.del(WriteFlags::CURRENT) {
                Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
                Err(err) => Err(err.into()),
            },
            Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn key_with_suffix(kind: u16, created_at: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(KIND_BYTES + 8);
        key.extend_from_slice(&Self::kind_key(kind));
        append_created_at(&mut key, created_at);
        key
    }

    /// Iterate over all seq_bytes for the given kinds, honoring optional created_at bounds.
    #[cfg(test)]
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        kinds: &[u16],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'env, SearchnosDBError> {
        if kinds.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let mut cursor = txn.open_ro_cursor(self.db)?;
        let mut results = Vec::new();

        for kind in kinds {
            let prefix = Self::kind_key(*kind);
            if !position_cursor_at_prefix_end(&mut cursor, &prefix)? {
                continue;
            }

            loop {
                let (key_bytes, value_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                    Ok((Some(key), value)) => (key, value),
                    Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                };

                if !key_bytes.starts_with(prefix.as_slice()) {
                    break;
                }

                let indexed_created_at = split_created_at_from_key(key_bytes)?;
                let seq_bytes = seq_from_value(value_bytes)?;

                if let Some(until_bound) = until
                    && indexed_created_at > until_bound
                {
                    match cursor.get(None, None, MDB_PREV) {
                        Ok(_) => continue,
                        Err(lmdb::Error::NotFound) => break,
                        Err(err) => return Err(err.into()),
                    }
                }

                if let Some(since_bound) = since
                    && indexed_created_at < since_bound
                {
                    break;
                }

                results.push(seq_bytes);

                match cursor.get(None, None, MDB_PREV) {
                    Ok(_) => continue,
                    Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            }
        }

        Ok(results.into_iter())
    }
}
