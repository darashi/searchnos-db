use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_LAST_DUP, MDB_PREV_DUP, MDB_SET_KEY};

use super::common::{decode_created_at_seq_value, delete_dup_value, put_no_dup};
use crate::db::{AUTHOR_INDEX_VALUE_BYTES, SEQ_BYTES, SearchnosDBError};

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
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError> {
        let key = Self::kind_key(kind);
        put_no_dup(self.db, txn, &key, value)
    }

    /// Remove a specific `(created_at, seq)` tuple for a kind.
    pub fn delete_value(
        &self,
        txn: &mut RwTransaction<'_>,
        kind: u16,
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError> {
        let key = Self::kind_key(kind);
        let mut cursor = txn.open_rw_cursor(self.db)?;
        delete_dup_value(&mut cursor, &key, value)
    }

    /// Decode value into (created_at, seq_bytes)
    pub fn decode_value(bytes: &[u8]) -> Result<(u64, [u8; SEQ_BYTES]), SearchnosDBError> {
        decode_created_at_seq_value(bytes)
    }

    /// Iterate over all seq_bytes for the given kinds
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        kinds: &[u16],
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'env, SearchnosDBError> {
        if kinds.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let cursor = txn.open_ro_cursor(self.db)?;
        let mut results = Vec::new();

        for kind in kinds {
            let key = Self::kind_key(*kind);

            match cursor.get(Some(&key), None, MDB_SET_KEY) {
                Ok(_) => match cursor.get(None, None, MDB_LAST_DUP) {
                    Ok(_) => loop {
                        let (_key_bytes, value_bytes) =
                            match cursor.get(None, None, MDB_GET_CURRENT) {
                                Ok((Some(key_bytes), value)) => {
                                    if key_bytes != key.as_slice() {
                                        break;
                                    }
                                    (key_bytes, value)
                                }
                                Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                                Err(err) => return Err(err.into()),
                            };

                        let (_indexed_created_at, seq_bytes) = Self::decode_value(value_bytes)?;
                        results.push(seq_bytes);

                        match cursor.get(None, None, MDB_PREV_DUP) {
                            Ok(_) => continue,
                            Err(lmdb::Error::NotFound) => break,
                            Err(err) => return Err(err.into()),
                        }
                    },
                    Err(lmdb::Error::NotFound) => continue,
                    Err(err) => return Err(err.into()),
                },
                Err(lmdb::Error::NotFound) => continue,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(results.into_iter())
    }
}
