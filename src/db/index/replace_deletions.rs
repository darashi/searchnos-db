use std::convert::TryInto;

use lmdb::{Cursor, Database, DatabaseFlags, Environment, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::{MDB_GET_BOTH, MDB_SET_RANGE};

use crate::db::{CREATED_AT_BYTES, SEQ_BYTES, SearchnosDBError};

const HASH_BYTES: usize = 32;
const KEY_BYTES: usize = HASH_BYTES + CREATED_AT_BYTES;

#[derive(Debug)]
pub struct ReplaceDeletionIndex {
    db: Database,
}

impl ReplaceDeletionIndex {
    pub const NAME: &'static str = "replace_deletions";

    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::DUP_SORT)?;
        Ok(Self { db })
    }

    pub fn database(&self) -> Database {
        self.db
    }

    pub fn key(hash: &[u8; HASH_BYTES], created_at: u64) -> [u8; KEY_BYTES] {
        let mut key = [0u8; KEY_BYTES];
        key[..HASH_BYTES].copy_from_slice(hash);
        key[HASH_BYTES..].copy_from_slice(&created_at.to_be_bytes());
        key
    }

    pub fn put(
        &self,
        txn: &mut RwTransaction<'_>,
        hash: &[u8; HASH_BYTES],
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = Self::key(hash, created_at);
        let value = seq.to_ne_bytes();
        match txn.put(self.db, &key, &value, WriteFlags::NO_DUP_DATA) {
            Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn delete_entry(
        &self,
        txn: &mut RwTransaction<'_>,
        hash: &[u8; HASH_BYTES],
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = Self::key(hash, created_at);
        let value = seq.to_ne_bytes();
        let mut cursor = txn.open_rw_cursor(self.db)?;
        match cursor.get(Some(&key), Some(&value), MDB_GET_BOTH) {
            Ok(_) => match cursor.del(WriteFlags::CURRENT) {
                Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
                Err(err) => Err(err.into()),
            },
            Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn get_blocking_seq<T>(
        &self,
        txn: &T,
        hash: &[u8; HASH_BYTES],
        created_at: u64,
    ) -> Result<Option<u64>, SearchnosDBError>
    where
        T: Transaction,
    {
        let key = Self::key(hash, created_at);
        let cursor = txn.open_ro_cursor(self.db)?;
        match cursor.get(Some(&key), None, MDB_SET_RANGE) {
            Ok((Some(found_key), value)) if found_key.starts_with(hash) => {
                let seq_bytes: [u8; SEQ_BYTES] = value
                    .try_into()
                    .map_err(|_| SearchnosDBError::InvalidSeqLength(value.len()))?;
                Ok(Some(u64::from_ne_bytes(seq_bytes)))
            }
            Ok(_) | Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}
