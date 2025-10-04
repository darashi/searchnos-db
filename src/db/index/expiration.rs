use std::convert::TryInto;

use lmdb::{Cursor, Database, DatabaseFlags, Environment, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::{MDB_FIRST, MDB_GET_BOTH, MDB_NEXT};

use crate::db::{EXPIRATION_BYTES, SEQ_BYTES, SearchnosDBError};

#[derive(Debug)]
pub struct ExpirationIndex {
    db: Database,
}

impl ExpirationIndex {
    pub const NAME: &'static str = "expiration_to_events";

    /// Open (or create) the expiration index database.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(
            Some(Self::NAME),
            DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT,
        )?;
        Ok(Self { db })
    }

    /// Access the LMDB database handle for the expiration index.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Insert an expiration timestamp mapping to a sequence number.
    pub fn put(
        &self,
        txn: &mut RwTransaction<'_>,
        expiration: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = expiration.to_ne_bytes();
        let value = seq.to_ne_bytes();
        match txn.put(self.db, &key, &value, WriteFlags::NO_DUP_DATA) {
            Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Remove a specific expiration/sequence pair from the index.
    pub fn delete_entry(
        &self,
        txn: &mut RwTransaction<'_>,
        expiration: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = expiration.to_ne_bytes();
        let value = seq.to_ne_bytes();
        let mut cursor = txn.open_rw_cursor(self.db)?;
        match cursor.get(Some(&key), Some(&value), MDB_GET_BOTH) {
            Ok(_) => match cursor.del(WriteFlags::CURRENT) {
                Ok(()) => Ok(()),
                Err(lmdb::Error::NotFound) => Ok(()),
                Err(err) => Err(err.into()),
            },
            Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Parse an expiration timestamp from raw key bytes.
    pub fn expiration_from_bytes(bytes: &[u8]) -> Result<u64, SearchnosDBError> {
        let expiration_bytes: [u8; EXPIRATION_BYTES] = bytes
            .try_into()
            .map_err(|_| SearchnosDBError::InvalidExpirationLength(bytes.len()))?;
        Ok(u64::from_ne_bytes(expiration_bytes))
    }

    /// Collect expired entries whose timestamp is not greater than `now`.
    pub fn collect_expired<T>(
        &self,
        txn: &T,
        now: u64,
    ) -> Result<Vec<(u64, [u8; SEQ_BYTES])>, SearchnosDBError>
    where
        T: Transaction,
    {
        let cursor = txn.open_ro_cursor(self.db)?;
        let mut results = Vec::new();
        let mut entry = cursor.get(None, None, MDB_FIRST);

        while let Ok((Some(key_bytes), value_bytes)) = entry {
            let expiration = Self::expiration_from_bytes(key_bytes)?;
            if expiration > now {
                break;
            }

            let seq_bytes: [u8; SEQ_BYTES] = value_bytes
                .try_into()
                .map_err(|_| SearchnosDBError::InvalidSeqLength(value_bytes.len()))?;
            results.push((expiration, seq_bytes));
            entry = cursor.get(None, None, MDB_NEXT);
        }

        match entry {
            Ok(_) | Err(lmdb::Error::NotFound) => {}
            Err(err) => return Err(err.into()),
        }

        Ok(results)
    }
}
