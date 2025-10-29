use std::convert::TryInto;

use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoCursor, RoTransaction, RwTransaction,
    Transaction, WriteFlags,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_LAST, MDB_PREV, MDB_SET_RANGE};

use crate::db::{CREATED_AT_BYTES, SEQ_BYTES, SearchnosDBError};

pub struct CreatedAtIter<'txn> {
    cursor: RoCursor<'txn>,
    since: Option<u64>,
    finished: bool,
}

#[derive(Debug)]
pub struct CreatedAtIndex {
    db: Database,
}

impl CreatedAtIndex {
    pub const NAME: &'static str = "created_at_to_events";

    /// Open (or create) the created-at index inside the environment.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(
            Some(Self::NAME),
            DatabaseFlags::DUP_SORT | DatabaseFlags::INTEGER_KEY,
        )?;
        Ok(Self { db })
    }

    /// Access the LMDB database backing this index.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Insert a mapping from timestamp to sequence number.
    pub fn put(
        &self,
        txn: &mut RwTransaction<'_>,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = created_at.to_ne_bytes();
        let value = seq.to_ne_bytes();
        self.put_raw(txn, &key, &value)
    }

    /// Insert a raw key/value pair into the index.
    pub fn put_raw(
        &self,
        txn: &mut RwTransaction<'_>,
        key: &[u8; CREATED_AT_BYTES],
        value: &[u8; SEQ_BYTES],
    ) -> Result<(), SearchnosDBError> {
        match txn.put(self.db, key, value, WriteFlags::NO_DUP_DATA) {
            Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Parse a timestamp from raw LMDB key bytes.
    pub fn created_at_from_bytes(bytes: &[u8]) -> Result<u64, SearchnosDBError> {
        let created_at_bytes: [u8; CREATED_AT_BYTES] = bytes
            .try_into()
            .map_err(|_| SearchnosDBError::InvalidCreatedAtLength(bytes.len()))?;
        Ok(u64::from_ne_bytes(created_at_bytes))
    }

    /// Position cursor to start scanning from (respecting until)
    fn position_cursor(
        cursor: &mut RoCursor<'_>,
        until: Option<u64>,
    ) -> Result<bool, SearchnosDBError> {
        if let Some(until) = until {
            let search_key = until.to_ne_bytes();
            match cursor.get(Some(&search_key), None, MDB_SET_RANGE) {
                Ok(_) => loop {
                    let (key_bytes, _) = match cursor.get(None, None, MDB_GET_CURRENT) {
                        Ok((Some(key), value)) => (key, value),
                        Ok((None, _)) | Err(lmdb::Error::NotFound) => return Ok(false),
                        Err(err) => return Err(err.into()),
                    };

                    let created_at = Self::created_at_from_bytes(key_bytes)?;
                    if created_at <= until {
                        return Ok(true);
                    }

                    match cursor.get(None, None, MDB_PREV) {
                        Ok(_) => continue,
                        Err(lmdb::Error::NotFound) => return Ok(false),
                        Err(err) => return Err(err.into()),
                    }
                },
                Err(lmdb::Error::NotFound) => {}
                Err(err) => return Err(err.into()),
            }
        }

        match cursor.get(None, None, MDB_LAST) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Iterate over all seq_bytes in reverse chronological order (respecting since and until)
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<CreatedAtIter<'env>, SearchnosDBError> {
        let mut cursor = txn.open_ro_cursor(self.db)?;

        let positioned = Self::position_cursor(&mut cursor, until)?;
        Ok(CreatedAtIter {
            cursor,
            since,
            finished: !positioned,
        })
    }
}

impl<'txn> Iterator for CreatedAtIter<'txn> {
    type Item = Result<[u8; SEQ_BYTES], SearchnosDBError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let (key_bytes, value_bytes) = match self.cursor.get(None, None, MDB_GET_CURRENT) {
            Ok((Some(key), value)) => (key, value),
            Ok((None, _)) | Err(lmdb::Error::NotFound) => {
                self.finished = true;
                return None;
            }
            Err(err) => {
                self.finished = true;
                return Some(Err(err.into()));
            }
        };

        if let Some(since) = self.since {
            match CreatedAtIndex::created_at_from_bytes(key_bytes) {
                Ok(created_at) if created_at < since => {
                    self.finished = true;
                    return None;
                }
                Ok(_) => {}
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            }
        }

        let seq_bytes: [u8; SEQ_BYTES] = match value_bytes.try_into() {
            Ok(bytes) => bytes,
            Err(_) => {
                self.finished = true;
                return Some(Err(SearchnosDBError::InvalidSeqLength(value_bytes.len())));
            }
        };

        match self.cursor.get(None, None, MDB_PREV) {
            Ok(_) => {}
            Err(lmdb::Error::NotFound) => {
                self.finished = true;
            }
            Err(err) => {
                self.finished = true;
                return Some(Err(err.into()));
            }
        }

        Some(Ok(seq_bytes))
    }
}
