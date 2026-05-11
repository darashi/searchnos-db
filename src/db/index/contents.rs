use lmdb::{Cursor, Database, DatabaseFlags, Environment, RoCursor, RwTransaction, WriteFlags};
use lmdb_sys::{MDB_LAST, MDB_PREV, MDB_SET_RANGE};

use crate::db::{CREATED_AT_BYTES, SEQ_BYTES, SearchnosDBError};

const CONTENTS_KEY_BYTES: usize = CREATED_AT_BYTES + SEQ_BYTES;

#[derive(Debug)]
pub struct ContentsStore {
    db: Database,
}

impl ContentsStore {
    pub const NAME: &'static str = "contents";

    /// Open (or create) the contents index database within the environment.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::empty())?;
        Ok(Self { db })
    }

    /// Access the underlying LMDB database handle.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Build a key ordered by created_at and then sequence number.
    pub fn key(created_at: u64, seq: u64) -> [u8; CONTENTS_KEY_BYTES] {
        let mut key = [0u8; CONTENTS_KEY_BYTES];
        key[..CREATED_AT_BYTES].copy_from_slice(&created_at.to_be_bytes());
        key[CREATED_AT_BYTES..].copy_from_slice(&seq.to_be_bytes());
        key
    }

    /// Decode a contents key into created_at and native-endian seq bytes.
    pub fn split_key(key: &[u8]) -> Result<(u64, [u8; SEQ_BYTES]), SearchnosDBError> {
        if key.len() != CONTENTS_KEY_BYTES {
            return Err(SearchnosDBError::InvalidKeyLength(key.len()));
        }

        let mut created_at_bytes = [0u8; CREATED_AT_BYTES];
        created_at_bytes.copy_from_slice(&key[..CREATED_AT_BYTES]);

        let mut seq_bytes_be = [0u8; SEQ_BYTES];
        seq_bytes_be.copy_from_slice(&key[CREATED_AT_BYTES..]);
        let seq = u64::from_be_bytes(seq_bytes_be);

        Ok((u64::from_be_bytes(created_at_bytes), seq.to_ne_bytes()))
    }

    /// Position a cursor at the newest content row not newer than `until`.
    pub fn position_cursor(
        cursor: &mut RoCursor<'_>,
        until: Option<u64>,
    ) -> Result<bool, SearchnosDBError> {
        if let Some(until) = until {
            let search_key = Self::key(until, u64::MAX);
            match cursor.get(Some(&search_key), None, MDB_SET_RANGE) {
                Ok((Some(key), _)) => {
                    let (created_at, _) = Self::split_key(key)?;
                    if created_at <= until {
                        return Ok(true);
                    }

                    return match cursor.get(None, None, MDB_PREV) {
                        Ok((Some(prev_key), _)) => {
                            let (created_at, _) = Self::split_key(prev_key)?;
                            Ok(created_at <= until)
                        }
                        Ok((None, _)) | Err(lmdb::Error::NotFound) => Ok(false),
                        Err(err) => Err(err.into()),
                    };
                }
                Ok((None, _)) | Err(lmdb::Error::NotFound) => {}
                Err(err) => return Err(err.into()),
            }
        }

        match cursor.get(None, None, MDB_LAST) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Store normalized content for an event.
    pub fn put<V>(
        &self,
        txn: &mut RwTransaction<'_>,
        created_at: u64,
        seq: u64,
        content: &V,
    ) -> Result<(), SearchnosDBError>
    where
        V: AsRef<[u8]>,
    {
        let key = Self::key(created_at, seq);
        txn.put(self.db, &key, content, WriteFlags::empty())?;
        Ok(())
    }

    /// Delete stored content for the provided event.
    pub fn delete(
        &self,
        txn: &mut RwTransaction<'_>,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError> {
        let key = Self::key(created_at, seq);
        match txn.del(self.db, &key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
