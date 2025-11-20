use std::convert::TryInto;

use lmdb::{Cursor, Database, DatabaseFlags, Environment, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::{MDB_NEXT, MDB_SET_RANGE};

use crate::db::{SEQ_BYTES, SearchnosDBError};

#[derive(Debug)]
pub struct EventIdIndex {
    db: Database,
}

impl EventIdIndex {
    pub const NAME: &'static str = "event_id_to_events";

    /// Open (or create) the event-id index.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::empty())?;
        Ok(Self { db })
    }

    /// Access the LMDB database used for event-id lookups.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Insert a mapping from event-id bytes to sequence number.
    pub fn put<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        key: &K,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        let value = seq.to_ne_bytes();
        txn.put(self.db, key, &value, WriteFlags::empty())?;
        Ok(())
    }

    /// Resolve the sequence number for an event-id if it exists.
    pub fn get_seq<T, K>(&self, txn: &T, key: &K) -> Result<Option<u64>, SearchnosDBError>
    where
        T: Transaction,
        K: AsRef<[u8]>,
    {
        match txn.get(self.db, key) {
            Ok(bytes) => {
                let seq_bytes: [u8; SEQ_BYTES] = bytes
                    .try_into()
                    .map_err(|_| SearchnosDBError::InvalidSeqLength(bytes.len()))?;
                Ok(Some(u64::from_ne_bytes(seq_bytes)))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Remove the mapping for an event-id, if present.
    pub fn delete<K>(&self, txn: &mut RwTransaction<'_>, key: &K) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        match txn.del(self.db, key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Iterate over all seq_bytes that match the given event IDs (prefix match)
    pub fn iter_candidates<'txn, T>(
        &self,
        txn: &'txn T,
        ids: &[&[u8]],
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'txn, SearchnosDBError>
    where
        T: Transaction,
    {
        if ids.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let cursor = txn.open_ro_cursor(self.db)?;
        let mut results = Vec::new();

        for id_bytes in ids {
            let mut current = cursor.get(Some(id_bytes), None, MDB_SET_RANGE);
            loop {
                let (_key_bytes, value_bytes) = match current {
                    Ok((Some(key), value)) => {
                        if !key.starts_with(id_bytes) {
                            break;
                        }
                        (key, value)
                    }
                    Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                };

                let seq_bytes: [u8; SEQ_BYTES] = value_bytes
                    .try_into()
                    .map_err(|_| SearchnosDBError::InvalidSeqLength(value_bytes.len()))?;
                results.push(seq_bytes);

                current = cursor.get(None, None, MDB_NEXT);
            }
        }

        Ok(results.into_iter())
    }
}
