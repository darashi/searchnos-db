use std::convert::TryInto;

use lmdb::{Database, DatabaseFlags, Environment, RwTransaction, Transaction, WriteFlags};

use crate::db::{SEQ_BYTES, SearchnosDBError};

#[derive(Debug)]
pub struct DeletionIndex {
    db: Database,
}

impl DeletionIndex {
    pub const NAME: &'static str = "deletions";

    /// Open (or create) the deletion markers index.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::empty())?;
        Ok(Self { db })
    }

    /// Access the LMDB database used to store deletion markers.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Record the latest deletion sequence for a composite key.
    pub fn put_marker<K>(
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

    /// Drop any deletion marker stored under the provided key.
    pub fn delete_marker<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        key: &K,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        match txn.del(self.db, key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Fetch the stored deletion marker sequence, if any.
    pub fn get_marker<T, K>(&self, txn: &T, key: &K) -> Result<Option<u64>, SearchnosDBError>
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
}
