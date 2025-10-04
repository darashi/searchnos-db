use std::convert::TryInto;

use lmdb::{Database, DatabaseFlags, Environment, RwTransaction, Transaction, WriteFlags};

use crate::db::{SEQ_BYTES, SearchnosDBError};

#[derive(Debug)]
pub struct ReplacableIndex {
    db: Database,
}

impl ReplacableIndex {
    pub const NAME: &'static str = "replacables";

    /// Open (or create) the replacable events index.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::empty())?;
        Ok(Self { db })
    }

    /// Access the LMDB database storing replacable slots.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Store the sequence number for a replacable slot key.
    pub fn put<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        slot_key: &K,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        let value = seq.to_ne_bytes();
        txn.put(self.db, slot_key, &value, WriteFlags::empty())?;
        Ok(())
    }

    /// Retrieve the sequence currently assigned to a replacable slot.
    pub fn get_seq<T, K>(&self, txn: &T, slot_key: &K) -> Result<Option<u64>, SearchnosDBError>
    where
        T: Transaction,
        K: AsRef<[u8]>,
    {
        match txn.get(self.db, slot_key) {
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

    /// Delete any state associated with a replacable slot.
    pub fn delete<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        slot_key: &K,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        match txn.del(self.db, slot_key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Remove a specific `(slot_key, seq)` association.
    pub fn delete_entry<K>(
        &self,
        txn: &mut RwTransaction<'_>,
        slot_key: &K,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        let value = seq.to_ne_bytes();
        match txn.del(self.db, slot_key, Some(&value)) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
