use lmdb::{Database, DatabaseFlags, Environment, RwTransaction, WriteFlags};

use crate::db::SearchnosDBError;

#[derive(Debug)]
pub struct ContentsStore {
    db: Database,
}

impl ContentsStore {
    pub const NAME: &'static str = "contents";

    /// Open (or create) the contents index database within the environment.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::INTEGER_KEY)?;
        Ok(Self { db })
    }

    /// Access the underlying LMDB database handle.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Store normalized content for an event sequence number.
    pub fn put<K, V>(
        &self,
        txn: &mut RwTransaction<'_>,
        key: &K,
        content: &V,
    ) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        txn.put(self.db, key, content, WriteFlags::empty())?;
        Ok(())
    }

    /// Delete any stored content for the provided key.
    pub fn delete<K>(&self, txn: &mut RwTransaction<'_>, key: &K) -> Result<(), SearchnosDBError>
    where
        K: AsRef<[u8]>,
    {
        match txn.del(self.db, key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
