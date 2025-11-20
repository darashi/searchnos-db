use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_PREV};

use super::common::{
    append_ts_seq, position_cursor_at_prefix_end, put_keyed_seq, split_ts_seq_from_key,
};
use crate::db::{SEQ_BYTES, SearchnosDBError};

#[derive(Debug)]
pub struct PubkeyKindIndex {
    db: Database,
}

impl PubkeyKindIndex {
    pub const NAME: &'static str = "pubkey_kind_to_events";

    /// Open (or create) the combined pubkey/kind index database.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::DUP_SORT)?;
        Ok(Self { db })
    }

    /// Access the LMDB database backing the composite index.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Build a composite key combining author bytes with the event kind.
    pub fn key<P>(pubkey: &P, kind: u16) -> Vec<u8>
    where
        P: AsRef<[u8]>,
    {
        let pubkey_bytes = pubkey.as_ref();
        let mut key = Vec::with_capacity(pubkey_bytes.len() + std::mem::size_of::<u16>());
        key.extend_from_slice(pubkey_bytes);
        key.extend_from_slice(&kind.to_be_bytes());
        key
    }

    /// Insert a `(created_at, seq)` tuple for a pubkey/kind combination.
    pub fn put<P>(
        &self,
        txn: &mut RwTransaction<'_>,
        pubkey: &P,
        kind: u16,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        P: AsRef<[u8]>,
    {
        let key = Self::key_with_suffix(pubkey, kind, created_at, seq);
        put_keyed_seq(self.db, txn, &key, seq)
    }

    /// Remove a specific tuple for a pubkey/kind combination.
    pub fn delete_value<P>(
        &self,
        txn: &mut RwTransaction<'_>,
        pubkey: &P,
        kind: u16,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        P: AsRef<[u8]>,
    {
        let key = Self::key_with_suffix(pubkey, kind, created_at, seq);
        match txn.del(self.db, &key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn key_with_suffix<P>(pubkey: &P, kind: u16, created_at: u64, seq: u64) -> Vec<u8>
    where
        P: AsRef<[u8]>,
    {
        let mut key = Self::key(pubkey, kind);
        append_ts_seq(&mut key, created_at, seq);
        key
    }

    /// Iterate over all seq_bytes for the given pubkey-kind combinations, optionally filtering by until
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        pubkeys: &[&[u8]],
        kinds: &[u16],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'env, SearchnosDBError> {
        if pubkeys.is_empty() || kinds.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let mut cursor = txn.open_ro_cursor(self.db)?;
        let mut results = Vec::new();

        for pubkey in pubkeys {
            for kind in kinds {
                let combination_key = Self::key(pubkey, *kind);

                if !position_cursor_at_prefix_end(&mut cursor, &combination_key)? {
                    continue;
                }

                loop {
                    let (key_bytes, _) = match cursor.get(None, None, MDB_GET_CURRENT) {
                        Ok((Some(key), value)) => (key, value),
                        Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                        Err(err) => return Err(err.into()),
                    };

                    if !key_bytes.starts_with(&combination_key) {
                        break;
                    }

                    let (indexed_created_at, seq_bytes) = split_ts_seq_from_key(key_bytes)?;

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
        }

        Ok(results.into_iter())
    }
}
