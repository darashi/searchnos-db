use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_LAST_DUP, MDB_PREV_DUP, MDB_SET_KEY};

use super::common::{decode_created_at_seq_value, delete_dup_value, put_no_dup};
use crate::db::{AUTHOR_INDEX_VALUE_BYTES, SEQ_BYTES, SearchnosDBError};

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
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        P: AsRef<[u8]>,
    {
        let key = Self::key(pubkey, kind);
        put_no_dup(self.db, txn, &key, value)
    }

    /// Remove a specific tuple for a pubkey/kind combination.
    pub fn delete_value<P>(
        &self,
        txn: &mut RwTransaction<'_>,
        pubkey: &P,
        kind: u16,
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        P: AsRef<[u8]>,
    {
        let key = Self::key(pubkey, kind);
        let mut cursor = txn.open_rw_cursor(self.db)?;
        delete_dup_value(&mut cursor, &key, value)
    }

    /// Decode value into (created_at, seq_bytes)
    pub fn decode_value(bytes: &[u8]) -> Result<(u64, [u8; SEQ_BYTES]), SearchnosDBError> {
        decode_created_at_seq_value(bytes)
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

        let cursor = txn.open_ro_cursor(self.db)?;
        let mut results = Vec::new();

        for pubkey in pubkeys {
            for kind in kinds {
                let combination_key = Self::key(pubkey, *kind);

                match cursor.get(Some(&combination_key), None, MDB_SET_KEY) {
                    Ok(_) => match cursor.get(None, None, MDB_LAST_DUP) {
                        Ok(_) => loop {
                            let value_bytes = match cursor.get(None, None, MDB_GET_CURRENT) {
                                Ok((Some(key), value)) => {
                                    if key != combination_key.as_slice() {
                                        break;
                                    }
                                    value
                                }
                                Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                                Err(err) => return Err(err.into()),
                            };

                            let (indexed_created_at, seq_bytes) = Self::decode_value(value_bytes)?;

                            if let Some(until) = until
                                && indexed_created_at > until
                            {
                                match cursor.get(None, None, MDB_PREV_DUP) {
                                    Ok(_) => continue,
                                    Err(lmdb::Error::NotFound) => break,
                                    Err(err) => return Err(err.into()),
                                }
                            }

                            if let Some(since) = since
                                && indexed_created_at < since
                            {
                                break;
                            }

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
        }

        Ok(results.into_iter())
    }
}
