use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
};

use super::common::{
    decode_created_at_seq_value, delete_dup_value, encode_created_at_seq_value, put_no_dup,
};
use crate::db::{AUTHOR_INDEX_VALUE_BYTES, SEQ_BYTES, SearchnosDBError};

#[derive(Debug)]
pub struct PubkeyIndex {
    db: Database,
}

impl PubkeyIndex {
    pub const NAME: &'static str = "pubkey_to_events";

    /// Open (or create) the author index database.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::DUP_SORT)?;
        Ok(Self { db })
    }

    /// Access the LMDB database backing the author index.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Encode (created_at, seq) pair into the value format
    pub fn encode_value(created_at: u64, seq: u64) -> [u8; AUTHOR_INDEX_VALUE_BYTES] {
        encode_created_at_seq_value(created_at, seq)
    }

    /// Decode value into (created_at, seq_bytes)
    pub fn decode_value(bytes: &[u8]) -> Result<(u64, [u8; SEQ_BYTES]), SearchnosDBError> {
        decode_created_at_seq_value(bytes)
    }

    /// Insert a `(created_at, seq)` tuple for an author.
    pub fn put<A>(
        &self,
        txn: &mut RwTransaction<'_>,
        author: &A,
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        A: AsRef<[u8]>,
    {
        put_no_dup(self.db, txn, author, value)
    }

    /// Remove a specific tuple for an author.
    pub fn delete_value<A>(
        &self,
        txn: &mut RwTransaction<'_>,
        author: &A,
        value: &[u8; AUTHOR_INDEX_VALUE_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        A: AsRef<[u8]>,
    {
        let mut cursor = txn.open_rw_cursor(self.db)?;
        delete_dup_value(&mut cursor, author.as_ref(), value)
    }

    /// Iterate over all seq_bytes for the given authors, optionally filtering by until
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        authors: &[&[u8]],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'env, SearchnosDBError> {
        if authors.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let mut cursor = txn.open_ro_cursor(self.db)?;
        let mut results = Vec::new();

        for author in authors {
            match cursor.iter_dup_of(author) {
                Ok(mut iter) => {
                    for (_key, value) in iter.by_ref() {
                        let (indexed_created_at, seq_bytes) = Self::decode_value(value)?;

                        if let Some(until) = until
                            && indexed_created_at > until
                        {
                            break;
                        }

                        if let Some(since) = since
                            && indexed_created_at < since
                        {
                            break;
                        }

                        results.push(seq_bytes);
                    }
                }
                Err(lmdb::Error::NotFound) => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(results.into_iter())
    }
}
