use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoCursor, RoTransaction, RwTransaction,
    Transaction, WriteFlags,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_PREV};

use super::common::{
    append_created_at, position_cursor_at_prefix_end, put_keyed_seq, seq_from_value,
    split_created_at_from_key,
};
use crate::db::{SEQ_BYTES, SearchnosDBError};

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

    /// Insert a `(created_at, seq)` tuple for an author.
    pub fn put<A>(
        &self,
        txn: &mut RwTransaction<'_>,
        author: &A,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        A: AsRef<[u8]>,
    {
        let key = Self::key_with_suffix(author, created_at);
        put_keyed_seq(self.db, txn, &key, seq)
    }

    /// Remove a specific tuple for an author.
    pub fn delete_value<A>(
        &self,
        txn: &mut RwTransaction<'_>,
        author: &A,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        A: AsRef<[u8]>,
    {
        let key = Self::key_with_suffix(author, created_at);
        let value = seq.to_ne_bytes();
        let mut cursor = txn.open_rw_cursor(self.db)?;
        match cursor.get(Some(&key), Some(&value), lmdb_sys::MDB_GET_BOTH) {
            Ok(_) => match cursor.del(WriteFlags::CURRENT) {
                Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
                Err(err) => Err(err.into()),
            },
            Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn key_with_suffix<A>(author: &A, created_at: u64) -> Vec<u8>
    where
        A: AsRef<[u8]>,
    {
        let mut key = Vec::with_capacity(author.as_ref().len() + 16);
        key.extend_from_slice(author.as_ref());
        append_created_at(&mut key, created_at);
        key
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
            let prefix = *author;
            if !Self::position_cursor_for_author(&mut cursor, prefix)? {
                continue;
            }

            loop {
                let (key_bytes, value_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                    Ok((Some(key), value)) => (key, value),
                    Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                };

                if !key_bytes.starts_with(prefix) {
                    break;
                }

                let indexed_created_at = split_created_at_from_key(key_bytes)?;
                let seq_bytes = seq_from_value(value_bytes)?;

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

        Ok(results.into_iter())
    }

    fn position_cursor_for_author(
        cursor: &mut RoCursor<'_>,
        author: &[u8],
    ) -> Result<bool, SearchnosDBError> {
        position_cursor_at_prefix_end(cursor, author)
    }
}
