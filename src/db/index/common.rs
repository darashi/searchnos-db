use lmdb::{Cursor, Database, RoCursor, RwTransaction, WriteFlags};
use lmdb_sys::{MDB_NEXT, MDB_PREV, MDB_SET_RANGE};

use crate::db::{CREATED_AT_BYTES, SEQ_BYTES, SearchnosDBError};

pub const TS_SEQ_BYTES: usize = CREATED_AT_BYTES + SEQ_BYTES;

/// Append created_at/seq suffix (big-endian) to the provided key buffer.
pub fn append_ts_seq(key: &mut Vec<u8>, created_at: u64, seq: u64) {
    key.extend_from_slice(&created_at.to_be_bytes());
    key.extend_from_slice(&seq.to_be_bytes());
}

/// Extract created_at and seq suffix from a key.
pub fn split_ts_seq_from_key(key: &[u8]) -> Result<(u64, [u8; SEQ_BYTES]), SearchnosDBError> {
    if key.len() < TS_SEQ_BYTES {
        return Err(SearchnosDBError::InvalidKeyLength(key.len()));
    }
    let suffix = &key[key.len() - TS_SEQ_BYTES..];
    let mut created_at_bytes = [0u8; CREATED_AT_BYTES];
    created_at_bytes.copy_from_slice(&suffix[..CREATED_AT_BYTES]);
    let mut seq_bytes_be = [0u8; SEQ_BYTES];
    seq_bytes_be.copy_from_slice(&suffix[CREATED_AT_BYTES..]);
    let seq = u64::from_be_bytes(seq_bytes_be);
    Ok((u64::from_be_bytes(created_at_bytes), seq.to_ne_bytes()))
}

/// Position a cursor at the last entry matching the prefix.
pub fn position_cursor_at_prefix_end(
    cursor: &mut RoCursor<'_>,
    prefix: &[u8],
) -> Result<bool, SearchnosDBError> {
    // Start at the first key >= prefix; then walk forward to the last with the same prefix.
    let positioned = match cursor.get(Some(prefix), None, MDB_SET_RANGE) {
        Ok((Some(key), _)) => {
            if key.starts_with(prefix) {
                true
            } else {
                false
            }
        }
        Ok((None, _)) | Err(lmdb::Error::NotFound) => false,
        Err(err) => return Err(err.into()),
    };

    if !positioned {
        return Ok(false);
    }

    loop {
        match cursor.get(None, None, MDB_NEXT) {
            Ok((Some(next_key), _)) => {
                if !next_key.starts_with(prefix) {
                    // Step back to the last matching entry.
                    match cursor.get(None, None, MDB_PREV) {
                        Ok((Some(prev_key), _)) => return Ok(prev_key.starts_with(prefix)),
                        Ok((None, _)) | Err(lmdb::Error::NotFound) => return Ok(false),
                        Err(err) => return Err(err.into()),
                    }
                }
            }
            Ok((None, _)) | Err(lmdb::Error::NotFound) => {
                // Already at last entry with the prefix.
                return Ok(true);
            }
            Err(err) => return Err(err.into()),
        }
    }
}

/// Helper to insert a key->seq mapping, ignoring duplicates.
pub fn put_keyed_seq<K>(
    db: Database,
    txn: &mut RwTransaction<'_>,
    key: &K,
    seq: u64,
) -> Result<(), SearchnosDBError>
where
    K: AsRef<[u8]>,
{
    let value = seq.to_ne_bytes();
    match txn.put(db, key, &value, WriteFlags::NO_OVERWRITE) {
        Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
        Err(err) => Err(err.into()),
    }
}
