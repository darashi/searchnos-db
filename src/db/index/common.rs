use lmdb::{Cursor, Database, RwCursor, RwTransaction, WriteFlags};
use lmdb_sys::MDB_GET_BOTH;

use crate::db::{AUTHOR_INDEX_VALUE_BYTES, CREATED_AT_BYTES, SEQ_BYTES, SearchnosDBError};

/// Encode a (created_at, seq) pair into the value format used by multiple indexes
pub fn encode_created_at_seq_value(created_at: u64, seq: u64) -> [u8; AUTHOR_INDEX_VALUE_BYTES] {
    let mut buffer = [0u8; AUTHOR_INDEX_VALUE_BYTES];
    buffer[..CREATED_AT_BYTES].copy_from_slice(&created_at.to_be_bytes());
    buffer[CREATED_AT_BYTES..].copy_from_slice(&seq.to_be_bytes());
    buffer
}

/// Decode a value from the format used by multiple indexes into (created_at, seq_bytes)
pub fn decode_created_at_seq_value(
    bytes: &[u8],
) -> Result<(u64, [u8; SEQ_BYTES]), SearchnosDBError> {
    if bytes.len() != AUTHOR_INDEX_VALUE_BYTES {
        return Err(SearchnosDBError::InvalidIndexValueLength(bytes.len()));
    }

    let mut created_at_bytes = [0u8; CREATED_AT_BYTES];
    created_at_bytes.copy_from_slice(&bytes[..CREATED_AT_BYTES]);
    let created_at = u64::from_be_bytes(created_at_bytes);

    let mut seq_bytes_be = [0u8; SEQ_BYTES];
    seq_bytes_be.copy_from_slice(&bytes[CREATED_AT_BYTES..]);
    let seq = u64::from_be_bytes(seq_bytes_be);

    Ok((created_at, seq.to_ne_bytes()))
}

/// Helper to delete a specific duplicate value from a DUP_SORT database
pub fn delete_dup_value(
    cursor: &mut RwCursor<'_>,
    key: &[u8],
    value: &[u8],
) -> Result<(), SearchnosDBError> {
    match cursor.get(Some(key), Some(value), MDB_GET_BOTH) {
        Ok(_) => match cursor.del(WriteFlags::CURRENT) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        },
        Err(lmdb::Error::NotFound) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Helper to put a value with NO_DUP_DATA flag, treating KeyExist as success
pub fn put_no_dup<K, V>(
    db: Database,
    txn: &mut RwTransaction<'_>,
    key: &K,
    value: &V,
) -> Result<(), SearchnosDBError>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    match txn.put(db, key, value, WriteFlags::NO_DUP_DATA) {
        Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
        Err(err) => Err(err.into()),
    }
}
