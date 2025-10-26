use std::{collections::HashSet, convert::TryInto};

use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
    WriteFlags,
};
use lmdb_sys::MDB_GET_BOTH;

use crate::db::{SEQ_BYTES, SearchnosDBError};
use crate::text::{MAX_NGRAM_SIZE, char_ngrams, preferred_min_query_ngram_size};

#[derive(Debug)]
pub struct NgramIndex {
    db: Database,
}

impl NgramIndex {
    pub const NAME: &'static str = "ngram_to_events";

    /// Open (or create) the n-gram index database.
    pub fn open(env: &Environment) -> Result<Self, lmdb::Error> {
        let db = env.create_db(Some(Self::NAME), DatabaseFlags::DUP_SORT)?;
        Ok(Self { db })
    }

    /// Access the LMDB database handle for the n-gram index.
    pub fn database(&self) -> Database {
        self.db
    }

    /// Associate an n-gram with the given event sequence number.
    pub fn put<G>(
        &self,
        txn: &mut RwTransaction<'_>,
        gram: &G,
        seq: &[u8; SEQ_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        G: AsRef<[u8]>,
    {
        match txn.put(self.db, gram, seq, WriteFlags::NO_DUP_DATA) {
            Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Remove a specific n-gram mapping for an event.
    pub fn delete_entry<G>(
        &self,
        txn: &mut RwTransaction<'_>,
        gram: &G,
        seq: &[u8; SEQ_BYTES],
    ) -> Result<(), SearchnosDBError>
    where
        G: AsRef<[u8]>,
    {
        let mut cursor = txn.open_rw_cursor(self.db)?;
        let gram_bytes = gram.as_ref();
        match cursor.get(Some(gram_bytes), Some(seq), MDB_GET_BOTH) {
            Ok(_) => match cursor.del(WriteFlags::CURRENT) {
                Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
                Err(err) => Err(err.into()),
            },
            Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Iterate over all seq_bytes that match ALL search terms (AND logic)
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        terms: &[String],
    ) -> Result<impl Iterator<Item = [u8; SEQ_BYTES]> + 'env, SearchnosDBError> {
        if terms.is_empty() {
            return Ok(Vec::new().into_iter());
        }

        let mut cursor = txn.open_ro_cursor(self.db)?;
        let mut global_candidates: Option<HashSet<[u8; SEQ_BYTES]>> = None;

        for term in terms {
            let min_gram = preferred_min_query_ngram_size(term);
            let grams = char_ngrams(term, min_gram, MAX_NGRAM_SIZE);
            if grams.is_empty() {
                return Ok(Vec::new().into_iter());
            }

            let mut term_candidates: Option<HashSet<[u8; SEQ_BYTES]>> = None;

            for gram in grams {
                let gram_candidates = Self::candidates_for_gram(&mut cursor, &gram)?;
                if gram_candidates.is_empty() {
                    term_candidates = Some(HashSet::new());
                    break;
                }

                term_candidates = Some(match term_candidates.take() {
                    None => gram_candidates,
                    Some(existing) => existing.intersection(&gram_candidates).cloned().collect(),
                });
            }

            let term_candidates = term_candidates.unwrap_or_default();
            if term_candidates.is_empty() {
                return Ok(Vec::new().into_iter());
            }

            global_candidates = Some(match global_candidates.take() {
                None => term_candidates,
                Some(existing) => existing.intersection(&term_candidates).cloned().collect(),
            });

            if global_candidates.as_ref().is_some_and(|set| set.is_empty()) {
                return Ok(Vec::new().into_iter());
            }
        }

        let candidate_set = global_candidates.unwrap_or_default();
        let mut candidates: Vec<[u8; SEQ_BYTES]> = candidate_set.into_iter().collect();
        candidates.sort_unstable();

        Ok(candidates.into_iter())
    }

    fn candidates_for_gram<K: AsRef<[u8]>>(
        cursor: &mut lmdb::RoCursor<'_>,
        gram: &K,
    ) -> Result<HashSet<[u8; SEQ_BYTES]>, SearchnosDBError> {
        match cursor.iter_dup_of(gram) {
            Ok(mut iter) => {
                let mut candidates = HashSet::new();
                for (_, value) in &mut iter {
                    let seq_bytes: [u8; SEQ_BYTES] = value
                        .try_into()
                        .map_err(|_| SearchnosDBError::InvalidSeqLength(value.len()))?;
                    candidates.insert(seq_bytes);
                }
                Ok(candidates)
            }
            Err(lmdb::Error::NotFound) => Ok(HashSet::new()),
            Err(err) => Err(err.into()),
        }
    }
}
