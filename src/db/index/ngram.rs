use std::collections::BTreeSet;

use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction,
    WriteFlags,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_PREV};

use super::common::{
    TS_SEQ_BYTES, append_ts_seq, position_cursor_at_prefix_end, split_ts_seq_from_key,
};
use crate::db::{SEQ_BYTES, SearchnosDBError};
use crate::text::{MAX_NGRAM_SIZE, char_ngrams, preferred_min_query_ngram_size};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PostingEntry {
    created_at: u64,
    seq: [u8; SEQ_BYTES],
}

#[derive(Debug)]
struct PostingCursor {
    entries: Vec<PostingEntry>,
    index: usize,
}

impl PostingCursor {
    fn new(entries: Vec<PostingEntry>) -> Self {
        Self { entries, index: 0 }
    }

    fn current(&self) -> Option<PostingEntry> {
        self.entries.get(self.index).copied()
    }

    fn advance(&mut self) {
        if self.index + 1 < self.entries.len() {
            self.index += 1;
        } else {
            self.index = self.entries.len();
        }
    }
}

pub struct NgramCandidates {
    postings: Vec<PostingCursor>,
}

impl NgramCandidates {
    fn empty() -> Self {
        Self {
            postings: Vec::new(),
        }
    }

    fn new(postings: Vec<PostingCursor>) -> Self {
        Self { postings }
    }
}

impl Iterator for NgramCandidates {
    type Item = Result<[u8; SEQ_BYTES], SearchnosDBError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.postings.is_empty() {
            return None;
        }

        loop {
            let mut target: Option<PostingEntry> = None;
            for posting in &self.postings {
                let entry = posting.current()?;

                if target.is_none_or(|existing| entry > existing) {
                    target = Some(entry);
                }
            }

            let target = target?;

            let all_match = self
                .postings
                .iter()
                .all(|posting| posting.current() == Some(target));

            if all_match {
                for posting in &mut self.postings {
                    posting.advance();
                }
                return Some(Ok(target.seq));
            }

            for posting in &mut self.postings {
                if posting.current() == Some(target) {
                    posting.advance();
                }
            }
        }
    }
}

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
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        G: AsRef<[u8]>,
    {
        let key = Self::key_with_suffix(gram, created_at, seq);
        match txn.put(self.db, &key, &[], WriteFlags::NO_OVERWRITE) {
            Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Remove a specific n-gram mapping for an event.
    pub fn delete_entry<G>(
        &self,
        txn: &mut RwTransaction<'_>,
        gram: &G,
        created_at: u64,
        seq: u64,
    ) -> Result<(), SearchnosDBError>
    where
        G: AsRef<[u8]>,
    {
        let key = Self::key_with_suffix(gram, created_at, seq);
        match txn.del(self.db, &key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Iterate over all seq_bytes that match ALL search terms (AND logic)
    pub fn iter_candidates(
        &self,
        txn: &RoTransaction<'_>,
        terms: &[String],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<NgramCandidates, SearchnosDBError> {
        if terms.is_empty() {
            return Ok(NgramCandidates::empty());
        }

        let mut gram_set = BTreeSet::new();

        for term in terms {
            let min_gram = preferred_min_query_ngram_size(term);
            let grams = char_ngrams(term, min_gram, MAX_NGRAM_SIZE);
            if grams.is_empty() {
                return Ok(NgramCandidates::empty());
            }

            for gram in grams {
                gram_set.insert(gram.into_bytes());
            }
        }

        if gram_set.is_empty() {
            return Ok(NgramCandidates::empty());
        }

        let mut postings = Vec::with_capacity(gram_set.len());
        for gram in gram_set {
            let entries = self.collect_entries_for_gram(txn, &gram, since, until)?;
            if entries.is_empty() {
                return Ok(NgramCandidates::empty());
            }
            postings.push(PostingCursor::new(entries));
        }

        if postings.is_empty() {
            return Ok(NgramCandidates::empty());
        }

        Ok(NgramCandidates::new(postings))
    }

    fn collect_entries_for_gram(
        &self,
        txn: &RoTransaction<'_>,
        gram: &[u8],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<Vec<PostingEntry>, SearchnosDBError> {
        let mut cursor = txn.open_ro_cursor(self.db)?;
        if !position_cursor_at_prefix_end(&mut cursor, gram)? {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();

        loop {
            let (key_bytes, _) = match cursor.get(None, None, MDB_GET_CURRENT) {
                Ok((Some(key), value)) => (key, value),
                Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                Err(err) => return Err(err.into()),
            };

            if !key_bytes.starts_with(gram) {
                break;
            }

            if key_bytes.len() != gram.len() + TS_SEQ_BYTES {
                match cursor.get(None, None, MDB_PREV) {
                    Ok(_) => continue,
                    Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            }

            if !key_bytes.starts_with(gram) {
                break;
            }

            let (created_at, seq_bytes) = split_ts_seq_from_key(key_bytes)?;

            if let Some(until_bound) = until
                && created_at > until_bound
            {
                match cursor.get(None, None, MDB_PREV) {
                    Ok(_) => continue,
                    Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            }

            if let Some(since_bound) = since
                && created_at < since_bound
            {
                break;
            }

            entries.push(PostingEntry {
                created_at,
                seq: seq_bytes,
            });

            match cursor.get(None, None, MDB_PREV) {
                Ok(_) => continue,
                Err(lmdb::Error::NotFound) => break,
                Err(err) => return Err(err.into()),
            }
        }

        if entries.len() > 1 {
            entries.sort_unstable_by(|a, b| match b.created_at.cmp(&a.created_at) {
                std::cmp::Ordering::Equal => b.seq.cmp(&a.seq),
                other => other,
            });
        }

        Ok(entries)
    }

    fn key_with_suffix<G>(gram: &G, created_at: u64, seq: u64) -> Vec<u8>
    where
        G: AsRef<[u8]>,
    {
        let gram_bytes = gram.as_ref();
        let mut key = Vec::with_capacity(gram_bytes.len() + TS_SEQ_BYTES);
        key.extend_from_slice(gram_bytes);
        append_ts_seq(&mut key, created_at, seq);
        key
    }
}
