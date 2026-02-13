use std::collections::BTreeSet;

use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, RoCursor, RoTransaction, RwTransaction,
    Transaction, WriteFlags,
};
use lmdb_sys::{MDB_GET_CURRENT, MDB_PREV};

use super::common::{
    TS_SEQ_BYTES, append_ts_seq, position_cursor_at_prefix_end, split_ts_seq_from_key,
};
use crate::db::{SEQ_BYTES, SearchnosDBError};
use crate::text::{MAX_NGRAM_SIZE, char_ngrams, preferred_min_query_ngram_size};

const USE_REPRESENTATIVE_QUERY_GRAMS: bool = true;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PostingEntry {
    created_at: u64,
    seq: [u8; SEQ_BYTES],
}

#[derive(Debug)]
struct PostingCursor<'txn> {
    cursor: RoCursor<'txn>,
    gram: Vec<u8>,
    since: Option<u64>,
    until: Option<u64>,
    current: Option<PostingEntry>,
    finished: bool,
}

impl<'txn> PostingCursor<'txn> {
    fn open(
        txn: &'txn RoTransaction<'txn>,
        db: Database,
        gram: Vec<u8>,
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<Self, SearchnosDBError> {
        let cursor = txn.open_ro_cursor(db)?;
        let mut posting = Self {
            cursor,
            gram,
            since,
            until,
            current: None,
            finished: false,
        };
        if !position_cursor_at_prefix_end(&mut posting.cursor, &posting.gram)? {
            posting.finished = true;
            return Ok(posting);
        }
        posting.current = posting.refresh_current()?;
        Ok(posting)
    }

    fn current(&self) -> Option<PostingEntry> {
        self.current
    }

    fn advance(&mut self) -> Result<(), SearchnosDBError> {
        if self.finished {
            return Ok(());
        }

        if !self.move_prev()? {
            self.finished = true;
            self.current = None;
            return Ok(());
        }

        self.current = self.refresh_current()?;
        Ok(())
    }

    fn refresh_current(&mut self) -> Result<Option<PostingEntry>, SearchnosDBError> {
        if self.finished {
            return Ok(None);
        }

        loop {
            let (key_bytes, _) = match self.cursor.get(None, None, MDB_GET_CURRENT) {
                Ok((Some(key), value)) => (key, value),
                Ok((None, _)) | Err(lmdb::Error::NotFound) => {
                    self.finished = true;
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            };

            if !key_bytes.starts_with(&self.gram) {
                self.finished = true;
                return Ok(None);
            }

            if key_bytes.len() != self.gram.len() + TS_SEQ_BYTES {
                if !self.move_prev()? {
                    self.finished = true;
                    return Ok(None);
                }
                continue;
            }

            let (created_at, seq_bytes) = split_ts_seq_from_key(key_bytes)?;

            if let Some(until_bound) = self.until
                && created_at > until_bound
            {
                if !self.move_prev()? {
                    self.finished = true;
                    return Ok(None);
                }
                continue;
            }

            if let Some(since_bound) = self.since
                && created_at < since_bound
            {
                self.finished = true;
                return Ok(None);
            }

            return Ok(Some(PostingEntry {
                created_at,
                seq: seq_bytes,
            }));
        }
    }

    fn move_prev(&mut self) -> Result<bool, SearchnosDBError> {
        match self.cursor.get(None, None, MDB_PREV) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }
}

pub struct NgramCandidates<'txn> {
    postings: Vec<PostingCursor<'txn>>,
}

impl<'txn> NgramCandidates<'txn> {
    fn empty() -> Self {
        Self {
            postings: Vec::new(),
        }
    }

    fn new(postings: Vec<PostingCursor<'txn>>) -> Self {
        Self { postings }
    }
}

impl Iterator for NgramCandidates<'_> {
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
                    if let Err(err) = posting.advance() {
                        return Some(Err(err));
                    }
                }
                return Some(Ok(target.seq));
            }

            for posting in &mut self.postings {
                if posting.current() == Some(target)
                    && let Err(err) = posting.advance()
                {
                    return Some(Err(err));
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
        let key = Self::key_with_suffix(&gram, created_at, seq);
        match txn.put(self.db, &key, &[], WriteFlags::NO_OVERWRITE) {
            Ok(()) | Err(lmdb::Error::KeyExist) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Check whether an n-gram mapping exists for (gram, created_at, seq).
    #[allow(dead_code)]
    pub fn contains(
        &self,
        txn: &RoTransaction<'_>,
        gram: impl AsRef<[u8]>,
        created_at: u64,
        seq: u64,
    ) -> bool {
        let key = Self::key_with_suffix(&gram, created_at, seq);
        txn.get(self.db, &key).is_ok()
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
    pub fn iter_candidates<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        terms: &[String],
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<NgramCandidates<'env>, SearchnosDBError> {
        if terms.is_empty() {
            return Ok(NgramCandidates::empty());
        }

        let mut gram_set = BTreeSet::new();

        for term in terms {
            let grams = if USE_REPRESENTATIVE_QUERY_GRAMS {
                representative_query_grams(term)
            } else {
                full_query_grams(term)
            };
            if grams.is_empty() {
                return Ok(NgramCandidates::empty());
            }

            for gram in grams {
                gram_set.insert(gram);
            }
        }

        if gram_set.is_empty() {
            return Ok(NgramCandidates::empty());
        }

        let mut postings = Vec::with_capacity(gram_set.len());
        for gram in gram_set {
            let posting = PostingCursor::open(txn, self.db, gram, since, until)?;
            if posting.current().is_none() {
                return Ok(NgramCandidates::empty());
            }
            postings.push(posting);
        }

        if postings.is_empty() {
            return Ok(NgramCandidates::empty());
        }

        Ok(NgramCandidates::new(postings))
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

fn representative_query_grams(term: &str) -> Vec<Vec<u8>> {
    let grams = full_query_grams(term);
    match grams.as_slice() {
        [] => Vec::new(),
        [one] => vec![one.clone()],
        [first, second] => vec![first.clone(), second.clone()],
        _ => {
            let first = grams.first().expect("non-empty").clone();
            let last = grams.last().expect("non-empty").clone();
            if first == last {
                vec![first]
            } else {
                vec![first, last]
            }
        }
    }
}

fn full_query_grams(term: &str) -> Vec<Vec<u8>> {
    let min_gram = preferred_min_query_ngram_size(term);
    char_ngrams(term, min_gram, MAX_NGRAM_SIZE)
        .into_iter()
        .map(String::into_bytes)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{full_query_grams, representative_query_grams};

    #[test]
    fn representative_query_grams_keeps_short_terms() {
        let term = "go";
        let all = full_query_grams(term);
        assert_eq!(representative_query_grams(term), all);
    }

    #[test]
    fn representative_query_grams_picks_subset_for_long_terms() {
        let term = "rustacean";
        let all = full_query_grams(term).into_iter().collect::<BTreeSet<_>>();
        let selected = representative_query_grams(term);
        assert!(!selected.is_empty());
        assert!(selected.len() <= 2);
        for gram in selected {
            assert!(all.contains(&gram));
        }
    }
}
