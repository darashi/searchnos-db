use std::mem;
use std::time::{Duration, Instant};

use lmdb::Transaction;
use rayon::prelude::*;

use super::{SearchnosDB, SearchnosDBError};

#[derive(Debug)]
pub(crate) struct BatchState {
    batch_size: usize,
    flush_interval: Duration,
    buffer: Vec<String>,
    last_flush: Instant,
}

impl BatchState {
    pub(crate) fn new(batch_size: usize, flush_interval: Duration) -> Self {
        assert!(batch_size > 0, "batch_size must be greater than zero");
        Self {
            batch_size,
            flush_interval,
            buffer: Vec::with_capacity(batch_size),
            last_flush: Instant::now(),
        }
    }

    pub(crate) fn push(&mut self, db: &SearchnosDB, raw: String) -> Result<(), SearchnosDBError> {
        self.buffer.push(raw);
        if self.buffer.len() >= self.batch_size || self.last_flush.elapsed() >= self.flush_interval
        {
            self.flush(db)?;
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self, db: &SearchnosDB) -> Result<(), SearchnosDBError> {
        if self.buffer.is_empty() {
            self.last_flush = Instant::now();
            return Ok(());
        }

        let raw_batch = mem::take(&mut self.buffer);
        let prepared = raw_batch
            .into_par_iter()
            .map(|raw| SearchnosDB::prepare_insert(&raw))
            .collect::<Result<Vec<_>, SearchnosDBError>>()?;

        let mut txn = db.begin_rw_txn()?;
        for item in prepared {
            db.insert_prepared(&mut txn, item)?;
        }
        txn.commit().map_err(SearchnosDBError::from)?;
        self.last_flush = Instant::now();
        Ok(())
    }
}
