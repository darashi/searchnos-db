use lmdb::{Cursor, Database, RoTransaction, Transaction};

use super::{
    DatabaseStats, EVENTS_DB_NAME, SearchnosDB, SearchnosDBError,
    index::{
        ContentsStore, DeletionIndex, EventIdIndex, ExpirationIndex, ReplacableIndex,
        ReplaceDeletionIndex,
    },
};

impl SearchnosDB {
    /// Report LMDB statistics for the main database and maintained secondary indexes.
    pub fn database_stats(&self) -> Result<Vec<DatabaseStats>, SearchnosDBError> {
        let txn = self.begin_ro_txn()?;
        Ok(vec![
            self.get_stats_for_db(&txn, self.events, EVENTS_DB_NAME)?,
            self.get_stats_for_db(&txn, self.event_id_index.database(), EventIdIndex::NAME)?,
            self.get_stats_for_db(&txn, self.deletions.database(), DeletionIndex::NAME)?,
            self.get_stats_for_db(&txn, self.replacables.database(), ReplacableIndex::NAME)?,
            self.get_stats_for_db(
                &txn,
                self.replace_deletions.database(),
                ReplaceDeletionIndex::NAME,
            )?,
            self.get_stats_for_db(&txn, self.contents.database(), ContentsStore::NAME)?,
            self.get_stats_for_db(
                &txn,
                self.expiration_index.database(),
                ExpirationIndex::NAME,
            )?,
        ])
    }

    fn get_stats_for_db(
        &self,
        txn: &RoTransaction<'_>,
        db: Database,
        name: &str,
    ) -> Result<DatabaseStats, SearchnosDBError> {
        let mut cursor = txn.open_ro_cursor(db)?;
        let mut count = 0usize;
        let mut key_bytes = 0usize;
        let mut value_bytes = 0usize;

        for (key, value) in cursor.iter() {
            count += 1;
            key_bytes += key.len();
            value_bytes += value.len();
        }

        Ok(DatabaseStats {
            name: name.to_string(),
            count,
            key_bytes,
            value_bytes,
            total_bytes: key_bytes + value_bytes,
        })
    }
}
