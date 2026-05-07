use super::{
    PurgePolicy, SearchnosDB, SearchnosDBError,
    index::{
        KindsIndex,
        common::{position_cursor_at_prefix_end, seq_from_value, split_created_at_from_key},
    },
};
use lmdb::{Cursor, RwTransaction, Transaction};
use lmdb_sys::{MDB_FIRST, MDB_GET_CURRENT, MDB_LAST_DUP, MDB_NEXT_NODUP, MDB_PREV, MDB_SET_RANGE};
use std::{
    cmp::Ordering,
    collections::HashSet,
    sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

impl SearchnosDB {
    fn deadline_reached(deadline: Option<Instant>) -> bool {
        deadline.is_some_and(|limit| Instant::now() >= limit)
    }

    fn purge_expired_entries(
        &self,
        txn: &mut RwTransaction<'_>,
        now: u64,
        max_events: usize,
        deadline: Option<Instant>,
        progress: Option<&AtomicUsize>,
    ) -> Result<usize, SearchnosDBError> {
        if max_events == 0 {
            return Ok(0);
        }

        let expired = self
            .expiration_index
            .collect_expired(txn, now, max_events)?;
        if expired.is_empty() {
            return Ok(0);
        }

        let mut removed = 0usize;

        for (expiration, seq_bytes) in expired {
            if Self::deadline_reached(deadline) {
                break;
            }
            let seq = u64::from_ne_bytes(seq_bytes);
            let removed_event = self.remove_event_by_seq_internal(txn, seq, true)?;
            if removed_event {
                removed += 1;
                if let Some(counter) = progress {
                    counter.store(removed, AtomicOrdering::Relaxed);
                }
            } else {
                self.expiration_index.delete_entry(txn, expiration, seq)?;
            }
        }

        Ok(removed)
    }

    /// Remove up to `max_events` stale items based on expiration and purge policy.
    pub fn purge_stale_events(&self, max_events: usize) -> Result<usize, SearchnosDBError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        self.purge_internal(max_events, now, None, None)
    }

    /// Same as [`purge_stale_events`] but updates `progress` with removed count.
    pub fn purge_stale_events_with_progress(
        &self,
        max_events: usize,
        progress: &AtomicUsize,
    ) -> Result<usize, SearchnosDBError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        self.purge_internal(max_events, now, None, Some(progress))
    }

    /// Remove stale items with both event-count and elapsed-time limits.
    pub fn purge_stale_events_with_budget(
        &self,
        max_events: usize,
        time_budget: Duration,
    ) -> Result<usize, SearchnosDBError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        let deadline = Instant::now().checked_add(time_budget);
        self.purge_internal(max_events, now, deadline, None)
    }

    /// Same as [`purge_stale_events_with_budget`] but updates `progress` with removed count.
    pub fn purge_stale_events_with_budget_and_progress(
        &self,
        max_events: usize,
        time_budget: Duration,
        progress: &AtomicUsize,
    ) -> Result<usize, SearchnosDBError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        let deadline = Instant::now().checked_add(time_budget);
        self.purge_internal(max_events, now, deadline, Some(progress))
    }

    pub(crate) fn purge_internal(
        &self,
        max_events: usize,
        now: u64,
        deadline: Option<Instant>,
        progress: Option<&AtomicUsize>,
    ) -> Result<usize, SearchnosDBError> {
        if max_events == 0 {
            return Ok(0);
        }

        if let Some(counter) = progress {
            counter.store(0, AtomicOrdering::Relaxed);
        }

        if Self::deadline_reached(deadline) {
            return Ok(0);
        }

        let Some(policy) = &self.purge_policy else {
            return Ok(0);
        };

        let mut txn = self.begin_rw_txn()?;
        let mut removed =
            self.purge_expired_entries(&mut txn, now, max_events, deadline, progress)?;
        let remaining = max_events.saturating_sub(removed);

        if remaining == 0 || Self::deadline_reached(deadline) {
            if removed > 0 {
                txn.commit()?;
            }
            return Ok(removed);
        }

        let mut candidates: Vec<(u64, u64)> = Vec::new();
        let mut seen = HashSet::new();

        self.append_candidates_by_kind(
            &mut txn,
            policy,
            now,
            remaining,
            deadline,
            &mut candidates,
            &mut seen,
        )?;

        if candidates.len() < remaining && !Self::deadline_reached(deadline) {
            self.append_candidates_by_default(
                &mut txn,
                policy,
                now,
                remaining,
                deadline,
                &mut candidates,
                &mut seen,
            )?;
        }

        if candidates.is_empty() {
            if removed > 0 {
                txn.commit()?;
            }
            return Ok(removed);
        }

        candidates.sort_unstable_by(|a, b| match a.0.cmp(&b.0) {
            Ordering::Equal => a.1.cmp(&b.1),
            other => other,
        });

        for (_created_at, seq) in candidates.into_iter().take(remaining) {
            if Self::deadline_reached(deadline) {
                break;
            }
            if self.remove_event_by_seq_internal(&mut txn, seq, true)? {
                removed += 1;
                if let Some(counter) = progress {
                    counter.store(removed, AtomicOrdering::Relaxed);
                }
            }
        }

        if removed > 0 {
            txn.commit()?;
        }

        Ok(removed)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_candidates_by_kind(
        &self,
        txn: &mut RwTransaction<'_>,
        policy: &PurgePolicy,
        now: u64,
        max_events: usize,
        deadline: Option<Instant>,
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError> {
        self.append_candidates_for_kinds(
            txn,
            policy.purge_overrides(),
            now,
            max_events,
            deadline,
            out,
            seen,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn append_candidates_by_default(
        &self,
        txn: &mut RwTransaction<'_>,
        policy: &PurgePolicy,
        now: u64,
        max_events: usize,
        deadline: Option<Instant>,
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError> {
        let Some(default_purge_after) = policy.default_duration() else {
            return Ok(());
        };

        if out.len() >= max_events || Self::deadline_reached(deadline) {
            return Ok(());
        }

        let mut kinds = Vec::new();
        {
            let cursor = txn.open_ro_cursor(self.kind_index.database())?;
            let mut result = cursor.get(None, None, MDB_FIRST);
            loop {
                if Self::deadline_reached(deadline) {
                    break;
                }
                match result {
                    Ok((Some(kind_bytes), _)) => {
                        if kind_bytes.len() < 2 {
                            result = cursor.get(None, None, MDB_NEXT_NODUP);
                            continue;
                        }
                        let mut buffer = [0u8; 2];
                        buffer.copy_from_slice(&kind_bytes[..2]);
                        let kind = u16::from_be_bytes(buffer);
                        if policy.has_override(kind) {
                            result = cursor.get(None, None, MDB_NEXT_NODUP);
                            continue;
                        }
                        kinds.push(kind);
                        result = cursor.get(None, None, MDB_NEXT_NODUP);
                    }
                    Ok(_) | Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            }
        }

        self.append_candidates_for_kinds(
            txn,
            kinds.into_iter().map(|kind| (kind, default_purge_after)),
            now,
            max_events,
            deadline,
            out,
            seen,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn append_candidates_for_kinds<I>(
        &self,
        txn: &mut RwTransaction<'_>,
        kinds: I,
        now: u64,
        max_events: usize,
        deadline: Option<Instant>,
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError>
    where
        I: IntoIterator<Item = (u16, Duration)>,
    {
        if max_events == 0 || out.len() >= max_events || Self::deadline_reached(deadline) {
            return Ok(());
        }

        for (kind, purge_after) in kinds.into_iter() {
            if out.len() >= max_events || Self::deadline_reached(deadline) {
                break;
            }
            self.append_candidates_for_kind(
                txn,
                kind,
                purge_after,
                now,
                max_events,
                deadline,
                out,
                seen,
            )?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn append_candidates_for_kind(
        &self,
        txn: &mut RwTransaction<'_>,
        kind: u16,
        purge_after: Duration,
        now: u64,
        max_events: usize,
        deadline: Option<Instant>,
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError> {
        if out.len() >= max_events || Self::deadline_reached(deadline) {
            return Ok(());
        }

        let retention_secs = purge_after.as_secs();
        let cutoff = now.saturating_sub(retention_secs);
        let mut cursor = txn.open_ro_cursor(self.kind_index.database())?;
        let prefix = KindsIndex::kind_key(kind);
        if self.position_cursor_at_kind_cutoff(&mut cursor, &prefix, cutoff)? {
            loop {
                if Self::deadline_reached(deadline) {
                    break;
                }
                let (key_bytes, value_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                    Ok((Some(key), value)) => (key, value),
                    Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                };

                if !key_bytes.starts_with(&prefix) {
                    break;
                }

                let created_at = split_created_at_from_key(key_bytes)?;
                let seq_bytes = seq_from_value(value_bytes)?;
                let seq = u64::from_ne_bytes(seq_bytes);
                if seen.insert(seq) {
                    out.push((created_at, seq));
                }
                if out.len() >= max_events {
                    break;
                }

                match cursor.get(None, None, MDB_PREV) {
                    Ok(_) => continue,
                    Err(lmdb::Error::NotFound) => break,
                    Err(err) => return Err(err.into()),
                }
            }
        }

        Ok(())
    }

    fn position_cursor_at_kind_cutoff(
        &self,
        cursor: &mut lmdb::RoCursor<'_>,
        prefix: &[u8],
        cutoff: u64,
    ) -> Result<bool, SearchnosDBError> {
        let mut seek_key = Vec::with_capacity(prefix.len() + std::mem::size_of::<u64>());
        seek_key.extend_from_slice(prefix);
        seek_key.extend_from_slice(&cutoff.to_be_bytes());

        match cursor.get(Some(&seek_key), None, MDB_SET_RANGE) {
            Ok((Some(_), _)) => {}
            Ok((None, _)) | Err(lmdb::Error::NotFound) => {
                if !position_cursor_at_prefix_end(cursor, prefix)? {
                    return Ok(false);
                }
            }
            Err(err) => return Err(err.into()),
        };

        loop {
            let (key_bytes, _) = match cursor.get(None, None, MDB_GET_CURRENT) {
                Ok((Some(key), value)) => (key, value),
                Ok((None, _)) | Err(lmdb::Error::NotFound) => return Ok(false),
                Err(err) => return Err(err.into()),
            };

            if key_bytes.starts_with(prefix) {
                let created_at = split_created_at_from_key(key_bytes)?;
                if created_at <= cutoff {
                    return match cursor.get(None, None, MDB_LAST_DUP) {
                        Ok(_) => Ok(true),
                        Err(lmdb::Error::NotFound) => Ok(false),
                        Err(err) => Err(err.into()),
                    };
                }
            }

            match cursor.get(None, None, MDB_PREV) {
                Ok((Some(prev_key), _)) => {
                    if !prev_key.starts_with(prefix) {
                        return Ok(false);
                    }
                }
                Ok((None, _)) | Err(lmdb::Error::NotFound) => return Ok(false),
                Err(err) => return Err(err.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{
        PurgePolicy, SearchnosDBOptions, purge_policy::PurgeSetting, test_support::TestDatabase,
    };
    use crate::nostr::test_utils::{EventBuilder, Keys};
    use crate::nostr::{Kind, Timestamp};
    use std::time::Duration;

    #[test]
    fn purge_respects_kind_specific_overrides() {
        let mut options = SearchnosDBOptions::default();
        let mut purge_policy = PurgePolicy {
            default_setting: Some(PurgeSetting::Retain(Duration::from_secs(30))),
            ..Default::default()
        };
        purge_policy
            .per_kind
            .insert(0, PurgeSetting::Retain(Duration::from_secs(120)));
        options.purge_policy = Some(purge_policy);

        let db = TestDatabase::with_options(options);
        let keys = Keys::generate();
        let base = 1_000_000u64;

        let purged_kind1 = EventBuilder::text_note("kind1 old")
            .custom_created_at(Timestamp::from(base - 40))
            .sign_with_keys(&keys)
            .expect("build kind1 old");
        let recent_kind1 = EventBuilder::text_note("kind1 recent")
            .custom_created_at(Timestamp::from(base - 10))
            .sign_with_keys(&keys)
            .expect("build kind1 recent");
        let retained_kind0 = EventBuilder::new(Kind::Metadata, "{}")
            .custom_created_at(Timestamp::from(base - 40))
            .sign_with_keys(&keys)
            .expect("build kind0 retained");
        let purged_kind0 = EventBuilder::new(Kind::Metadata, "{}")
            .custom_created_at(Timestamp::from(base - 150))
            .sign_with_keys(&keys)
            .expect("build kind0 purge");

        let seq_purged_kind1 = db.insert(&purged_kind1);
        let seq_recent_kind1 = db.insert(&recent_kind1);
        let seq_retained_kind0 = db.insert(&retained_kind0);
        let seq_rejected_kind0 = db.insert(&purged_kind0);
        // purged_kind0 is older than retained_kind0, so it's rejected and returns the existing seq
        assert_eq!(seq_rejected_kind0, seq_retained_kind0);

        let removed = db.purge_with_time(10, base);
        assert_eq!(removed, 1);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, seq_purged_kind1, &purged_kind1);
        db.assert_event_stored(&txn, seq_recent_kind1, &recent_kind1);
        db.assert_event_stored(&txn, seq_retained_kind0, &retained_kind0);
    }

    #[test]
    fn purge_respects_never_overrides() {
        let mut options = SearchnosDBOptions::default();
        let purge_policy = PurgePolicy::from_specs(["30d", "0:never"]).unwrap();
        options.purge_policy = Some(purge_policy);

        let db = TestDatabase::with_options(options);
        let keys = Keys::generate();
        let base = 2_000_000u64;

        let old_default_kind = EventBuilder::text_note("old default kind")
            .custom_created_at(Timestamp::from(base - 40))
            .sign_with_keys(&keys)
            .expect("build default kind");
        let old_kind_zero = EventBuilder::new(Kind::Metadata, "{}")
            .custom_created_at(Timestamp::from(base - 40))
            .sign_with_keys(&keys)
            .expect("build kind0");

        let seq_default = db.insert(&old_default_kind);
        let seq_zero = db.insert(&old_kind_zero);

        let removed = db.purge_with_time(10, base + 2_600_000);
        assert_eq!(removed, 1);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, seq_default, &old_default_kind);
        db.assert_event_stored(&txn, seq_zero, &old_kind_zero);
    }

    #[test]
    fn purge_includes_entries_at_cutoff_created_at() {
        let mut options = SearchnosDBOptions::default();
        let purge_policy = PurgePolicy {
            default_setting: Some(PurgeSetting::Retain(Duration::from_secs(30))),
            ..Default::default()
        };
        options.purge_policy = Some(purge_policy);

        let db = TestDatabase::with_options(options);
        let keys = Keys::generate();
        let base = 3_000_000u64;

        let old_before_cutoff = EventBuilder::text_note("old before cutoff")
            .custom_created_at(Timestamp::from(base - 31))
            .sign_with_keys(&keys)
            .expect("build old before cutoff");
        let old_at_cutoff_1 = EventBuilder::text_note("old at cutoff 1")
            .custom_created_at(Timestamp::from(base - 30))
            .sign_with_keys(&keys)
            .expect("build old at cutoff 1");
        let old_at_cutoff_2 = EventBuilder::text_note("old at cutoff 2")
            .custom_created_at(Timestamp::from(base - 30))
            .sign_with_keys(&keys)
            .expect("build old at cutoff 2");
        let new_after_cutoff = EventBuilder::text_note("new after cutoff")
            .custom_created_at(Timestamp::from(base - 29))
            .sign_with_keys(&keys)
            .expect("build new after cutoff");

        let seq_old_before_cutoff = db.insert(&old_before_cutoff);
        let seq_old_at_cutoff_1 = db.insert(&old_at_cutoff_1);
        let seq_old_at_cutoff_2 = db.insert(&old_at_cutoff_2);
        let seq_new_after_cutoff = db.insert(&new_after_cutoff);

        let removed = db.purge_with_time(10, base);
        assert_eq!(removed, 3);

        let txn = db.ro_txn();
        db.assert_event_removed(&txn, seq_old_before_cutoff, &old_before_cutoff);
        db.assert_event_removed(&txn, seq_old_at_cutoff_1, &old_at_cutoff_1);
        db.assert_event_removed(&txn, seq_old_at_cutoff_2, &old_at_cutoff_2);
        db.assert_event_stored(&txn, seq_new_after_cutoff, &new_after_cutoff);
    }
}
