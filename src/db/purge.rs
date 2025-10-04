use super::{PurgePolicy, SearchnosDB, SearchnosDBError, index::KindsIndex};
use lmdb::{Cursor, RwTransaction, Transaction};
use lmdb_sys::{MDB_FIRST, MDB_NEXT_NODUP};
use std::{
    cmp::Ordering,
    collections::HashSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

impl SearchnosDB {
    fn purge_expired_entries(
        &self,
        txn: &mut RwTransaction<'_>,
        now: u64,
        max_events: usize,
    ) -> Result<usize, SearchnosDBError> {
        if max_events == 0 {
            return Ok(0);
        }

        let expired = self.expiration_index.collect_expired(txn, now)?;
        if expired.is_empty() {
            return Ok(0);
        }

        let mut removed = 0usize;

        for (expiration, seq_bytes) in expired.into_iter().take(max_events) {
            let seq = u64::from_ne_bytes(seq_bytes);
            let removed_event = self.remove_event_by_seq_internal(txn, seq, true)?;
            if removed_event {
                removed += 1;
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
        self.purge_internal(max_events, now)
    }

    pub(crate) fn purge_internal(
        &self,
        max_events: usize,
        now: u64,
    ) -> Result<usize, SearchnosDBError> {
        if max_events == 0 {
            return Ok(0);
        }

        let Some(policy) = &self.purge_policy else {
            return Ok(0);
        };

        let mut txn = self.begin_rw_txn()?;
        let mut removed = self.purge_expired_entries(&mut txn, now, max_events)?;
        let remaining = max_events.saturating_sub(removed);

        if remaining == 0 {
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
            &mut candidates,
            &mut seen,
        )?;

        if candidates.len() < remaining {
            self.append_candidates_by_default(
                &mut txn,
                policy,
                now,
                remaining,
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
            if self.remove_event_by_seq_internal(&mut txn, seq, true)? {
                removed += 1;
            }
        }

        if removed > 0 {
            txn.commit()?;
        }

        Ok(removed)
    }

    fn append_candidates_by_kind(
        &self,
        txn: &mut RwTransaction<'_>,
        policy: &PurgePolicy,
        now: u64,
        max_events: usize,
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError> {
        self.append_candidates_for_kinds(txn, policy.purge_overrides(), now, max_events, out, seen)
    }

    fn append_candidates_by_default(
        &self,
        txn: &mut RwTransaction<'_>,
        policy: &PurgePolicy,
        now: u64,
        max_events: usize,
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError> {
        let Some(default_purge_after) = policy.default_duration() else {
            return Ok(());
        };

        if out.len() >= max_events {
            return Ok(());
        }

        let mut kinds = Vec::new();
        {
            let cursor = txn.open_ro_cursor(self.kind_index.database())?;
            let mut result = cursor.get(None, None, MDB_FIRST);
            loop {
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
            out,
            seen,
        )
    }

    fn append_candidates_for_kinds<I>(
        &self,
        txn: &mut RwTransaction<'_>,
        kinds: I,
        now: u64,
        max_events: usize,
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError>
    where
        I: IntoIterator<Item = (u16, Duration)>,
    {
        if max_events == 0 || out.len() >= max_events {
            return Ok(());
        }

        for (kind, purge_after) in kinds.into_iter() {
            if out.len() >= max_events {
                break;
            }
            self.append_candidates_for_kind(txn, kind, purge_after, now, max_events, out, seen)?;
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
        out: &mut Vec<(u64, u64)>,
        seen: &mut HashSet<u64>,
    ) -> Result<(), SearchnosDBError> {
        if out.len() >= max_events {
            return Ok(());
        }

        let retention_secs = purge_after.as_secs();
        let cutoff = now.saturating_sub(retention_secs);
        let mut cursor = txn.open_ro_cursor(self.kind_index.database())?;
        let key = KindsIndex::kind_key(kind);

        match cursor.iter_dup_of(&key) {
            Ok(mut iter) => {
                for (_key, value) in iter.by_ref() {
                    let (created_at, seq_bytes) = KindsIndex::decode_value(value)?;
                    if created_at <= cutoff {
                        let seq = u64::from_ne_bytes(seq_bytes);
                        if seen.insert(seq) {
                            out.push((created_at, seq));
                        }
                        if out.len() >= max_events {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            Err(lmdb::Error::NotFound) => {}
            Err(err) => return Err(err.into()),
        }

        Ok(())
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
}
