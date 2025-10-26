use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::HashSet,
    time::{Duration, Instant},
};

use lmdb::{RoTransaction, Transaction};
use ndb::{Filter as NdbFilter, NdbNote, from_ndb_note};

use crate::nostr::{Filter, Kind, extract_note_expiration};

use crate::db::{
    FilterPlanStats, QueryResult, QueryStats, SEQ_BYTES, SearchnosDB, SearchnosDBError,
};
use crate::text::{normalize_query_terms, normalize_text};

struct PlanCollectionResult {
    keys: Vec<(u64, [u8; SEQ_BYTES])>,
    candidate_count: usize,
}

impl SearchnosDB {
    pub(crate) fn to_ndb_filter(filter: &Filter, include_search: bool) -> NdbFilter {
        let ids = filter
            .ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| *id.as_bytes()).collect())
            .unwrap_or_default();

        let authors = filter
            .authors
            .as_ref()
            .map(|authors| authors.iter().map(|pk| *pk.as_bytes()).collect())
            .unwrap_or_default();

        let kinds = filter
            .kinds
            .as_ref()
            .map(|kinds| kinds.iter().map(|kind| kind.as_u16() as u32).collect())
            .unwrap_or_default();

        let since = filter.since.map(|ts| ts.as_u64());
        let until = filter.until.map(|ts| ts.as_u64());

        let generic_tags = filter
            .generic_tags
            .iter()
            .map(|(tag, values)| (*tag as u8, values.to_vec()))
            .collect();

        let search = if include_search {
            filter
                .search
                .as_ref()
                .map(|s| normalize_query_terms(s).join(" "))
        } else {
            None
        };

        NdbFilter {
            ids,
            authors,
            kinds,
            since,
            until,
            generic_tags,
            search,
        }
    }

    /// Execute the provided filters and return matching events alongside timing details.
    pub fn query_with_stats(&self, filters_json: &str) -> Result<QueryResult, SearchnosDBError> {
        let total_start = Instant::now();
        let filters = Self::parse_filters_json(filters_json)?;
        let txn = self.begin_ro_txn()?;
        let normalized_filters = self.normalized_filters_or_default(&filters);
        let mut entries: Vec<(u64, [u8; SEQ_BYTES])> = Vec::new();
        let mut seen = HashSet::new();
        let mut filter_stats = Vec::with_capacity(normalized_filters.len());
        let mut index_scan_duration = Duration::default();
        let mut post_processing_duration = Duration::default();

        for filter in &normalized_filters {
            let stats = self.append_keys_for_filter(&txn, filter, &mut seen, &mut entries)?;
            index_scan_duration += stats.index_scan_duration;
            post_processing_duration += stats.post_processing_duration;
            filter_stats.push(stats);
        }

        let final_processing_start = Instant::now();
        entries.sort_unstable_by(|a, b| match b.0.cmp(&a.0) {
            Ordering::Equal => b.1.cmp(&a.1),
            other => other,
        });

        let keys: Vec<[u8; SEQ_BYTES]> = entries.into_iter().map(|(_, seq)| seq).collect();
        let events = self.event_keys_to_json(&txn, &keys)?;
        post_processing_duration += final_processing_start.elapsed();

        let stats = QueryStats {
            total_elapsed: total_start.elapsed(),
            index_scan_duration,
            post_processing_duration,
            filters: filter_stats,
        };

        Ok(QueryResult { events, stats })
    }

    /// Backwards-compatible query helper that drops timing stats.
    pub fn query(&self, filters_json: &str) -> Result<Vec<String>, SearchnosDBError> {
        self.query_with_stats(filters_json)
            .map(|result| result.events)
    }

    fn append_keys_for_filter<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        filter: &Filter,
        seen: &mut HashSet<[u8; SEQ_BYTES]>,
        entries: &mut Vec<(u64, [u8; SEQ_BYTES])>,
    ) -> Result<FilterPlanStats, SearchnosDBError> {
        let plan = super::QueryPlan::for_filter(filter);
        let index_start = Instant::now();
        let mut collection = self.collect_event_keys_for_plan(txn, filter, &plan)?;
        let index_scan_duration = index_start.elapsed();

        let post_start = Instant::now();
        let matched_event_count = collection.keys.len();
        for (created_at, seq_bytes) in collection.keys.drain(..) {
            if seen.insert(seq_bytes) {
                entries.push((created_at, seq_bytes));
            }
        }
        let post_processing_duration = post_start.elapsed();

        Ok(FilterPlanStats {
            plan,
            index_scan_duration,
            post_processing_duration,
            matched_event_count,
            candidate_count: collection.candidate_count,
        })
    }

    fn collect_event_keys_for_plan<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        filter: &Filter,
        plan: &super::QueryPlan,
    ) -> Result<PlanCollectionResult, SearchnosDBError> {
        if matches!(filter.limit, Some(0)) {
            return Ok(PlanCollectionResult {
                keys: Vec::new(),
                candidate_count: 0,
            });
        }

        let search_terms = filter
            .search
            .as_ref()
            .map(|search| normalize_query_terms(search));

        if search_terms.as_ref().is_some_and(|terms| terms.is_empty()) {
            return Ok(PlanCollectionResult {
                keys: Vec::new(),
                candidate_count: 0,
            });
        }

        // Only include search in ndb_filter if it will be checked (nip50 == true)
        let ndb_filter = Self::to_ndb_filter(filter, plan.match_opts.nip50);
        let since = filter.since.map(|ts| ts.as_u64());
        let until = filter.until.map(|ts| ts.as_u64());
        let early_exit_limit = if plan.source.produces_descending_created_at() {
            filter.limit.filter(|&limit| limit > 0)
        } else {
            None
        };

        // Get iterator of candidates from the chosen index
        let candidates: Box<dyn Iterator<Item = [u8; SEQ_BYTES]>> = match &plan.source {
            super::PlanSource::EventIds { ids } => {
                let id_refs: Vec<&[u8]> = ids.iter().map(|id| id.as_bytes() as &[u8]).collect();
                Box::new(self.event_id_index.iter_candidates(txn, &id_refs)?)
            }
            super::PlanSource::NgramSearch { .. } => {
                let terms = search_terms
                    .as_ref()
                    .expect("filters with search queries must provide normalized terms");
                Box::new(self.ngram_index.iter_candidates(txn, terms)?)
            }
            super::PlanSource::PubkeyKinds { pubkeys, kinds } => {
                let pubkey_refs: Vec<&[u8]> =
                    pubkeys.iter().map(|pk| pk.as_bytes() as &[u8]).collect();
                let kind_u16s: Vec<u16> = kinds.iter().map(|k| k.as_u16()).collect();
                Box::new(self.pubkey_kind_index.iter_candidates(
                    txn,
                    &pubkey_refs,
                    &kind_u16s,
                    since,
                    until,
                )?)
            }
            super::PlanSource::Tags { entries } => {
                Box::new(self.tag_index.iter_candidates(txn, entries)?)
            }
            super::PlanSource::Authors { pubkeys } => {
                let pubkey_refs: Vec<&[u8]> =
                    pubkeys.iter().map(|pk| pk.as_bytes() as &[u8]).collect();
                Box::new(
                    self.pubkey_index
                        .iter_candidates(txn, &pubkey_refs, since, until)?,
                )
            }
            super::PlanSource::Kinds { kinds } => {
                let kind_u16s: Vec<u16> = kinds.iter().map(|k| k.as_u16()).collect();
                Box::new(
                    self.kind_index
                        .iter_candidates(txn, &kind_u16s, since, until)?,
                )
            }
            super::PlanSource::CreatedAt => {
                Box::new(self.created_at_index.iter_candidates(txn, since, until)?)
            }
        };

        // Apply unified filtering logic
        let mut keys = Vec::new();
        let mut candidate_count = 0usize;
        for seq_bytes in candidates {
            candidate_count = candidate_count.saturating_add(1);
            let (note, content_bytes) = self.load_note_and_content(txn, &seq_bytes)?;

            // Check all filter conditions
            if !note.matches_filter(&ndb_filter, plan.match_opts, content_bytes.as_ref()) {
                continue;
            }

            if let Some(expiration) = extract_note_expiration(&note)
                && !Self::note_is_ephemeral(&note)
                && Self::is_expired(expiration)
            {
                continue;
            }

            let created_at = note.created_at();
            keys.push((created_at, seq_bytes));

            if let Some(limit) = early_exit_limit
                && keys.len() >= limit
            {
                break;
            }
        }

        if let Some(limit) = filter.limit
            && keys.len() > limit
        {
            keys.sort_unstable_by(|a, b| match b.0.cmp(&a.0) {
                Ordering::Equal => b.1.cmp(&a.1),
                other => other,
            });
            keys.truncate(limit);
        }

        Ok(PlanCollectionResult {
            keys,
            candidate_count,
        })
    }

    fn load_note_and_content<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        seq_bytes: &[u8; SEQ_BYTES],
    ) -> Result<(NdbNote<'env>, Cow<'env, [u8]>), SearchnosDBError> {
        let event_bytes = txn.get(self.events, seq_bytes)?;
        let note = NdbNote::from_bytes(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
        let content_bytes = match txn.get(self.contents.database(), seq_bytes) {
            Ok(bytes) => Cow::Borrowed(bytes),
            Err(lmdb::Error::NotFound) => match note.content_str() {
                Ok(text) => Cow::Owned(normalize_text(text).into_bytes()),
                Err(_) => Cow::Owned(note.content().to_vec()),
            },
            Err(err) => return Err(err.into()),
        };
        Ok((note, content_bytes))
    }

    pub(crate) fn note_is_ephemeral(note: &NdbNote<'_>) -> bool {
        let kind_value = note.kind();
        if kind_value <= u16::MAX as u32 {
            Kind::from_u16(kind_value as u16).is_ephemeral()
        } else {
            false
        }
    }

    fn event_keys_to_json<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        keys: &[[u8; SEQ_BYTES]],
    ) -> Result<Vec<String>, SearchnosDBError> {
        let mut events = Vec::with_capacity(keys.len());

        for key in keys {
            let event_bytes = txn.get(self.events, key)?;
            let event_json = from_ndb_note(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
            events.push(event_json);
        }

        Ok(events)
    }
}
