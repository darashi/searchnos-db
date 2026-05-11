use std::{
    cmp::Ordering,
    collections::HashSet,
    time::{Duration, Instant},
};

use lmdb::{Cursor, RoCursor, RoTransaction, Transaction};
use lmdb_sys::{MDB_GET_CURRENT, MDB_PREV};
use ndb::NdbNote;

use crate::nostr::{Filter, Kind, extract_note_expiration};

use crate::db::{
    FilterStats, QueryResult, QueryStats, SEQ_BYTES, SearchnosDB, SearchnosDBError,
    index::ContentsStore,
};
use crate::ndb_ext::{MatchEventOptions, NdbFilter, from_ndb_note, note_matches_filter};
use crate::text::normalize_query_terms;

struct FilterCollectionResult {
    keys: Vec<(u64, [u8; SEQ_BYTES])>,
    candidate_count: usize,
}

struct QueryKeyResult<'env> {
    txn: RoTransaction<'env>,
    keys: Vec<[u8; SEQ_BYTES]>,
    total_start: Instant,
    index_scan_duration: Duration,
    post_processing_duration: Duration,
    filter_stats: Vec<FilterStats>,
}

struct StreamingFilterStats {
    index_scan_duration: Duration,
    matched_event_count: usize,
    candidate_count: usize,
    completed: bool,
}

struct StreamingQueryStats {
    index_scan_duration: Duration,
    filter_stats: Vec<StreamingFilterStats>,
    completed: bool,
}

struct StreamingFilterState {
    ndb_filter: NdbFilter,
    match_opts: MatchEventOptions,
    since: Option<u64>,
    until: Option<u64>,
    limit: Option<usize>,
    matched_event_count: usize,
    candidate_count: usize,
    completed: bool,
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
        let QueryKeyResult {
            txn,
            keys,
            total_start,
            index_scan_duration,
            post_processing_duration,
            filter_stats,
        } = self.query_keys(filters_json)?;
        let events = self.event_keys_to_json(&txn, &keys)?;
        let stats = Self::finish_query_stats(
            total_start,
            index_scan_duration,
            post_processing_duration,
            filter_stats,
        );

        Ok(QueryResult { events, stats })
    }

    /// Execute the provided filters and pass each matching event to `on_event`.
    ///
    /// Events are delivered in the same order as `query_with_stats`. Returning
    /// `false` from `on_event` stops event delivery and scanning early.
    pub fn stream_query_with_stats<F>(
        &self,
        filters_json: &str,
        mut on_event: F,
    ) -> Result<QueryStats, SearchnosDBError>
    where
        F: FnMut(String) -> bool,
    {
        self.stream_query_incrementally(filters_json, &mut on_event)
    }

    fn stream_query_incrementally<F>(
        &self,
        filters_json: &str,
        on_event: &mut F,
    ) -> Result<QueryStats, SearchnosDBError>
    where
        F: FnMut(String) -> bool,
    {
        let total_start = Instant::now();
        let filters = Self::parse_filters_json(filters_json)?;
        let normalized_filters = self.normalized_filters_or_default(&filters);

        let txn = self.begin_ro_txn()?;
        let stream_stats =
            self.stream_filters_events_as_json(&txn, &normalized_filters, on_event)?;

        let filter_stats = stream_stats
            .filter_stats
            .into_iter()
            .map(|stats| FilterStats {
                index_scan_duration: stats.index_scan_duration,
                post_processing_duration: Duration::default(),
                matched_event_count: stats.matched_event_count,
                candidate_count: stats.candidate_count,
            })
            .collect();
        let total_elapsed = total_start.elapsed();
        let stats = QueryStats {
            total_elapsed,
            index_scan_duration: stream_stats.index_scan_duration,
            post_processing_duration: if stream_stats.completed {
                total_elapsed.saturating_sub(stream_stats.index_scan_duration)
            } else {
                Duration::default()
            },
            filters: filter_stats,
        };

        Ok(stats)
    }

    /// Execute the provided filters and stream matching events without timing stats.
    pub fn stream_query<F>(&self, filters_json: &str, on_event: F) -> Result<(), SearchnosDBError>
    where
        F: FnMut(String) -> bool,
    {
        self.stream_query_with_stats(filters_json, on_event)
            .map(|_| ())
    }

    /// Backwards-compatible query helper that drops timing stats.
    pub fn query(&self, filters_json: &str) -> Result<Vec<String>, SearchnosDBError> {
        self.query_with_stats(filters_json)
            .map(|result| result.events)
    }

    fn query_keys(&self, filters_json: &str) -> Result<QueryKeyResult<'_>, SearchnosDBError> {
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
        post_processing_duration += final_processing_start.elapsed();

        Ok(QueryKeyResult {
            txn,
            keys,
            total_start,
            index_scan_duration,
            post_processing_duration,
            filter_stats,
        })
    }

    fn finish_query_stats(
        total_start: Instant,
        index_scan_duration: Duration,
        post_processing_duration: Duration,
        filter_stats: Vec<FilterStats>,
    ) -> QueryStats {
        QueryStats {
            total_elapsed: total_start.elapsed(),
            index_scan_duration,
            post_processing_duration,
            filters: filter_stats,
        }
    }

    fn append_keys_for_filter<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        filter: &Filter,
        seen: &mut HashSet<[u8; SEQ_BYTES]>,
        entries: &mut Vec<(u64, [u8; SEQ_BYTES])>,
    ) -> Result<FilterStats, SearchnosDBError> {
        let index_start = Instant::now();
        let mut collection = self.collect_event_keys_for_filter(txn, filter)?;
        let index_scan_duration = index_start.elapsed();

        let post_start = Instant::now();
        let matched_event_count = collection.keys.len();
        for (created_at, seq_bytes) in collection.keys.drain(..) {
            if seen.insert(seq_bytes) {
                entries.push((created_at, seq_bytes));
            }
        }
        let post_processing_duration = post_start.elapsed();

        Ok(FilterStats {
            index_scan_duration,
            post_processing_duration,
            matched_event_count,
            candidate_count: collection.candidate_count,
        })
    }

    fn collect_event_keys_for_filter<'env>(
        &self,
        txn: &'env RoTransaction<'env>,
        filter: &Filter,
    ) -> Result<FilterCollectionResult, SearchnosDBError> {
        if matches!(filter.limit, Some(0)) {
            return Ok(FilterCollectionResult {
                keys: Vec::new(),
                candidate_count: 0,
            });
        }

        let search_terms = filter
            .search
            .as_ref()
            .map(|search| normalize_query_terms(search));

        if search_terms.as_ref().is_some_and(|terms| terms.is_empty()) {
            return Ok(FilterCollectionResult {
                keys: Vec::new(),
                candidate_count: 0,
            });
        }

        let match_opts = MatchEventOptions::new().nip50(search_terms.is_some());
        let ndb_filter = Self::to_ndb_filter(filter, match_opts.nip50);
        // Apply unified filtering logic over normalized content rows.
        let mut keys = Vec::new();
        let mut candidate_count = 0usize;
        let since = filter.since.map(|ts| ts.as_u64());
        let until = filter.until.map(|ts| ts.as_u64());
        let early_exit_limit = filter.limit.filter(|&limit| limit > 0);
        let mut cursor = txn.open_ro_cursor(self.contents.database())?;
        let mut positioned = ContentsStore::position_cursor(&mut cursor, until)?;

        while positioned {
            let (contents_key, content_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                Ok((Some(key), value)) => (key, value),
                Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                Err(err) => return Err(err.into()),
            };

            let (created_at, seq_bytes) = ContentsStore::split_key(contents_key)?;

            if let Some(since) = since
                && created_at < since
            {
                break;
            }

            if let Some(until) = until
                && created_at > until
            {
                positioned = Self::move_contents_cursor_prev(&mut cursor)?;
                continue;
            }

            candidate_count = candidate_count.saturating_add(1);
            let event_bytes = txn.get(self.events, &seq_bytes)?;
            let note = NdbNote::from_bytes(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;

            // Check all filter conditions
            if !note_matches_filter(&note, &ndb_filter, match_opts, content_bytes) {
                positioned = Self::move_contents_cursor_prev(&mut cursor)?;
                continue;
            }

            if let Some(expiration) = extract_note_expiration(&note)
                && !Self::note_is_ephemeral(&note)
                && Self::is_expired(expiration)
            {
                positioned = Self::move_contents_cursor_prev(&mut cursor)?;
                continue;
            }

            keys.push((created_at, seq_bytes));

            if let Some(limit) = early_exit_limit
                && keys.len() >= limit
            {
                break;
            }

            positioned = Self::move_contents_cursor_prev(&mut cursor)?;
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

        Ok(FilterCollectionResult {
            keys,
            candidate_count,
        })
    }

    fn stream_filter_events_as_json<'env, F>(
        &self,
        txn: &'env RoTransaction<'env>,
        filter: &Filter,
        mut on_event: F,
    ) -> Result<StreamingFilterStats, SearchnosDBError>
    where
        F: FnMut(String) -> bool,
    {
        let index_start = Instant::now();

        if matches!(filter.limit, Some(0)) {
            return Ok(StreamingFilterStats {
                index_scan_duration: index_start.elapsed(),
                matched_event_count: 0,
                candidate_count: 0,
                completed: true,
            });
        }

        let search_terms = filter
            .search
            .as_ref()
            .map(|search| normalize_query_terms(search));

        if search_terms.as_ref().is_some_and(|terms| terms.is_empty()) {
            return Ok(StreamingFilterStats {
                index_scan_duration: index_start.elapsed(),
                matched_event_count: 0,
                candidate_count: 0,
                completed: true,
            });
        }

        let match_opts = MatchEventOptions::new().nip50(search_terms.is_some());
        let ndb_filter = Self::to_ndb_filter(filter, match_opts.nip50);
        let since = filter.since.map(|ts| ts.as_u64());
        let until = filter.until.map(|ts| ts.as_u64());
        let limit = filter.limit.filter(|&limit| limit > 0);
        let mut cursor = txn.open_ro_cursor(self.contents.database())?;
        let mut positioned = ContentsStore::position_cursor(&mut cursor, until)?;
        let mut matched_event_count = 0usize;
        let mut candidate_count = 0usize;
        let mut completed = true;

        while positioned {
            let (contents_key, content_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                Ok((Some(key), value)) => (key, value),
                Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                Err(err) => return Err(err.into()),
            };

            let (created_at, seq_bytes) = ContentsStore::split_key(contents_key)?;

            if let Some(since) = since
                && created_at < since
            {
                break;
            }

            if let Some(until) = until
                && created_at > until
            {
                positioned = Self::move_contents_cursor_prev(&mut cursor)?;
                continue;
            }

            candidate_count = candidate_count.saturating_add(1);
            let event_bytes = txn.get(self.events, &seq_bytes)?;
            let note = NdbNote::from_bytes(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;

            if !note_matches_filter(&note, &ndb_filter, match_opts, content_bytes) {
                positioned = Self::move_contents_cursor_prev(&mut cursor)?;
                continue;
            }

            if let Some(expiration) = extract_note_expiration(&note)
                && !Self::note_is_ephemeral(&note)
                && Self::is_expired(expiration)
            {
                positioned = Self::move_contents_cursor_prev(&mut cursor)?;
                continue;
            }

            matched_event_count = matched_event_count.saturating_add(1);
            let event_json = from_ndb_note(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
            if !on_event(event_json) {
                completed = false;
                break;
            }

            if let Some(limit) = limit
                && matched_event_count >= limit
            {
                break;
            }

            positioned = Self::move_contents_cursor_prev(&mut cursor)?;
        }

        Ok(StreamingFilterStats {
            index_scan_duration: index_start.elapsed(),
            matched_event_count,
            candidate_count,
            completed,
        })
    }

    fn stream_filters_events_as_json<'env, F>(
        &self,
        txn: &'env RoTransaction<'env>,
        filters: &[Filter],
        mut on_event: F,
    ) -> Result<StreamingQueryStats, SearchnosDBError>
    where
        F: FnMut(String) -> bool,
    {
        if filters.len() == 1 {
            let stats = self.stream_filter_events_as_json(txn, &filters[0], on_event)?;
            let completed = stats.completed;
            return Ok(StreamingQueryStats {
                index_scan_duration: stats.index_scan_duration,
                filter_stats: vec![stats],
                completed,
            });
        }

        let index_start = Instant::now();
        let mut states = filters
            .iter()
            .map(Self::streaming_filter_state)
            .collect::<Vec<_>>();
        let start_until = Self::streaming_start_until(&states);
        let mut cursor = txn.open_ro_cursor(self.contents.database())?;
        let mut positioned = ContentsStore::position_cursor(&mut cursor, start_until)?;
        let mut seen = HashSet::new();
        let mut completed = true;

        while positioned && states.iter().any(|state| !state.completed) {
            let (contents_key, content_bytes) = match cursor.get(None, None, MDB_GET_CURRENT) {
                Ok((Some(key), value)) => (key, value),
                Ok((None, _)) | Err(lmdb::Error::NotFound) => break,
                Err(err) => return Err(err.into()),
            };

            let (created_at, seq_bytes) = ContentsStore::split_key(contents_key)?;
            let eligible = Self::mark_streaming_candidates(&mut states, created_at);

            if !eligible {
                // TODO: Jump to the next active filter's `until` boundary when all
                // filters skip the current created_at range. This keeps correctness
                // simple for now, but sparse disjoint ranges still walk index rows
                // between filter windows.
                positioned = Self::move_contents_cursor_prev(&mut cursor)?;
                continue;
            }

            let event_bytes = txn.get(self.events, &seq_bytes)?;
            let note = NdbNote::from_bytes(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
            let expired = extract_note_expiration(&note).is_some_and(|expiration| {
                !Self::note_is_ephemeral(&note) && Self::is_expired(expiration)
            });

            let mut should_deliver = false;
            if !expired {
                for state in states.iter_mut().filter(|state| !state.completed) {
                    if !Self::streaming_filter_covers_created_at(state, created_at) {
                        continue;
                    }

                    if note_matches_filter(
                        &note,
                        &state.ndb_filter,
                        state.match_opts,
                        content_bytes,
                    ) {
                        state.matched_event_count = state.matched_event_count.saturating_add(1);
                        should_deliver = true;

                        if let Some(limit) = state.limit
                            && state.matched_event_count >= limit
                        {
                            state.completed = true;
                        }
                    }
                }
            }

            if should_deliver && seen.insert(seq_bytes) {
                let event_json =
                    from_ndb_note(event_bytes).map_err(SearchnosDBError::DecodeEvent)?;
                if !on_event(event_json) {
                    completed = false;
                    break;
                }
            }

            positioned = Self::move_contents_cursor_prev(&mut cursor)?;
        }

        let index_scan_duration = index_start.elapsed();
        let filter_stats = states
            .into_iter()
            .map(|state| StreamingFilterStats {
                index_scan_duration,
                matched_event_count: state.matched_event_count,
                candidate_count: state.candidate_count,
                completed: state.completed,
            })
            .collect();

        Ok(StreamingQueryStats {
            index_scan_duration,
            filter_stats,
            completed,
        })
    }

    fn streaming_filter_state(filter: &Filter) -> StreamingFilterState {
        let search_terms = filter
            .search
            .as_ref()
            .map(|search| normalize_query_terms(search));
        let completed =
            matches!(filter.limit, Some(0)) || search_terms.as_ref().is_some_and(Vec::is_empty);
        let match_opts = MatchEventOptions::new().nip50(search_terms.is_some());

        StreamingFilterState {
            ndb_filter: Self::to_ndb_filter(filter, match_opts.nip50),
            match_opts,
            since: filter.since.map(|ts| ts.as_u64()),
            until: filter.until.map(|ts| ts.as_u64()),
            limit: filter.limit.filter(|&limit| limit > 0),
            matched_event_count: 0,
            candidate_count: 0,
            completed,
        }
    }

    fn streaming_start_until(states: &[StreamingFilterState]) -> Option<u64> {
        if states
            .iter()
            .any(|state| !state.completed && state.until.is_none())
        {
            None
        } else {
            states
                .iter()
                .filter(|state| !state.completed)
                .filter_map(|state| state.until)
                .max()
        }
    }

    fn mark_streaming_candidates(states: &mut [StreamingFilterState], created_at: u64) -> bool {
        let mut eligible = false;

        for state in states.iter_mut().filter(|state| !state.completed) {
            if let Some(since) = state.since
                && created_at < since
            {
                state.completed = true;
                continue;
            }

            if !Self::streaming_filter_covers_created_at(state, created_at) {
                continue;
            }

            state.candidate_count = state.candidate_count.saturating_add(1);
            eligible = true;
        }

        eligible
    }

    fn streaming_filter_covers_created_at(state: &StreamingFilterState, created_at: u64) -> bool {
        if let Some(until) = state.until
            && created_at > until
        {
            return false;
        }

        if let Some(since) = state.since
            && created_at < since
        {
            return false;
        }

        true
    }

    fn move_contents_cursor_prev(cursor: &mut RoCursor<'_>) -> Result<bool, SearchnosDBError> {
        match cursor.get(None, None, MDB_PREV) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
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
