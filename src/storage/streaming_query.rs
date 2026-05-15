use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Instant;

use tracing::{info, warn};

use crate::nostr::Filter;

use super::cursor::{PacketCursor, best_day_cursor_index};
use super::event::{EventPacket, error_with_path};
use super::hot::HotEvents;
use super::partition::partition_path;
use super::query::{
    filter_has_search_terms, partition_days, query_packets_with_index,
    retain_visible_packets_if_needed, sort_packets,
};
use super::search::{remove_file_if_exists, search_bloom_may_match, search_index_path};
use super::visibility::VisibilityIndex;

struct StreamingFilterState<'a> {
    filter: &'a Filter,
    remaining: Option<usize>,
    has_search_terms: bool,
    visibility: Option<VisibilityIndex>,
    hot_by_day: BTreeMap<u64, Vec<EventPacket>>,
    partition_days: BTreeSet<u64>,
    missing_search_sidecars: u64,
    invalid_search_sidecars: u64,
    rebuilt_search_sidecars: u64,
    bloom_skipped_partitions: u64,
    searched_partitions: u64,
}

impl HotEvents {
    pub(crate) fn query_streaming(
        &self,
        filters: &[Filter],
        mut emit: impl FnMut(EventPacket) -> Result<(), Box<dyn Error>>,
    ) -> Result<(), Box<dyn Error>> {
        let started_at = Instant::now();
        let (hot_packets, hot_search_index) = self.hot_snapshot()?;
        let mut states = Vec::with_capacity(filters.len());
        let mut days = BTreeSet::new();

        for filter in filters {
            let mut hot_matches =
                query_packets_with_index(hot_packets.clone(), Some(&hot_search_index), filter)?;
            let has_search_terms = filter_has_search_terms(filter);
            let visibility = if has_search_terms {
                Some(VisibilityIndex::from_packets(&hot_packets)?)
            } else {
                None
            };
            retain_visible_packets_if_needed(&mut hot_matches, visibility.as_ref(), None)?;

            let mut hot_by_day: BTreeMap<u64, Vec<EventPacket>> = BTreeMap::new();
            for packet in hot_matches {
                hot_by_day
                    .entry(packet.unix_day())
                    .or_default()
                    .push(packet);
            }
            days.extend(hot_by_day.keys().copied());

            let partition_days = partition_days(&self.partitions_dir, filter)?;
            days.extend(partition_days.iter().copied());

            states.push(StreamingFilterState {
                filter,
                remaining: filter.limit,
                has_search_terms,
                visibility,
                hot_by_day,
                partition_days: partition_days.into_iter().collect(),
                missing_search_sidecars: 0,
                invalid_search_sidecars: 0,
                rebuilt_search_sidecars: 0,
                bloom_skipped_partitions: 0,
                searched_partitions: 0,
            });
        }

        let mut emitted = BTreeSet::new();
        for unix_day in days.into_iter().rev() {
            let mut day_cursors = Vec::new();

            for (state_index, state) in states.iter_mut().enumerate() {
                if state.remaining == Some(0) {
                    continue;
                }

                let mut filter_cursors = Vec::new();
                if let Some(mut hot_packets) = state.hot_by_day.remove(&unix_day) {
                    sort_packets(&mut hot_packets);
                    if let Some(cursor) = PacketCursor::buffered(hot_packets) {
                        filter_cursors.push(cursor);
                    }
                }

                if state.partition_days.contains(&unix_day) {
                    let partition_path = partition_path(&self.partitions_dir, unix_day);
                    let mut query_partition = true;
                    if state.has_search_terms {
                        let search_path = search_index_path(&partition_path);
                        if !search_path.exists() {
                            state.missing_search_sidecars += 1;
                            match self.rebuild_partition_sidecars(&partition_path) {
                                Ok(events) => {
                                    state.rebuilt_search_sidecars += 1;
                                    info!(
                                        path = %partition_path.display(),
                                        events,
                                        "rebuilt missing search sidecar during query"
                                    );
                                }
                                Err(err) => {
                                    warn!(
                                        path = %partition_path.display(),
                                        error = %err,
                                        "skipped partition after failing to rebuild missing search sidecar"
                                    );
                                    query_partition = false;
                                }
                            }
                        }

                        if query_partition {
                            match search_bloom_may_match(
                                &partition_path,
                                state.filter,
                                self.searchable_kinds.as_deref(),
                            ) {
                                Ok(true) => {}
                                Ok(false) => {
                                    state.bloom_skipped_partitions += 1;
                                    query_partition = false;
                                }
                                Err(err) => {
                                    warn!(
                                        path = %search_path.display(),
                                        error = %err,
                                        "rebuilding unreadable search sidecar during query"
                                    );
                                    state.invalid_search_sidecars += 1;
                                    remove_file_if_exists(&search_path).map_err(|err| {
                                        error_with_path("remove search index", &search_path, err)
                                    })?;
                                    match self.rebuild_partition_sidecars(&partition_path) {
                                        Ok(events) => {
                                            state.rebuilt_search_sidecars += 1;
                                            info!(
                                                path = %partition_path.display(),
                                                events,
                                                "rebuilt unreadable search sidecar during query"
                                            );
                                            match search_bloom_may_match(
                                                &partition_path,
                                                state.filter,
                                                self.searchable_kinds.as_deref(),
                                            ) {
                                                Ok(true) => {}
                                                Ok(false) => {
                                                    state.bloom_skipped_partitions += 1;
                                                    query_partition = false;
                                                }
                                                Err(err) => {
                                                    warn!(
                                                        path = %search_path.display(),
                                                        error = %err,
                                                        "skipped partition after rebuilt search sidecar remained unreadable"
                                                    );
                                                    query_partition = false;
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            warn!(
                                                path = %partition_path.display(),
                                                error = %err,
                                                "skipped partition after failing to rebuild unreadable search sidecar"
                                            );
                                            query_partition = false;
                                        }
                                    }
                                }
                            }
                        } else {
                            debug_assert!(!query_partition);
                        }
                    }

                    if query_partition {
                        if state.has_search_terms {
                            state.searched_partitions += 1;
                        }
                        let partition_cursor = match PacketCursor::partition(
                            &partition_path,
                            state.filter,
                            self.searchable_kinds.as_deref(),
                            state.visibility.as_ref(),
                            Some(self.visibility_store.clone()),
                        ) {
                            Ok(cursor) => cursor,
                            Err(err) if state.has_search_terms => {
                                let search_path = search_index_path(&partition_path);
                                warn!(
                                    path = %partition_path.display(),
                                    error = %err,
                                    "rebuilding unreadable search index during partition scan"
                                );
                                remove_file_if_exists(&search_path).map_err(|err| {
                                    error_with_path("remove search index", &search_path, err)
                                })?;
                                state.invalid_search_sidecars += 1;
                                match self.rebuild_partition_sidecars(&partition_path) {
                                    Ok(events) => {
                                        state.rebuilt_search_sidecars += 1;
                                        info!(
                                            path = %partition_path.display(),
                                            events,
                                            "rebuilt unreadable search index during partition scan"
                                        );
                                        PacketCursor::partition(
                                            &partition_path,
                                            state.filter,
                                            self.searchable_kinds.as_deref(),
                                            state.visibility.as_ref(),
                                            Some(self.visibility_store.clone()),
                                        )
                                        .unwrap_or_else(|err| {
                                            warn!(
                                                path = %partition_path.display(),
                                                error = %err,
                                                "skipped partition after rebuilt search index remained unreadable"
                                            );
                                            None
                                        })
                                    }
                                    Err(err) => {
                                        warn!(
                                            path = %partition_path.display(),
                                            error = %err,
                                            "skipped partition after failing to rebuild unreadable search index"
                                        );
                                        None
                                    }
                                }
                            }
                            Err(err) => return Err(err),
                        };
                        if let Some(cursor) = partition_cursor {
                            filter_cursors.push(cursor);
                        }
                    }
                }

                if let Some(cursor) = PacketCursor::merged(filter_cursors, state.remaining) {
                    day_cursors.push((state_index, cursor));
                }
            }

            while let Some(cursor_index) = best_day_cursor_index(&mut day_cursors)? {
                let state_index = day_cursors[cursor_index].0;
                let packet = day_cursors[cursor_index]
                    .1
                    .pop()?
                    .expect("cursor selected from non-empty peek");
                if let Some(remaining) = &mut states[state_index].remaining {
                    *remaining = remaining.saturating_sub(1);
                }
                if !emitted.insert(packet.id) {
                    continue;
                }
                emit(packet)?;
            }
        }

        for state in states {
            if state.has_search_terms {
                let inaccessible_search_sidecars =
                    state.missing_search_sidecars + state.invalid_search_sidecars;
                info!(
                    searched_partitions = state.searched_partitions,
                    bloom_skipped_partitions = state.bloom_skipped_partitions,
                    inaccessible_search_sidecars,
                    missing_search_sidecars = state.missing_search_sidecars,
                    invalid_search_sidecars = state.invalid_search_sidecars,
                    rebuilt_search_sidecars = state.rebuilt_search_sidecars,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "processed streaming search query"
                );
                if inaccessible_search_sidecars > 0 {
                    warn!(
                        inaccessible_search_sidecars,
                        missing_search_sidecars = state.missing_search_sidecars,
                        invalid_search_sidecars = state.invalid_search_sidecars,
                        rebuilt_search_sidecars = state.rebuilt_search_sidecars,
                        "encountered inaccessible search sidecars during query"
                    );
                }
            }
        }

        Ok(())
    }
}
