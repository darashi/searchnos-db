use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use tracing::{info, warn};

use crate::nostr::Filter;

use super::event::{EventPacket, error_with_path, read_event_packet};
use super::hot::HotEvents;
use super::partition::partition_event_paths;
use super::query::{compare_packets, partition_day_overlaps_filter};
use super::search::{
    SearchIndex, read_event_packet_at, read_search_bloom_for_events, read_search_index_for_events,
    remove_file_if_exists, search_index_path,
};
use super::text;
use super::visibility::VisibilityIndex;

struct BatchedFilterState<'a> {
    query_index: usize,
    filter: &'a Filter,
    remaining: Option<usize>,
    search_group: Option<usize>,
}

#[derive(Default)]
struct BatchedQueryState {
    active: bool,
    emitted: BTreeSet<[u8; 32]>,
}

struct SearchGroup {
    terms: Vec<String>,
    pattern_indexes: Vec<usize>,
}

struct SearchMatcher {
    automaton: Option<AhoCorasick>,
    groups: Vec<SearchGroup>,
    matched_generation: Vec<u32>,
    generation: u32,
}

struct Candidate {
    packet: EventPacket,
    search_groups: Vec<usize>,
    from_partition: bool,
}

enum PartitionCandidateSource<'a> {
    Indexed {
        file: File,
        index: &'a SearchIndex,
        position: usize,
    },
    Scan {
        file: File,
    },
    Empty,
}

#[derive(Default)]
struct BatchedQueryStats {
    missing_search_sidecars: u64,
    invalid_search_sidecars: u64,
    rebuilt_search_sidecars: u64,
    bloom_skipped_groups: u64,
    searched_partitions: u64,
}

impl SearchMatcher {
    fn new(group_terms: Vec<Vec<String>>) -> Result<Self, Box<dyn Error>> {
        let unique_terms = group_terms
            .iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let term_indexes = unique_terms
            .iter()
            .enumerate()
            .map(|(index, term)| (term.clone(), index))
            .collect::<BTreeMap<_, _>>();
        let groups = group_terms
            .into_iter()
            .map(|terms| SearchGroup {
                pattern_indexes: terms.iter().map(|term| term_indexes[term]).collect(),
                terms,
            })
            .collect();
        let automaton = if unique_terms.is_empty() {
            None
        } else {
            Some(
                AhoCorasickBuilder::new()
                    .match_kind(MatchKind::Standard)
                    .build(&unique_terms)?,
            )
        };

        Ok(Self {
            automaton,
            groups,
            matched_generation: vec![0; unique_terms.len()],
            generation: 0,
        })
    }

    fn matching_groups(
        &mut self,
        bytes: &[u8],
        enabled_groups: &[bool],
        requested_groups: &[bool],
    ) -> Vec<usize> {
        let Some(automaton) = &self.automaton else {
            return Vec::new();
        };

        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.matched_generation.fill(0);
            self.generation = 1;
        }
        for found in automaton.find_overlapping_iter(bytes) {
            self.matched_generation[found.pattern().as_usize()] = self.generation;
        }

        self.groups
            .iter()
            .enumerate()
            .filter(|(group_index, group)| {
                enabled_groups.get(*group_index) == Some(&true)
                    && requested_groups.get(*group_index) == Some(&true)
                    && group
                        .pattern_indexes
                        .iter()
                        .all(|index| self.matched_generation[*index] == self.generation)
            })
            .map(|(group_index, _)| group_index)
            .collect()
    }
}

impl HotEvents {
    pub(crate) fn query_streaming_batch(
        &self,
        queries: &[&[Filter]],
        mut is_query_active: impl FnMut(usize) -> bool,
        mut emit: impl FnMut(EventPacket, &[usize]) -> Result<Vec<usize>, Box<dyn Error>>,
    ) -> Result<(), Box<dyn Error>> {
        let started_at = Instant::now();
        let (mut filter_states, mut search_matcher) = build_filter_states(queries)?;
        let mut query_states = (0..queries.len())
            .map(|_| BatchedQueryState {
                active: true,
                ..BatchedQueryState::default()
            })
            .collect::<Vec<_>>();
        let (hot_packets, hot_search_index) = self.hot_snapshot()?;
        let hot_visibility = if search_matcher.groups.is_empty() {
            None
        } else {
            Some(VisibilityIndex::from_packets(&hot_packets)?)
        };
        let mut hot_by_day = BTreeMap::<u64, Vec<(usize, EventPacket)>>::new();
        for (index, packet) in hot_packets.into_iter().enumerate() {
            hot_by_day
                .entry(packet.unix_day())
                .or_default()
                .push((index, packet));
        }

        let mut partition_by_day = BTreeMap::<u64, PathBuf>::new();
        for path in partition_event_paths(&self.partitions_dir)? {
            partition_by_day.insert(partition_day(&path)?, path);
        }
        let mut days = partition_by_day.keys().copied().collect::<BTreeSet<_>>();
        days.extend(hot_by_day.keys().copied());

        let mut stats = BatchedQueryStats::default();
        for unix_day in days.into_iter().rev() {
            refresh_active_queries(&mut query_states, &mut is_query_active);
            let (requested_groups, include_non_search) = requested_searches_for_day(
                unix_day,
                &filter_states,
                &query_states,
                search_matcher.groups.len(),
            );
            if !include_non_search && !requested_groups.iter().any(|requested| *requested) {
                continue;
            }

            let mut hot_candidates = if let Some(hot) = hot_by_day.remove(&unix_day) {
                hot_candidates(
                    hot,
                    &hot_search_index,
                    &requested_groups,
                    include_non_search,
                    &mut search_matcher,
                )
            } else {
                Vec::new()
            };
            hot_candidates.sort_by(|a, b| compare_packets(&a.packet, &b.packet));

            let (search_index, enabled_groups) = if let Some(path) = partition_by_day.get(&unix_day)
            {
                let (search_index, enabled_groups) =
                    if requested_groups.iter().any(|requested| *requested) {
                        self.read_batched_search_index(
                            path,
                            &search_matcher.groups,
                            &requested_groups,
                            &mut stats,
                        )?
                    } else {
                        (None, requested_groups.clone())
                    };
                (search_index, enabled_groups)
            } else {
                (None, requested_groups.clone())
            };
            let mut partition_source = match partition_by_day.get(&unix_day) {
                Some(path) => PartitionCandidateSource::open(path, search_index.as_ref())?,
                None => PartitionCandidateSource::Empty,
            };
            let mut hot_candidates = hot_candidates.into_iter().peekable();
            let mut partition_candidate = None;
            let mut partition_finished = false;
            let mut seen = BTreeSet::new();

            loop {
                let (requested_groups, include_non_search) = requested_searches_for_day(
                    unix_day,
                    &filter_states,
                    &query_states,
                    search_matcher.groups.len(),
                );
                if !include_non_search && !requested_groups.iter().any(|requested| *requested) {
                    break;
                }
                if partition_candidate.is_none() && !partition_finished {
                    partition_candidate = partition_source.next_candidate(
                        &enabled_groups,
                        &requested_groups,
                        include_non_search,
                        &mut search_matcher,
                    )?;
                    partition_finished = partition_candidate.is_none();
                }

                let take_hot = match (hot_candidates.peek(), partition_candidate.as_ref()) {
                    (Some(hot), Some(partition)) => {
                        !compare_packets(&hot.packet, &partition.packet).is_gt()
                    }
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => break,
                };
                let candidate = if take_hot {
                    hot_candidates.next().expect("peeked hot candidate exists")
                } else {
                    partition_candidate
                        .take()
                        .expect("partition candidate exists")
                };
                if !seen.insert(candidate.packet.id) {
                    continue;
                }
                emit_candidate(
                    candidate,
                    unix_day,
                    &mut filter_states,
                    &mut query_states,
                    hot_visibility.as_ref(),
                    &self.visibility_store,
                    &mut emit,
                )?;
            }
        }

        info!(
            queries = queries.len(),
            filters = filter_states.len(),
            search_groups = search_matcher.groups.len(),
            searched_partitions = stats.searched_partitions,
            bloom_skipped_groups = stats.bloom_skipped_groups,
            missing_search_sidecars = stats.missing_search_sidecars,
            invalid_search_sidecars = stats.invalid_search_sidecars,
            rebuilt_search_sidecars = stats.rebuilt_search_sidecars,
            elapsed_ms = started_at.elapsed().as_millis(),
            "processed batched subscription snapshots"
        );

        Ok(())
    }

    fn read_batched_search_index(
        &self,
        path: &Path,
        groups: &[SearchGroup],
        requested_groups: &[bool],
        stats: &mut BatchedQueryStats,
    ) -> Result<(Option<SearchIndex>, Vec<bool>), Box<dyn Error>> {
        let search_path = search_index_path(path);
        if !search_path.exists() {
            stats.missing_search_sidecars += 1;
            if let Err(err) = self.rebuild_partition_sidecars(path) {
                warn!(
                    path = %path.display(),
                    error = %err,
                    "skipped batched search after failing to rebuild missing sidecar"
                );
                return Ok((None, vec![false; groups.len()]));
            }
            stats.rebuilt_search_sidecars += 1;
        }

        match read_batched_search_index_once(
            path,
            self.searchable_kinds.as_deref(),
            groups,
            requested_groups,
            stats,
        ) {
            Ok(result) => Ok(result),
            Err(err) => {
                stats.invalid_search_sidecars += 1;
                warn!(
                    path = %search_path.display(),
                    error = %err,
                    "rebuilding unreadable search sidecar during batched query"
                );
                remove_file_if_exists(&search_path)
                    .map_err(|err| error_with_path("remove search index", &search_path, err))?;
                if let Err(rebuild_err) = self.rebuild_partition_sidecars(path) {
                    warn!(
                        path = %path.display(),
                        error = %rebuild_err,
                        "skipped batched search after failing to rebuild unreadable sidecar"
                    );
                    return Ok((None, vec![false; groups.len()]));
                }
                stats.rebuilt_search_sidecars += 1;
                match read_batched_search_index_once(
                    path,
                    self.searchable_kinds.as_deref(),
                    groups,
                    requested_groups,
                    stats,
                ) {
                    Ok(result) => Ok(result),
                    Err(err) => {
                        warn!(
                            path = %search_path.display(),
                            error = %err,
                            "skipped batched search after rebuilt sidecar remained unreadable"
                        );
                        Ok((None, vec![false; groups.len()]))
                    }
                }
            }
        }
    }
}

fn build_filter_states<'a>(
    queries: &[&'a [Filter]],
) -> Result<(Vec<BatchedFilterState<'a>>, SearchMatcher), Box<dyn Error>> {
    let mut group_indexes = BTreeMap::<Vec<String>, usize>::new();
    let mut group_terms = Vec::new();
    let mut states = Vec::new();

    for (query_index, filters) in queries.iter().copied().enumerate() {
        for filter in filters {
            let mut terms = filter
                .search
                .as_deref()
                .map_or_else(Vec::new, text::search_terms);
            terms.sort();
            terms.dedup();
            let search_group = if terms.is_empty() {
                None
            } else if let Some(index) = group_indexes.get(&terms) {
                Some(*index)
            } else {
                let index = group_terms.len();
                group_indexes.insert(terms.clone(), index);
                group_terms.push(terms);
                Some(index)
            };
            states.push(BatchedFilterState {
                query_index,
                filter,
                remaining: filter.limit,
                search_group,
            });
        }
    }

    Ok((states, SearchMatcher::new(group_terms)?))
}

fn refresh_active_queries(
    query_states: &mut [BatchedQueryState],
    is_query_active: &mut impl FnMut(usize) -> bool,
) {
    for (query_index, state) in query_states.iter_mut().enumerate() {
        if state.active && !is_query_active(query_index) {
            state.active = false;
        }
    }
}

fn requested_searches_for_day(
    unix_day: u64,
    filter_states: &[BatchedFilterState<'_>],
    query_states: &[BatchedQueryState],
    search_group_count: usize,
) -> (Vec<bool>, bool) {
    let mut groups = vec![false; search_group_count];
    let mut include_non_search = false;
    for state in filter_states {
        if state.remaining == Some(0)
            || !query_states[state.query_index].active
            || !partition_day_overlaps_filter(unix_day, state.filter)
        {
            continue;
        }
        if let Some(group_index) = state.search_group {
            groups[group_index] = true;
        } else {
            include_non_search = true;
        }
    }
    (groups, include_non_search)
}

fn hot_candidates(
    hot: Vec<(usize, EventPacket)>,
    search_index: &SearchIndex,
    enabled_groups: &[bool],
    include_non_search: bool,
    matcher: &mut SearchMatcher,
) -> Vec<Candidate> {
    hot.into_iter()
        .filter_map(|(event_index, packet)| {
            let search_groups = matcher.matching_groups(
                search_index.record_text(event_index),
                enabled_groups,
                enabled_groups,
            );
            if include_non_search || !search_groups.is_empty() {
                Some(Candidate {
                    packet,
                    search_groups,
                    from_partition: false,
                })
            } else {
                None
            }
        })
        .collect()
}

impl<'a> PartitionCandidateSource<'a> {
    fn open(path: &Path, search_index: Option<&'a SearchIndex>) -> Result<Self, Box<dyn Error>> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Self::Empty),
            Err(err) => return Err(error_with_path("open events", path, err)),
        };
        Ok(match search_index {
            Some(index) => Self::Indexed {
                file,
                index,
                position: 0,
            },
            None => Self::Scan { file },
        })
    }

    fn next_candidate(
        &mut self,
        enabled_groups: &[bool],
        requested_groups: &[bool],
        include_non_search: bool,
        matcher: &mut SearchMatcher,
    ) -> Result<Option<Candidate>, Box<dyn Error>> {
        match self {
            Self::Indexed {
                file,
                index,
                position,
            } => {
                while *position < index.record_count() {
                    let event_index = *position;
                    *position += 1;
                    let search_groups = matcher.matching_groups(
                        index.record_text(event_index),
                        enabled_groups,
                        requested_groups,
                    );
                    if !include_non_search && search_groups.is_empty() {
                        continue;
                    }
                    let packet = read_event_packet_at(file, index.record(event_index))?;
                    return Ok(Some(Candidate {
                        packet,
                        search_groups,
                        from_partition: true,
                    }));
                }
                Ok(None)
            }
            Self::Scan { file } => {
                if !include_non_search {
                    return Ok(None);
                }
                Ok(read_event_packet(file)?.map(|packet| Candidate {
                    packet,
                    search_groups: Vec::new(),
                    from_partition: true,
                }))
            }
            Self::Empty => Ok(None),
        }
    }
}

fn read_batched_search_index_once(
    path: &Path,
    searchable_kinds: Option<&[u32]>,
    groups: &[SearchGroup],
    requested_groups: &[bool],
    stats: &mut BatchedQueryStats,
) -> Result<(Option<SearchIndex>, Vec<bool>), Box<dyn Error>> {
    let bloom = read_search_bloom_for_events(path, searchable_kinds)?;
    let mut enabled_groups = requested_groups.to_vec();
    for (group_index, enabled) in enabled_groups.iter_mut().enumerate() {
        if *enabled && !bloom.may_match_terms(&groups[group_index].terms) {
            *enabled = false;
            stats.bloom_skipped_groups += 1;
        }
    }
    if !enabled_groups.iter().any(|enabled| *enabled) {
        return Ok((None, enabled_groups));
    }

    let index = read_search_index_for_events(path, searchable_kinds)?;
    stats.searched_partitions += 1;
    Ok((Some(index), enabled_groups))
}

fn emit_candidate(
    candidate: Candidate,
    unix_day: u64,
    filter_states: &mut [BatchedFilterState<'_>],
    query_states: &mut [BatchedQueryState],
    hot_visibility: Option<&VisibilityIndex>,
    visibility_store: &super::visibility::VisibilityStore,
    emit: &mut impl FnMut(EventPacket, &[usize]) -> Result<Vec<usize>, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut visible = None;
    let mut query_indexes = Vec::new();
    for state in filter_states.iter_mut() {
        if state.remaining == Some(0)
            || !query_states[state.query_index].active
            || !partition_day_overlaps_filter(unix_day, state.filter)
        {
            continue;
        }
        if let Some(group_index) = state.search_group
            && candidate.search_groups.binary_search(&group_index).is_err()
        {
            continue;
        }
        if !candidate
            .packet
            .matches_filter_without_search(state.filter)?
        {
            continue;
        }
        if state.search_group.is_some() {
            let is_visible = match visible {
                Some(is_visible) => is_visible,
                None => {
                    let is_visible = match hot_visibility {
                        Some(index) => index.is_visible(&candidate.packet)?,
                        None => true,
                    } && (!candidate.from_partition
                        || visibility_store.is_visible(&candidate.packet)?);
                    visible = Some(is_visible);
                    is_visible
                }
            };
            if !is_visible {
                continue;
            }
        }

        if let Some(remaining) = &mut state.remaining {
            *remaining = remaining.saturating_sub(1);
        }
        if query_states[state.query_index]
            .emitted
            .insert(candidate.packet.id)
        {
            query_indexes.push(state.query_index);
        }
    }

    if query_indexes.is_empty() {
        return Ok(());
    }
    for query_index in emit(candidate.packet, &query_indexes)? {
        if let Some(state) = query_states.get_mut(query_index) {
            state.active = false;
        }
    }
    Ok(())
}

fn partition_day(path: &Path) -> Result<u64, Box<dyn Error>> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "missing partition file stem".into())
        .and_then(|value| value.parse().map_err(Into::into))
}

#[cfg(test)]
mod tests {
    use super::SearchMatcher;

    #[test]
    fn matcher_finds_overlapping_and_contained_terms() {
        let mut matcher = SearchMatcher::new(vec![
            vec!["aba".to_owned(), "ba".to_owned()],
            vec!["bab".to_owned()],
        ])
        .unwrap();

        assert_eq!(
            matcher.matching_groups(b"abab", &[true, true], &[true, true]),
            vec![0, 1]
        );
    }
}
