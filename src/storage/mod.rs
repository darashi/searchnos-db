#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use tracing::{info, warn};

use crate::nostr::Filter;

mod cursor;
mod event;
mod partition;
mod query;
mod reindex;
mod search;
mod text;
mod visibility;
use cursor::{PacketCursor, best_day_cursor_index};
use event::{
    EventPacket, error_with_path, read_event_packet, read_event_packets,
    read_event_packets_from_path, write_packet,
};
use partition::{
    compacting_hot_path, orphaned_compacting_hot_paths, partition_event_paths, partition_path,
    tmp_partition_path,
};
use query::{
    compare_packets, filter_has_search_terms, partition_days, query_packets_with_index,
    retain_visible_packets_if_needed, sort_packets,
};
use reindex::{ReindexJob, reindex_partition};
use search::{
    SearchIndex, append_to_search_index, build_search_index, empty_search_index,
    remove_file_if_exists, search_bloom_may_match, search_index_path, search_sidecar_is_current,
    tmp_search_index_path, write_built_search_index,
};
use visibility::{VisibilityIndex, VisibilityStore, VisibilitySummary, visibility_store_path};

const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

const DEFAULT_STORAGE_DIR: &str = "data";
const HOT_EVENTS_FILE: &str = "hot.events";
const STORAGE_LOCK_FILE: &str = "storage.lock";
const PARTITIONS_DIR: &str = "partitions";
const FIELD_SEPARATOR: &str = "\u{1f}";
const LONG_FORM_KIND: u32 = 30_023;
const SEARCH_INDEX_MAGIC: &[u8; 8] = b"SRCHSI01";
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub const DEFAULT_HOT_MAX_BYTES: u64 = 1024 * 1024;
pub type NegentropyItem = (u64, [u8; 32]);

pub struct Storage {
    hot_events: HotEvents,
}

impl Storage {
    pub fn open(
        hot_max_bytes: u64,
        searchable_kinds: Option<&[u32]>,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            hot_events: HotEvents::open(hot_max_bytes, searchable_kinds)?,
        })
    }

    pub fn append_packet(&self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        self.hot_events.append_packet(data)
    }

    pub fn compact(&self) -> Result<CompactStats, Box<dyn Error>> {
        self.hot_events.compact()
    }

    pub fn query(&self, filters: &[Filter]) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
        let mut packets = Vec::new();
        self.query_streaming(filters, |packet| {
            packets.push(packet);
            Ok(())
        })?;
        Ok(packets)
    }

    pub fn query_streaming(
        &self,
        filters: &[Filter],
        mut emit: impl FnMut(Vec<u8>) -> Result<(), Box<dyn Error>>,
    ) -> Result<(), Box<dyn Error>> {
        self.hot_events
            .query_streaming(filters, |packet| emit(packet.data))
    }

    pub fn packet_matches_filter(
        &self,
        data: &[u8],
        filter: &Filter,
    ) -> Result<bool, Box<dyn Error>> {
        self.hot_events.packet_matches_filter(data, filter)
    }

    pub fn negentropy_items_for_unix_day(
        &self,
        unix_day: u64,
    ) -> Result<Vec<NegentropyItem>, Box<dyn Error>> {
        self.hot_events.negentropy_items_for_unix_day(unix_day)
    }

    pub fn reindex(&self) -> Result<ReindexStats, Box<dyn Error>> {
        self.reindex_with_progress(false, |_| {})
    }

    pub fn reindex_all(&self) -> Result<ReindexStats, Box<dyn Error>> {
        self.reindex_with_progress(true, |_| {})
    }

    pub fn reindex_with_progress(
        &self,
        force: bool,
        progress: impl FnMut(ReindexProgress),
    ) -> Result<ReindexStats, Box<dyn Error>> {
        self.hot_events.reindex(force, progress)
    }

    pub fn open_at(
        storage_dir: impl Into<PathBuf>,
        hot_max_bytes: u64,
    ) -> Result<Self, Box<dyn Error>> {
        Self::open_at_with_searchable_kinds(storage_dir, hot_max_bytes, None)
    }

    pub fn open_at_with_searchable_kinds(
        storage_dir: impl Into<PathBuf>,
        hot_max_bytes: u64,
        searchable_kinds: Option<&[u32]>,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            hot_events: HotEvents::open_at_with_searchable_kinds(
                storage_dir,
                hot_max_bytes,
                searchable_kinds,
            )?,
        })
    }
}

#[derive(Debug, Default)]
pub struct CompactStats {
    pub files: u64,
    pub events: u64,
    pub bytes: u64,
}

#[derive(Debug, Default)]
pub struct ReindexStats {
    pub files: u64,
    pub skipped_files: u64,
    pub events: u64,
}

#[derive(Debug)]
pub struct ReindexProgress {
    pub phase: ReindexProgressPhase,
    pub path: PathBuf,
    pub file_index: u64,
    pub file_total: u64,
    pub events: u64,
}

#[derive(Debug)]
pub enum ReindexProgressPhase {
    Started,
    Finished,
    Skipped,
}

struct HotEvents {
    path: PathBuf,
    partitions_dir: PathBuf,
    _lock_file: File,
    max_bytes: u64,
    searchable_kinds: Option<Vec<u32>>,
    visibility_store: Arc<VisibilityStore>,
    state: Arc<Mutex<HotState>>,
    sidecar_updates: Arc<SidecarUpdateQueue>,
    compact_running: Arc<AtomicBool>,
    compact_pending: Arc<AtomicBool>,
    compacting_paths: Arc<Mutex<Vec<PathBuf>>>,
    deferred_compaction_last_log_unix: AtomicU64,
}

struct HotState {
    file: File,
    hot_search_index: SearchIndex,
}

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

struct SidecarUpdateQueue {
    state: Mutex<SidecarUpdateState>,
    available: Condvar,
}

#[derive(Default)]
struct SidecarUpdateState {
    active: bool,
    pending_compactions: u64,
}

enum SidecarUpdateKind {
    Compaction,
    Reindex,
}

struct SidecarUpdateGuard<'a> {
    queue: &'a SidecarUpdateQueue,
}

impl SidecarUpdateQueue {
    fn new() -> Self {
        Self {
            state: Mutex::new(SidecarUpdateState::default()),
            available: Condvar::new(),
        }
    }

    fn acquire_compaction(&self) -> Result<SidecarUpdateGuard<'_>, Box<dyn Error>> {
        self.acquire(SidecarUpdateKind::Compaction)
    }

    fn acquire_reindex(&self) -> Result<SidecarUpdateGuard<'_>, Box<dyn Error>> {
        self.acquire(SidecarUpdateKind::Reindex)
    }

    fn acquire(&self, kind: SidecarUpdateKind) -> Result<SidecarUpdateGuard<'_>, Box<dyn Error>> {
        let mut state = self
            .state
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?;
        if matches!(kind, SidecarUpdateKind::Compaction) {
            state.pending_compactions += 1;
        }

        while state.active
            || (matches!(kind, SidecarUpdateKind::Reindex) && state.pending_compactions > 0)
        {
            state = self
                .available
                .wait(state)
                .map_err(|err| io::Error::other(err.to_string()))?;
        }

        if matches!(kind, SidecarUpdateKind::Compaction) {
            state.pending_compactions -= 1;
        }
        state.active = true;
        Ok(SidecarUpdateGuard { queue: self })
    }
}

impl Drop for SidecarUpdateGuard<'_> {
    fn drop(&mut self) {
        let Ok(mut state) = self.queue.state.lock() else {
            return;
        };
        state.active = false;
        self.queue.available.notify_all();
    }
}

impl HotEvents {
    fn open(max_bytes: u64, searchable_kinds: Option<&[u32]>) -> Result<Self, Box<dyn Error>> {
        Self::open_at_with_searchable_kinds(DEFAULT_STORAGE_DIR, max_bytes, searchable_kinds)
    }

    #[cfg(test)]
    fn open_at(storage_dir: impl Into<PathBuf>, max_bytes: u64) -> Result<Self, Box<dyn Error>> {
        Self::open_at_with_searchable_kinds(storage_dir, max_bytes, None)
    }

    fn open_at_with_searchable_kinds(
        storage_dir: impl Into<PathBuf>,
        max_bytes: u64,
        searchable_kinds: Option<&[u32]>,
    ) -> Result<Self, Box<dyn Error>> {
        let storage_dir = storage_dir.into();
        let path = storage_dir.join(HOT_EVENTS_FILE);
        let partitions_dir = storage_dir.join(PARTITIONS_DIR);
        let searchable_kinds = text::normalize_searchable_kinds(searchable_kinds);

        fs::create_dir_all(&storage_dir)?;
        let lock_file = acquire_storage_lock(&storage_dir)?;
        fs::create_dir_all(&partitions_dir)?;
        let visibility_store = Arc::new(VisibilityStore::open(visibility_store_path(
            &partitions_dir,
        ))?);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let hot_packets = read_event_packets(&mut file)
            .map_err(|err| error_with_path("read events", &path, err))?;
        let hot_search_index = build_search_index(&hot_packets, searchable_kinds.as_deref())
            .map_err(|err| error_with_path("build search index for events", &path, err))?;
        file.seek(SeekFrom::End(0))?;
        remove_file_if_exists(search_index_path(&path))?;

        let hot_events = Self {
            path,
            partitions_dir,
            _lock_file: lock_file,
            max_bytes,
            searchable_kinds,
            visibility_store,
            state: Arc::new(Mutex::new(HotState {
                file,
                hot_search_index,
            })),
            sidecar_updates: Arc::new(SidecarUpdateQueue::new()),
            compact_running: Arc::new(AtomicBool::new(false)),
            compact_pending: Arc::new(AtomicBool::new(false)),
            compacting_paths: Arc::new(Mutex::new(Vec::new())),
            deferred_compaction_last_log_unix: AtomicU64::new(0),
        };
        hot_events.recover_compacting_hot_files()?;
        hot_events.reindex(false, |_| {})?;
        Ok(hot_events)
    }

    fn recover_compacting_hot_files(&self) -> Result<(), Box<dyn Error>> {
        let compacting_paths = orphaned_compacting_hot_paths(&self.path)?;
        for compact_path in compacting_paths {
            let started_at = Instant::now();
            let hot_bytes = fs::metadata(&compact_path)?.len();
            info!(
                path = %compact_path.display(),
                hot_bytes,
                "recovering compacting hot events file"
            );
            let output = compact_hot_file(
                &compact_path,
                &self.partitions_dir,
                self.searchable_kinds.as_deref(),
                &self.visibility_store,
                &self.sidecar_updates,
            )?;
            fs::remove_file(&compact_path).map_err(|err| {
                error_with_path("remove recovered compacting hot events", &compact_path, err)
            })?;
            info!(
                path = %compact_path.display(),
                hot_bytes,
                output_partitions = output.output_partitions,
                elapsed_ms = started_at.elapsed().as_millis(),
                "recovered compacting hot events file"
            );
        }
        Ok(())
    }

    pub fn append_packet(&self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut state = self
            .state
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?;
        let len = u32::try_from(data.len())?;
        let mut packet = Vec::with_capacity(size_of::<u32>() + data.len());
        packet.extend_from_slice(&len.to_le_bytes());
        packet.extend_from_slice(data);

        state.file.write_all(&packet)?;
        append_to_search_index(
            &mut state.hot_search_index,
            data,
            self.searchable_kinds.as_deref(),
        )?;
        let hot_bytes = state.file.metadata()?.len();
        if hot_bytes > self.max_bytes {
            if self
                .compact_running
                .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                match self.rotate_hot_for_compaction(&mut state) {
                    Ok(compact_path) => self.spawn_compaction(compact_path, hot_bytes),
                    Err(err) => {
                        self.compact_running.store(false, AtomicOrdering::Release);
                        return Err(err);
                    }
                }
            } else {
                self.compact_pending.store(true, AtomicOrdering::Release);
                if self.should_log_deferred_compaction() {
                    info!(
                        hot_bytes,
                        "queued hot event compaction while another compaction is running"
                    );
                }
            }
        }
        Ok(())
    }

    fn compact(&self) -> Result<CompactStats, Box<dyn Error>> {
        if self
            .compact_running
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return Err(io::Error::other("compaction is already running").into());
        }

        let result = (|| {
            let mut state = self
                .state
                .lock()
                .map_err(|err| io::Error::other(err.to_string()))?;
            let hot_bytes = state.file.metadata()?.len();
            if hot_bytes == 0 {
                return Ok(CompactStats::default());
            }

            let compact_path = self.rotate_hot_for_compaction(&mut state)?;
            drop(state);

            let context = CompactionContext {
                path: self.path.clone(),
                partitions_dir: self.partitions_dir.clone(),
                max_bytes: self.max_bytes,
                searchable_kinds: self.searchable_kinds.clone(),
                visibility_store: self.visibility_store.clone(),
                state: self.state.clone(),
                sidecar_updates: self.sidecar_updates.clone(),
                compact_running: self.compact_running.clone(),
                compact_pending: self.compact_pending.clone(),
                compacting_paths: self.compacting_paths.clone(),
                compact_path,
                hot_bytes,
            };
            run_one_compaction(&context)
        })();

        self.compact_running.store(false, AtomicOrdering::Release);
        if self.compact_pending.swap(false, AtomicOrdering::AcqRel)
            && self
                .compact_running
                .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
        {
            self.spawn_pending_compaction();
        }

        result
    }

    fn packet_matches_filter(&self, data: &[u8], filter: &Filter) -> Result<bool, Box<dyn Error>> {
        EventPacket::from_data(data.to_vec())?
            .matches_filter(filter, self.searchable_kinds.as_deref())
    }

    fn should_log_deferred_compaction(&self) -> bool {
        const DEFERRED_COMPACTION_LOG_INTERVAL_SECS: u64 = 10;

        let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            return false;
        };
        let now = elapsed.as_secs();
        let last = self
            .deferred_compaction_last_log_unix
            .load(AtomicOrdering::Acquire);
        if now.saturating_sub(last) < DEFERRED_COMPACTION_LOG_INTERVAL_SECS {
            return false;
        }
        self.deferred_compaction_last_log_unix
            .compare_exchange(last, now, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_ok()
    }

    fn rotate_hot_for_compaction(&self, state: &mut HotState) -> Result<PathBuf, Box<dyn Error>> {
        rotate_hot_for_compaction(
            &self.path,
            self.searchable_kinds.as_deref(),
            &self.compacting_paths,
            state,
        )
    }

    fn spawn_compaction(&self, compact_path: PathBuf, hot_bytes: u64) {
        #[cfg(test)]
        {
            run_compaction_queue(CompactionContext {
                path: self.path.clone(),
                partitions_dir: self.partitions_dir.clone(),
                max_bytes: self.max_bytes,
                searchable_kinds: self.searchable_kinds.clone(),
                visibility_store: self.visibility_store.clone(),
                state: self.state.clone(),
                sidecar_updates: self.sidecar_updates.clone(),
                compact_running: self.compact_running.clone(),
                compact_pending: self.compact_pending.clone(),
                compacting_paths: self.compacting_paths.clone(),
                compact_path,
                hot_bytes,
            });
        }

        #[cfg(not(test))]
        {
            let context = CompactionContext {
                path: self.path.clone(),
                partitions_dir: self.partitions_dir.clone(),
                max_bytes: self.max_bytes,
                searchable_kinds: self.searchable_kinds.clone(),
                visibility_store: self.visibility_store.clone(),
                state: self.state.clone(),
                sidecar_updates: self.sidecar_updates.clone(),
                compact_running: self.compact_running.clone(),
                compact_pending: self.compact_pending.clone(),
                compacting_paths: self.compacting_paths.clone(),
                compact_path,
                hot_bytes,
            };
            std::thread::Builder::new()
                .name("searchnos-compact".to_owned())
                .spawn(move || run_compaction_queue(context))
                .expect("spawn hot events compaction thread");
        }
    }

    fn pending_compaction_context(&self) -> CompactionContext {
        CompactionContext {
            path: self.path.clone(),
            partitions_dir: self.partitions_dir.clone(),
            max_bytes: self.max_bytes,
            searchable_kinds: self.searchable_kinds.clone(),
            visibility_store: self.visibility_store.clone(),
            state: self.state.clone(),
            sidecar_updates: self.sidecar_updates.clone(),
            compact_running: self.compact_running.clone(),
            compact_pending: self.compact_pending.clone(),
            compacting_paths: self.compacting_paths.clone(),
            compact_path: PathBuf::new(),
            hot_bytes: 0,
        }
    }

    fn spawn_pending_compaction(&self) {
        let context = self.pending_compaction_context();
        #[cfg(test)]
        {
            run_pending_compaction_queue(context);
        }

        #[cfg(not(test))]
        {
            std::thread::Builder::new()
                .name("searchnos-compact".to_owned())
                .spawn(move || run_pending_compaction_queue(context))
                .expect("spawn hot events compaction thread");
        }
    }

    fn compacting_paths_snapshot(&self) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        Ok(self
            .compacting_paths
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .clone())
    }

    fn read_compacting_packets(&self) -> Result<Vec<EventPacket>, Box<dyn Error>> {
        let mut packets = Vec::new();
        for path in self.compacting_paths_snapshot()? {
            packets.extend(read_event_packets_from_path(&path)?);
        }
        Ok(packets)
    }

    fn query_streaming(
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

    fn rebuild_partition_sidecars(&self, path: &Path) -> Result<u64, Box<dyn Error>> {
        let _sidecar_updates = self.sidecar_updates.acquire_reindex()?;
        let result = reindex_partition(
            ReindexJob {
                path: path.to_path_buf(),
                file_index: 0,
            },
            self.searchable_kinds.as_deref(),
        )?;
        self.visibility_store
            .merge_summary(&result.visibility_summary)
            .map_err(|err| error_with_path("write visibility index", &self.partitions_dir, err))?;
        Ok(result.events)
    }

    fn hot_snapshot(&self) -> Result<(Vec<EventPacket>, SearchIndex), Box<dyn Error>> {
        let mut state = self
            .state
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?;
        let hot_packets = read_event_packets(&mut state.file)
            .map_err(|err| error_with_path("read events", &self.path, err))?;
        let mut compacting_packets = self.read_compacting_packets()?;
        if compacting_packets.is_empty() {
            Ok((hot_packets, state.hot_search_index.clone()))
        } else {
            let mut packets = hot_packets;
            packets.append(&mut compacting_packets);
            let search_index = build_search_index(&packets, self.searchable_kinds.as_deref())?;
            Ok((packets, search_index))
        }
    }

    fn negentropy_items_for_unix_day(
        &self,
        unix_day: u64,
    ) -> Result<Vec<NegentropyItem>, Box<dyn Error>> {
        let mut items = Vec::new();
        for packet in read_event_packets_from_path(&partition_path(&self.partitions_dir, unix_day))?
        {
            items.push((packet.created_at, packet.id));
        }
        let (hot_packets, _) = self.hot_snapshot()?;
        for packet in hot_packets {
            if packet.unix_day() == unix_day {
                items.push((packet.created_at, packet.id));
            }
        }
        items.sort_unstable();
        items.dedup_by_key(|(_, id)| *id);
        Ok(items)
    }

    fn reindex(
        &self,
        force: bool,
        mut progress: impl FnMut(ReindexProgress),
    ) -> Result<ReindexStats, Box<dyn Error>> {
        let mut stats = ReindexStats::default();

        let partition_paths = partition_event_paths(&self.partitions_dir)?;
        let file_total = partition_paths.len() as u64;
        let mut reindex_jobs = Vec::new();
        let mut last_plan_log = Instant::now();

        info!(file_total, force, "checking partition index freshness");

        for (index, path) in partition_paths.iter().enumerate() {
            let file_index = index as u64 + 1;
            if file_index == 1 || last_plan_log.elapsed() >= Duration::from_secs(10) {
                info!(
                    path = %path.display(),
                    file_index,
                    file_total,
                    "checking partition index freshness"
                );
                last_plan_log = Instant::now();
            }

            if !force && search_sidecar_is_current(path, self.searchable_kinds.as_deref())? {
                stats.skipped_files += 1;
                progress(ReindexProgress {
                    phase: ReindexProgressPhase::Skipped,
                    path: path.clone(),
                    file_index,
                    file_total,
                    events: 0,
                });
                continue;
            }

            reindex_jobs.push(ReindexJob {
                path: path.clone(),
                file_index,
            });
        }

        info!(
            files = reindex_jobs.len(),
            skipped_files = stats.skipped_files,
            file_total,
            "planned partition index updates"
        );

        for job in reindex_jobs {
            progress(ReindexProgress {
                phase: ReindexProgressPhase::Started,
                path: job.path.clone(),
                file_index: job.file_index,
                file_total,
                events: 0,
            });
            let _sidecar_updates = self.sidecar_updates.acquire_reindex()?;
            let result = reindex_partition(job, self.searchable_kinds.as_deref())?;
            self.visibility_store
                .merge_summary(&result.visibility_summary)
                .map_err(|err| {
                    error_with_path("write visibility index", &self.partitions_dir, err)
                })?;
            stats.files += 1;
            stats.events += result.events;
            progress(ReindexProgress {
                phase: ReindexProgressPhase::Finished,
                path: result.path,
                file_index: result.file_index,
                file_total,
                events: result.events,
            });
        }

        Ok(stats)
    }
}

fn acquire_storage_lock(storage_dir: &Path) -> Result<File, Box<dyn Error>> {
    let lock_path = storage_dir.join(STORAGE_LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|err| error_with_path("open storage lock", &lock_path, err))?;
    file.try_lock_exclusive()
        .map_err(|err| error_with_path("lock storage", &lock_path, err))?;
    Ok(file)
}

struct CompactionContext {
    path: PathBuf,
    partitions_dir: PathBuf,
    max_bytes: u64,
    searchable_kinds: Option<Vec<u32>>,
    visibility_store: Arc<VisibilityStore>,
    state: Arc<Mutex<HotState>>,
    sidecar_updates: Arc<SidecarUpdateQueue>,
    compact_running: Arc<AtomicBool>,
    compact_pending: Arc<AtomicBool>,
    compacting_paths: Arc<Mutex<Vec<PathBuf>>>,
    compact_path: PathBuf,
    hot_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct CompactOutput {
    output_partitions: usize,
    events: u64,
}

impl CompactOutput {
    fn stats(self, bytes: u64) -> CompactStats {
        CompactStats {
            files: self.output_partitions as u64,
            events: self.events,
            bytes,
        }
    }
}

fn run_pending_compaction_queue(mut context: CompactionContext) {
    match prepare_pending_compaction(&context) {
        Ok(Some((compact_path, hot_bytes))) => {
            context.compact_path = compact_path;
            context.hot_bytes = hot_bytes;
            run_compaction_queue(context);
        }
        Ok(None) => {
            context
                .compact_running
                .store(false, AtomicOrdering::Release);
        }
        Err(err) => {
            warn!(%err, "failed to prepare queued hot event compaction");
            context
                .compact_running
                .store(false, AtomicOrdering::Release);
        }
    }
}

fn run_compaction_queue(mut context: CompactionContext) {
    loop {
        let _ = run_one_compaction(&context);

        if !context.compact_pending.swap(false, AtomicOrdering::AcqRel) {
            context
                .compact_running
                .store(false, AtomicOrdering::Release);
            if !context.compact_pending.swap(false, AtomicOrdering::AcqRel) {
                return;
            }
            if context
                .compact_running
                .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_err()
            {
                return;
            }
        }

        let next = prepare_pending_compaction(&context);
        match next {
            Ok(Some((compact_path, hot_bytes))) => {
                context.compact_path = compact_path;
                context.hot_bytes = hot_bytes;
            }
            Ok(None) => {
                context
                    .compact_running
                    .store(false, AtomicOrdering::Release);
                return;
            }
            Err(err) => {
                warn!(%err, "failed to prepare queued hot event compaction");
                context
                    .compact_running
                    .store(false, AtomicOrdering::Release);
                return;
            }
        }
    }
}

fn run_one_compaction(context: &CompactionContext) -> Result<CompactStats, Box<dyn Error>> {
    let started_at = Instant::now();
    info!(
        path = %context.compact_path.display(),
        hot_bytes = context.hot_bytes,
        "compacting hot events"
    );
    match compact_hot_file(
        &context.compact_path,
        &context.partitions_dir,
        context.searchable_kinds.as_deref(),
        &context.visibility_store,
        &context.sidecar_updates,
    ) {
        Ok(output) => {
            if let Ok(mut paths) = context.compacting_paths.lock() {
                paths.retain(|path| path != &context.compact_path);
            }
            if let Err(err) = fs::remove_file(&context.compact_path) {
                warn!(
                    path = %context.compact_path.display(),
                    %err,
                    "failed to remove compacted hot events file"
                );
            }
            info!(
                hot_bytes = context.hot_bytes,
                output_partitions = output.output_partitions,
                elapsed_ms = started_at.elapsed().as_millis(),
                "compacted hot events"
            );
            Ok(output.stats(context.hot_bytes))
        }
        Err(err) => {
            warn!(
                path = %context.compact_path.display(),
                %err,
                "failed to compact hot events"
            );
            Err(err)
        }
    }
}

fn prepare_pending_compaction(
    context: &CompactionContext,
) -> Result<Option<(PathBuf, u64)>, Box<dyn Error>> {
    let mut state = context
        .state
        .lock()
        .map_err(|err| io::Error::other(err.to_string()))?;
    let hot_bytes = state.file.metadata()?.len();
    if hot_bytes <= context.max_bytes {
        return Ok(None);
    }

    let compact_path = rotate_hot_for_compaction(
        &context.path,
        context.searchable_kinds.as_deref(),
        &context.compacting_paths,
        &mut state,
    )?;
    Ok(Some((compact_path, hot_bytes)))
}

fn rotate_hot_for_compaction(
    path: &Path,
    searchable_kinds: Option<&[u32]>,
    compacting_paths: &Mutex<Vec<PathBuf>>,
    state: &mut HotState,
) -> Result<PathBuf, Box<dyn Error>> {
    state.file.flush()?;
    let compact_path = compacting_hot_path(path);
    fs::rename(path, &compact_path)?;
    state.file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    state.hot_search_index = empty_search_index(searchable_kinds);
    remove_file_if_exists(search_index_path(path))?;
    state.file.seek(SeekFrom::End(0))?;
    compacting_paths
        .lock()
        .map_err(|err| io::Error::other(err.to_string()))?
        .push(compact_path.clone());
    Ok(compact_path)
}

fn compact_hot_file(
    compact_path: &Path,
    partitions_dir: &Path,
    searchable_kinds: Option<&[u32]>,
    visibility_store: &VisibilityStore,
    sidecar_updates: &SidecarUpdateQueue,
) -> Result<CompactOutput, Box<dyn Error>> {
    let hot_packets = read_event_packets_from_path(compact_path)?;
    let events = hot_packets.len() as u64;
    let mut packets_by_day = BTreeMap::<u64, Vec<EventPacket>>::new();
    for packet in hot_packets {
        packets_by_day
            .entry(packet.unix_day())
            .or_default()
            .push(packet);
    }

    let output_partitions = packets_by_day.len();
    let _sidecar_updates = sidecar_updates.acquire_compaction()?;
    let jobs = Mutex::new(packets_by_day.into_iter());
    thread::scope(|scope| {
        let worker_count = thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(output_partitions);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let jobs = &jobs;
            handles.push(scope.spawn(move || {
                loop {
                    let Some((unix_day, packets)) =
                        jobs.lock().map_err(|err| err.to_string())?.next()
                    else {
                        return Ok::<(), String>(());
                    };
                    let partition_path = partition_path(partitions_dir, unix_day);
                    merge_packets_into_partition(
                        &partition_path,
                        packets,
                        searchable_kinds,
                        visibility_store,
                    )
                    .map_err(|err| {
                        error_with_path("merge packets into partition", &partition_path, err)
                            .to_string()
                    })?;
                }
            }));
        }

        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err.into()),
                Err(_) => return Err("partition update thread panicked".into()),
            }
        }
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(CompactOutput {
        output_partitions,
        events,
    })
}

fn merge_packets_into_partition(
    partition_path: &Path,
    new_packets: Vec<EventPacket>,
    searchable_kinds: Option<&[u32]>,
    visibility_store: &VisibilityStore,
) -> Result<(), Box<dyn Error>> {
    let mut new_packets = new_packets;
    new_packets.sort_by(compare_packets);
    let mut new_packets = new_packets.into_iter().peekable();

    let mut existing_partition = match File::open(partition_path) {
        Ok(file) => Some(file),
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => return Err(error_with_path("open events", partition_path, err)),
    };
    let mut existing_packet = match existing_partition.as_mut() {
        Some(file) => read_event_packet(file)
            .map_err(|err| error_with_path("read events", partition_path, err))?,
        None => None,
    };

    let tmp_path = tmp_partition_path(partition_path);
    let search_path = search_index_path(partition_path);
    let tmp_search_path = tmp_search_index_path(&search_path);
    let mut tmp_partition = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;

    let mut search_index = empty_search_index(searchable_kinds);
    let mut visibility_summary = VisibilitySummary::default();
    loop {
        let take_new = match (existing_packet.as_ref(), new_packets.peek()) {
            (Some(existing), Some(new)) => compare_packets(existing, new).is_gt(),
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (None, None) => break,
        };

        if take_new {
            let packet = new_packets.next().expect("new packet is present");
            write_merged_packet(
                &mut tmp_partition,
                &mut search_index,
                &mut visibility_summary,
                &packet,
                searchable_kinds,
            )?;
        } else {
            let packet = existing_packet.take().expect("existing packet is present");
            write_merged_packet(
                &mut tmp_partition,
                &mut search_index,
                &mut visibility_summary,
                &packet,
                searchable_kinds,
            )?;
            existing_packet = read_event_packet(
                existing_partition
                    .as_mut()
                    .expect("existing partition is open"),
            )
            .map_err(|err| error_with_path("read events", partition_path, err))?;
        }
    }

    tmp_partition.sync_all()?;
    drop(tmp_partition);
    write_built_search_index(&tmp_search_path, &search_index)?;
    fs::rename(&tmp_path, partition_path)?;
    fs::rename(&tmp_search_path, search_path)?;
    visibility_store.merge_summary(&visibility_summary)?;
    Ok(())
}

fn write_merged_packet(
    partition: &mut File,
    search_index: &mut SearchIndex,
    visibility_summary: &mut VisibilitySummary,
    packet: &EventPacket,
    searchable_kinds: Option<&[u32]>,
) -> Result<(), Box<dyn Error>> {
    write_packet(partition, &packet.data)?;
    append_to_search_index(search_index, &packet.data, searchable_kinds)?;
    visibility_summary.add_packet(packet)?;
    Ok(())
}

#[cfg(test)]
mod tests;
