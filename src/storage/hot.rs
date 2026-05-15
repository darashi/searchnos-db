use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use tracing::info;

use crate::nostr::Filter;

use super::compaction::{
    CompactionContext, compact_hot_file, rotate_hot_for_compaction, run_compaction_queue,
    run_one_compaction, run_pending_compaction_queue,
};
use super::event::{
    EventPacket, error_with_path, read_event_packets, read_event_packets_from_path,
};
use super::partition::{orphaned_compacting_hot_paths, partition_path};
use super::search::{
    SearchIndex, append_to_search_index, build_search_index, remove_file_if_exists,
    search_index_path,
};
use super::sidecar_queue::SidecarUpdateQueue;
use super::text;
use super::visibility::{VisibilityStore, visibility_store_path};
use super::{CompactStats, NegentropyItem};

const DEFAULT_STORAGE_DIR: &str = "data";
const HOT_EVENTS_FILE: &str = "hot.events";
const STORAGE_LOCK_FILE: &str = "storage.lock";
const PARTITIONS_DIR: &str = "partitions";

pub(crate) struct HotEvents {
    pub(crate) path: PathBuf,
    pub(crate) partitions_dir: PathBuf,
    _lock_file: File,
    max_bytes: u64,
    pub(crate) searchable_kinds: Option<Vec<u32>>,
    pub(crate) visibility_store: Arc<VisibilityStore>,
    state: Arc<Mutex<HotState>>,
    pub(crate) sidecar_updates: Arc<SidecarUpdateQueue>,
    compact_running: Arc<AtomicBool>,
    compact_pending: Arc<AtomicBool>,
    compacting_paths: Arc<Mutex<Vec<PathBuf>>>,
    deferred_compaction_last_log_unix: AtomicU64,
}

pub(crate) struct HotState {
    pub(crate) file: File,
    pub(crate) hot_search_index: SearchIndex,
}

impl HotEvents {
    pub(crate) fn open(
        max_bytes: u64,
        searchable_kinds: Option<&[u32]>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::open_at_with_searchable_kinds(DEFAULT_STORAGE_DIR, max_bytes, searchable_kinds)
    }

    #[cfg(test)]
    pub(crate) fn open_at(
        storage_dir: impl Into<PathBuf>,
        max_bytes: u64,
    ) -> Result<Self, Box<dyn Error>> {
        Self::open_at_with_searchable_kinds(storage_dir, max_bytes, None)
    }

    pub(crate) fn open_at_with_searchable_kinds(
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

    pub(crate) fn append_packet(&self, data: &[u8]) -> Result<(), Box<dyn Error>> {
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

    pub(crate) fn compact(&self) -> Result<CompactStats, Box<dyn Error>> {
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

    pub(crate) fn packet_matches_filter(
        &self,
        data: &[u8],
        filter: &Filter,
    ) -> Result<bool, Box<dyn Error>> {
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

    pub(crate) fn hot_snapshot(&self) -> Result<(Vec<EventPacket>, SearchIndex), Box<dyn Error>> {
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

    pub(crate) fn negentropy_items_for_unix_day(
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
