use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use tracing::{info, warn};

use super::event::{EventPacket, error_with_path, read_event_packet, read_event_packets_from_path};
use super::partition::{compacting_hot_path, partition_path, tmp_partition_path};
use super::query::compare_packets;
use super::search::{
    SearchIndex, append_to_search_index, empty_search_index, remove_file_if_exists,
    search_index_path, tmp_search_index_path, write_built_search_index,
};
use super::sidecar_queue::SidecarUpdateQueue;
use super::visibility::{VisibilityStore, VisibilitySummary};
use super::{CompactStats, HotState};

pub(crate) struct CompactionContext {
    pub(crate) path: PathBuf,
    pub(crate) partitions_dir: PathBuf,
    pub(crate) max_bytes: u64,
    pub(crate) searchable_kinds: Option<Vec<u32>>,
    pub(crate) visibility_store: Arc<VisibilityStore>,
    pub(crate) state: Arc<Mutex<HotState>>,
    pub(crate) sidecar_updates: Arc<SidecarUpdateQueue>,
    pub(crate) compact_running: Arc<AtomicBool>,
    pub(crate) compact_pending: Arc<AtomicBool>,
    pub(crate) compacting_paths: Arc<Mutex<Vec<PathBuf>>>,
    pub(crate) compact_path: PathBuf,
    pub(crate) hot_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CompactOutput {
    pub(crate) output_partitions: usize,
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

pub(crate) fn run_pending_compaction_queue(mut context: CompactionContext) {
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

pub(crate) fn run_compaction_queue(mut context: CompactionContext) {
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

pub(crate) fn run_one_compaction(
    context: &CompactionContext,
) -> Result<CompactStats, Box<dyn Error>> {
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

pub(crate) fn rotate_hot_for_compaction(
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

pub(crate) fn compact_hot_file(
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
    super::event::write_packet(partition, &packet.data)?;
    append_to_search_index(search_index, &packet.data, searchable_kinds)?;
    visibility_summary.add_packet(packet)?;
    Ok(())
}
