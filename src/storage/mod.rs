#![allow(dead_code)]

use std::error::Error;
use std::path::PathBuf;

use crate::nostr::Filter;

mod compaction;
mod cursor;
mod event;
mod hot;
mod partition;
mod query;
mod reindex;
mod search;
mod sidecar_queue;
mod streaming_query;
mod text;
mod visibility;
use event::EventPacket;
use hot::{HotEvents, HotState};

const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

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

#[cfg(test)]
mod tests;
