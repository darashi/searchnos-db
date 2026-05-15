use std::error::Error;
use std::path::PathBuf;

use super::event::{error_with_path, read_event_packets_from_path};
use super::search::{search_index_path, write_search_index_atomic};
use super::visibility::VisibilitySummary;

pub(crate) struct ReindexJob {
    pub(crate) path: PathBuf,
    pub(crate) file_index: u64,
}

pub(crate) struct ReindexResult {
    pub(crate) path: PathBuf,
    pub(crate) file_index: u64,
    pub(crate) events: u64,
    pub(crate) visibility_summary: VisibilitySummary,
}

pub(crate) struct ReindexError {
    file_index: u64,
    err: Box<dyn Error>,
}

impl std::fmt::Debug for ReindexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReindexError")
            .field("file_index", &self.file_index)
            .field("err", &self.err.to_string())
            .finish()
    }
}

impl std::fmt::Display for ReindexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl Error for ReindexError {}

pub(crate) fn reindex_partition(
    job: ReindexJob,
    searchable_kinds: Option<&[u32]>,
) -> Result<ReindexResult, ReindexError> {
    reindex_partition_inner(job.path.clone(), job.file_index, searchable_kinds).map_err(|err| {
        ReindexError {
            file_index: job.file_index,
            err,
        }
    })
}

fn reindex_partition_inner(
    path: PathBuf,
    file_index: u64,
    searchable_kinds: Option<&[u32]>,
) -> Result<ReindexResult, Box<dyn Error>> {
    let packets = read_event_packets_from_path(&path)
        .map_err(|err| error_with_path("read events", &path, err))?;
    write_search_index_atomic(&search_index_path(&path), &packets, searchable_kinds)
        .map_err(|err| error_with_path("write search index for events", &path, err))?;
    let visibility_summary = VisibilitySummary::from_packets(&packets)
        .map_err(|err| error_with_path("build visibility index for events", &path, err))?;
    Ok(ReindexResult {
        path,
        file_index,
        events: packets.len() as u64,
        visibility_summary,
    })
}
