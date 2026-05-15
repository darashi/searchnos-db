use std::error::Error;
use std::path::Path;
use std::time::{Duration, Instant};

use tracing::info;

use super::event::error_with_path;
use super::hot::HotEvents;
use super::partition::partition_event_paths;
use super::reindex::{ReindexJob, reindex_partition};
use super::search::search_sidecar_is_current;
use super::{ReindexProgress, ReindexProgressPhase, ReindexStats};

impl HotEvents {
    pub(crate) fn rebuild_partition_sidecars(&self, path: &Path) -> Result<u64, Box<dyn Error>> {
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

    pub(crate) fn reindex(
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
