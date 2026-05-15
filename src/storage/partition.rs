use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::event::error_with_path;

static COMPACTION_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn partition_event_paths(partitions_dir: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let entries = match fs::read_dir(partitions_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };

    let mut paths = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) == Some("events") {
            let unix_day = partition_day_from_event_path(&path)
                .map_err(|err| error_with_path("parse partition day", &path, err))?;
            paths.push((unix_day, path));
        }
    }
    paths.sort_by_key(|(unix_day, _)| std::cmp::Reverse(*unix_day));
    Ok(paths.into_iter().map(|(_, path)| path).collect())
}

fn partition_day_from_event_path(path: &Path) -> Result<u64, Box<dyn Error>> {
    let file_stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or("missing partition file stem")?;
    Ok(file_stem.parse()?)
}

pub(crate) fn compacting_hot_path(hot_path: &Path) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let counter = COMPACTION_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let file_name = hot_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("hot.events");
    hot_path.with_file_name(format!("{file_name}.compacting-{millis}-{counter}"))
}

pub(crate) fn orphaned_compacting_hot_paths(
    hot_path: &Path,
) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let Some(parent) = hot_path.parent() else {
        return Ok(Vec::new());
    };
    let file_name = hot_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("hot.events");
    let compacting_prefix = format!("{file_name}.compacting-");

    let mut paths = Vec::new();
    for entry in fs::read_dir(parent)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(&compacting_prefix) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub(crate) fn partition_path(partitions_dir: &Path, unix_day: u64) -> PathBuf {
    partitions_dir.join(format!("{unix_day}.events"))
}

pub(crate) fn tmp_partition_path(partition_path: &Path) -> PathBuf {
    let mut file_name = partition_path
        .file_name()
        .expect("partition path has a file name")
        .to_os_string();
    file_name.push(".tmp");
    partition_path.with_file_name(file_name)
}
