use crate::cmd::{CliError, default_progress_style};
use indicatif::ProgressBar;
use searchnos_db::{SearchnosDB, SearchnosDBOptions};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Import newline-delimited event JSON into a database with progress reporting.
pub fn run(
    db_path: &str,
    import_paths: &[PathBuf],
    batch_size: usize,
    flush_interval: Duration,
) -> Result<(), CliError> {
    let mut total = 0usize;
    for path in import_paths {
        total += count_non_empty_lines(path)?;
    }
    if total == 0 {
        return Ok(());
    }

    let options = SearchnosDBOptions {
        batch_size,
        flush_interval,
        purge_policy: None,
        ..SearchnosDBOptions::default()
    };
    let db = SearchnosDB::open_with_options(db_path, options)?;

    let pb = ProgressBar::new(total as u64);
    pb.set_style(default_progress_style());

    for path in import_paths {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        for (idx, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            db.insert_event_json_owned(line)
                .map_err(|source| CliError::Import {
                    path: path.to_string_lossy().into_owned(),
                    line: idx + 1,
                    source,
                })?;

            pb.inc(1);
        }
    }

    db.flush()?;

    pb.finish_with_message(format!("Imported {total} events into {db_path}"));

    Ok(())
}

fn count_non_empty_lines(path: &Path) -> Result<usize, CliError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0usize;
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}
