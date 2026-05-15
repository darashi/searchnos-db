use crate::cmd::{CliError, default_progress_style, open_database};
use indicatif::ProgressBar;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// Import newline-delimited event JSON into a database with progress reporting.
pub fn run(db_path: &str, import_paths: &[PathBuf]) -> Result<(), CliError> {
    let mut total = 0usize;
    for path in import_paths {
        total += count_non_empty_lines(path)?;
    }
    if total == 0 {
        return Ok(());
    }

    let db = open_database(db_path)?;

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

            db.insert_event_json(&line)
                .map_err(|source| CliError::Import {
                    path: path.to_string_lossy().into_owned(),
                    line: idx + 1,
                    source: Box::new(source.into()),
                })?;

            pb.inc(1);
        }
    }

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
