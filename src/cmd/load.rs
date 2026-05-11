use crate::cmd::{CliError, byte_progress_style};
use indicatif::{HumanBytes, ProgressBar};
use searchnos_db::{LoadProgress, SearchnosDB};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Load stored ndb notes from a length-prefixed binary stream.
pub fn run(db_path: &str, input_path: &Path) -> Result<(), CliError> {
    let db = SearchnosDB::open(db_path)?;
    let file = File::open(input_path)?;
    let total_bytes = file.metadata()?.len();
    let reader = BufReader::new(file);

    let pb = ProgressBar::new(total_bytes);
    pb.set_style(byte_progress_style());

    let mut last_progress = LoadProgress {
        events_loaded: 0,
        bytes_read: 0,
    };

    let count = db.load_events_with_progress(reader, |progress| {
        pb.set_position(progress.bytes_read.min(total_bytes));
        last_progress = progress;
    })?;

    if count == 0 {
        pb.set_position(total_bytes);
    }
    pb.finish_with_message(format!(
        "Loaded {count} events ({}) from {}",
        HumanBytes(last_progress.bytes_read),
        input_path.display()
    ));

    Ok(())
}
