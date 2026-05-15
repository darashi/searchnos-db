use crate::cmd::{CliError, byte_progress_style, open_database};
use indicatif::{HumanBytes, ProgressBar};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Load stored ndb notes from a length-prefixed binary stream.
pub fn run(db_path: &str, input_path: &Path) -> Result<(), CliError> {
    let db = open_database(db_path)?;
    let file = File::open(input_path)?;
    let total_bytes = file.metadata()?.len();
    let reader = BufReader::new(file);

    let pb = ProgressBar::new(total_bytes);
    pb.set_style(byte_progress_style());

    let mut bytes_read = 0u64;
    let count = db.load_events_with_progress(reader, |progress| {
        bytes_read = progress.bytes_read;
        pb.set_position(bytes_read.min(total_bytes));
    })?;

    if count == 0 {
        pb.set_position(total_bytes);
    }
    pb.finish_with_message(format!(
        "Loaded {count} events ({}) from {}",
        HumanBytes(bytes_read),
        input_path.display()
    ));

    Ok(())
}
