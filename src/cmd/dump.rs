use crate::cmd::{CliError, event_stream_progress_style, open_database};
use indicatif::{HumanBytes, ProgressBar};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

/// Dump stored ndb notes to a length-prefixed binary stream.
pub fn run(db_path: &str, output_path: &Path) -> Result<(), CliError> {
    let db = open_database(db_path)?;
    let file = File::create(output_path)?;
    let writer = BufWriter::new(file);

    let pb = ProgressBar::new_spinner();
    pb.set_style(event_stream_progress_style());

    let mut bytes_written = 0;
    let events_written = db.dump_events_with_progress(writer, |progress| {
        bytes_written = progress.bytes_written;
        pb.inc(1);
    })?;

    pb.finish_with_message(format!(
        "Dumped {} events ({}) to {}",
        events_written,
        HumanBytes(bytes_written),
        output_path.display()
    ));

    Ok(())
}
