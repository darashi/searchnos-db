use crate::cmd::{CliError, open_database};
use indicatif::HumanBytes;

/// Compact the current hot event file into per-day partitions.
pub fn run(db_path: &str) -> Result<(), CliError> {
    let db = open_database(db_path)?;
    let stats = db.compact()?;
    println!(
        "Compacted {} events from {} into {} partition files",
        stats.events,
        HumanBytes(stats.bytes),
        stats.files
    );
    Ok(())
}
