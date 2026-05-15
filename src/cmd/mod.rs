use indicatif::{ProgressState, ProgressStyle};
use searchnos_db::SearchnosDB;
use std::fmt::Write;

pub mod compact;
pub mod dump;
pub mod import;
pub mod load;
pub mod query;
pub mod reindex;
pub mod stat;

mod error {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum CliError {
        #[error(transparent)]
        Io(#[from] std::io::Error),
        #[error(transparent)]
        Database(#[from] searchnos_db::SearchnosDBError),
        #[error("failed to encode or decode ndb note: {0}")]
        Ndb(#[from] ndb::Error),
        #[error("import failed in {path} at line {line}: {source}")]
        Import {
            path: String,
            line: usize,
            #[source]
            source: Box<CliError>,
        },
        #[error("failed to parse filter JSON: {0}")]
        FilterJson(#[from] serde_json::Error),
    }

    pub use CliError as Exported;
}

pub use error::Exported as CliError;

pub(crate) fn open_database(db_path: &str) -> Result<SearchnosDB, CliError> {
    Ok(SearchnosDB::open(db_path)?)
}

pub(crate) fn default_progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{percent:>3}%|{bar:40}| {pos}/{len} [{elapsed_precise}<{eta_precise}, {per_sec_ev}]",
    )
    .expect("progress style")
    .with_key("per_sec_ev", |state: &ProgressState, w: &mut dyn Write| {
        let _ = write!(w, "{:.2} ev/s", state.per_sec());
    })
}

pub(crate) fn event_stream_progress_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner} {pos} events [{elapsed_precise}, {per_sec_ev}]")
        .expect("progress style")
        .with_key("per_sec_ev", |state: &ProgressState, w: &mut dyn Write| {
            let _ = write!(w, "{:.2} ev/s", state.per_sec());
        })
}

pub(crate) fn byte_progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{percent:>3}%|{bar:40}| {bytes}/{total_bytes} [{elapsed_precise}<{eta_precise}, {bytes_per_sec}]",
    )
    .expect("progress style")
}
