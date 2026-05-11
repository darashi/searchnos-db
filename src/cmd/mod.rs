use indicatif::{ProgressState, ProgressStyle};
use std::fmt::Write;

pub mod dump;
pub mod import;
pub mod query;
pub mod stat;

mod error {
    use searchnos_db::SearchnosDBError;
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum CliError {
        #[error(transparent)]
        Db(#[from] SearchnosDBError),
        #[error(transparent)]
        Io(#[from] std::io::Error),
        #[error("import failed in {path} at line {line}: {source}")]
        Import {
            path: String,
            line: usize,
            #[source]
            source: SearchnosDBError,
        },
        #[error("failed to parse filter JSON: {0}")]
        FilterJson(#[from] serde_json::Error),
        #[error("filters must be a JSON object or array")]
        InvalidFiltersFormat,
    }

    pub use CliError as Exported;
}

pub use error::Exported as CliError;

pub(crate) fn default_progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{percent:>3}%|{bar:40}| {pos}/{len} [{elapsed_precise}<{eta_precise}, {per_sec_ev}]",
    )
    .expect("progress style")
    .with_key("per_sec_ev", |state: &ProgressState, w: &mut dyn Write| {
        let _ = write!(w, "{:.2} ev/s", state.per_sec());
    })
}
