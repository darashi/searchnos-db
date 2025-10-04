use std::time::Instant;

use crate::cmd::CliError;
use searchnos_db::SearchnosDB;
use searchnos_db::nostr::Filter;
use serde_json;

/// Execute a query against the database and print matching events as JSON.
pub fn run(db_path: &str, filters_json: Option<String>) -> Result<(), CliError> {
    let start = Instant::now();
    let db = SearchnosDB::open(db_path)?;
    let filters = build_filters(filters_json.as_deref())?;
    let events = db.query(filters.as_str())?;
    let elapsed = start.elapsed();

    for event_json in &events {
        println!("{event_json}");
    }

    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    eprintln!("fetched {} event(s) in {:.3} ms", events.len(), elapsed_ms);

    Ok(())
}

fn build_filters(filters_json: Option<&str>) -> Result<String, CliError> {
    match filters_json.map(str::trim) {
        None | Some("") => Ok("[]".to_owned()),
        Some(raw) if raw.starts_with('[') => Ok(raw.to_owned()),
        Some(raw) if raw.starts_with('{') => {
            let filter: Filter = serde_json::from_str(raw)?;
            Ok(serde_json::to_string(&vec![filter])?)
        }
        Some(_) => Err(CliError::InvalidFiltersFormat),
    }
}
