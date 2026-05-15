use std::time::Instant;

use crate::cmd::{CliError, open_database};

/// Execute a query against the database and print matching events as JSON.
pub fn run(db_path: &str, filters_json: Option<String>) -> Result<(), CliError> {
    let db = open_database(db_path)?;
    let filters = filters_json.unwrap_or_else(|| "{}".to_string());
    let started_at = Instant::now();
    let events = db.query(&filters)?;

    for event in &events {
        println!("{event}");
    }

    eprintln!(
        "fetched {} event(s) in {:.3} ms",
        events.len(),
        started_at.elapsed().as_secs_f64() * 1000.0,
    );

    Ok(())
}
