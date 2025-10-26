use std::time::Duration;

use crate::cmd::CliError;
use searchnos_db::SearchnosDB;
use searchnos_db::nostr::Filter;
use serde_json;

/// Execute a query against the database and print matching events as JSON.
pub fn run(db_path: &str, filters_json: Option<String>) -> Result<(), CliError> {
    let db = SearchnosDB::open(db_path)?;
    let filters = build_filters(filters_json.as_deref())?;
    let result = db.query_with_stats(filters.as_str())?;

    for event_json in &result.events {
        println!("{event_json}");
    }

    report_stats(&result);

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

fn report_stats(result: &searchnos_db::QueryResult) {
    let stats = &result.stats;
    let total = duration_ms(stats.total_elapsed);
    let index = duration_ms(stats.index_scan_duration);
    let post = duration_ms(stats.post_processing_duration);
    eprintln!(
        "fetched {} event(s) in {:.3} ms (index: {:.3} ms, post: {:.3} ms)",
        result.events.len(),
        total,
        index,
        post,
    );

    for (idx, filter_stats) in stats.filters.iter().enumerate() {
        eprintln!(
            "  filter #{idx}: {:?} (index: {:.3} ms, post: {:.3} ms, matches: {})",
            filter_stats.plan.source,
            duration_ms(filter_stats.index_scan_duration),
            duration_ms(filter_stats.post_processing_duration),
            filter_stats.matched_event_count,
        );
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
