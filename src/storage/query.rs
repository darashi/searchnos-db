use std::cmp::Ordering;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;

use super::SECONDS_PER_DAY;
use super::event::EventPacket;
use super::search::{SearchIndex, search_candidate_indexes};
use super::text;
use super::visibility::{VisibilityIndex, VisibilityStore};
use crate::nostr::Filter;

pub(crate) fn partition_days(
    partitions_dir: &Path,
    filter: &Filter,
) -> Result<Vec<u64>, Box<dyn Error>> {
    let mut days = Vec::new();
    match fs::read_dir(partitions_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) != Some("events") {
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                    continue;
                };
                let Ok(day) = stem.parse::<u64>() else {
                    continue;
                };
                if partition_day_overlaps_filter(day, filter) {
                    days.push(day);
                }
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }

    days.sort_unstable_by(|a, b| b.cmp(a));
    Ok(days)
}

pub(crate) fn partition_day_overlaps_filter(unix_day: u64, filter: &Filter) -> bool {
    let day_start = unix_day.saturating_mul(SECONDS_PER_DAY);
    let day_end = day_start.saturating_add(SECONDS_PER_DAY - 1);
    let since = filter.since.map_or(0, |timestamp| timestamp.as_u64());
    let until = filter
        .until
        .map_or(u64::MAX, |timestamp| timestamp.as_u64());

    since <= day_end && until >= day_start
}

pub(crate) fn sort_packets(packets: &mut [EventPacket]) {
    packets.sort_by(compare_packets);
}

pub(crate) fn compare_packets(a: &EventPacket, b: &EventPacket) -> Ordering {
    b.created_at
        .cmp(&a.created_at)
        .then_with(|| a.id.cmp(&b.id))
}

fn retain_visible_packets(
    packets: &mut Vec<EventPacket>,
    visibility: &VisibilityIndex,
    visibility_store: Option<&VisibilityStore>,
) -> Result<(), Box<dyn Error>> {
    let mut visible = Vec::with_capacity(packets.len());
    for packet in packets.drain(..) {
        if visibility.is_visible(&packet)? {
            visible.push(packet);
        }
    }
    if let Some(store) = visibility_store {
        store.retain_visible(&mut visible)?;
    }
    *packets = visible;
    Ok(())
}

pub(crate) fn retain_visible_packets_if_needed(
    packets: &mut Vec<EventPacket>,
    visibility: Option<&VisibilityIndex>,
    visibility_store: Option<&VisibilityStore>,
) -> Result<(), Box<dyn Error>> {
    if let Some(visibility) = visibility {
        retain_visible_packets(packets, visibility, visibility_store)?;
    }
    Ok(())
}

pub(crate) fn filter_has_search_terms(filter: &Filter) -> bool {
    filter
        .search
        .as_ref()
        .is_some_and(|query| !text::search_terms(query).is_empty())
}

pub(crate) fn query_packets_with_index(
    packets: Vec<EventPacket>,
    search_index: Option<&SearchIndex>,
    filter: &Filter,
) -> Result<Vec<EventPacket>, Box<dyn Error>> {
    let candidate_indexes = search_candidate_indexes(search_index, filter)?;
    match candidate_indexes {
        Some(indexes) => indexes
            .into_iter()
            .filter_map(|index| packets.get(index).cloned())
            .filter_map(
                |packet| match packet.matches_filter_without_search(filter) {
                    Ok(true) => Some(Ok(packet)),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect(),
        None => packets
            .into_iter()
            .filter_map(
                |packet| match packet.matches_filter_without_search(filter) {
                    Ok(true) => Some(Ok(packet)),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect(),
    }
}
