//! Helpers for [NIP-40](https://github.com/nostr-protocol/nips/blob/master/40.md) event
//! expiration handling.

use std::str;

use ndb::{NdbNote, NdbValue};

use super::Event;

/// Read the expiration timestamp from a raw `nostr::Event` if present.
pub fn extract_event_expiration(event: &Event) -> Option<u64> {
    for tag in event.tags.iter() {
        let Some(name) = tag.first() else {
            continue;
        };
        if name != "expiration" {
            continue;
        }
        let Some(timestamp) = tag.get(1) else {
            continue;
        };
        if let Ok(value) = timestamp.parse::<u64>() {
            return Some(value);
        }
    }

    None
}

/// Read the expiration timestamp from an encoded `ndb` note if present.
pub fn extract_note_expiration(note: &NdbNote<'_>) -> Option<u64> {
    for tag in note.tags() {
        let mut values = tag.iter();
        let Some(NdbValue::Text(kind)) = values.next() else {
            continue;
        };

        if kind != b"expiration" {
            continue;
        }

        let Some(NdbValue::Text(timestamp_bytes)) = values.next() else {
            continue;
        };

        let Ok(timestamp_str) = str::from_utf8(timestamp_bytes) else {
            continue;
        };

        if let Ok(value) = timestamp_str.parse::<u64>() {
            return Some(value);
        }
    }

    None
}
