//! Helpers for [NIP-40](https://github.com/nostr-protocol/nips/blob/master/40.md) event
//! expiration handling.

use ndb::{NdbNote, TagElement};

use super::Event;
use crate::ndb_ext::tag_text;

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
        let Ok(tag) = tag else {
            continue;
        };
        let mut values = tag.elements();
        let Some(Ok(kind)) = values.next() else {
            continue;
        };

        if tag_text(kind) != Some("expiration") {
            continue;
        }

        let Some(Ok(TagElement::Text(timestamp_str))) = values.next() else {
            continue;
        };

        if let Ok(value) = timestamp_str.parse::<u64>() {
            return Some(value);
        }
    }

    None
}
