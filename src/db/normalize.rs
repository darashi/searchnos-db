use crate::nostr::{Event, extract_event_expiration};
use crate::text::{extract_text, normalize_text};

#[derive(Debug, Clone)]
pub(super) struct EventIndexData {
    pub event_index_key: Vec<u8>,
    pub created_at: u64,
    pub normalized_content: Vec<u8>,
    pub expiration: Option<u64>,
}

impl EventIndexData {
    pub(super) fn from_event(event: &Event) -> Self {
        let event_index_key = build_event_index_key(event);
        let created_at = event.created_at.as_u64();
        let normalized_content = normalize_event_content(event);
        let expiration = extract_event_expiration(event);

        Self {
            event_index_key,
            created_at,
            normalized_content: normalized_content.into_bytes(),
            expiration,
        }
    }
}

/// Build event index key from event ID and created_at
pub(super) fn build_event_index_key(event: &Event) -> Vec<u8> {
    let mut key = event.id.as_bytes().to_vec();
    key.extend_from_slice(&event.created_at.as_u64().to_be_bytes());
    key
}

/// Normalize event content for search filtering.
pub(super) fn normalize_event_content(event: &Event) -> String {
    normalize_text(&extract_text(event))
}
