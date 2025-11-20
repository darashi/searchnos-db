use crate::nostr::{Event, TagExt, TagKind, extract_event_expiration};

use crate::text::{MAX_NGRAM_SIZE, MIN_NGRAM_SIZE, char_ngrams, extract_text, normalize_text};

use super::index::TagIndex;

#[derive(Debug, Clone)]
pub(super) struct EventIndexData {
    pub event_index_key: Vec<u8>,
    pub created_at: u64,
    pub normalized_content: Vec<u8>,
    pub ngrams: Vec<Vec<u8>>,
    pub tag_keys: Vec<Vec<u8>>,
    pub expiration: Option<u64>,
}

impl EventIndexData {
    pub(super) fn from_event(event: &Event) -> Self {
        let event_index_key = build_event_index_key(event);
        let created_at = event.created_at.as_u64();
        let (normalized_content, ngrams) = normalize_and_extract_ngrams(event);
        let tag_keys = collect_tag_keys(event);
        let expiration = extract_event_expiration(event);

        Self {
            event_index_key,
            created_at,
            normalized_content: normalized_content.into_bytes(),
            ngrams,
            tag_keys,
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

/// Normalize event content and generate n-grams
pub(super) fn normalize_and_extract_ngrams(event: &Event) -> (String, Vec<Vec<u8>>) {
    let normalized_content = normalize_text(&extract_text(event));
    let ngrams = char_ngrams(&normalized_content, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE)
        .into_iter()
        .map(|gram| gram.into_bytes())
        .collect();
    (normalized_content, ngrams)
}

/// Collect tag keys for indexing
pub(super) fn collect_tag_keys(event: &Event) -> Vec<Vec<u8>> {
    let mut tag_keys = Vec::new();
    for tag in event.tags.iter() {
        let TagKind::SingleLetter(single) = tag.kind() else {
            continue;
        };
        let Some(content) = tag.content() else {
            continue;
        };
        if content.is_empty() {
            continue;
        }
        if let Some(key) = TagIndex::key(single, content) {
            tag_keys.push(key);
        }
    }
    tag_keys
}
