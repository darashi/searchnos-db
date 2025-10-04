use std::str;

use crate::types::{NdbNote, NdbValue};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    pub ids: Vec<[u8; 32]>,
    pub authors: Vec<[u8; 32]>,
    pub kinds: Vec<u32>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub generic_tags: Vec<(u8, Vec<String>)>,
    pub search: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchEventOptions {
    pub id: bool,
    pub author: bool,
    pub kind: bool,
    pub tags: bool,
    pub since: bool,
    pub until: bool,
    pub nip50: bool,
}

impl MatchEventOptions {
    /// Create a new options set with all checks enabled.
    pub fn new() -> Self {
        Self {
            id: true,
            author: true,
            kind: true,
            tags: true,
            since: true,
            until: true,
            nip50: true,
        }
    }

    /// Enable or disable matching against event identifiers.
    pub fn id(mut self, enabled: bool) -> Self {
        self.id = enabled;
        self
    }

    /// Enable or disable matching against authors.
    pub fn author(mut self, enabled: bool) -> Self {
        self.author = enabled;
        self
    }

    /// Enable or disable matching against event kinds.
    pub fn kind(mut self, enabled: bool) -> Self {
        self.kind = enabled;
        self
    }

    /// Enable or disable matching against generic tags.
    pub fn tags(mut self, enabled: bool) -> Self {
        self.tags = enabled;
        self
    }

    /// Enable or disable rejection of events created before the filter's `since` value.
    pub fn since(mut self, enabled: bool) -> Self {
        self.since = enabled;
        self
    }

    /// Enable or disable rejection of events created after the filter's `until` value.
    pub fn until(mut self, enabled: bool) -> Self {
        self.until = enabled;
        self
    }

    /// Enable or disable text matching for NIP-50 search queries.
    pub fn nip50(mut self, enabled: bool) -> Self {
        self.nip50 = enabled;
        self
    }
}

impl Default for MatchEventOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> NdbNote<'a> {
    /// Determine whether the note satisfies a filter using the provided options and normalized content.
    pub fn matches_filter(&self, filter: &Filter, opts: MatchEventOptions, content: &[u8]) -> bool {
        if opts.id && !filter.ids.is_empty() {
            let id_bytes = self.id();
            if !filter.ids.iter().any(|candidate| candidate == id_bytes) {
                return false;
            }
        }

        if opts.author && !filter.authors.is_empty() {
            let pubkey_bytes = self.pubkey();
            if !filter
                .authors
                .iter()
                .any(|candidate| candidate == pubkey_bytes)
            {
                return false;
            }
        }

        if opts.kind && !filter.kinds.is_empty() && !filter.kinds.contains(&self.kind()) {
            return false;
        }

        if opts.since
            && let Some(since) = filter.since
            && self.created_at() < since
        {
            return false;
        }

        if opts.until
            && let Some(until) = filter.until
            && self.created_at() > until
        {
            return false;
        }

        if opts.tags && !matches_generic_tags(self, filter) {
            return false;
        }

        if opts.nip50
            && let Some(query) = filter.search.as_ref()
            && !query.is_empty()
        {
            let needle = query.as_bytes();
            if !content
                .windows(needle.len())
                .any(|window| window.eq_ignore_ascii_case(needle))
            {
                return false;
            }
        }

        true
    }
}

fn matches_generic_tags(note: &NdbNote<'_>, filter: &Filter) -> bool {
    if filter.generic_tags.is_empty() {
        return true;
    }

    let tags = note.tags();
    if tags.is_empty() {
        return false;
    }

    for (tag, values) in filter.generic_tags.iter() {
        let prepared = prepare_filter_tag_values(values);
        let tag_char = *tag;
        let mut matched = false;

        for note_tag in tags {
            let mut iter = note_tag.iter();
            let Some(NdbValue::Text(identifier)) = iter.next() else {
                continue;
            };

            if identifier.len() != 1 || identifier[0] != tag_char {
                continue;
            }

            if iter.any(|value| matches_tag_value(value, &prepared)) {
                matched = true;
                break;
            }
        }

        if !matched {
            return false;
        }
    }

    true
}

#[derive(Clone)]
enum FilterTagValue {
    Text(String),
    Id([u8; 32]),
}

fn prepare_filter_tag_values(values: &[String]) -> Vec<FilterTagValue> {
    values
        .iter()
        .map(|value| {
            if value.len() == 64 {
                let mut decoded = [0u8; 32];
                if hex::decode_to_slice(value, &mut decoded).is_ok() {
                    return FilterTagValue::Id(decoded);
                }
            }
            FilterTagValue::Text(value.clone())
        })
        .collect()
}

fn matches_tag_value(value: NdbValue<'_>, expected: &[FilterTagValue]) -> bool {
    match value {
        NdbValue::Text(bytes) => match str::from_utf8(bytes) {
            Ok(text) => expected.iter().any(|candidate| match candidate {
                FilterTagValue::Text(expected_text) => expected_text == text,
                FilterTagValue::Id(_) => false,
            }),
            Err(_) => false,
        },
        NdbValue::Id(bytes) => expected.iter().any(|candidate| match candidate {
            FilterTagValue::Id(expected_id) => expected_id == bytes,
            FilterTagValue::Text(_) => false,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    const SAMPLE_ID: &str = "1726bf37195e345ddc4bba9560d9499c918a544afbf72057a595d68fbe908ee5";
    const SAMPLE_PUBKEY: &str = "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

    #[test]
    fn matches_filter_respects_fields() {
        let json = json!({
            "id": SAMPLE_ID,
            "pubkey": SAMPLE_PUBKEY,
            "sig": "62bc7e232a6ba074e6d360390566fda1baeccc438e3e65b75f6a7675ca250b1723e69a71ffd70185170331d5d95cb5333e8f7a51937ab2e188452c44fa9e91a5",
            "created_at": 1_758_755_707u64,
            "kind": 1u32,
            "content": "hello world",
            "tags": [
                ["t", "hello", "world"],
                ["client", "test"],
            ]
        })
        .to_string();

        let bytes = crate::codec::to_ndb_note(&json).expect("encode");
        let note = NdbNote::from_bytes(&bytes).expect("parse note");

        let filter = Filter {
            ids: vec![crate::codec::decode_hex_array::<32>(SAMPLE_ID, "id").unwrap()],
            authors: vec![crate::codec::decode_hex_array::<32>(SAMPLE_PUBKEY, "pubkey").unwrap()],
            kinds: vec![1],
            since: Some(1_758_755_700),
            until: Some(1_758_755_800),
            generic_tags: vec![(b't', vec!["hello".into()])],
            search: Some("HELLO".into()),
        };

        let opts = MatchEventOptions::new();
        assert!(note.matches_filter(&filter, opts, b"hello world"));

        let mut mismatched = filter.clone();
        mismatched.ids = vec![
            crate::codec::decode_hex_array::<32>(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "id",
            )
            .unwrap(),
        ];
        assert!(!note.matches_filter(&mismatched, opts, b"hello world"));
    }
}
