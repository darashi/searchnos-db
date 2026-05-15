#![allow(dead_code)]

use ndb::{NdbNote, NdbNoteBuf, TagElement};
use secp256k1::schnorr::Signature as SchnorrSignature;
use secp256k1::{Secp256k1, XOnlyPublicKey};
use sha2::{Digest, Sha256};

use crate::nostr::{EventError, Kind};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct NdbFilter {
    pub(crate) ids: Vec<[u8; 32]>,
    pub(crate) authors: Vec<[u8; 32]>,
    pub(crate) kinds: Vec<u32>,
    pub(crate) since: Option<u64>,
    pub(crate) until: Option<u64>,
    pub(crate) generic_tags: Vec<(u8, Vec<String>)>,
    pub(crate) search: Option<String>,
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

    pub fn id(mut self, enabled: bool) -> Self {
        self.id = enabled;
        self
    }

    pub fn author(mut self, enabled: bool) -> Self {
        self.author = enabled;
        self
    }

    pub fn kind(mut self, enabled: bool) -> Self {
        self.kind = enabled;
        self
    }

    pub fn tags(mut self, enabled: bool) -> Self {
        self.tags = enabled;
        self
    }

    pub fn since(mut self, enabled: bool) -> Self {
        self.since = enabled;
        self
    }

    pub fn until(mut self, enabled: bool) -> Self {
        self.until = enabled;
        self
    }

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

#[cfg(test)]
pub(crate) fn to_ndb_note(json: &str) -> Result<Vec<u8>, ndb::Error> {
    Ok(NdbNoteBuf::from_json(json)?.into_bytes())
}

pub(crate) fn to_ndb_note_buf(json: &str) -> Result<NdbNoteBuf, ndb::Error> {
    NdbNoteBuf::from_json(json)
}

pub(crate) fn from_ndb_note(bytes: &[u8]) -> Result<String, ndb::Error> {
    NdbNote::from_bytes(bytes)?.to_json_string()
}

pub(crate) fn verify_note(note: &NdbNote<'_>) -> Result<(), EventError> {
    let expected_id = compute_note_id(note)?;
    if note.id() != &expected_id {
        return Err(EventError::InvalidId);
    }

    let secp = Secp256k1::verification_only();
    let pubkey = XOnlyPublicKey::from_byte_array(*note.pubkey())
        .map_err(|_| EventError::InvalidPublicKey)?;
    let signature = SchnorrSignature::from_byte_array(*note.sig());

    secp.verify_schnorr(&signature, &expected_id, &pubkey)
        .map_err(|_| EventError::InvalidSignature)
}

fn compute_note_id(note: &NdbNote<'_>) -> Result<[u8; 32], EventError> {
    let mut serialized = Vec::new();
    serialized.push(b'[');
    serialized.push(b'0');
    serialized.push(b',');
    write_json_string(&mut serialized, &hex::encode(note.pubkey()))?;
    serialized.push(b',');
    serialized.extend_from_slice(note.created_at().to_string().as_bytes());
    serialized.push(b',');
    serialized.extend_from_slice(note.kind().to_string().as_bytes());
    serialized.push(b',');
    serialized.push(b'[');

    for (tag_index, tag) in note.tags().enumerate() {
        let tag = tag.map_err(note_error_to_event_error)?;
        if tag_index != 0 {
            serialized.push(b',');
        }
        serialized.push(b'[');
        for (elem_index, elem) in tag.elements().enumerate() {
            let elem = elem.map_err(note_error_to_event_error)?;
            if elem_index != 0 {
                serialized.push(b',');
            }
            write_json_string(
                &mut serialized,
                &elem.to_json_string().map_err(note_error_to_event_error)?,
            )?;
        }
        serialized.push(b']');
    }

    serialized.push(b']');
    serialized.push(b',');
    write_json_string(
        &mut serialized,
        note.content().map_err(note_error_to_event_error)?,
    )?;
    serialized.push(b']');

    let hash = Sha256::digest(&serialized);
    let mut id = [0u8; 32];
    id.copy_from_slice(&hash);
    Ok(id)
}

fn write_json_string(out: &mut Vec<u8>, value: &str) -> Result<(), EventError> {
    serde_json::to_writer(out, value).map_err(|err| EventError::InvalidJson(err.to_string()))
}

fn note_error_to_event_error(err: ndb::Error) -> EventError {
    EventError::InvalidJson(err.to_string())
}

pub(crate) fn note_event_index_key(note: &NdbNote<'_>) -> Vec<u8> {
    let mut key = note.id().to_vec();
    key.extend_from_slice(&note.created_at().to_be_bytes());
    key
}

fn first_note_d_tag_bytes(note: &NdbNote<'_>) -> Option<Vec<u8>> {
    for tag in note.tags() {
        let Ok(tag) = tag else {
            continue;
        };
        let mut elements = tag.elements();
        let Some(Ok(identifier)) = elements.next() else {
            continue;
        };
        if tag_text(identifier) != Some("d") {
            continue;
        }

        return match elements.next() {
            Some(Ok(TagElement::Text(value))) => Some(value.as_bytes().to_vec()),
            Some(Ok(TagElement::Id(value))) => Some(value.to_vec()),
            Some(Err(_)) | None => Some(Vec::new()),
        };
    }

    None
}

pub(crate) fn note_replacable_key(note: &NdbNote<'_>) -> Option<Vec<u8>> {
    let kind_value = note.kind();
    if kind_value > u16::MAX as u32 {
        return None;
    }

    let kind = Kind::from_u16(kind_value as u16);
    let is_replaceable = kind.is_replaceable();
    let is_addressable = kind.is_addressable();

    if !is_replaceable && !is_addressable {
        return None;
    }

    let slot = if is_addressable {
        first_note_d_tag_bytes(note).unwrap_or_default()
    } else {
        Vec::new()
    };

    let pubkey = note.pubkey();
    let mut key = Vec::with_capacity(pubkey.len() + 2 + 4 + slot.len());
    key.extend_from_slice(pubkey);
    key.extend_from_slice(&(kind_value as u16).to_be_bytes());
    key.extend_from_slice(&(slot.len() as u32).to_be_bytes());
    key.extend_from_slice(&slot);
    Some(key)
}

pub(crate) fn note_deletion_ids(note: &NdbNote<'_>) -> Vec<[u8; 32]> {
    let mut ids = Vec::new();

    for tag in note.tags() {
        let Ok(tag) = tag else {
            continue;
        };
        let mut elements = tag.elements();
        let Some(Ok(identifier)) = elements.next() else {
            continue;
        };
        if tag_text(identifier) != Some("e") {
            continue;
        }

        let Some(Ok(value)) = elements.next() else {
            continue;
        };
        match value {
            TagElement::Id(id) => ids.push(*id),
            TagElement::Text(text) => {
                let mut id = [0u8; 32];
                if hex::decode_to_slice(text, &mut id).is_ok() {
                    ids.push(id);
                }
            }
        }
    }

    ids
}

pub(crate) fn parse_a_tag(value: &str) -> Option<(u32, [u8; 32], &str)> {
    let mut parts = value.splitn(3, ':');
    let kind = parts.next()?.parse::<u32>().ok()?;
    let pubkey_hex = parts.next()?;
    let d_tag = parts.next()?;
    if pubkey_hex.len() != 64 {
        return None;
    }

    let mut pubkey = [0u8; 32];
    hex::decode_to_slice(pubkey_hex, &mut pubkey).ok()?;
    Some((kind, pubkey, d_tag))
}

pub(crate) fn replace_deletion_hash(a_tag: &str) -> [u8; 32] {
    Sha256::digest(a_tag.as_bytes()).into()
}

pub(crate) fn replace_deletion_hash_from_parts(
    kind: u32,
    pubkey: &[u8; 32],
    d_tag: &str,
) -> [u8; 32] {
    replace_deletion_hash(&format!("{}:{}:{}", kind, hex::encode(pubkey), d_tag))
}

pub(crate) fn note_replace_deletion_hashes(note: &NdbNote<'_>) -> Vec<[u8; 32]> {
    if note.kind() != Kind::EventDeletion.as_u32() {
        return Vec::new();
    }

    let mut hashes = Vec::new();
    for tag in note.tags() {
        let Ok(tag) = tag else {
            continue;
        };
        let mut elements = tag.elements();
        let Some(Ok(identifier)) = elements.next() else {
            continue;
        };
        if tag_text(identifier) != Some("a") {
            continue;
        }

        let Some(Ok(TagElement::Text(a_tag))) = elements.next() else {
            continue;
        };
        let Some((kind, pubkey, d_tag)) = parse_a_tag(a_tag) else {
            continue;
        };
        if (30_000..40_000).contains(&kind) && pubkey == *note.pubkey() {
            hashes.push(replace_deletion_hash_from_parts(kind, &pubkey, d_tag));
        }
    }

    hashes
}

pub(crate) fn note_matches_filter(
    note: &NdbNote<'_>,
    filter: &NdbFilter,
    opts: MatchEventOptions,
    content: &[u8],
) -> bool {
    if opts.id
        && !filter.ids.is_empty()
        && !filter.ids.iter().any(|candidate| candidate == note.id())
    {
        return false;
    }

    if opts.author
        && !filter.authors.is_empty()
        && !filter
            .authors
            .iter()
            .any(|candidate| candidate == note.pubkey())
    {
        return false;
    }

    if opts.kind && !filter.kinds.is_empty() && !filter.kinds.contains(&note.kind()) {
        return false;
    }

    if opts.since
        && let Some(since) = filter.since
        && note.created_at() < since
    {
        return false;
    }

    if opts.until
        && let Some(until) = filter.until
        && note.created_at() > until
    {
        return false;
    }

    if opts.tags && !matches_generic_tags(note, filter) {
        return false;
    }

    if opts.nip50
        && let Some(query) = filter.search.as_ref()
    {
        let terms: Vec<&str> = query.split_whitespace().collect();
        if !terms.is_empty() && !terms.iter().all(|term| contains_search_term(content, term)) {
            return false;
        }
    }

    true
}

fn contains_search_term(content: &[u8], term: &str) -> bool {
    memchr::memmem::find(content, term.as_bytes()).is_some()
}

fn matches_generic_tags(note: &NdbNote<'_>, filter: &NdbFilter) -> bool {
    if filter.generic_tags.is_empty() {
        return true;
    }

    let prepared_tags = filter
        .generic_tags
        .iter()
        .map(|(tag, values)| (*tag, prepare_filter_tag_values(values)))
        .collect::<Vec<_>>();
    let mut matched = vec![false; prepared_tags.len()];

    for tag in note.tags() {
        let Ok(tag) = tag else {
            return false;
        };
        let mut elements = tag.elements();
        let Some(Ok(TagElement::Text(identifier))) = elements.next() else {
            continue;
        };
        let identifier = identifier.as_bytes();
        if identifier.len() != 1 {
            continue;
        }

        for (index, (expected_tag, expected_values)) in prepared_tags.iter().enumerate() {
            if matched[index] || identifier[0] != *expected_tag {
                continue;
            }

            let tag_matched = tag.elements().skip(1).any(|value| {
                value
                    .map(|value| matches_tag_value(value, expected_values))
                    .unwrap_or(false)
            });
            if tag_matched {
                matched[index] = true;
            }
        }
    }

    matched.into_iter().all(|value| value)
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

fn matches_tag_value(value: TagElement<'_>, expected: &[FilterTagValue]) -> bool {
    match value {
        TagElement::Text(text) => expected.iter().any(|candidate| match candidate {
            FilterTagValue::Text(expected_text) => expected_text == text,
            FilterTagValue::Id(_) => false,
        }),
        TagElement::Id(bytes) => expected.iter().any(|candidate| match candidate {
            FilterTagValue::Id(expected_id) => expected_id == bytes,
            FilterTagValue::Text(_) => false,
        }),
    }
}

pub(crate) fn tag_text<'a>(value: TagElement<'a>) -> Option<&'a str> {
    match value {
        TagElement::Text(text) => Some(text),
        TagElement::Id(_) => None,
    }
}
