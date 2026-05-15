use std::error::Error;

use ndb::{NdbNote, TagElement};
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

use super::{FIELD_SEPARATOR, FNV_OFFSET_BASIS, FNV_PRIME, LONG_FORM_KIND};

pub(crate) fn normalize_searchable_kinds(searchable_kinds: Option<&[u32]>) -> Option<Vec<u32>> {
    let mut searchable_kinds = searchable_kinds?.to_vec();
    searchable_kinds.sort_unstable();
    searchable_kinds.dedup();
    Some(searchable_kinds)
}

pub(crate) fn searchable_kinds_hash(searchable_kinds: Option<&[u32]>) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    match normalize_searchable_kinds(searchable_kinds) {
        None => update_hash(&mut hash, b"all"),
        Some(kinds) => {
            update_hash(&mut hash, b"kinds");
            for kind in kinds {
                update_hash(&mut hash, &kind.to_le_bytes());
            }
        }
    }
    hash
}

pub(crate) fn normalized_search_text(
    data: &[u8],
    searchable_kinds: Option<&[u32]>,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let note = NdbNote::from_bytes(data)?;
    Ok(normalize_search_text(&searchable_content(&note, searchable_kinds)?).into_bytes())
}

pub(crate) fn search_terms(query: &str) -> Vec<String> {
    normalize_search_text(query)
        .split(' ')
        .filter(|term| !term.is_empty())
        .map(str::to_owned)
        .collect()
}

pub(crate) fn normalize_search_text(text: &str) -> String {
    text.nfkc()
        .flat_map(char::to_lowercase)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn tag_element_to_string(element: TagElement<'_>) -> Result<String, Box<dyn Error>> {
    Ok(match element {
        TagElement::Text(value) => value.to_owned(),
        TagElement::Id(_) => element.to_json_string()?,
    })
}

fn searchable_content(
    note: &NdbNote<'_>,
    searchable_kinds: Option<&[u32]>,
) -> Result<String, Box<dyn Error>> {
    if !is_search_indexed_kind(note.kind(), searchable_kinds) {
        return Ok(String::new());
    }

    let content = note.content()?;
    match note.kind() {
        0 => Ok(metadata_search_text(content)),
        LONG_FORM_KIND => long_form_search_text(note, content),
        _ => Ok(content.to_owned()),
    }
}

fn metadata_search_text(content: &str) -> String {
    let Ok(Value::Object(metadata)) = serde_json::from_str::<Value>(content) else {
        return String::new();
    };

    metadata
        .values()
        .filter_map(|value| match value {
            Value::String(value) => Some(value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(FIELD_SEPARATOR)
}

fn is_search_indexed_kind(kind: u32, searchable_kinds: Option<&[u32]>) -> bool {
    searchable_kinds.is_none_or(|kinds| kinds.contains(&kind))
}

fn long_form_search_text(note: &NdbNote<'_>, content: &str) -> Result<String, Box<dyn Error>> {
    let mut items = vec![content.to_owned()];

    for tag in note.tags() {
        let tag = tag?;
        let mut elements = tag.elements();
        let Some(name) = elements.next() else {
            continue;
        };
        let name = name?;
        let Ok(name) = name.as_str() else {
            continue;
        };
        if !matches!(name, "title" | "Title" | "summary" | "Summary") {
            continue;
        }

        let Some(value) = elements.next() else {
            continue;
        };
        items.push(tag_element_to_string(value?)?);
    }

    Ok(items.join(FIELD_SEPARATOR))
}

fn update_hash(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}
