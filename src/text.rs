#![allow(dead_code)]

use std::collections::HashMap;

use ndb::NdbNote;

use crate::ndb_ext::tag_text;
#[cfg(test)]
use crate::nostr::Event;
use crate::nostr::Kind;
use unicode_normalization::UnicodeNormalization;

/// Extract event text with kind-specific handling to support normalized content.
#[cfg(test)]
pub fn extract_text(event: &Event) -> String {
    match event.kind {
        Kind::Metadata => {
            let content: HashMap<String, String> =
                serde_json::from_str(&event.content).unwrap_or_default();
            content
                .values()
                .map(|value| value.to_owned())
                .collect::<Vec<String>>()
                .join(" ")
        }
        Kind::LongFormTextNote => {
            let mut items = Vec::with_capacity(1 + event.tags.len());
            items.push(event.content.clone());

            for tag in event.tags.iter() {
                let Some(name) = tag.first() else {
                    continue;
                };
                let Some(value) = tag.get(1) else {
                    continue;
                };
                match name.as_str() {
                    "title" | "Title" => items.push(value.to_owned()),
                    "summary" | "Summary" => items.push(value.to_owned()),
                    _ => {}
                }
            }

            items.join(" ")
        }
        _ => event.content.clone(),
    }
}

/// Extract event text from an encoded note with kind-specific handling.
pub(crate) fn extract_note_text(note: &NdbNote<'_>) -> Result<String, ndb::Error> {
    match Kind::from_u32(note.kind()) {
        Kind::Metadata => {
            let content: HashMap<String, String> =
                serde_json::from_str(note.content()?).unwrap_or_default();
            Ok(content
                .values()
                .map(|value| value.to_owned())
                .collect::<Vec<String>>()
                .join(" "))
        }
        Kind::LongFormTextNote => {
            let mut items = Vec::new();
            items.push(note.content()?.to_owned());

            for tag in note.tags() {
                let tag = tag?;
                let mut elements = tag.elements();
                let Some(name) = elements.next().transpose()? else {
                    continue;
                };
                let Some(value) = elements.next().transpose()? else {
                    continue;
                };
                let Some(value) = tag_text(value) else {
                    continue;
                };

                if let Some("title" | "Title" | "summary" | "Summary") = tag_text(name) {
                    items.push(value.to_owned());
                }
            }

            Ok(items.join(" "))
        }
        _ => Ok(note.content()?.to_owned()),
    }
}

/// Normalize text by applying NFKC, lowercasing, and collapsing whitespace.
pub fn normalize_text(input: &str) -> String {
    let nfkc: String = input.nfkc().collect();
    let lower = nfkc.to_lowercase();
    let mut collapsed = String::with_capacity(lower.len());
    let mut in_space = false;

    for ch in lower.chars() {
        if ch.is_whitespace() {
            if !in_space {
                collapsed.push(' ');
                in_space = true;
            }
        } else {
            collapsed.push(ch);
            in_space = false;
        }
    }

    collapsed.trim().to_string()
}

/// Split normalized text into AND terms separated by spaces.
pub fn normalize_query_terms(input: &str) -> Vec<String> {
    normalize_text(input)
        .split_whitespace()
        .map(|term| term.to_string())
        .collect()
}

pub(crate) fn normalize_note_content(note: &NdbNote<'_>) -> Result<String, ndb::Error> {
    Ok(normalize_text(&extract_note_text(note)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::Metadata;
    use crate::nostr::test_utils::{EventBuilder, Keys};

    fn title_tag(value: &str) -> Vec<String> {
        vec!["title".to_string(), value.to_string()]
    }

    fn summary_tag(value: &str) -> Vec<String> {
        vec!["summary".to_string(), value.to_string()]
    }

    #[test]
    fn extract_text_from_metadata_collects_field_values() {
        let keys = Keys::generate();
        let metadata = Metadata::new().display_name("Alice").about("Rustacean");
        let event = EventBuilder::metadata(&metadata)
            .sign_with_keys(&keys)
            .expect("failed to sign metadata event");

        let extracted = extract_text(&event);

        assert!(extracted.contains("Alice"));
        assert!(extracted.contains("Rustacean"));
    }

    #[test]
    fn extract_text_from_long_form_includes_title_and_summary() {
        let keys = Keys::generate();
        let event = EventBuilder::long_form_text_note("Body text")
            .tag(title_tag("Article Title"))
            .tag(summary_tag("Concise summary"))
            .sign_with_keys(&keys)
            .expect("failed to sign long-form event");

        let extracted = extract_text(&event);

        assert!(extracted.contains("Body text"));
        assert!(extracted.contains("Article Title"));
        assert!(extracted.contains("Concise summary"));
    }

    #[test]
    fn normalize_text_converts_fullwidth_to_halfwidth() {
        let fullwidth = "！";
        let halfwidth = "!";

        let normalized_fullwidth = normalize_text(fullwidth);
        let normalized_halfwidth = normalize_text(halfwidth);

        assert_eq!(normalized_fullwidth, normalized_halfwidth);
        assert_eq!(normalized_fullwidth, "!");
    }

    #[test]
    fn normalize_text_handles_fullwidth_exclamation_in_text() {
        let text = "wow！";
        let normalized = normalize_text(text);

        assert_eq!(normalized, "wow!");
        assert!(normalized.contains("!"));
    }
}
