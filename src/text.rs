use std::collections::{BTreeSet, HashMap};

use crate::nostr::{Event, Kind};
use unicode_normalization::UnicodeNormalization;

/// Extract event text with kind-specific handling to support normalized content.
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

/// Choose a minimum n-gram size for search queries based on the term length.
pub fn preferred_min_query_ngram_size(term: &str) -> usize {
    let len = term.chars().count();
    len.clamp(MIN_NGRAM_SIZE, MAX_NGRAM_SIZE)
}

/// Default minimum and maximum character counts for generated n-grams.
pub const MIN_NGRAM_SIZE: usize = 1;
pub const MAX_NGRAM_SIZE: usize = 3;

/// Build a deduplicated list of character n-grams within the provided bounds.
pub fn char_ngrams(text: &str, min: usize, max: usize) -> Vec<String> {
    if text.is_empty() || min == 0 || min > max {
        return Vec::new();
    }

    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return Vec::new();
    }

    let mut uniques = BTreeSet::new();
    let len = chars.len();

    for n in min..=max {
        if n == 0 {
            continue;
        }

        if len < n {
            continue;
        }

        for window in chars.windows(n) {
            let gram: String = window.iter().collect();
            uniques.insert(gram);
        }
    }

    if uniques.is_empty() {
        uniques.insert(chars.into_iter().collect());
    }

    uniques.into_iter().collect()
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
    fn char_ngrams_generates_range() {
        let grams = char_ngrams("abc", MIN_NGRAM_SIZE, MAX_NGRAM_SIZE);

        assert!(grams.contains(&"a".to_string()));
        assert!(grams.contains(&"ab".to_string()));
        assert!(grams.contains(&"abc".to_string()));
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

    #[test]
    fn preferred_min_query_ngram_size_scales_with_length() {
        assert_eq!(preferred_min_query_ngram_size("h"), MIN_NGRAM_SIZE);
        assert_eq!(preferred_min_query_ngram_size("hi"), 2);
        assert_eq!(preferred_min_query_ngram_size("abc"), MAX_NGRAM_SIZE);
        assert_eq!(preferred_min_query_ngram_size("rust"), MAX_NGRAM_SIZE);
        assert_eq!(preferred_min_query_ngram_size("nostr"), MAX_NGRAM_SIZE);
    }
}
