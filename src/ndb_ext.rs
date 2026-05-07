use ndb::{NdbNote, NdbNoteBuf, TagElement};

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

pub(crate) fn to_ndb_note(json: &str) -> Result<Vec<u8>, ndb::Error> {
    Ok(NdbNoteBuf::from_json(json)?.into_bytes())
}

pub(crate) fn from_ndb_note(bytes: &[u8]) -> Result<String, ndb::Error> {
    NdbNote::from_bytes(bytes)?.to_json_string()
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
        if !terms.is_empty()
            && !terms.iter().all(|term| {
                let needle = term.as_bytes();
                content
                    .windows(needle.len())
                    .any(|window| window.eq_ignore_ascii_case(needle))
            })
        {
            return false;
        }
    }

    true
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
