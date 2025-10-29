use ndb::MatchEventOptions;

use crate::nostr::{EventId, Filter, Kind, PublicKey};

use crate::text::normalize_query_terms;

const PUBKEY_KIND_COMBINATION_LIMIT: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanSource {
    EventIds {
        ids: Vec<EventId>,
    },
    NgramSearch {
        terms: Vec<String>,
    },
    PubkeyKinds {
        pubkeys: Vec<PublicKey>,
        kinds: Vec<Kind>,
    },
    Tags {
        entries: Vec<(char, Vec<String>)>,
    },
    Authors {
        pubkeys: Vec<PublicKey>,
    },
    Kinds {
        kinds: Vec<Kind>,
    },
    CreatedAt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub source: PlanSource,
    pub match_opts: MatchEventOptions,
}

impl PlanSource {
    /// Whether candidate iteration yields events in descending created_at order.
    pub fn produces_descending_created_at(&self) -> bool {
        match self {
            PlanSource::CreatedAt
            | PlanSource::Kinds { .. }
            | PlanSource::Authors { .. }
            | PlanSource::PubkeyKinds { .. }
            | PlanSource::Tags { .. }
            | PlanSource::NgramSearch { .. } => true,
            PlanSource::EventIds { .. } => false,
        }
    }
}

impl QueryPlan {
    /// Choose an index-backed plan for a single Nostr filter and adjust match options accordingly.
    pub fn for_filter(filter: &Filter) -> Self {
        let mut normalized_terms: Option<Vec<String>> = None;
        let has_search_terms = if let Some(search) = filter.search.as_ref() {
            let terms = normalize_query_terms(search);
            if terms.is_empty() {
                false
            } else {
                normalized_terms = Some(terms);
                true
            }
        } else {
            false
        };

        if let Some(ids) = filter.ids.as_ref()
            && !ids.is_empty()
        {
            // EventIds index resolves: id
            let match_opts = MatchEventOptions::new().id(false).nip50(has_search_terms);
            return Self {
                source: PlanSource::EventIds { ids: ids.to_vec() },
                match_opts,
            };
        }

        if let Some(terms) = normalized_terms.clone() {
            // NgramSearch index resolves: search content
            let match_opts = MatchEventOptions::new().nip50(true);
            return Self {
                source: PlanSource::NgramSearch { terms },
                match_opts,
            };
        }

        if let (Some(authors), Some(kinds)) = (filter.authors.as_ref(), filter.kinds.as_ref())
            && !authors.is_empty()
            && !kinds.is_empty()
        {
            let combinations = authors.len().saturating_mul(kinds.len());
            if combinations <= PUBKEY_KIND_COMBINATION_LIMIT {
                // PubkeyKinds index resolves: author, kind, since, until
                let match_opts = MatchEventOptions::new()
                    .author(false)
                    .kind(false)
                    .since(false)
                    .until(false)
                    .nip50(has_search_terms);
                return Self {
                    source: PlanSource::PubkeyKinds {
                        pubkeys: authors.to_vec(),
                        kinds: kinds.to_vec(),
                    },
                    match_opts,
                };
            }
        }

        if !filter.generic_tags.is_empty() {
            let entries: Vec<(char, Vec<String>)> = filter
                .generic_tags
                .iter()
                .map(|(tag, values)| (*tag, values.to_vec()))
                .filter(|(_, values)| !values.is_empty())
                .collect();
            if !entries.is_empty() {
                // Tags index resolves: generic_tags
                let match_opts = MatchEventOptions::new().tags(false).nip50(has_search_terms);
                return Self {
                    source: PlanSource::Tags { entries },
                    match_opts,
                };
            }
        }

        if let Some(authors) = filter.authors.as_ref()
            && !authors.is_empty()
        {
            // Authors index resolves: author, since, until
            let match_opts = MatchEventOptions::new()
                .author(false)
                .since(false)
                .until(false)
                .nip50(has_search_terms);
            return Self {
                source: PlanSource::Authors {
                    pubkeys: authors.to_vec(),
                },
                match_opts,
            };
        }

        if let Some(kinds) = filter.kinds.as_ref()
            && !kinds.is_empty()
        {
            // Kinds index resolves: kind
            let match_opts = MatchEventOptions::new().kind(false).nip50(has_search_terms);
            return Self {
                source: PlanSource::Kinds {
                    kinds: kinds.to_vec(),
                },
                match_opts,
            };
        }

        // CreatedAt index resolves: since, until
        Self {
            source: PlanSource::CreatedAt,
            match_opts: MatchEventOptions::new()
                .since(false)
                .until(false)
                .nip50(has_search_terms),
        }
    }
}
