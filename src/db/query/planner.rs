use crate::ndb_ext::MatchEventOptions;
use crate::nostr::Filter;

use crate::text::normalize_query_terms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanSource {
    ContentsScan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub source: PlanSource,
    pub match_opts: MatchEventOptions,
}

impl PlanSource {
    /// Whether candidate iteration yields events in descending created_at order.
    pub fn produces_descending_created_at(&self) -> bool {
        true
    }
}

impl QueryPlan {
    /// Choose the contents-scan plan for a single Nostr filter.
    pub fn for_filter(filter: &Filter) -> Self {
        let has_search_terms = filter
            .search
            .as_ref()
            .is_some_and(|search| !normalize_query_terms(search).is_empty());

        Self {
            source: PlanSource::ContentsScan,
            match_opts: MatchEventOptions::new().nip50(has_search_terms),
        }
    }
}
