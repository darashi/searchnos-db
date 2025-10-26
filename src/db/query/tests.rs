use crate::db::{SearchnosDBOptions, test_support::TestDatabase};
use crate::nostr::{
    EventDeletionRequest, EventId, Filter, JsonUtil, Kind, PublicKey, Timestamp, event_tag,
    test_utils::{EventBuilder, Keys},
};

fn hashtag_tag(value: impl Into<String>) -> Vec<String> {
    vec!["t".to_string(), value.into()]
}

fn public_key_tag(pk: PublicKey) -> Vec<String> {
    vec!["p".to_string(), pk.to_hex()]
}

fn filter_with_hashtag(value: &str) -> Filter {
    Filter::new().tag('t', value.to_string())
}

fn add_hashtag(filter: Filter, value: &str) -> Filter {
    filter.tag('t', value.to_string())
}

fn filter_with_event(event_id: EventId) -> Filter {
    Filter::new().tag('e', event_id.to_hex())
}

fn add_pubkey(filter: Filter, pubkey: PublicKey) -> Filter {
    filter.tag('p', pubkey.to_hex())
}

#[test]
fn query_plan_uses_event_id_index_when_ids_specified() {
    let filter = Filter::new().id(EventId::from([0u8; 32]));
    let plan = super::QueryPlan::for_filter(&filter);

    assert!(matches!(plan.source, super::PlanSource::EventIds { .. }));
    assert!(!plan.match_opts.id);
}

#[test]
fn query_plan_defaults_to_created_at_scan_without_ids() {
    let filter = Filter::new();
    let plan = super::QueryPlan::for_filter(&filter);

    assert!(matches!(plan.source, super::PlanSource::CreatedAt));
    assert!(plan.match_opts.id);
}

#[test]
fn query_plan_prefers_ngram_search_for_search_terms() {
    let filter = Filter::new().search("rustacean");
    let plan = super::QueryPlan::for_filter(&filter);

    assert!(matches!(plan.source, super::PlanSource::NgramSearch { .. }));
    assert!(plan.match_opts.id);
}

#[test]
fn query_plan_uses_author_index_when_authors_specified() {
    let keys = Keys::generate();
    let filter = Filter::new().author(keys.public_key());
    let plan = super::QueryPlan::for_filter(&filter);

    assert!(matches!(plan.source, super::PlanSource::Authors { .. }));
}

#[test]
fn query_plan_uses_kind_index_when_kinds_specified() {
    let filter = Filter::new().kind(Kind::LongFormTextNote);
    let plan = super::QueryPlan::for_filter(&filter);

    assert!(matches!(plan.source, super::PlanSource::Kinds { .. }));
}

#[test]
fn query_plan_uses_pubkey_kind_index_when_author_and_kind_filters_small() {
    let keys = Keys::generate();
    let filter = Filter::new().author(keys.public_key()).kind(Kind::TextNote);
    let plan = super::QueryPlan::for_filter(&filter);

    assert!(matches!(plan.source, super::PlanSource::PubkeyKinds { .. }));
}

#[test]
fn query_plan_uses_tag_index_when_tags_specified() {
    let filter = filter_with_hashtag("nostr");
    let plan = super::QueryPlan::for_filter(&filter);

    assert!(matches!(plan.source, super::PlanSource::Tags { .. }));
    assert!(!plan.match_opts.nip50);
}

#[test]
fn query_without_filters_orders_by_created_at_desc() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let newest = EventBuilder::text_note("newest")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");
    let middle = EventBuilder::text_note("middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");

    db.insert(&oldest);
    db.insert(&newest);
    db.insert(&middle);

    let results = db.query(&[]);

    assert_eq!(
        results,
        vec![newest.as_json(), middle.as_json(), oldest.as_json()]
    );
}

#[test]
fn query_deduplicates_results_from_multiple_filters() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let earlier = EventBuilder::text_note("first")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build earlier event");
    let later = EventBuilder::text_note("second")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build later event");

    db.insert(&earlier);
    db.insert(&later);

    let filters = vec![
        Filter::new().id(later.id),
        Filter::new().author(keys.public_key()),
    ];

    let results = db.query(&filters);

    assert_eq!(results, vec![later.as_json(), earlier.as_json()]);
}

#[test]
fn query_applies_search_terms_to_normalized_content() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let matching = EventBuilder::text_note("Rust coding tips")
        .custom_created_at(Timestamp::from_secs(150))
        .sign_with_keys(&keys)
        .expect("failed to build matching event");
    let only_rust = EventBuilder::text_note("RUSTACEAN life")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build rust-only event");
    let only_coding = EventBuilder::text_note("coding in go")
        .custom_created_at(Timestamp::from_secs(250))
        .sign_with_keys(&keys)
        .expect("failed to build coding-only event");

    db.insert(&matching);
    db.insert(&only_rust);
    db.insert(&only_coding);

    let filters = vec![Filter::new().search("Rust   Coding")];
    let results = db.query(&filters);

    assert_eq!(results, vec![matching.as_json()]);
}

#[test]
fn query_handles_short_search_terms_via_ngram_index() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let go_note = EventBuilder::text_note("Go is concise")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build go note");
    let rust_note = EventBuilder::text_note("Rust systems programming")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build rust note");

    db.insert(&go_note);
    db.insert(&rust_note);

    let filters = vec![Filter::new().search("go")];
    let results = db.query(&filters);

    assert_eq!(results, vec![go_note.as_json()]);
}

#[test]
fn query_handles_single_character_search_via_ngram_index() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let a_note = EventBuilder::text_note("a quick test")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build a note");
    let z_note = EventBuilder::text_note("zzz")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build z note");

    db.insert(&a_note);
    db.insert(&z_note);

    let filters = vec![Filter::new().search("a")];
    let results = db.query(&filters);

    assert_eq!(results, vec![a_note.as_json()]);
}

#[test]
fn query_search_rejects_false_positive_candidates() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let matching = EventBuilder::text_note("lole")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build matching event");
    let false_positive = EventBuilder::text_note("console lol")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build false-positive event");

    db.insert(&matching);
    db.insert(&false_positive);

    let filters = vec![Filter::new().search("lole")];
    let results = db.query(&filters);

    assert_eq!(
        results,
        vec![matching.as_json()],
        "only the event containing the full search term should be returned"
    );
}

#[test]
fn query_search_respects_since_boundary() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let older = EventBuilder::text_note("rust match older")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build older event");
    let newer = EventBuilder::text_note("rust match newer")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build newer event");

    db.insert(&older);
    db.insert(&newer);

    let filters = vec![
        Filter::new()
            .search("rust match")
            .since(Timestamp::from_secs(150)),
    ];
    let results = db.query(&filters);

    assert_eq!(results, vec![newer.as_json()]);
}

#[test]
fn query_search_respects_since_and_until_bounds() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let before = EventBuilder::text_note("rust match before")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build before event");
    let lower = EventBuilder::text_note("rust match lower")
        .custom_created_at(Timestamp::from_secs(150))
        .sign_with_keys(&keys)
        .expect("failed to build lower event");
    let upper = EventBuilder::text_note("rust match upper")
        .custom_created_at(Timestamp::from_secs(180))
        .sign_with_keys(&keys)
        .expect("failed to build upper event");
    let after = EventBuilder::text_note("rust match after")
        .custom_created_at(Timestamp::from_secs(220))
        .sign_with_keys(&keys)
        .expect("failed to build after event");

    db.insert(&before);
    db.insert(&lower);
    db.insert(&upper);
    db.insert(&after);

    let filters = vec![
        Filter::new()
            .search("rust match")
            .since(Timestamp::from_secs(140))
            .until(Timestamp::from_secs(200)),
    ];
    let results = db.query(&filters);

    assert_eq!(results, vec![upper.as_json(), lower.as_json()]);
}

#[test]
fn query_search_respects_since_until_and_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let before = EventBuilder::text_note("rust limit before")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build before event");
    let lower = EventBuilder::text_note("rust limit lower")
        .custom_created_at(Timestamp::from_secs(150))
        .sign_with_keys(&keys)
        .expect("failed to build lower event");
    let middle = EventBuilder::text_note("rust limit middle")
        .custom_created_at(Timestamp::from_secs(170))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let upper = EventBuilder::text_note("rust limit upper")
        .custom_created_at(Timestamp::from_secs(190))
        .sign_with_keys(&keys)
        .expect("failed to build upper event");
    let after = EventBuilder::text_note("rust limit after")
        .custom_created_at(Timestamp::from_secs(230))
        .sign_with_keys(&keys)
        .expect("failed to build after event");

    db.insert(&before);
    db.insert(&lower);
    db.insert(&middle);
    db.insert(&upper);
    db.insert(&after);

    let filters = vec![
        Filter::new()
            .search("rust limit")
            .since(Timestamp::from_secs(140))
            .until(Timestamp::from_secs(220))
            .limit(2),
    ];
    let results = db.query(&filters);

    assert_eq!(results, vec![upper.as_json(), middle.as_json()]);
}

#[test]
fn query_returns_empty_results_for_blank_search() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let note = EventBuilder::text_note("some searchable content")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build note");

    db.insert(&note);

    let whitespace_filters = vec![Filter::new().search("   ")];
    assert!(db.query(&whitespace_filters).is_empty());

    let empty_filters = vec![Filter::new().search("")];
    assert!(db.query(&empty_filters).is_empty());
}

#[test]
fn query_respects_filter_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let first = EventBuilder::text_note("first")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build first event");
    let second = EventBuilder::text_note("second")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build second event");
    let third = EventBuilder::text_note("third")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build third event");

    db.insert(&first);
    db.insert(&second);
    db.insert(&third);

    let filters = vec![Filter::new().limit(2)];
    let results = db.query(&filters);

    assert_eq!(results, vec![third.as_json(), second.as_json()]);
}

#[test]
fn query_applies_default_limit_when_unspecified() {
    let options = SearchnosDBOptions {
        default_limit: Some(2),
        ..Default::default()
    };
    let db = TestDatabase::with_options(options);
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![Filter::new()];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_clamps_limit_to_max_limit() {
    let options = SearchnosDBOptions {
        max_limit: Some(2),
        ..Default::default()
    };
    let db = TestDatabase::with_options(options);
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![Filter::new().limit(5)];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_created_at_index_prefers_newest_with_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![Filter::new().limit(2)];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_author_index_prefers_newest_with_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![Filter::new().author(keys.public_key()).limit(2)];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_kind_index_prefers_newest_with_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![Filter::new().kind(Kind::TextNote).limit(2)];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_tag_index_prefers_newest_with_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .tag(hashtag_tag("nostrdev"))
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("middle")
        .tag(hashtag_tag("nostrdev"))
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest")
        .tag(hashtag_tag("nostrdev"))
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![filter_with_hashtag("nostrdev").limit(2)];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_pubkey_kind_index_prefers_newest_with_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("oldest")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![
        Filter::new()
            .author(keys.public_key())
            .kind(Kind::TextNote)
            .limit(2),
    ];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_ngram_index_prefers_newest_with_limit() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let oldest = EventBuilder::text_note("go oldest note")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build oldest event");
    let middle = EventBuilder::text_note("note for go middle")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build middle event");
    let newest = EventBuilder::text_note("newest go entry")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build newest event");

    db.insert(&oldest);
    db.insert(&middle);
    db.insert(&newest);

    let filters = vec![Filter::new().search("go").limit(2)];
    let results = db.query(&filters);

    assert_eq!(results, vec![newest.as_json(), middle.as_json()]);
}

#[test]
fn query_filters_by_author_using_author_index() {
    let db = TestDatabase::new();
    let primary_keys = Keys::generate();
    let other_keys = Keys::generate();

    let older_primary = EventBuilder::text_note("primary older")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&primary_keys)
        .expect("failed to build older primary event");
    let newer_primary = EventBuilder::text_note("primary newer")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&primary_keys)
        .expect("failed to build newer primary event");
    let foreign = EventBuilder::text_note("foreign")
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&other_keys)
        .expect("failed to build foreign event");

    db.insert(&older_primary);
    db.insert(&newer_primary);
    db.insert(&foreign);

    let filters = vec![Filter::new().author(primary_keys.public_key())];
    let results = db.query(&filters);

    assert_eq!(
        results,
        vec![newer_primary.as_json(), older_primary.as_json()]
    );
}

#[test]
fn query_filters_by_kind_using_kind_index() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let text_note = EventBuilder::text_note("short form")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build text note");
    let long_form = EventBuilder::long_form_text_note("extended body")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build long form note");

    db.insert(&text_note);
    db.insert(&long_form);

    let filters = vec![Filter::new().kind(Kind::LongFormTextNote)];
    let results = db.query(&filters);

    assert_eq!(results, vec![long_form.as_json()]);
}

#[test]
fn query_filters_by_author_and_kind_using_pubkey_kind_index() {
    let db = TestDatabase::new();
    let primary_keys = Keys::generate();
    let other_keys = Keys::generate();

    let matching = EventBuilder::text_note("matching")
        .custom_created_at(Timestamp::from_secs(150))
        .sign_with_keys(&primary_keys)
        .expect("failed to build matching event");
    let different_kind = EventBuilder::long_form_text_note("different kind")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&primary_keys)
        .expect("failed to build different kind event");
    let different_author = EventBuilder::text_note("different author")
        .custom_created_at(Timestamp::from_secs(250))
        .sign_with_keys(&other_keys)
        .expect("failed to build different author event");

    db.insert(&matching);
    db.insert(&different_kind);
    db.insert(&different_author);

    let filters = vec![
        Filter::new()
            .author(primary_keys.public_key())
            .kind(Kind::TextNote),
    ];
    let results = db.query(&filters);

    assert_eq!(results, vec![matching.as_json()]);
}

#[test]
fn query_with_zero_limit_returns_empty() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let event = EventBuilder::text_note("should be skipped")
        .custom_created_at(Timestamp::from_secs(123))
        .sign_with_keys(&keys)
        .expect("failed to build event");
    db.insert(&event);

    let filters = vec![Filter::new().limit(0)];
    let results = db.query(&filters);

    assert!(results.is_empty());
}

#[test]
fn query_respects_since_and_until_bounds() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let before = EventBuilder::text_note("before")
        .custom_created_at(Timestamp::from_secs(90))
        .sign_with_keys(&keys)
        .expect("failed to build before event");
    let lower = EventBuilder::text_note("lower-bound")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build lower event");
    let upper = EventBuilder::text_note("upper-bound")
        .custom_created_at(Timestamp::from_secs(110))
        .sign_with_keys(&keys)
        .expect("failed to build upper event");
    let after = EventBuilder::text_note("after")
        .custom_created_at(Timestamp::from_secs(120))
        .sign_with_keys(&keys)
        .expect("failed to build after event");

    db.insert(&before);
    db.insert(&lower);
    db.insert(&upper);
    db.insert(&after);

    let filters = vec![
        Filter::new()
            .since(Timestamp::from_secs(100))
            .until(Timestamp::from_secs(110)),
    ];
    let results = db.query(&filters);

    assert_eq!(results, vec![upper.as_json(), lower.as_json()]);
}

#[test]
fn query_matches_events_by_event_tag() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let referenced = EventBuilder::text_note("target")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build referenced event");
    let unrelated = EventBuilder::text_note("unrelated")
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build unrelated event");
    let reply = EventBuilder::text_note("reply")
        .tag(event_tag(referenced.id))
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build reply event");

    db.insert(&referenced);
    db.insert(&unrelated);
    db.insert(&reply);

    let filters = vec![filter_with_event(referenced.id)];
    let results = db.query(&filters);

    assert_eq!(results, vec![reply.as_json()]);
}

#[test]
fn query_filters_by_hashtag_using_tag_index() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let matching = EventBuilder::text_note("tagged")
        .tag(hashtag_tag("nostrdev"))
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build matching event");
    let other = EventBuilder::text_note("other")
        .tag(hashtag_tag("other"))
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build other event");

    db.insert(&matching);
    db.insert(&other);

    let filters = vec![filter_with_hashtag("nostrdev")];
    let results = db.query(&filters);

    assert_eq!(results, vec![matching.as_json()]);
}

#[test]
fn query_tag_filters_require_exact_match() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let exact = EventBuilder::text_note("exact")
        .tag(hashtag_tag("nostr"))
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build exact event");
    let prefixed = EventBuilder::text_note("prefixed")
        .tag(hashtag_tag("nostrdev"))
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build prefixed event");

    db.insert(&exact);
    db.insert(&prefixed);

    let filters = vec![filter_with_hashtag("nostr")];
    let results = db.query(&filters);

    assert_eq!(results, vec![exact.as_json()]);
}

#[test]
fn query_uppercase_tag_filter_does_not_match_lowercase_event_tag() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let tagged = EventBuilder::text_note("tagged")
        .tag(hashtag_tag("foo"))
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build tagged event");

    db.insert(&tagged);

    let uppercase_filter = Filter::new().tag('T', "foo");
    assert!(
        db.query(&[uppercase_filter]).is_empty(),
        "uppercase tag filter unexpectedly matched lowercase tag"
    );

    let lowercase_filter = Filter::new().tag('t', "foo");
    assert_eq!(db.query(&[lowercase_filter]), vec![tagged.as_json()]);
}

#[test]
fn query_filters_by_multiple_tag_values() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let alpha = EventBuilder::text_note("alpha")
        .tag(hashtag_tag("alpha"))
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build alpha event");
    let beta = EventBuilder::text_note("beta")
        .tag(hashtag_tag("beta"))
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build beta event");
    let gamma = EventBuilder::text_note("gamma")
        .tag(hashtag_tag("gamma"))
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&keys)
        .expect("failed to build gamma event");

    db.insert(&alpha);
    db.insert(&beta);
    db.insert(&gamma);

    let filter = ["alpha", "beta"]
        .into_iter()
        .fold(Filter::new(), add_hashtag);
    let results = db.query(&[filter]);

    assert_eq!(results, vec![beta.as_json(), alpha.as_json()]);
}

#[test]
fn query_requires_all_tag_categories() {
    let db = TestDatabase::new();
    let author_keys = Keys::generate();
    let tagged_pubkey = Keys::generate();

    let matching = EventBuilder::text_note("matching")
        .tag(hashtag_tag("nostrdev"))
        .tag(public_key_tag(tagged_pubkey.public_key()))
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&author_keys)
        .expect("failed to build matching event");
    let missing_pubkey = EventBuilder::text_note("missing")
        .tag(hashtag_tag("nostrdev"))
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&author_keys)
        .expect("failed to build missing event");
    let different_hashtag = EventBuilder::text_note("different")
        .tag(hashtag_tag("other"))
        .tag(public_key_tag(tagged_pubkey.public_key()))
        .custom_created_at(Timestamp::from_secs(300))
        .sign_with_keys(&author_keys)
        .expect("failed to build different event");

    db.insert(&matching);
    db.insert(&missing_pubkey);
    db.insert(&different_hashtag);

    let filters = vec![add_pubkey(
        add_hashtag(Filter::new(), "nostrdev"),
        tagged_pubkey.public_key(),
    )];
    let results = db.query(&filters);

    assert_eq!(results, vec![matching.as_json()]);
}

#[test]
fn query_omits_events_removed_by_deletion_request() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let original = EventBuilder::text_note("to be deleted")
        .custom_created_at(Timestamp::from_secs(100))
        .sign_with_keys(&keys)
        .expect("failed to build original event");
    db.insert(&original);

    let deletion = EventBuilder::delete(EventDeletionRequest::new().id(original.id))
        .custom_created_at(Timestamp::from_secs(200))
        .sign_with_keys(&keys)
        .expect("failed to build deletion event");
    db.insert(&deletion);

    let filters = vec![Filter::new().author(keys.public_key())];
    let results = db.query(&filters);

    assert_eq!(results, vec![deletion.as_json()]);

    let id_filter = vec![Filter::new().id(original.id)];
    let id_results = db.query(&id_filter);
    assert!(id_results.is_empty());
}

#[test]
fn query_with_stats_reports_plan_details() {
    let db = TestDatabase::new();
    let keys = Keys::generate();

    let event = EventBuilder::text_note("plan timing")
        .sign_with_keys(&keys)
        .expect("failed to build event");
    db.insert(&event);

    let filters = vec![Filter::new().author(keys.public_key())];
    let result = db.query_with_stats(&filters);

    assert_eq!(result.events, vec![event.as_json()]);
    assert_eq!(result.stats.filters.len(), 1);
    assert!(result.stats.total_elapsed >= result.stats.index_scan_duration);
    assert!(result.stats.total_elapsed >= result.stats.post_processing_duration);
    assert!(result.stats.filters[0].candidate_count >= result.stats.filters[0].matched_event_count);
    match &result.stats.filters[0].plan.source {
        super::PlanSource::Authors { pubkeys } => {
            assert_eq!(pubkeys, &vec![keys.public_key()]);
        }
        other => panic!("expected Authors plan, got {other:?}"),
    }
}
