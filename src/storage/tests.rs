use super::search::{events_fingerprint, read_search_index};
use super::*;
use crate::nostr::{EventId, Filter, Kind, PublicKey, Timestamp};
use ndb::NdbNoteBuf;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn append_packet_keeps_hot_when_under_size_limit() {
    let dir = test_dir("under-limit");
    let hot_path = dir.join("hot.events");
    let partitions_dir = dir.join("partitions");
    let hot = HotEvents::open_at(&dir, 1000).unwrap();
    let note = note("00", 10);

    hot.append_packet(&note).unwrap();

    assert_eq!(fs::read(&hot_path).unwrap(), packet(&note));
    assert!(!search_index_path(&hot_path).exists());
    assert_eq!(partition_event_paths(&partitions_dir).unwrap().len(), 0);
}

#[test]
fn append_packet_compacts_hot_to_created_at_day_partition_when_over_size_limit() {
    let dir = test_dir("over-limit");
    let hot_path = dir.join("hot.events");
    let partitions_dir = dir.join("partitions");
    let hot = HotEvents::open_at(&dir, 1).unwrap();
    let note = note("00", 10);

    hot.append_packet(&note).unwrap();

    assert_eq!(fs::read(&hot_path).unwrap(), Vec::<u8>::new());
    assert_eq!(
        fs::read(partition_path(&partitions_dir, 0)).unwrap(),
        packet(&note)
    );
    assert!(visibility_store_path(&partitions_dir).exists());
}

#[test]
fn open_recovers_orphaned_compacting_hot_file() {
    let dir = test_dir("recover-compacting");
    let partitions_dir = dir.join("partitions");
    fs::create_dir_all(&dir).unwrap();
    fs::create_dir_all(&partitions_dir).unwrap();
    let compacting_path = dir.join("hot.events.compacting-1-0");
    let event = note("00", SECONDS_PER_DAY + 10);
    fs::write(&compacting_path, packet(&event)).unwrap();

    let storage = Storage::open_at(&dir, 1000).unwrap();

    assert!(!compacting_path.exists());
    assert_eq!(
        fs::read(partition_path(&partitions_dir, 1)).unwrap(),
        packet(&event)
    );
    assert_eq!(storage.query(&[Filter::new()]).unwrap(), vec![event]);
}

#[test]
fn open_exclusively_locks_storage_directory() {
    let dir = test_dir("exclusive-storage-lock");
    let first = Storage::open_at(&dir, 1000).unwrap();

    let err = match Storage::open_at(&dir, 1000) {
        Ok(_) => panic!("opening locked storage unexpectedly succeeded"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("storage.lock"));
    drop(first);
    Storage::open_at(&dir, 1000).unwrap();
}

#[test]
fn sidecar_update_queue_prioritizes_waiting_compaction_over_reindex() {
    let queue = Arc::new(SidecarUpdateQueue::new());
    let active = queue.acquire_reindex().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    {
        let queue = queue.clone();
        let ready_tx = ready_tx.clone();
        let done_tx = done_tx.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let _guard = queue.acquire_compaction().unwrap();
            done_tx.send("compaction").unwrap();
        });
    }
    ready_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(10));

    {
        let queue = queue.clone();
        let ready_tx = ready_tx.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let _guard = queue.acquire_reindex().unwrap();
            done_tx.send("reindex").unwrap();
        });
    }
    ready_rx.recv().unwrap();

    drop(active);

    assert_eq!(done_rx.recv().unwrap(), "compaction");
    assert_eq!(done_rx.recv().unwrap(), "reindex");
}

#[test]
fn compact_sorts_partition_by_nip01_order() {
    let dir = test_dir("sort");
    let hot_path = dir.join("hot.events");
    let partitions_dir = dir.join("partitions");
    let hot = HotEvents::open_at(&dir, 1).unwrap();

    let old = note("11", 10);
    let newer_high_id = note("ff", 20);
    let newer_low_id = note("00", 20);

    hot.append_packet(&old).unwrap();
    hot.append_packet(&newer_high_id).unwrap();
    hot.append_packet(&newer_low_id).unwrap();

    assert_eq!(fs::read(&hot_path).unwrap(), Vec::<u8>::new());
    assert_eq!(
        fs::read(partition_path(&partitions_dir, 0)).unwrap(),
        packets([&newer_low_id, &newer_high_id, &old])
    );
}

#[test]
fn compact_writes_events_to_created_at_day_partitions() {
    let dir = test_dir("multi-day");
    let hot_path = dir.join("hot.events");
    let partitions_dir = dir.join("partitions");
    let hot = HotEvents::open_at(&dir, 1).unwrap();

    let day0 = note("00", 10);
    let day1 = note("11", SECONDS_PER_DAY + 10);

    hot.append_packet(&day0).unwrap();
    hot.append_packet(&day1).unwrap();

    assert_eq!(fs::read(&hot_path).unwrap(), Vec::<u8>::new());
    assert_eq!(
        fs::read(partition_path(&partitions_dir, 0)).unwrap(),
        packet(&day0)
    );
    assert_eq!(
        fs::read(partition_path(&partitions_dir, 1)).unwrap(),
        packet(&day1)
    );
}

#[test]
fn partition_event_paths_orders_newest_first() {
    let dir = test_dir("partition-order");
    let partitions_dir = dir.join("partitions");
    fs::create_dir_all(&partitions_dir).unwrap();
    fs::write(partition_path(&partitions_dir, 10), []).unwrap();
    fs::write(partition_path(&partitions_dir, 1), []).unwrap();
    fs::write(partition_path(&partitions_dir, 2), []).unwrap();

    assert_eq!(
        partition_event_paths(&partitions_dir).unwrap(),
        vec![
            partition_path(&partitions_dir, 10),
            partition_path(&partitions_dir, 2),
            partition_path(&partitions_dir, 1),
        ]
    );
}

#[test]
fn query_matches_nip01_filter_fields() {
    let dir = test_dir("query-fields");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let matching = note_with(
        "aa",
        "bb",
        20,
        1,
        r#"[["e","cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]]"#,
        "hello nostr",
    );
    let other = note_with("dd", "bb", 30, 1, "[]", "hello nostr");
    storage.append_packet(&matching).unwrap();
    storage.append_packet(&other).unwrap();

    let filter = Filter::new()
        .author(PublicKey::from_byte_array([0xbb; 32]))
        .kind(Kind::from(1u16))
        .since(Timestamp::from_secs(20))
        .until(Timestamp::from_secs(20))
        .event(EventId::from_byte_array([0xcc; 32]))
        .search("NOSTR");

    assert_eq!(storage.query(&[filter]).unwrap(), vec![matching]);
}

#[test]
fn query_uses_since_and_until_to_select_partitions() {
    let dir = test_dir("query-time");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let day0 = note("00", 10);
    let day1 = note("11", SECONDS_PER_DAY + 10);
    storage.append_packet(&day0).unwrap();
    storage.append_packet(&day1).unwrap();

    let filter = Filter::new()
        .since(Timestamp::from_secs(SECONDS_PER_DAY))
        .until(Timestamp::from_secs(SECONDS_PER_DAY * 2 - 1));

    assert_eq!(storage.query(&[filter]).unwrap(), vec![day1]);
}

#[test]
fn query_limit_returns_newest_events_in_nip01_order() {
    let dir = test_dir("query-limit");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let old = note("11", 10);
    let newer_high_id = note("ff", SECONDS_PER_DAY + 10);
    let newer_low_id = note("00", SECONDS_PER_DAY + 10);
    storage.append_packet(&old).unwrap();
    storage.append_packet(&newer_high_id).unwrap();
    storage.append_packet(&newer_low_id).unwrap();

    assert_eq!(
        storage.query(&[Filter::new().limit(2)]).unwrap(),
        vec![newer_low_id, newer_high_id]
    );
}

#[test]
fn query_limit_zero_returns_no_events() {
    let dir = test_dir("query-limit-zero");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    storage.append_packet(&note("00", 10)).unwrap();

    assert_eq!(
        storage.query(&[Filter::new().limit(0)]).unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn query_matches_any_filter_and_deduplicates_results() {
    let dir = test_dir("query-or");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let first = note_with("00", "aa", 10, 1, "[]", "");
    let second = note_with("11", "bb", 20, 2, "[]", "");
    storage.append_packet(&first).unwrap();
    storage.append_packet(&second).unwrap();

    let filters = [
        Filter::new().author(PublicKey::from_byte_array([0xaa; 32])),
        Filter::new().kind(Kind::from(2u16)),
        Filter::new().id(EventId::from_byte_array([0x11; 32])),
    ];

    assert_eq!(storage.query(&filters).unwrap(), vec![second, first]);
}

#[test]
fn query_applies_limit_per_filter_before_or_deduplication() {
    let dir = test_dir("query-or-limit");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let overlap = note_with("00", "aa", 30, 2, "[]", "");
    let author_only = note_with("11", "aa", 20, 1, "[]", "");
    let kind_only = note_with("22", "bb", 10, 2, "[]", "");
    let older_kind_only = note_with("33", "bb", 5, 2, "[]", "");
    storage.append_packet(&author_only).unwrap();
    storage.append_packet(&kind_only).unwrap();
    storage.append_packet(&older_kind_only).unwrap();
    storage.append_packet(&overlap).unwrap();

    let filters = [
        Filter::new()
            .author(PublicKey::from_byte_array([0xaa; 32]))
            .limit(1),
        Filter::new().kind(Kind::from(2u16)).limit(2),
    ];

    assert_eq!(storage.query(&filters).unwrap(), vec![overlap, kind_only]);
}

#[test]
fn query_matches_custom_single_letter_tag() {
    let dir = test_dir("query-custom-tag");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let matching = note_with("00", "aa", 10, 1, r#"[["t","rust"]]"#, "");
    let other = note_with("11", "aa", 20, 1, r#"[["t","nostr"]]"#, "");
    storage.append_packet(&matching).unwrap();
    storage.append_packet(&other).unwrap();

    let filter = Filter::new().tag('t', "rust");

    assert_eq!(storage.query(&[filter]).unwrap(), vec![matching]);
}

#[test]
fn query_search_returns_only_latest_replaceable_event() {
    let dir = test_dir("query-replaceable");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let older = note_with("00", "aa", 10, 0, "[]", r#"{"name":"profile old"}"#);
    let newer = note_with(
        "11",
        "aa",
        SECONDS_PER_DAY + 10,
        0,
        "[]",
        r#"{"name":"profile new"}"#,
    );
    storage.append_packet(&older).unwrap();
    storage.append_packet(&newer).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(0u16)).search("profile")])
            .unwrap(),
        vec![newer]
    );
}

#[test]
fn query_without_search_keeps_historical_replaceable_events() {
    let dir = test_dir("query-replaceable-without-search");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let older = note_with("00", "aa", 10, 0, "[]", "{}");
    let newer = note_with("11", "aa", 20, 0, "[]", "{}");
    storage.append_packet(&older).unwrap();
    storage.append_packet(&newer).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(0u16))])
            .unwrap(),
        vec![newer, older]
    );
}

#[test]
fn query_search_returns_lowest_id_for_replaceable_timestamp_tie() {
    let dir = test_dir("query-replaceable-tie");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let high_id = note_with("ff", "aa", 10, 0, "[]", r#"{"name":"profile"}"#);
    let low_id = note_with("00", "aa", 10, 0, "[]", r#"{"name":"profile"}"#);
    storage.append_packet(&high_id).unwrap();
    storage.append_packet(&low_id).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(0u16)).search("profile")])
            .unwrap(),
        vec![low_id]
    );
}

#[test]
fn query_search_returns_only_latest_addressable_event_per_d_tag() {
    let dir = test_dir("query-addressable");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let older = note_with(
        "00",
        "aa",
        10,
        30_023,
        r#"[["d","post"],["title","old"]]"#,
        "post old",
    );
    let newer = note_with(
        "11",
        "aa",
        SECONDS_PER_DAY + 10,
        30_023,
        r#"[["d","post"],["title","new"]]"#,
        "post new",
    );
    let other_d = note_with(
        "22",
        "aa",
        20,
        30_023,
        r#"[["d","other"],["title","other"]]"#,
        "post other",
    );
    storage.append_packet(&older).unwrap();
    storage.append_packet(&newer).unwrap();
    storage.append_packet(&other_d).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(30_023u16)).search("post")])
            .unwrap(),
        vec![newer, other_d]
    );
}

#[test]
fn reindex_handles_addressable_event_with_unindexed_long_d_tag() {
    let dir = test_dir("long-d-tag");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let long_d = "x".repeat(600);
    let older = note_with(
        "00",
        "aa",
        10,
        30_023,
        &format!(r#"[["d","{long_d}"]]"#),
        "long d older",
    );
    let newer = note_with(
        "11",
        "aa",
        SECONDS_PER_DAY + 10,
        30_023,
        &format!(r#"[["d","{long_d}"]]"#),
        "long d newer",
    );
    storage.append_packet(&older).unwrap();
    storage.append_packet(&newer).unwrap();

    let stats = storage.reindex_all().unwrap();

    assert_eq!(stats.events, 2);
    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(30_023u16)).search("long")])
            .unwrap(),
        vec![newer]
    );
}

#[test]
fn query_hides_event_deleted_by_same_pubkey() {
    let dir = test_dir("query-delete-e");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let target = note_with("00", "aa", 10, 1, "[]", "target");
    let deletion = note_with(
        "11",
        "aa",
        SECONDS_PER_DAY + 10,
        5,
        &format!(r#"[["e","{}"]]"#, "00".repeat(32)),
        "delete",
    );
    storage.append_packet(&target).unwrap();
    storage.append_packet(&deletion).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(1u16)).search("target")])
            .unwrap(),
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(5u16)).search("delete")])
            .unwrap(),
        vec![deletion]
    );
}

#[test]
fn query_does_not_hide_event_deleted_by_different_pubkey() {
    let dir = test_dir("query-delete-e-other-pubkey");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let target = note_with("00", "aa", 10, 1, "[]", "target");
    let deletion = note_with(
        "11",
        "bb",
        SECONDS_PER_DAY + 10,
        5,
        &format!(r#"[["e","{}"]]"#, "00".repeat(32)),
        "delete",
    );
    storage.append_packet(&target).unwrap();
    storage.append_packet(&deletion).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(1u16)).search("target")])
            .unwrap(),
        vec![target]
    );
}

#[test]
fn query_hides_addressable_event_deleted_by_a_tag() {
    let dir = test_dir("query-delete-a");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let target = note_with("00", "aa", 10, 30_023, r#"[["d","post"]]"#, "target");
    let deletion = note_with(
        "11",
        "aa",
        SECONDS_PER_DAY + 10,
        5,
        &format!(r#"[["a","30023:{}:post"]]"#, "aa".repeat(32)),
        "delete",
    );
    storage.append_packet(&target).unwrap();
    storage.append_packet(&deletion).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(30_023u16)).search("target")])
            .unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn query_limit_skips_hidden_newer_events() {
    let dir = test_dir("query-limit-hidden");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let visible = note_with("00", "aa", 10, 1, "[]", "event visible");
    let hidden = note_with("11", "aa", SECONDS_PER_DAY + 10, 1, "[]", "event hidden");
    let deletion = note_with(
        "22",
        "aa",
        SECONDS_PER_DAY * 2 + 10,
        5,
        &format!(r#"[["e","{}"]]"#, "11".repeat(32)),
        "delete",
    );
    storage.append_packet(&visible).unwrap();
    storage.append_packet(&hidden).unwrap();
    storage.append_packet(&deletion).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new()
                .kind(Kind::from(1u16))
                .search("event")
                .limit(1)])
            .unwrap(),
        vec![visible]
    );
}

#[test]
fn query_search_normalizes_content_and_matches_all_terms() {
    let dir = test_dir("query-search-normalized");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let matching = note_with("00", "aa", 10, 1, "[]", "Ｈｅｌｌｏ　　WORLD");
    let other = note_with("11", "aa", 20, 1, "[]", "hello nostr");
    storage.append_packet(&matching).unwrap();
    storage.append_packet(&other).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().search("world   hello")])
            .unwrap(),
        vec![matching]
    );
}

#[test]
fn packet_matches_filter_uses_storage_search_logic() {
    let dir = test_dir("packet-matches-search");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let packet = note_with("00", "aa", 10, 1, "[]", "Ｈｅｌｌｏ　　WORLD");

    assert!(
        storage
            .packet_matches_filter(&packet, &Filter::new().search("world   hello"))
            .unwrap()
    );
    assert!(
        !storage
            .packet_matches_filter(&packet, &Filter::new().search("hello missing"))
            .unwrap()
    );
}

#[test]
fn query_search_does_not_match_when_any_term_is_missing() {
    let dir = test_dir("query-search-and");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    storage
        .append_packet(&note_with("00", "aa", 10, 1, "[]", "hello world"))
        .unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().search("hello missing")])
            .unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn query_search_indexes_all_kinds_by_default() {
    let dir = test_dir("query-search-all-kinds");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let newer = note_with("11", "aa", 20, 2, "[]", "delete target");
    let older = note_with("00", "aa", 10, 5, "[]", "delete target");
    storage.append_packet(&older).unwrap();
    storage.append_packet(&newer).unwrap();

    assert_eq!(
        storage.query(&[Filter::new().search("delete")]).unwrap(),
        vec![newer, older]
    );
}

#[test]
fn query_search_indexes_no_kinds_when_explicitly_empty() {
    let dir = test_dir("query-search-no-kinds");
    let storage = Storage::open_at_with_searchable_kinds(&dir, 1000, Some(&[])).unwrap();

    storage
        .append_packet(&note_with("00", "aa", 10, 1, "[]", "delete target"))
        .unwrap();

    assert_eq!(
        storage.query(&[Filter::new().search("delete")]).unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn reindex_rebuilds_sidecar_when_searchable_kinds_change() {
    let dir = test_dir("query-search-kinds-change");

    let kind1 = note_with("00", "aa", SECONDS_PER_DAY + 10, 1, "[]", "target text");
    let kind2 = note_with("11", "aa", SECONDS_PER_DAY + 20, 2, "[]", "target text");

    {
        let storage = Storage::open_at_with_searchable_kinds(&dir, 1, Some(&[1])).unwrap();
        storage.append_packet(&kind1).unwrap();
        storage.append_packet(&kind2).unwrap();

        assert_eq!(
            storage.query(&[Filter::new().search("target")]).unwrap(),
            vec![kind1.clone()]
        );
    }

    let storage = Storage::open_at_with_searchable_kinds(&dir, 1, Some(&[2])).unwrap();

    assert_eq!(
        storage.query(&[Filter::new().search("target")]).unwrap(),
        vec![kind2]
    );
    assert!(search_index_path(&partition_path(&dir.join("partitions"), 1)).exists());
}

#[test]
fn query_search_uses_kind_0_metadata_values() {
    let dir = test_dir("query-search-kind0");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let matching = note_with(
        "00",
        "aa",
        10,
        0,
        "[]",
        r#"{"name":"Alice","about":"Rust Developer","ignored":["Tokyo"]}"#,
    );
    let other = note_with(
        "11",
        "bb",
        20,
        0,
        "[]",
        r#"{"name":"Bob","about":"Nostr User"}"#,
    );
    storage.append_packet(&matching).unwrap();
    storage.append_packet(&other).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new()
                .kind(Kind::from(0u16))
                .search("developer alice")])
            .unwrap(),
        vec![matching]
    );
}

#[test]
fn query_search_ignores_non_string_kind_0_metadata_values() {
    let dir = test_dir("query-search-kind0-non-string");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    storage
        .append_packet(&note_with(
            "00",
            "aa",
            10,
            0,
            "[]",
            r#"{"name":"Alice","ignored":["Tokyo"]}"#,
        ))
        .unwrap();

    assert_eq!(
        storage.query(&[Filter::new().search("tokyo")]).unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn query_search_does_not_match_across_kind_0_metadata_fields() {
    let dir = test_dir("query-search-kind0-fields");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let event = note_with("00", "aa", 10, 0, "[]", r#"{"name":"ab","about":"cd"}"#);
    storage.append_packet(&event).unwrap();

    assert_eq!(
        storage.query(&[Filter::new().search("bc")]).unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn query_search_uses_kind_30023_title_and_summary_tags() {
    let dir = test_dir("query-search-kind30023");
    let storage = Storage::open_at(&dir, 1000).unwrap();

    let event = note_with(
        "00",
        "aa",
        10,
        30_023,
        r#"[["title","Rust Title"],["summary","Nostr Summary"]]"#,
        "Long form body",
    );
    storage.append_packet(&event).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().search("summary body title")])
            .unwrap(),
        vec![event]
    );
}

#[test]
fn reindex_rebuilds_missing_search_sidecars() {
    let dir = test_dir("reindex");
    let hot_path = dir.join("hot.events");
    let partitions_dir = dir.join("partitions");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let hot = note_with("00", "aa", 10, 1, "[]", "hot searchable");
    let partitioned = note_with(
        "11",
        "aa",
        SECONDS_PER_DAY + 10,
        1,
        "[]",
        "partition searchable",
    );
    storage.append_packet(&hot).unwrap();
    storage.append_packet(&partitioned).unwrap();

    assert!(!search_index_path(&hot_path).exists());
    fs::remove_file(search_index_path(&partition_path(&partitions_dir, 1))).unwrap();

    let stats = storage.reindex().unwrap();

    assert_eq!(stats.files, 1);
    assert_eq!(stats.skipped_files, 1);
    assert_eq!(stats.events, 1);
    assert_eq!(
        storage.query(&[Filter::new().search("partition")]).unwrap(),
        vec![partitioned.clone()]
    );
    assert_eq!(
        storage.query(&[Filter::new().search("hot")]).unwrap(),
        vec![hot.clone()]
    );

    let stats = storage.reindex_all().unwrap();

    assert_eq!(stats.files, 2);
    assert_eq!(stats.skipped_files, 0);
    assert_eq!(stats.events, 2);
    assert_eq!(
        storage.query(&[Filter::new().search("partition")]).unwrap(),
        vec![partitioned]
    );
    assert_eq!(
        storage.query(&[Filter::new().search("hot")]).unwrap(),
        vec![hot]
    );
}

#[test]
fn query_rebuilds_partition_when_search_sidecar_is_missing() {
    let dir = test_dir("query-missing-search-sidecar");
    let partitions_dir = dir.join("partitions");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let partitioned = note_with(
        "00",
        "aa",
        SECONDS_PER_DAY + 10,
        1,
        "[]",
        "missing sidecar searchable",
    );
    storage.append_packet(&partitioned).unwrap();
    fs::remove_file(search_index_path(&partition_path(&partitions_dir, 1))).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().search("searchable")])
            .unwrap(),
        vec![partitioned]
    );
}

#[test]
fn search_query_allows_empty_visibility_store() {
    let dir = test_dir("visibility-rebuild");
    let partitions_dir = dir.join("partitions");
    {
        let storage = Storage::open_at(&dir, 1).unwrap();
        storage
            .append_packet(&note_with(
                "00",
                "aa",
                SECONDS_PER_DAY + 10,
                0,
                "[]",
                r#"{"name":"profile"}"#,
            ))
            .unwrap();
    }

    let visibility_path = visibility_store_path(&partitions_dir);
    fs::remove_dir_all(&visibility_path).unwrap();

    let storage = Storage::open_at(&dir, 1).unwrap();

    assert!(visibility_path.exists());
    let event = note_with(
        "00",
        "aa",
        SECONDS_PER_DAY + 10,
        0,
        "[]",
        r#"{"name":"profile"}"#,
    );

    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(0u16)).search("profile")])
            .unwrap(),
        vec![event.clone()]
    );
    assert!(visibility_path.exists());

    drop(storage);
    let storage = Storage::open_at(&dir, 1).unwrap();
    storage.reindex().unwrap();
    assert_eq!(
        storage
            .query(&[Filter::new().kind(Kind::from(0u16)).search("profile")])
            .unwrap(),
        vec![event]
    );
}

#[test]
fn reindex_ignores_deletion_tags_with_non_text_names() {
    let dir = test_dir("deletion-tag-non-text-name");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let invalid_deletion = note_with(
        "00",
        "aa",
        SECONDS_PER_DAY + 10,
        5,
        r#"[["abababababababababababababababababababababababababababababababab"]]"#,
        "",
    );
    storage.append_packet(&invalid_deletion).unwrap();

    let stats = storage.reindex_all().unwrap();

    assert_eq!(stats.files, 1);
    assert_eq!(stats.events, 1);
}

#[test]
fn query_rebuilds_invalid_search_sidecar() {
    let dir = test_dir("query-rebuild-search");
    let partitions_dir = dir.join("partitions");
    let storage = Storage::open_at(&dir, 1).unwrap();

    let event = note_with(
        "00",
        "aa",
        SECONDS_PER_DAY + 10,
        1,
        "[]",
        "rebuildable search text",
    );
    storage.append_packet(&event).unwrap();

    let search_path = search_index_path(&partition_path(&partitions_dir, 1));
    fs::write(&search_path, b"stale search sidecar").unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().search("rebuildable")])
            .unwrap(),
        vec![event]
    );
    assert!(search_path.exists());
    assert!(
        read_search_index(
            &search_path,
            events_fingerprint(
                &read_event_packets_from_path(&partition_path(&partitions_dir, 1)).unwrap()
            ),
            text::searchable_kinds_hash(None)
        )
        .is_ok()
    );
}

#[test]
fn open_rebuilds_invalid_search_sidecar() {
    let dir = test_dir("open-rebuild-search");
    let partitions_dir = dir.join("partitions");
    let event = note_with(
        "00",
        "aa",
        SECONDS_PER_DAY + 10,
        1,
        "[]",
        "rebuildable search text",
    );

    {
        let storage = Storage::open_at(&dir, 1).unwrap();
        storage.append_packet(&event).unwrap();
    }

    let search_path = search_index_path(&partition_path(&partitions_dir, 1));
    fs::write(&search_path, b"stale search sidecar").unwrap();
    let storage = Storage::open_at(&dir, 1).unwrap();

    assert_eq!(
        storage
            .query(&[Filter::new().search("rebuildable")])
            .unwrap(),
        vec![event]
    );
    assert!(
        read_search_index(
            &search_path,
            events_fingerprint(
                &read_event_packets_from_path(&partition_path(&partitions_dir, 1)).unwrap()
            ),
            text::searchable_kinds_hash(None)
        )
        .is_ok()
    );
}

fn packet(data: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(size_of::<u32>() + data.len());
    packet.extend_from_slice(&(data.len() as u32).to_le_bytes());
    packet.extend_from_slice(data);
    packet
}

fn packets<const N: usize>(items: [&[u8]; N]) -> Vec<u8> {
    let mut packets = Vec::new();
    for item in items {
        packets.extend_from_slice(&packet(item));
    }
    packets
}

fn note(id_byte: &str, created_at: u64) -> Vec<u8> {
    note_with(id_byte, "11", created_at, 1, "[]", "")
}

fn note_with(
    id_byte: &str,
    pubkey_byte: &str,
    created_at: u64,
    kind: u16,
    tags_json: &str,
    content: &str,
) -> Vec<u8> {
    let id = id_byte.repeat(32);
    let pubkey = pubkey_byte.repeat(32);
    let sig = "22".repeat(64);
    let content_json = serde_json::to_string(content).unwrap();
    let json = format!(
        r#"{{"id":"{id}","pubkey":"{pubkey}","created_at":{created_at},"kind":{kind},"tags":{tags_json},"content":{content_json},"sig":"{sig}"}}"#
    );
    NdbNoteBuf::from_json(&json).unwrap().into_bytes()
}

fn test_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("searchnos-{name}-{unique}"))
}
