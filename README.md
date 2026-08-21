# searchnos-db

searchnos-db is a local Nostr event store and query tool. Its storage layer is implemented directly in this repository as an internal Rust module instead of being used as an external crate or path dependency.

Events are stored as `ndb_note` payloads. Recent writes go to a hot append-only event file, and older data is compacted into per-day partition files with sidecar search and visibility indexes. Queries use NIP-01 filters plus NIP-50-style `search` terms.

The crate can be used as both a CLI utility and a library.

## Highlights

- **Local storage layer**: stores event packets in `hot.events`, per-day partition files, search sidecars, and a visibility LMDB used by the storage layer.
- **NIP-01 query support**: filters by ids, authors, kinds, time ranges, limits, and single-letter generic tags.
- **NIP-50 search support**: normalizes searchable text and uses partition search sidecars when available.
- **Deletion and replaceable visibility**: stores raw events append-only, while query visibility hides deleted events and superseded replaceable/addressable events.
- **Dump/load tooling**: exports and imports raw `ndb_note` payloads using a simple length-prefixed binary stream.

> Stability notice: searchnos-db is under active development. Public interfaces and on-disk storage formats may change without prior notice.

## Storage Layout

Given `--db-path ./data`, searchnos-db manages files under that directory:

```text
./data/
  hot.events
  partitions/
    <unix-day>.events
    <unix-day>.search
  visibility/
    data.mdb
    lock.mdb
```

`hot.events` contains newly appended event packets. When the hot file exceeds the configured size, storage rotates and compacts it into per-day partition files under `partitions/`. The `.search` sidecars support search queries, and `visibility/` stores deletion and replaceable-event visibility metadata.

The `.search` files are derived data that can be rebuilt from their matching `.events` files. Opening storage runs a non-forced reindex, which rebuilds missing, stale, or unreadable sidecars while leaving current sidecars intact. Search queries also repair a missing or unreadable partition sidecar on demand and use the rebuilt sidecar in the same query when repair succeeds. Reindexing writes rebuilt sidecars through a temporary file followed by an atomic rename.

This repository contains the storage source directly under `src/storage/`. Keep storage changes local to that module unless the project intentionally moves back to an external crate.

## Current Scope

Included:

- Event append and query through the local storage layer.
- Search sidecar creation during compaction.
- Reindex implementation inside `src/storage`.
- Storage-level negentropy item collection: `(created_at, event_id)` values for a Unix day.

Not currently implemented by the CLI:

- Relay negentropy reconciliation loop.
- CLI flags such as `--negentropy-relay` or `--negentropy-days`.
- Automatic purge policy behavior from the older LMDB-backed implementation.

## Getting Started

```bash
cargo build
mkdir -p data
```

Every CLI subcommand accepts `--db-path` to target an alternate storage directory. The default is `./data`.

## CLI Usage

All commands can be run through `cargo run --`.

### Help

```bash
cargo run -- --help
```

### Show statistics

```bash
cargo run -- --db-path ./data stat
```

Prints the number of currently query-visible events and the total bytes of their `ndb_note` payloads. This is not an LMDB page-level report.

### Compact hot events

```bash
cargo run -- --db-path ./data compact
```

Moves the current `hot.events` contents into per-day partition files immediately, even when the hot file has not reached the automatic compaction size.

### Rebuild indexes

```bash
cargo run -- --db-path ./data reindex
cargo run -- --db-path ./data reindex --force
```

Rebuilds missing, stale, or unreadable partition sidecars. Use `--force` to rebuild every partition sidecar.

### Import events

```bash
cargo run -- --db-path ./data import ./events-a.jsonl ./events-b.jsonl
```

Reads newline-delimited event JSON and appends valid events with a progress bar. Multiple files are processed sequentially, and blank lines are skipped.

### Dump events

```bash
cargo run -- --db-path ./data dump ./events.dump
```

Writes query-visible `ndb_note` payloads to a binary dump file with a progress bar. The dump format is a repeated sequence of:

1. 4-byte unsigned payload length encoded as big-endian `u32`
2. Raw `ndb_note` payload bytes of that length

### Load events

```bash
cargo run -- --db-path ./data load ./events.dump
```

Reads the binary dump format produced by `dump`, verifies each event, and appends valid payloads to storage. Invalid records are skipped with a warning.

### Query events

```bash
cargo run -- --db-path ./data query '{"authors": ["<hex-pubkey>"], "kinds": [1]}'
```

Provide a JSON object for one filter or a JSON array for multiple filters. Matching events are printed as JSON on stdout, and execution timing is logged to stderr.

## Library Usage

The crate exposes `SearchnosDB` as the high-level API. It accepts event JSON,
verifies events before inserting them, returns query results as event JSON, and
provides dump/load helpers.

```rust
use searchnos_db::{InsertOptions, SearchnosDB};

let db = SearchnosDB::open("./data")?;

let raw_event = r#"{"id":"...","pubkey":"...","kind":1,"content":"hello","tags":[],"created_at":0,"sig":"..."}"#;
db.insert_event_json(raw_event, InsertOptions::default())?;
```

Configure automatic compaction workers with `SearchnosDBOptions` when opening a
database:

```rust
use std::num::NonZeroUsize;
use searchnos_db::{SearchnosDB, SearchnosDBOptions};

let db = SearchnosDB::open_with_options(
    "./data",
    SearchnosDBOptions {
        compact_workers: NonZeroUsize::new(4),
        ..SearchnosDBOptions::default()
    },
)?;
```

Maintenance operations are also available from Rust:

```rust
db.compact()?;
db.reindex()?;
db.reindex_all()?;
```

### Query from Rust

```rust
use searchnos_db::SearchnosDB;

let db = SearchnosDB::open("./data")?;
let events = db.query(r#"{"limit":100}"#)?;
```

Use `stream_query` when the caller wants to process matching events without materializing the full result vector:

```rust
db.stream_query(r#"{"limit":100}"#, |event_json| {
    // Process event JSON here.
    true
})?;
```

Return `false` from the callback to stop delivery early.

### Subscriptions

Initial snapshots created by `subscribe` are handled by one shared worker. The
worker coalesces subscriptions received within 10 milliseconds and scans each
daily search index once for the whole batch, including subscriptions with
different search terms. Results retain each subscription's filter order,
per-filter limits, event deduplication, and newest-first ordering. After `EOSE`,
matching events continue through the live subscription path.

## Development Workflow

Run the following checks before sending changes:

```bash
cargo fmt
cargo check
cargo clippy
cargo test
```

## License

See [`LICENSE`](./LICENSE).
