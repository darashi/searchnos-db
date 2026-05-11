# searchnos-db

searchnos-db is a local LMDB-backed database designed for fast storage and retrieval of Nostr events. It provides normalized text search via n-gram indexes, several secondary indexes (event ID, pubkey, kind, tags, expiration), and tools to automatically purge expired data. The crate can be used both as a CLI utility and as a library embedding the database in your own application.

Events are serialized in a format that is (hopefully) compatible with `ndb_note` (v1) of [`nostrdb`](https://github.com/damus-io/nostrdb) when stored on disk.
The overall layout takes cues from both [`strfry`](https://github.com/hoytech/strfry) and [`nostrdb`](https://github.com/damus-io/nostrdb).
The main difference is an n-gram–based full-text search index.

## Highlights
- **LMDB storage**: relies on a durable B+Tree datastore optimized for random access workloads.
- **Rich indexing**: maintains secondary indexes for event IDs, authors, kinds, tags, n-grams, creation timestamps, and expiration timestamps.
- **Text normalization**: normalizes Unicode text (NFKC), lowercases, and collapses whitespace to improve search quality.
- **Expiration handling**: reads `expiration` tags and optional purge policies to drop stale events.
- **Operational tooling**: ships with CLI subcommands for statistics, imports, dumps, and queries.

> ⚠️ **Stability notice:** searchnos-db is under active development. Public interfaces may change without prior notice, and on-disk storage formats are not guaranteed to remain compatible between releases.

## Getting Started
```bash
# Build the project and download dependencies
cargo build

# Create the default data directory used by the CLI
mkdir -p data
```

You can adjust LMDB settings such as map size through `SearchnosDBOptions`. Every CLI subcommand accepts `--db-path` to target an alternate directory (default: `./data`).

## CLI Usage
All commands can be run through `cargo run --`. Omitting `--db-path` falls back to `./data`.

### Help
```bash
cargo run -- --help
```

### Show statistics
```bash
cargo run -- stat --db-path ./data
```
Prints a table with entry counts and total key/value bytes for each LMDB database.

### Import events
```bash
cargo run -- import ./events-a.jsonl ./events-b.jsonl --db-path ./data
```
Reads newline-delimited JSON and inserts events with a progress bar. Multiple files are processed sequentially, and blank lines are skipped.

### Dump events
```bash
cargo run -- dump ./events.dump --db-path ./data
```
Writes all stored `ndb_note` payloads to a binary dump file with a progress bar. The dump format is a repeated sequence of:

1. 4-byte unsigned payload length encoded as big-endian `u32`
2. Raw `ndb_note` payload bytes of that length

### Query events
```bash
cargo run -- query '{"authors": ["<hex pubkey>"], "kinds": [1]}'
```
Provide a JSON object for one filter or a JSON array for multiple filters. The `search` field follows NIP-50 semantics. Matching events are printed as JSON on stdout, and execution time is logged to stderr.

## Library Usage
The crate exposes the `SearchnosDB` type for embedding in other Rust applications.

```rust
use searchnos_db::{SearchnosDB, SearchnosDBOptions};

let options = SearchnosDBOptions::default();
let db = SearchnosDB::open_with_options("./data", options)?;

let raw_event = r#"{"id":"...","pubkey":"...","kind":1,"content":"hello","tags":[],"created_at":0,"sig":"..."}"#;
db.insert_event_json(raw_event)?;

db.flush()?; // ensure pending batches are written
```

### Dump from Rust
Use `dump_events` when the caller only needs the serialized stream, or `dump_events_with_progress` when the caller wants to report progress. The crate writes to any `std::io::Write`, so file creation, compression, or network transport can remain the caller's responsibility.

```rust
use searchnos_db::{DumpProgress, SearchnosDB};
use std::fs::File;
use std::io::BufWriter;

let db = SearchnosDB::open("./data")?;
let file = File::create("./events.dump")?;
let writer = BufWriter::new(file);

db.dump_events_with_progress(writer, |progress: DumpProgress| {
    eprintln!(
        "dumped {}/{} events ({} bytes)",
        progress.events_written,
        progress.total_events,
        progress.bytes_written,
    );
})?;
```

Refer to the Rustdoc comments for details on purge policies, index behavior, and the embedded `ndb` format helpers.

## Development Workflow
Run the following checks before sending changes:
```bash
cargo fmt
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## License
See [`LICENSE`](./LICENSE).
