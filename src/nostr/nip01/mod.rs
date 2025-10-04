//! Minimal Nostr primitives scoped to [NIP-01](https://github.com/nostr-protocol/nips/blob/master/01.md).
//! The implementation focuses on the core data structures (events, tags, filters) that the
//! database relies on while keeping the surface area intentionally small.

pub use serde_json;

pub mod event;
mod event_id;
mod filter;
mod keys;
mod kind;
mod timestamp;

pub use event::tag::{Tag, TagExt, TagKind, Tags};
pub use event::{Event, EventError, JsonUtil};
pub use event_id::EventId;
pub use filter::Filter;
pub use keys::{PublicKey, Signature};
pub use kind::Kind;
pub use timestamp::Timestamp;
