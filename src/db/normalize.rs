use ndb::NdbNote;

use crate::text::normalize_note_content;

use crate::ndb_ext::note_event_index_key;

#[derive(Debug, Clone)]
pub(super) struct EventIndexData {
    pub event_index_key: Vec<u8>,
    pub created_at: u64,
    pub normalized_content: Vec<u8>,
    pub expiration: Option<u64>,
}

impl EventIndexData {
    pub(super) fn from_note(note: &NdbNote<'_>) -> Result<Self, ndb::Error> {
        let event_index_key = note_event_index_key(note);
        let created_at = note.created_at();
        let normalized_content = normalize_note_content(note)?;
        let expiration = crate::nostr::extract_note_expiration(note);

        Ok(Self {
            event_index_key,
            created_at,
            normalized_content: normalized_content.into_bytes(),
            expiration,
        })
    }
}
