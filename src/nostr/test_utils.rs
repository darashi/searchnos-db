use rand::rngs::OsRng;
use secp256k1::{Keypair, Message, Secp256k1};
use serde_json::{Map, Value};

use super::{
    Event, EventDeletionRequest, EventError, EventId, Filter, Kind, PublicKey, Signature, Tag,
    Tags, Timestamp,
};

pub fn event_tag(id: EventId) -> Tag {
    vec!["e".to_string(), id.to_hex()]
}

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    fields: Map<String, Value>,
}

impl Metadata {
    pub fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub fn display_name(mut self, value: impl Into<String>) -> Self {
        self.fields
            .insert("display_name".to_string(), Value::String(value.into()));
        self
    }

    pub fn about(mut self, value: impl Into<String>) -> Self {
        self.fields
            .insert("about".to_string(), Value::String(value.into()));
        self
    }

    pub fn to_json_string(&self) -> String {
        if self.fields.is_empty() {
            "{}".to_string()
        } else {
            serde_json::to_string(&self.fields).unwrap_or_else(|_| "{}".to_string())
        }
    }
}

impl Filter {
    pub fn id(mut self, id: EventId) -> Self {
        self.ids.get_or_insert_with(Vec::new).push(id);
        self
    }

    pub fn author(mut self, author: PublicKey) -> Self {
        self.authors.get_or_insert_with(Vec::new).push(author);
        self
    }

    pub fn kind(mut self, kind: Kind) -> Self {
        self.kinds.get_or_insert_with(Vec::new).push(kind);
        self
    }

    pub fn since(mut self, ts: Timestamp) -> Self {
        self.since = Some(ts);
        self
    }

    pub fn until(mut self, ts: Timestamp) -> Self {
        self.until = Some(ts);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn search(mut self, query: impl Into<String>) -> Self {
        self.search = Some(query.into());
        self
    }

    pub fn tag(mut self, tag: char, value: impl Into<String>) -> Self {
        if !tag.is_ascii_alphabetic() {
            return self;
        }
        let value = value.into();
        if value.is_empty() {
            return self;
        }
        if let Some((_, existing)) = self
            .generic_tags
            .iter_mut()
            .find(|(existing_tag, _)| existing_tag == &tag)
        {
            existing.push(value);
        } else {
            self.generic_tags.push((tag, vec![value]));
        }
        self
    }
}

#[derive(Debug, Clone)]
pub struct Keys {
    keypair: Keypair,
    public_key: PublicKey,
}

impl Keys {
    pub fn generate() -> Self {
        let secp = Secp256k1::new();
        let mut rng = OsRng;
        let keypair = Keypair::new(&secp, &mut rng);
        let (xonly, _) = keypair.x_only_public_key();
        let public_key = PublicKey::from_bytes(xonly.serialize());
        Self {
            keypair,
            public_key,
        }
    }

    pub fn public_key(&self) -> PublicKey {
        self.public_key
    }

    pub fn sign(&self, message: &[u8; 32]) -> Result<Signature, EventError> {
        let secp = Secp256k1::new();
        let msg = Message::from_digest_slice(message).map_err(|_| EventError::InvalidId)?;
        let mut rng = OsRng;
        let signature = secp.sign_schnorr_with_rng(&msg, &self.keypair, &mut rng);
        Ok(Signature::from_bytes(signature.serialize()))
    }
}

pub struct EventBuilder {
    kind: Kind,
    content: String,
    tags: Vec<Tag>,
    created_at: Option<Timestamp>,
}

impl EventBuilder {
    pub fn new(kind: Kind, content: impl Into<String>) -> Self {
        Self {
            kind,
            content: content.into(),
            tags: Vec::new(),
            created_at: None,
        }
    }

    pub fn text_note(content: impl Into<String>) -> Self {
        Self::new(Kind::TextNote, content)
    }

    pub fn long_form_text_note(content: impl Into<String>) -> Self {
        Self::new(Kind::LongFormTextNote, content)
    }

    pub fn metadata(metadata: &Metadata) -> Self {
        Self::new(Kind::Metadata, metadata.to_json_string())
    }

    pub fn delete(request: EventDeletionRequest) -> Self {
        let (tags, reason) = request.into_components();
        Self {
            kind: Kind::EventDeletion,
            content: reason.unwrap_or_default(),
            tags,
            created_at: None,
        }
    }

    pub fn tag(mut self, tag: Tag) -> Self {
        self.tags.push(tag);
        self
    }

    pub fn custom_created_at(mut self, timestamp: Timestamp) -> Self {
        self.created_at = Some(timestamp);
        self
    }

    pub fn sign_with_keys(self, keys: &Keys) -> Result<Event, EventError> {
        let created_at = self.created_at.unwrap_or_else(Timestamp::now);
        let tags = Tags::new(self.tags);
        let id = Event::compute_id(
            &keys.public_key(),
            created_at,
            self.kind,
            &tags,
            &self.content,
        );
        let signature = keys.sign(id.as_bytes())?;

        Ok(Event {
            id,
            pubkey: keys.public_key(),
            created_at,
            kind: self.kind,
            tags,
            content: self.content,
            sig: signature,
        })
    }
}
