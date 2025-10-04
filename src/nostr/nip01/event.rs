use secp256k1::schnorr::Signature as SchnorrSignature;
use secp256k1::{Message, Secp256k1, XOnlyPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{EventId, Kind, PublicKey, Signature, Timestamp};
use tag::Tags;

pub mod tag {
    use serde::{Deserialize, Serialize};
    use std::ops::{Deref, DerefMut};

    pub type Tag = Vec<String>;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
    #[serde(transparent)]
    pub struct Tags(Vec<Tag>);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TagKind {
        SingleLetter(char),
        Other,
    }

    pub trait TagExt {
        fn kind(&self) -> TagKind;
        fn content(&self) -> Option<&str>;
    }

    impl TagExt for [String] {
        fn kind(&self) -> TagKind {
            self.first()
                .and_then(|identifier| single_letter_from(identifier))
                .map(TagKind::SingleLetter)
                .unwrap_or(TagKind::Other)
        }

        fn content(&self) -> Option<&str> {
            self.get(1).map(|value| value.as_str())
        }
    }

    impl TagExt for Vec<String> {
        fn kind(&self) -> TagKind {
            self.as_slice().kind()
        }

        fn content(&self) -> Option<&str> {
            self.as_slice().content()
        }
    }

    impl Tags {
        pub fn new(values: Vec<Tag>) -> Self {
            Self(values)
        }

        pub fn to_vec_vec(&self) -> Vec<Vec<String>> {
            self.0.clone()
        }
    }

    impl Deref for Tags {
        type Target = Vec<Tag>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for Tags {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    fn single_letter_from(identifier: &str) -> Option<char> {
        if identifier.len() != 1 {
            return None;
        }
        let ch = identifier.chars().next()?;
        if !ch.is_ascii_alphabetic() {
            return None;
        }
        Some(ch)
    }
}

#[derive(Debug, Error)]
pub enum EventError {
    #[error("invalid event json: {0}")]
    InvalidJson(String),
    #[error("invalid event id")]
    InvalidId,
    #[error("invalid public key")]
    InvalidPublicKey,
    #[error("invalid signature")]
    InvalidSignature,
}

pub trait JsonUtil: Sized + Serialize + for<'de> Deserialize<'de> {
    fn as_json(&self) -> String {
        serde_json::to_string(self).expect("json serialization failed")
    }

    fn from_json(value: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub id: EventId,
    pub pubkey: PublicKey,
    pub created_at: Timestamp,
    pub kind: Kind,
    #[serde(default)]
    pub tags: Tags,
    pub content: String,
    pub sig: Signature,
}

impl Event {
    pub fn from_json(input: &str) -> Result<Self, EventError> {
        serde_json::from_str::<Event>(input).map_err(|err| EventError::InvalidJson(err.to_string()))
    }

    pub fn verify(&self) -> Result<(), EventError> {
        let expected_id = Self::compute_id(
            &self.pubkey,
            self.created_at,
            self.kind,
            &self.tags,
            &self.content,
        );

        if self.id.as_bytes() != expected_id.as_bytes() {
            return Err(EventError::InvalidId);
        }

        let secp = Secp256k1::verification_only();
        let pubkey = XOnlyPublicKey::from_slice(self.pubkey.as_bytes())
            .map_err(|_| EventError::InvalidPublicKey)?;
        let signature = SchnorrSignature::from_slice(self.sig.as_bytes())
            .map_err(|_| EventError::InvalidSignature)?;

        let msg = Message::from_digest_slice(expected_id.as_bytes())
            .map_err(|_| EventError::InvalidId)?;
        secp.verify_schnorr(&signature, &msg, &pubkey)
            .map_err(|_| EventError::InvalidSignature)
    }

    pub(crate) fn compute_id(
        pubkey: &PublicKey,
        created_at: Timestamp,
        kind: Kind,
        tags: &Tags,
        content: &str,
    ) -> EventId {
        let json_value = serde_json::json!([
            0,
            pubkey.to_hex(),
            created_at.as_u64(),
            kind.as_u32(),
            tags.to_vec_vec(),
            content,
        ]);
        let serialized = serde_json::to_vec(&json_value).expect("json serialization");
        let hash = Sha256::digest(&serialized);
        let mut id_bytes = [0u8; 32];
        id_bytes.copy_from_slice(&hash);
        EventId::from(id_bytes)
    }
}

impl JsonUtil for Event {}
