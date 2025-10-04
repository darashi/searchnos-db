use std::fmt;
use std::str::FromStr;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Default)]
pub struct EventId([u8; 32]);

impl EventId {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl Serialize for EventId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        if hex.len() != 64 {
            return Err(DeError::custom("event id must be 64 hex characters"));
        }
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(&hex, &mut bytes)
            .map_err(|_| DeError::custom("invalid event id hex"))?;
        Ok(Self(bytes))
    }
}

impl FromStr for EventId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(s, &mut bytes)?;
        Ok(Self(bytes))
    }
}

impl From<[u8; 32]> for EventId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}
