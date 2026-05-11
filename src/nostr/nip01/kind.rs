use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Kind(u32);

#[allow(non_upper_case_globals)]
impl Kind {
    pub const Metadata: Kind = Kind(0);
    pub const TextNote: Kind = Kind(1);
    pub const EventDeletion: Kind = Kind(5);
    pub const LongFormTextNote: Kind = Kind(30_023);

    pub fn from_u16(value: u16) -> Self {
        Self(value as u32)
    }

    pub fn from_u32(value: u32) -> Self {
        Self(value)
    }

    pub fn as_u16(self) -> u16 {
        self.0 as u16
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }

    pub fn is_ephemeral(self) -> bool {
        (20_000..30_000).contains(&self.0)
    }

    pub fn is_replaceable(self) -> bool {
        matches!(self.0, 0 | 3) || (10_000..20_000).contains(&self.0)
    }

    pub fn is_addressable(self) -> bool {
        (30_000..40_000).contains(&self.0)
    }
}
