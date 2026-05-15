use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Error as LmdbError,
    RoTransaction, Transaction, WriteFlags,
};
use ndb::{NdbNote, TagElement};
use sha2::{Digest, Sha256};

use super::{EventPacket, text};

const DELETION_KIND: u32 = 5;
const MAX_INDEXED_TAG_VALUE_BYTES: usize = 255;
#[cfg(not(test))]
const VISIBILITY_LMDB_MAP_SIZE: usize = 10 * 1024 * 1024 * 1024 * 1024;
#[cfg(test)]
const VISIBILITY_LMDB_MAP_SIZE: usize = 1024 * 1024 * 1024;
const VISIBILITY_LMDB_MAX_READERS: u32 = 256;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplaceableKey {
    pubkey: [u8; 32],
    kind: u32,
    d: Option<String>,
}

#[derive(Clone, Copy)]
struct Winner {
    id: [u8; 32],
    created_at: u64,
}

#[derive(Clone, Default)]
pub(crate) struct VisibilityIndex {
    replaceable_winners: BTreeMap<ReplaceableKey, Winner>,
    deleted_ids: BTreeMap<[u8; 32], BTreeSet<[u8; 32]>>,
    deleted_addresses: BTreeMap<ReplaceableKey, u64>,
}

pub(crate) struct VisibilityStore {
    env: Environment,
    event_replace: Database,
    event_deletion: Database,
    event_replace_deletion: Database,
}

#[derive(Default)]
pub(crate) struct VisibilitySummary {
    replaceable_winners: BTreeMap<ReplaceableKey, Winner>,
    deleted_ids: BTreeMap<[u8; 32], BTreeSet<[u8; 32]>>,
    deleted_addresses: BTreeMap<ReplaceableKey, u64>,
}

impl VisibilityIndex {
    pub(crate) fn from_packets(packets: &[EventPacket]) -> Result<Self, Box<dyn Error>> {
        let mut index = Self::default();
        index.merge(VisibilitySummary::from_packets(packets)?);
        Ok(index)
    }

    pub(crate) fn is_visible(&self, packet: &EventPacket) -> Result<bool, Box<dyn Error>> {
        if packet.kind == DELETION_KIND {
            return Ok(true);
        }

        if let Some(key) = replaceable_key(packet)
            && self
                .replaceable_winners
                .get(&key)
                .is_some_and(|winner| winner.id != packet.id)
        {
            return Ok(false);
        }

        if self
            .deleted_ids
            .get(&packet.id)
            .is_some_and(|pubkeys| pubkeys.contains(&packet.pubkey))
        {
            return Ok(false);
        }

        let Some(key) = replaceable_key(packet) else {
            return Ok(true);
        };
        Ok(self
            .deleted_addresses
            .get(&key)
            .is_none_or(|deleted_at| packet.created_at > *deleted_at))
    }

    fn merge(&mut self, summary: VisibilitySummary) {
        for (key, winner) in summary.replaceable_winners {
            update_winner(&mut self.replaceable_winners, key, winner);
        }
        for (id, pubkeys) in summary.deleted_ids {
            self.deleted_ids.entry(id).or_default().extend(pubkeys);
        }
        for (key, deleted_at) in summary.deleted_addresses {
            self.deleted_addresses
                .entry(key)
                .and_modify(|current| *current = (*current).max(deleted_at))
                .or_insert(deleted_at);
        }
    }
}

impl VisibilityStore {
    pub(crate) fn open(path: PathBuf) -> Result<Self, Box<dyn Error>> {
        fs::create_dir_all(&path)?;
        let env = Environment::new()
            .set_flags(EnvironmentFlags::NO_TLS)
            .set_max_readers(VISIBILITY_LMDB_MAX_READERS)
            .set_max_dbs(3)
            .set_map_size(VISIBILITY_LMDB_MAP_SIZE)
            .open(&path)?;
        let event_replace = env.create_db(Some("Event__replace"), DatabaseFlags::empty())?;
        let event_deletion = env.create_db(Some("Event__deletion"), DatabaseFlags::empty())?;
        let event_replace_deletion =
            env.create_db(Some("Event__replaceDeletion"), DatabaseFlags::empty())?;
        Ok(Self {
            env,
            event_replace,
            event_deletion,
            event_replace_deletion,
        })
    }

    pub(crate) fn merge_summary(&self, summary: &VisibilitySummary) -> Result<(), Box<dyn Error>> {
        let mut txn = self.env.begin_rw_txn()?;
        for (key, winner) in &summary.replaceable_winners {
            let db_key = strfry_replace_key(key);
            let should_write = match txn.get(self.event_replace, &db_key) {
                Ok(value) => compare_winner(*winner, parse_winner_value(value)?).is_lt(),
                Err(LmdbError::NotFound) => true,
                Err(err) => return Err(err.into()),
            };
            if should_write {
                txn.put(
                    self.event_replace,
                    &db_key,
                    &winner_value(winner),
                    WriteFlags::empty(),
                )?;
            }
        }
        for (id, pubkeys) in &summary.deleted_ids {
            for pubkey in pubkeys {
                txn.put(
                    self.event_deletion,
                    &strfry_deletion_key(id, pubkey),
                    &[],
                    WriteFlags::empty(),
                )?;
            }
        }
        for (key, created_at) in &summary.deleted_addresses {
            txn.put(
                self.event_replace_deletion,
                &strfry_replace_deletion_key(key, *created_at),
                &[],
                WriteFlags::empty(),
            )?;
        }
        txn.commit()?;
        Ok(())
    }

    pub(crate) fn retain_visible(
        &self,
        packets: &mut Vec<EventPacket>,
    ) -> Result<(), Box<dyn Error>> {
        let txn = self.env.begin_ro_txn()?;
        let mut visible = Vec::with_capacity(packets.len());
        for packet in packets.drain(..) {
            if self.is_visible_in_txn(&txn, &packet)? {
                visible.push(packet);
            }
        }
        *packets = visible;
        Ok(())
    }

    pub(crate) fn is_visible(&self, packet: &EventPacket) -> Result<bool, Box<dyn Error>> {
        let txn = self.env.begin_ro_txn()?;
        self.is_visible_in_txn(&txn, packet)
    }

    fn is_visible_in_txn(
        &self,
        txn: &RoTransaction<'_>,
        packet: &EventPacket,
    ) -> Result<bool, Box<dyn Error>> {
        if packet.kind == DELETION_KIND {
            return Ok(true);
        }

        if let Some(key) = replaceable_key(packet) {
            match txn.get(self.event_replace, &strfry_replace_key(&key)) {
                Ok(value) => {
                    let stored_winner = parse_winner_value(value)?;
                    let packet_winner = Winner {
                        id: packet.id,
                        created_at: packet.created_at,
                    };
                    if compare_winner(packet_winner, stored_winner).is_gt() {
                        return Ok(false);
                    }
                }
                Err(LmdbError::NotFound) => {}
                Err(err) => return Err(err.into()),
            }

            let address_hash = strfry_address_hash(&key);
            let cursor = txn.open_ro_cursor(self.event_replace_deletion)?;
            let mut current = match cursor.get(Some(&address_hash), None, lmdb_sys::MDB_SET_RANGE) {
                Ok((key, value)) => (key.unwrap_or(&address_hash), value),
                Err(LmdbError::NotFound) => return self.is_not_deleted_by_id(txn, packet),
                Err(err) => return Err(err.into()),
            };
            loop {
                let (stored_key, _) = current;
                if !stored_key.starts_with(&address_hash) {
                    break;
                }
                let (hash, deleted_at) = parse_strfry_replace_deletion_key(stored_key)?;
                if hash == address_hash && packet.created_at <= deleted_at {
                    return Ok(false);
                }
                current = match cursor.get(None, None, lmdb_sys::MDB_NEXT) {
                    Ok(item) => (item.0.unwrap_or(stored_key), item.1),
                    Err(LmdbError::NotFound) => break,
                    Err(err) => return Err(err.into()),
                };
            }
        }

        self.is_not_deleted_by_id(txn, packet)
    }

    fn is_not_deleted_by_id(
        &self,
        txn: &RoTransaction<'_>,
        packet: &EventPacket,
    ) -> Result<bool, Box<dyn Error>> {
        match txn.get(
            self.event_deletion,
            &strfry_deletion_key(&packet.id, &packet.pubkey),
        ) {
            Ok(_) => Ok(false),
            Err(LmdbError::NotFound) => Ok(true),
            Err(err) => Err(err.into()),
        }
    }
}

pub(crate) fn visibility_store_path(partitions_dir: &Path) -> PathBuf {
    partitions_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(
            || PathBuf::from("visibility"),
            |parent| parent.join("visibility"),
        )
}

impl VisibilitySummary {
    pub(crate) fn from_packets(packets: &[EventPacket]) -> Result<Self, Box<dyn Error>> {
        let mut summary = Self::default();

        for packet in packets {
            summary.add_packet(packet)?;
        }

        Ok(summary)
    }

    pub(crate) fn add_packet(&mut self, packet: &EventPacket) -> Result<(), Box<dyn Error>> {
        if let Some(key) = replaceable_key(packet) {
            update_winner(
                &mut self.replaceable_winners,
                key,
                Winner {
                    id: packet.id,
                    created_at: packet.created_at,
                },
            );
        }

        if packet.kind == DELETION_KIND {
            self.apply_deletion(packet)?;
        }

        Ok(())
    }

    fn apply_deletion(&mut self, packet: &EventPacket) -> Result<(), Box<dyn Error>> {
        let note = NdbNote::from_bytes(&packet.data)?;
        for tag in note.tags() {
            let tag = tag?;
            let mut elements = tag.elements();
            let Some(name) = elements.next() else {
                continue;
            };
            let name = name?;
            let Ok(name) = name.as_str() else {
                continue;
            };
            let Some(value) = elements.next() else {
                continue;
            };

            match name {
                "e" => {
                    if let Some(id) = tag_value_to_id(value?)? {
                        self.deleted_ids
                            .entry(id)
                            .or_default()
                            .insert(packet.pubkey);
                    }
                }
                "a" => {
                    let value = text::tag_element_to_string(value?)?;
                    if value.len() <= MAX_INDEXED_TAG_VALUE_BYTES
                        && let Some(key) = parse_address_tag(&value)
                        && key.pubkey == packet.pubkey
                    {
                        self.deleted_addresses
                            .entry(key)
                            .and_modify(|current| *current = (*current).max(packet.created_at))
                            .or_insert(packet.created_at);
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}

fn update_winner(
    winners: &mut BTreeMap<ReplaceableKey, Winner>,
    key: ReplaceableKey,
    candidate: Winner,
) {
    winners
        .entry(key)
        .and_modify(|current| {
            if compare_winner(candidate, *current).is_lt() {
                *current = candidate;
            }
        })
        .or_insert(candidate);
}

fn compare_winner(a: Winner, b: Winner) -> std::cmp::Ordering {
    b.created_at
        .cmp(&a.created_at)
        .then_with(|| a.id.cmp(&b.id))
}

fn replaceable_key(packet: &EventPacket) -> Option<ReplaceableKey> {
    if is_regular_replaceable_kind(packet.kind) {
        return Some(ReplaceableKey {
            pubkey: packet.pubkey,
            kind: packet.kind,
            d: None,
        });
    }
    if is_addressable_kind(packet.kind) {
        return Some(ReplaceableKey {
            pubkey: packet.pubkey,
            kind: packet.kind,
            d: Some(first_d_tag(&packet.data).unwrap_or_default()),
        });
    }
    None
}

fn is_regular_replaceable_kind(kind: u32) -> bool {
    kind == 0 || kind == 3 || kind == 41 || (10_000..20_000).contains(&kind)
}

fn is_addressable_kind(kind: u32) -> bool {
    (30_000..40_000).contains(&kind)
}

fn first_d_tag(data: &[u8]) -> Option<String> {
    let note = NdbNote::from_bytes(data).ok()?;
    for tag in note.tags() {
        let tag = tag.ok()?;
        let mut elements = tag.elements();
        let name = elements.next()?.ok()?;
        if name.as_str().ok()? != "d" {
            continue;
        }
        return elements
            .next()
            .and_then(Result::ok)
            .and_then(|value| text::tag_element_to_string(value).ok())
            .filter(|value| value.len() <= MAX_INDEXED_TAG_VALUE_BYTES);
    }
    None
}

fn parse_address_tag(value: &str) -> Option<ReplaceableKey> {
    let mut parts = value.splitn(3, ':');
    let kind = parts.next()?.parse::<u32>().ok()?;
    let pubkey = hex_to_32(parts.next()?)?;
    let d = parts.next().unwrap_or_default().to_owned();
    Some(ReplaceableKey {
        pubkey,
        kind,
        d: if is_regular_replaceable_kind(kind) {
            None
        } else {
            Some(d)
        },
    })
}

fn tag_value_to_id(value: TagElement<'_>) -> Result<Option<[u8; 32]>, Box<dyn Error>> {
    Ok(hex_to_32(&text::tag_element_to_string(value)?))
}

fn hex_to_32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }

    let mut bytes = [0; 32];
    for index in 0..32 {
        bytes[index] = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

fn strfry_replace_key(key: &ReplaceableKey) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&key.pubkey);
    if let Some(d) = &key.d {
        bytes.extend_from_slice(d.as_bytes());
    }
    bytes.extend_from_slice(&u64::from(key.kind).to_ne_bytes());
    bytes
}

fn strfry_deletion_key(id: &[u8; 32], pubkey: &[u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(id);
    bytes.extend_from_slice(pubkey);
    bytes
}

fn strfry_replace_deletion_key(key: &ReplaceableKey, created_at: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(40);
    bytes.extend_from_slice(&strfry_address_hash(key));
    bytes.extend_from_slice(&created_at.to_ne_bytes());
    bytes
}

fn parse_strfry_replace_deletion_key(bytes: &[u8]) -> Result<([u8; 32], u64), Box<dyn Error>> {
    if bytes.len() != 40 {
        return Err("invalid Event__replaceDeletion key".into());
    }
    let mut hash = [0; 32];
    hash.copy_from_slice(&bytes[..32]);
    let mut created_at = [0; 8];
    created_at.copy_from_slice(&bytes[32..]);
    Ok((hash, u64::from_ne_bytes(created_at)))
}

fn strfry_address_hash(key: &ReplaceableKey) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(key.kind.to_string());
    hasher.update(b":");
    hasher.update(hex_32(&key.pubkey));
    hasher.update(b":");
    if let Some(d) = &key.d {
        hasher.update(d.as_bytes());
    }
    hasher.finalize().into()
}

fn winner_value(winner: &Winner) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(40);
    bytes.extend_from_slice(&winner.id);
    bytes.extend_from_slice(&winner.created_at.to_ne_bytes());
    bytes
}

fn parse_winner_value(bytes: &[u8]) -> Result<Winner, Box<dyn Error>> {
    if bytes.len() != 40 {
        return Err("invalid Event__replace value".into());
    }
    let mut id = [0; 32];
    id.copy_from_slice(&bytes[..32]);
    let mut created_at = [0; 8];
    created_at.copy_from_slice(&bytes[32..]);
    Ok(Winner {
        id,
        created_at: u64::from_ne_bytes(created_at),
    })
}

fn hex_32(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
