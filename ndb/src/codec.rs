use std::str;

use serde::{Deserialize, Serialize};

use crate::types::{
    Error, MAX_STRING_TABLE_SIZE, NOTE_HEADER_SIZE, NdbNote, NdbValue, PACKED_STR_ID_FLAG,
    PACKED_STR_INLINE_CAP, PACKED_STR_INLINE_FLAG, PACKED_STR_MAX_OFFSET, PACKED_STR_SIZE, Result,
    VERSION,
};

pub(crate) fn decode_hex_array<const N: usize>(
    value: &str,
    field: &'static str,
) -> Result<[u8; N]> {
    if value.len() != N * 2 {
        return Err(Error::InvalidBinary(format!(
            "{field} hex length mismatch: expected {} got {}",
            N * 2,
            value.len()
        )));
    }

    let mut buf = [0u8; N];
    hex::decode_to_slice(value, &mut buf).map_err(|source| Error::Hex { field, source })?;
    Ok(buf)
}

fn parse_note_json(json: &str) -> Result<NoteJson> {
    serde_json::from_str::<NoteJson>(json).map_err(|err| Error::InvalidJson(err.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NoteJson {
    id: String,
    pubkey: String,
    created_at: u64,
    kind: u32,
    #[serde(default)]
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
}

/// Convert a Nostr event JSON string into a packed `ndb_note` binary buffer.
pub fn to_ndb_note(json: &str) -> Result<Vec<u8>> {
    let event = parse_note_json(json)?;

    let content_bytes = event.content.as_bytes();
    let content_length = usize_to_u32(content_bytes.len(), "content length")?;

    let mut string_table = Vec::with_capacity(128);
    let content_packed = encode_text_value(content_bytes, &mut string_table)?;

    let mut tags_payload = Vec::new();

    for tag in event.tags.iter() {
        let values = tag_to_values(tag);
        let count = u16::try_from(values.len()).map_err(|_| Error::LengthOverflow {
            field: "tag value count",
            value: values.len(),
        })?;
        tags_payload.extend_from_slice(&count.to_le_bytes());

        for value in values {
            let packed = match value {
                TagValue::Text(text) => encode_text_value(text.as_bytes(), &mut string_table)?,
                TagValue::Id(id) => encode_id_value(&id, &mut string_table)?,
            };
            tags_payload.extend_from_slice(&packed);
        }
    }

    let tags_count = u16::try_from(event.tags.len()).map_err(|_| Error::LengthOverflow {
        field: "tag count",
        value: event.tags.len(),
    })?;
    let strings_offset = NOTE_HEADER_SIZE
        .checked_add(tags_payload.len())
        .ok_or_else(|| Error::InvalidBinary("note size overflow".to_string()))?;
    let strings_offset_u32 = usize_to_u32(strings_offset, "strings offset")?;

    let mut buffer = Vec::with_capacity(strings_offset + string_table.len() + 8);
    buffer.push(VERSION);
    buffer.extend_from_slice(&[0u8; 3]);
    buffer.extend_from_slice(&decode_hex_array::<32>(&event.id, "id")?);
    buffer.extend_from_slice(&decode_hex_array::<32>(&event.pubkey, "pubkey")?);
    buffer.extend_from_slice(&decode_hex_array::<64>(&event.sig, "sig")?);
    buffer.extend_from_slice(&event.created_at.to_le_bytes());
    let kind_bytes = event.kind.to_le_bytes();
    buffer.extend_from_slice(&kind_bytes);
    buffer.extend_from_slice(&content_length.to_le_bytes());
    buffer.extend_from_slice(&content_packed);
    buffer.extend_from_slice(&strings_offset_u32.to_le_bytes());
    buffer.extend_from_slice(&[0u8; 2]);
    buffer.extend_from_slice(&tags_count.to_le_bytes());

    debug_assert_eq!(buffer.len(), NOTE_HEADER_SIZE);

    buffer.extend_from_slice(&tags_payload);

    debug_assert_eq!(buffer.len(), strings_offset);

    buffer.extend_from_slice(&string_table);

    let padding = (8 - (buffer.len() % 8)) % 8;
    buffer.extend(std::iter::repeat_n(0u8, padding));

    Ok(buffer)
}

/// Restore an event JSON string from a packed `ndb_note` buffer.
pub fn from_ndb_note(bytes: &[u8]) -> Result<String> {
    let note = NdbNote::from_bytes(bytes)?;

    let id = hex::encode(note.id());
    let pubkey = hex::encode(note.pubkey());
    let sig = hex::encode(note.sig());
    let content = note.content_str().map(|value| value.to_owned())?;

    let mut tags_vec = Vec::with_capacity(note.tags().len());
    for tag in note.tags() {
        let mut values = Vec::with_capacity(tag.len());
        for value in tag.iter() {
            match value {
                NdbValue::Text(bytes) => {
                    let text = str::from_utf8(bytes).map_err(|source| Error::Utf8 {
                        field: "tag text",
                        source,
                    })?;
                    values.push(text.to_owned());
                }
                NdbValue::Id(bytes) => values.push(hex::encode(bytes)),
            }
        }
        tags_vec.push(values);
    }

    let note_json = NoteJson {
        id,
        pubkey,
        created_at: note.created_at(),
        kind: note.kind(),
        tags: tags_vec,
        content,
        sig,
    };

    serde_json::to_string(&note_json).map_err(|err| Error::InvalidJson(err.to_string()))
}

#[derive(Debug)]
enum TagValue {
    Text(String),
    Id([u8; 32]),
}

fn tag_to_values(tag: &[String]) -> Vec<TagValue> {
    tag.iter().cloned().map(TagValue::from_string).collect()
}

impl TagValue {
    fn from_string(value: String) -> Self {
        if value.len() == 64 {
            let mut id = [0u8; 32];
            if hex::decode_to_slice(value.as_str(), &mut id).is_ok() {
                return Self::Id(id);
            }
        }
        Self::Text(value)
    }
}

fn usize_to_u32(value: usize, field: &'static str) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::LengthOverflow { field, value })
}

fn pack_offset(offset: usize, flag: u8) -> Result<[u8; 4]> {
    if offset > PACKED_STR_MAX_OFFSET {
        return Err(Error::LengthOverflow {
            field: "string table offset",
            value: offset,
        });
    }

    let mut packed = [0u8; PACKED_STR_SIZE];
    packed[0] = (offset & 0xFF) as u8;
    packed[1] = ((offset >> 8) & 0xFF) as u8;
    packed[2] = ((offset >> 16) & 0xFF) as u8;
    packed[3] = flag;
    Ok(packed)
}

fn append_to_string_table(table: &mut Vec<u8>, bytes: &[u8], append_nul: bool) -> Result<usize> {
    let offset = table.len();
    if offset > PACKED_STR_MAX_OFFSET {
        return Err(Error::LengthOverflow {
            field: "string table size",
            value: offset,
        });
    }

    table.extend_from_slice(bytes);
    if append_nul {
        table.push(0);
    }

    if table.len() > MAX_STRING_TABLE_SIZE {
        return Err(Error::LengthOverflow {
            field: "string table size",
            value: table.len(),
        });
    }

    Ok(offset)
}

fn encode_text_value(bytes: &[u8], table: &mut Vec<u8>) -> Result<[u8; 4]> {
    if bytes.len() <= PACKED_STR_INLINE_CAP {
        let mut packed = [0u8; PACKED_STR_SIZE];
        packed[..bytes.len()].copy_from_slice(bytes);
        packed[PACKED_STR_SIZE - 1] = PACKED_STR_INLINE_FLAG;
        return Ok(packed);
    }

    let offset = append_to_string_table(table, bytes, true)?;
    pack_offset(offset, 0)
}

fn encode_id_value(id: &[u8; 32], table: &mut Vec<u8>) -> Result<[u8; 4]> {
    let offset = append_to_string_table(table, id, false)?;
    pack_offset(offset, PACKED_STR_ID_FLAG)
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    const SAMPLE_ID: &str = "1726bf37195e345ddc4bba9560d9499c918a544afbf72057a595d68fbe908ee5";
    const SAMPLE_PUBKEY: &str = "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
    const SAMPLE_SIG: &str = "62bc7e232a6ba074e6d360390566fda1baeccc438e3e65b75f6a7675ca250b1723e69a71ffd70185170331d5d95cb5333e8f7a51937ab2e188452c44fa9e91a5";

    fn sample_json() -> String {
        json!({
            "id": SAMPLE_ID,
            "pubkey": SAMPLE_PUBKEY,
            "sig": SAMPLE_SIG,
            "created_at": 1_758_755_707u64,
            "kind": 1u32,
            "content": "hello world",
            "tags": [
                ["t", "hello", "world"],
                ["client", "test"],
            ]
        })
        .to_string()
    }

    #[test]
    fn json_round_trip() {
        let json = sample_json();
        let bytes = to_ndb_note(&json).expect("encode");
        let decoded = from_ndb_note(&bytes).expect("decode");

        let original: serde_json::Value = serde_json::from_str(&json).unwrap();
        let recovered: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn binary_rejects_unknown_version() {
        let json = sample_json();
        let mut bytes = to_ndb_note(&json).expect("encode");
        bytes[0] = VERSION + 1;
        let err = from_ndb_note(&bytes).expect_err("expected error");
        match err {
            Error::InvalidBinary(message) => {
                assert!(message.contains("unsupported version"));
            }
            _ => panic!("unexpected error: {err}"),
        }
    }

    #[test]
    fn tag_id_round_trip() {
        let json = json!({
            "id": SAMPLE_ID,
            "pubkey": SAMPLE_PUBKEY,
            "sig": SAMPLE_SIG,
            "created_at": 1_758_755_707u64,
            "kind": 1u32,
            "content": "hello world",
            "tags": [[
                "p",
                "d4c5ad9f0d3749c19f1bff6a57d16889bd21245ffb391d4b2666ebadc177a64d"
            ]]
        })
        .to_string();

        let bytes = to_ndb_note(&json).expect("encode");
        let decoded = from_ndb_note(&bytes).expect("decode");
        let recovered: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        let tags = recovered["tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0][0], "p");
        assert_eq!(
            tags[0][1],
            "d4c5ad9f0d3749c19f1bff6a57d16889bd21245ffb391d4b2666ebadc177a64d"
        );
    }
}
