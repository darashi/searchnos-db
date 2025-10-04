use std::{convert::TryInto, str};

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) const VERSION: u8 = 1;
pub(crate) const NOTE_HEADER_SIZE: usize = 160;
pub(crate) const PACKED_STR_SIZE: usize = 4;
pub(crate) const PACKED_STR_INLINE_FLAG: u8 = 0x01;
pub(crate) const PACKED_STR_ID_FLAG: u8 = 0x02;
pub(crate) const PACKED_STR_INLINE_CAP: usize = 2;
pub(crate) const PACKED_STR_MAX_OFFSET: usize = 0x00FF_FFFF;
pub(crate) const MAX_STRING_TABLE_SIZE: usize = PACKED_STR_MAX_OFFSET + 1;

/// Errors surfaced by `ndb` helpers.
#[derive(Debug, Error)]
pub enum Error {
    #[error("value too large for {field}: {value}")]
    LengthOverflow { field: &'static str, value: usize },
    #[error("invalid binary data: {0}")]
    InvalidBinary(String),
    #[error("invalid UTF-8 for {field}: {source}")]
    Utf8 {
        field: &'static str,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("invalid event JSON: {0}")]
    InvalidJson(String),
    #[error("invalid hex for {field}: {source}")]
    Hex {
        field: &'static str,
        #[source]
        source: hex::FromHexError,
    },
}

/// Zero-copy view over a packed `ndb_note` buffer.
#[derive(Debug)]
pub struct NdbNote<'a> {
    raw: &'a [u8],
    version: u8,
    id: &'a [u8; 32],
    pubkey: &'a [u8; 32],
    sig: &'a [u8; 64],
    created_at: u64,
    kind: u32,
    content: &'a [u8],
    tags: Vec<NdbTag<'a>>,
}

impl<'a> NdbNote<'a> {
    /// Construct a zero-copy view from raw bytes.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < NOTE_HEADER_SIZE {
            return Err(Error::InvalidBinary("note shorter than header".to_string()));
        }

        let mut cursor = 0usize;

        let version = read_u8(bytes, &mut cursor, "version")?;
        if version != VERSION {
            return Err(Error::InvalidBinary(format!(
                "unsupported version: expected {VERSION} got {version}"
            )));
        }

        let padding = read_slice(bytes, &mut cursor, 3, "padding")?;
        if padding.iter().any(|&byte| byte != 0) {
            return Err(Error::InvalidBinary("non-zero padding".to_string()));
        }

        let id = read_array_ref::<32>(bytes, &mut cursor, "id")?;
        let pubkey = read_array_ref::<32>(bytes, &mut cursor, "pubkey")?;
        let sig = read_array_ref::<64>(bytes, &mut cursor, "sig")?;

        let created_at = read_u64(bytes, &mut cursor, "created_at")?;
        let kind = read_u32(bytes, &mut cursor, "kind")?;

        let content_length_u32 = read_u32(bytes, &mut cursor, "content length")?;
        let content_length = content_length_u32 as usize;
        let content_packed = read_array_ref::<4>(bytes, &mut cursor, "content packed string")?;

        let strings_offset_u32 = read_u32(bytes, &mut cursor, "strings offset")?;
        let strings_offset = strings_offset_u32 as usize;

        let tags_padding = read_u16(bytes, &mut cursor, "tags padding")?;
        if tags_padding != 0 {
            return Err(Error::InvalidBinary("non-zero tag padding".to_string()));
        }

        let tags_count_u16 = read_u16(bytes, &mut cursor, "tags count")?;
        let tags_count = tags_count_u16 as usize;

        if cursor != NOTE_HEADER_SIZE {
            return Err(Error::InvalidBinary("unexpected header size".to_string()));
        }

        if strings_offset < NOTE_HEADER_SIZE || strings_offset > bytes.len() {
            return Err(Error::InvalidBinary(
                "strings offset out of bounds".to_string(),
            ));
        }

        let strings_slice = &bytes[strings_offset..];
        let content = decode_content_slice(content_packed, strings_slice, content_length)?;

        let mut tag_cursor = cursor;
        let mut tags = Vec::with_capacity(tags_count);
        for _ in 0..tags_count {
            if tag_cursor + 2 > strings_offset {
                return Err(Error::InvalidBinary("truncated tag header".to_string()));
            }
            let mut header_cursor = tag_cursor;
            let value_count = read_u16(bytes, &mut header_cursor, "tag value count")? as usize;
            tag_cursor = header_cursor;

            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                if tag_cursor + PACKED_STR_SIZE > strings_offset {
                    return Err(Error::InvalidBinary("truncated tag value".to_string()));
                }
                let value_packed =
                    read_array_ref::<4>(bytes, &mut tag_cursor, "tag packed string")?;
                let value = decode_tag_value(value_packed, strings_slice)?;
                values.push(value);
            }

            tags.push(NdbTag { values });
        }

        if tag_cursor != strings_offset {
            return Err(Error::InvalidBinary(
                "tag region length mismatch".to_string(),
            ));
        }

        Ok(Self {
            raw: bytes,
            version,
            id,
            pubkey,
            sig,
            created_at,
            kind,
            content,
            tags,
        })
    }

    /// Return the raw buffer backing this note.
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// Packed format version.
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Event identifier bytes.
    pub fn id(&self) -> &'a [u8; 32] {
        self.id
    }

    /// Event author public key bytes.
    pub fn pubkey(&self) -> &'a [u8; 32] {
        self.pubkey
    }

    /// Schnorr signature bytes.
    pub fn sig(&self) -> &'a [u8; 64] {
        self.sig
    }

    /// Event creation timestamp (seconds since epoch).
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Event kind value.
    pub fn kind(&self) -> u32 {
        self.kind
    }

    /// Content bytes (UTF-8 but not validated here).
    pub fn content(&self) -> &'a [u8] {
        self.content
    }

    /// Content interpreted as UTF-8 string.
    pub fn content_str(&self) -> Result<&'a str> {
        str::from_utf8(self.content).map_err(|source| Error::Utf8 {
            field: "content",
            source,
        })
    }

    /// Tags included with this note.
    pub fn tags(&self) -> &[NdbTag<'a>] {
        &self.tags
    }
}

/// A tag recorded inside an `ndb_note`.
#[derive(Debug)]
pub struct NdbTag<'a> {
    values: Vec<NdbValue<'a>>,
}

impl<'a> NdbTag<'a> {
    /// Number of values carried by this tag.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the tag is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Borrow all values.
    pub fn values(&self) -> &[NdbValue<'a>] {
        &self.values
    }

    /// Iterate over values.
    pub fn iter(&self) -> impl Iterator<Item = NdbValue<'a>> + '_ {
        self.values.iter().copied()
    }
}

/// Type of value contained in an `ndb_note` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NdbValueKind {
    /// UTF-8 text.
    Text,
    /// 32-byte identifier stored in binary form.
    Id,
}

/// Zero-copy view of a tag value stored in an `ndb_note`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NdbValue<'a> {
    Text(&'a [u8]),
    Id(&'a [u8; 32]),
}

impl<'a> NdbValue<'a> {
    /// Returns the kind of this value.
    pub fn kind(&self) -> NdbValueKind {
        match self {
            Self::Text(_) => NdbValueKind::Text,
            Self::Id(_) => NdbValueKind::Id,
        }
    }

    /// Access the raw bytes for this value.
    pub fn as_bytes(&self) -> &'a [u8] {
        match self {
            Self::Text(bytes) => bytes,
            Self::Id(bytes) => &bytes[..],
        }
    }

    /// Access text bytes when this value is textual.
    pub fn as_text_bytes(&self) -> Option<&'a [u8]> {
        match self {
            Self::Text(bytes) => Some(bytes),
            Self::Id(_) => None,
        }
    }

    /// Access the 32-byte identifier when this value stores an ID.
    pub fn as_id_bytes(&self) -> Option<&'a [u8; 32]> {
        match self {
            Self::Text(_) => None,
            Self::Id(bytes) => Some(bytes),
        }
    }

    /// Interpret this value as UTF-8 text.
    pub fn as_str(&self) -> Result<&'a str> {
        match self {
            Self::Text(bytes) => str::from_utf8(bytes).map_err(|source| Error::Utf8 {
                field: "tag text",
                source,
            }),
            Self::Id(_) => Err(Error::InvalidBinary(
                "tag value is a binary identifier".to_string(),
            )),
        }
    }
}

pub(crate) fn decode_offset(packed: &[u8; 4]) -> usize {
    (packed[0] as usize) | ((packed[1] as usize) << 8) | ((packed[2] as usize) << 16)
}

pub(crate) fn inline_length(packed: &[u8; 4]) -> usize {
    packed[..PACKED_STR_SIZE - 1]
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(PACKED_STR_SIZE - 1)
}

pub(crate) fn read_c_string(strings: &[u8], offset: usize) -> Result<&[u8]> {
    if offset >= strings.len() {
        return Err(Error::InvalidBinary(
            "string offset out of bounds".to_string(),
        ));
    }

    let remaining = &strings[offset..];
    match remaining.iter().position(|&byte| byte == 0) {
        Some(len) => Ok(&remaining[..len]),
        None => Err(Error::InvalidBinary(
            "unterminated string in string table".to_string(),
        )),
    }
}

pub(crate) fn read_id_bytes(strings: &[u8], offset: usize) -> Result<&[u8; 32]> {
    let end = offset
        .checked_add(32)
        .ok_or_else(|| Error::InvalidBinary("id offset overflow".to_string()))?;
    if end > strings.len() {
        return Err(Error::InvalidBinary("id offset out of bounds".to_string()));
    }

    strings[offset..end]
        .try_into()
        .map_err(|_| Error::InvalidBinary("invalid id slice".to_string()))
}

fn decode_content_slice<'a>(
    packed: &'a [u8; 4],
    strings: &'a [u8],
    length: usize,
) -> Result<&'a [u8]> {
    match packed[3] {
        0 => {
            let offset = decode_offset(packed);
            let end = offset
                .checked_add(length)
                .ok_or_else(|| Error::InvalidBinary("content offset overflow".to_string()))?;
            if end > strings.len() {
                return Err(Error::InvalidBinary(
                    "content offset out of bounds".to_string(),
                ));
            }
            Ok(&strings[offset..end])
        }
        PACKED_STR_INLINE_FLAG => {
            if length > PACKED_STR_INLINE_CAP {
                return Err(Error::InvalidBinary(
                    "inline content exceeds packed capacity".to_string(),
                ));
            }
            if length > inline_length(packed) {
                return Err(Error::InvalidBinary(
                    "inline content shorter than declared length".to_string(),
                ));
            }
            Ok(&packed[..length])
        }
        PACKED_STR_ID_FLAG => Err(Error::InvalidBinary(
            "content stored as binary identifier".to_string(),
        )),
        other => Err(Error::InvalidBinary(format!(
            "unknown packed string flag: {other}",
        ))),
    }
}

fn decode_tag_value<'a>(packed: &'a [u8; 4], strings: &'a [u8]) -> Result<NdbValue<'a>> {
    match packed[3] {
        0 => {
            let offset = decode_offset(packed);
            let text = read_c_string(strings, offset)?;
            Ok(NdbValue::Text(text))
        }
        PACKED_STR_INLINE_FLAG => {
            let len = inline_length(packed);
            if len > PACKED_STR_INLINE_CAP {
                return Err(Error::InvalidBinary(
                    "inline tag value exceeds packed capacity".to_string(),
                ));
            }
            Ok(NdbValue::Text(&packed[..len]))
        }
        PACKED_STR_ID_FLAG => {
            let offset = decode_offset(packed);
            let id = read_id_bytes(strings, offset)?;
            Ok(NdbValue::Id(id))
        }
        other => Err(Error::InvalidBinary(format!(
            "unknown packed string flag: {other}",
        ))),
    }
}

fn read_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    field: &'static str,
) -> Result<&'a [u8]> {
    if bytes.len().saturating_sub(*cursor) < len {
        return Err(Error::InvalidBinary(format!(
            "unexpected end of input while reading {field}"
        )));
    }
    let start = *cursor;
    let end = start + len;
    *cursor = end;
    Ok(&bytes[start..end])
}

fn read_array_ref<'a, const N: usize>(
    bytes: &'a [u8],
    cursor: &mut usize,
    field: &'static str,
) -> Result<&'a [u8; N]> {
    let slice = read_slice(bytes, cursor, N, field)?;
    slice
        .try_into()
        .map_err(|_| Error::InvalidBinary(format!("invalid length for {field}")))
}

fn read_u8(bytes: &[u8], cursor: &mut usize, field: &'static str) -> Result<u8> {
    let slice = read_slice(bytes, cursor, 1, field)?;
    Ok(slice[0])
}

fn read_u16(bytes: &[u8], cursor: &mut usize, field: &'static str) -> Result<u16> {
    let slice = read_slice(bytes, cursor, 2, field)?;
    let mut buf = [0u8; 2];
    buf.copy_from_slice(slice);
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(bytes: &[u8], cursor: &mut usize, field: &'static str) -> Result<u32> {
    let slice = read_slice(bytes, cursor, 4, field)?;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(slice);
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(bytes: &[u8], cursor: &mut usize, field: &'static str) -> Result<u64> {
    let slice = read_slice(bytes, cursor, 8, field)?;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(slice);
    Ok(u64::from_le_bytes(buf))
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
    fn content_inline_encoding() {
        let json = json!({
            "id": SAMPLE_ID,
            "pubkey": SAMPLE_PUBKEY,
            "sig": SAMPLE_SIG,
            "created_at": 1_758_755_707u64,
            "kind": 1u32,
            "content": "hi",
            "tags": Vec::<Vec<String>>::new(),
        })
        .to_string();

        let bytes = crate::codec::to_ndb_note(&json).expect("encode");
        let content_len = u32::from_le_bytes(bytes[144..148].try_into().unwrap());
        assert_eq!(content_len, 2);

        let packed: [u8; 4] = bytes[148..152].try_into().unwrap();
        assert_eq!(packed[3], PACKED_STR_INLINE_FLAG);
        assert_eq!(&packed[..2], b"hi");

        let strings_offset = u32::from_le_bytes(bytes[152..156].try_into().unwrap()) as usize;
        assert_eq!(strings_offset, NOTE_HEADER_SIZE);
    }

    #[test]
    fn content_offset_encoding() {
        let json = sample_json();
        let content_bytes = b"hello world".to_vec();
        assert!(content_bytes.len() > PACKED_STR_INLINE_CAP);

        let bytes = crate::codec::to_ndb_note(&json).expect("encode");
        let packed: [u8; 4] = bytes[148..152].try_into().unwrap();
        assert_eq!(packed[3], 0);

        let strings_offset = u32::from_le_bytes(bytes[152..156].try_into().unwrap()) as usize;
        let strings_slice = &bytes[strings_offset..];
        let offset = decode_offset(&packed);

        let content_len = u32::from_le_bytes(bytes[144..148].try_into().unwrap()) as usize;
        assert_eq!(content_len, content_bytes.len());
        assert_eq!(
            &strings_slice[offset..offset + content_bytes.len()],
            &content_bytes
        );
        assert_eq!(strings_slice[offset + content_bytes.len()], 0);
    }

    #[test]
    fn tag_id_encoding_uses_binary_flag() {
        let json = json!({
            "id": SAMPLE_ID,
            "pubkey": SAMPLE_PUBKEY,
            "sig": SAMPLE_SIG,
            "created_at": 1_758_755_707u64,
            "kind": 1u32,
            "content": "hello world",
            "tags": [["p", "d4c5ad9f0d3749c19f1bff6a57d16889bd21245ffb391d4b2666ebadc177a64d"]]
        })
        .to_string();

        let bytes = crate::codec::to_ndb_note(&json).expect("encode");
        let strings_offset = u32::from_le_bytes(bytes[152..156].try_into().unwrap()) as usize;
        let mut cursor = NOTE_HEADER_SIZE;

        let tag_count = u16::from_le_bytes(bytes[158..160].try_into().unwrap());
        assert_eq!(tag_count, 1);

        let value_count = u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().unwrap());
        assert_eq!(value_count, 2);
        cursor += 2;

        let first_packed: [u8; 4] = bytes[cursor..cursor + 4].try_into().unwrap();
        assert_eq!(first_packed[3], PACKED_STR_INLINE_FLAG);
        assert_eq!(first_packed[0], b'p');
        cursor += 4;

        let second_packed: [u8; 4] = bytes[cursor..cursor + 4].try_into().unwrap();
        assert_eq!(second_packed[3], PACKED_STR_ID_FLAG);
        let offset = decode_offset(&second_packed);
        let strings_slice = &bytes[strings_offset..];
        let id_bytes = read_id_bytes(strings_slice, offset).expect("id slice");
        assert_eq!(
            hex::encode(id_bytes),
            "d4c5ad9f0d3749c19f1bff6a57d16889bd21245ffb391d4b2666ebadc177a64d"
        );
    }

    #[test]
    fn zero_copy_view_matches_json() {
        let json = sample_json();
        let bytes = crate::codec::to_ndb_note(&json).expect("encode");

        let note = NdbNote::from_bytes(&bytes).expect("parse note");

        assert_eq!(note.version(), VERSION);
        assert_eq!(
            note.id(),
            &crate::codec::decode_hex_array::<32>(SAMPLE_ID, "id").unwrap()
        );
        assert_eq!(
            note.pubkey(),
            &crate::codec::decode_hex_array::<32>(SAMPLE_PUBKEY, "pubkey").unwrap()
        );
        assert_eq!(
            note.sig(),
            &crate::codec::decode_hex_array::<64>(SAMPLE_SIG, "sig").unwrap()
        );
        assert_eq!(note.created_at(), 1_758_755_707);
        assert_eq!(note.kind(), 1);
        assert_eq!(note.content(), b"hello world");
        assert_eq!(note.content_str().expect("content utf8"), "hello world");

        let note_tags: Vec<Vec<String>> = note
            .tags()
            .iter()
            .map(|tag| {
                tag.iter()
                    .map(|value| match value {
                        NdbValue::Text(bytes) => {
                            str::from_utf8(bytes).expect("tag text utf8").to_string()
                        }
                        NdbValue::Id(bytes) => hex::encode(bytes),
                    })
                    .collect()
            })
            .collect();

        let expected_tags: Vec<Vec<String>> = json!([["t", "hello", "world"], ["client", "test"],])
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| {
                tag.as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap().to_string())
                    .collect()
            })
            .collect();

        assert_eq!(note_tags, expected_tags);
    }
}
