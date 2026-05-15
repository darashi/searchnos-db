use std::collections::BTreeSet;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memchr::memmem::Finder;
use memmap2::Mmap;

use crate::nostr::Filter;

use super::text;
use super::{EventPacket, FNV_OFFSET_BASIS, FNV_PRIME, SEARCH_INDEX_MAGIC};

const SEARCH_INDEX_RECORD_BYTES: usize = size_of::<u64>() + size_of::<u32>();
const SEARCH_BLOOM_BYTES: usize = 256 * 1024;
const SEARCH_BLOOM_BITS: u64 = (SEARCH_BLOOM_BYTES as u64) * 8;
const SEARCH_BLOOM_HASHES: u64 = 7;

#[derive(Clone)]
pub(crate) struct SearchIndex {
    fingerprint: EventsFingerprint,
    searchable_kinds_hash: u64,
    records: SearchIndexRecords,
    offsets: SearchIndexOffsets,
    bloom: SearchBloom,
    text: SearchIndexText,
}

impl SearchIndex {
    pub(crate) fn record(&self, index: usize) -> SearchIndexRecord {
        self.records.get(index)
    }

    pub(crate) fn record_count(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn record_matches_terms(&self, index: usize, terms: &[String]) -> bool {
        let start = self.offsets.get(index) as usize;
        let end = self.offsets.get(index + 1) as usize;
        let text = &self.text.as_slice()[start..end];
        terms.iter().all(|term| {
            let term = term.as_bytes();
            text.windows(term.len()).any(|window| window == term)
        })
    }
}

#[derive(Clone)]
enum SearchIndexRecords {
    Owned(Vec<SearchIndexRecord>),
    Mapped {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl SearchIndexRecords {
    fn empty() -> Self {
        Self::Owned(Vec::new())
    }

    fn push(&mut self, record: SearchIndexRecord) {
        match self {
            Self::Owned(records) => records.push(record),
            Self::Mapped { .. } => unreachable!("mapped search records are read-only"),
        }
    }

    fn get(&self, index: usize) -> SearchIndexRecord {
        match self {
            Self::Owned(records) => records[index],
            Self::Mapped { mmap, offset, len } => {
                assert!(index < *len);
                let offset = offset + index * SEARCH_INDEX_RECORD_BYTES;
                SearchIndexRecord {
                    packet_offset: read_u64_at(mmap, offset),
                    packet_len: read_u32_at(mmap, offset + size_of::<u64>()),
                }
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Owned(records) => records.len(),
            Self::Mapped { len, .. } => *len,
        }
    }

    fn as_slice(&self) -> &[SearchIndexRecord] {
        match self {
            Self::Owned(records) => records,
            Self::Mapped { .. } => unreachable!("mapped search records are not contiguous structs"),
        }
    }
}

#[derive(Clone)]
enum SearchIndexOffsets {
    Owned(Vec<u64>),
    Mapped {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl SearchIndexOffsets {
    fn with_zero() -> Self {
        Self::Owned(vec![0])
    }

    fn push(&mut self, offset: u64) {
        match self {
            Self::Owned(offsets) => offsets.push(offset),
            Self::Mapped { .. } => unreachable!("mapped search offsets are read-only"),
        }
    }

    fn get(&self, index: usize) -> u64 {
        match self {
            Self::Owned(offsets) => offsets[index],
            Self::Mapped { mmap, offset, len } => {
                assert!(index < *len);
                read_u64_at(mmap, offset + index * size_of::<u64>())
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Owned(offsets) => offsets.len(),
            Self::Mapped { len, .. } => *len,
        }
    }

    fn last(&self) -> Option<u64> {
        self.len().checked_sub(1).map(|index| self.get(index))
    }

    fn as_slice(&self) -> &[u64] {
        match self {
            Self::Owned(offsets) => offsets,
            Self::Mapped { .. } => unreachable!("mapped search offsets are not contiguous u64s"),
        }
    }

    fn is_monotonic(&self) -> bool {
        let mut previous = None;
        for index in 0..self.len() {
            let current = self.get(index);
            if let Some(previous) = previous
                && previous > current
            {
                return false;
            }
            previous = Some(current);
        }
        true
    }
}

#[derive(Clone)]
enum SearchIndexText {
    Owned(Vec<u8>),
    Mapped { mmap: Arc<Mmap>, offset: usize },
}

impl SearchIndexText {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(text) => text,
            Self::Mapped { mmap, offset } => &mmap[*offset..],
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        match self {
            Self::Owned(text) => text.extend_from_slice(bytes),
            Self::Mapped { .. } => unreachable!("mapped search text is read-only"),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SearchIndexRecord {
    packet_offset: u64,
    packet_len: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EventsFingerprint {
    pub(crate) count: u64,
    pub(crate) bytes: u64,
    pub(crate) hash: u64,
}

pub(crate) fn search_index_path(events_path: &Path) -> PathBuf {
    events_path.with_extension("search")
}

pub(crate) fn remove_file_if_exists(path: impl AsRef<Path>) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

pub(crate) fn empty_search_index(searchable_kinds: Option<&[u32]>) -> SearchIndex {
    SearchIndex {
        fingerprint: EventsFingerprint {
            count: 0,
            bytes: 0,
            hash: FNV_OFFSET_BASIS,
        },
        searchable_kinds_hash: text::searchable_kinds_hash(searchable_kinds),
        records: SearchIndexRecords::empty(),
        offsets: SearchIndexOffsets::with_zero(),
        bloom: SearchBloom::empty(),
        text: SearchIndexText::Owned(Vec::new()),
    }
}

pub(crate) fn search_candidate_indexes(
    search_index: Option<&SearchIndex>,
    filter: &Filter,
) -> Result<Option<BTreeSet<usize>>, Box<dyn Error>> {
    let Some(query) = &filter.search else {
        return Ok(None);
    };
    let terms = text::search_terms(query);
    if terms.is_empty() {
        return Ok(None);
    }

    let Some(index) = search_index else {
        return Err("missing search index".into());
    };

    let mut matched = BTreeSet::new();
    for (term_index, term) in terms.iter().enumerate() {
        let term_matches = search_index_term_matches(index, term.as_bytes());
        if term_index == 0 {
            matched = term_matches;
        } else {
            matched = matched.intersection(&term_matches).copied().collect();
        }

        if matched.is_empty() {
            break;
        }
    }

    Ok(Some(matched))
}

pub(crate) fn append_to_search_index(
    index: &mut SearchIndex,
    data: &[u8],
    searchable_kinds: Option<&[u32]>,
) -> Result<(), Box<dyn Error>> {
    let text = text::normalized_search_text(data, searchable_kinds)?;
    index.records.push(SearchIndexRecord {
        packet_offset: index.fingerprint.bytes,
        packet_len: data.len().try_into()?,
    });
    if let SearchBloom::Owned(bits) = &mut index.bloom {
        for gram in search_bloom_text_grams(&text) {
            insert_bloom_gram(bits, gram.as_bytes());
        }
    }
    index.text.extend_from_slice(&text);
    index.offsets.push(index.text.len() as u64);
    index.fingerprint.count += 1;
    index.fingerprint.bytes += size_of::<u32>() as u64 + data.len() as u64;
    update_hash(
        &mut index.fingerprint.hash,
        &(data.len() as u32).to_le_bytes(),
    );
    update_hash(&mut index.fingerprint.hash, data);
    Ok(())
}

pub(crate) fn build_search_index(
    packets: &[EventPacket],
    searchable_kinds: Option<&[u32]>,
) -> Result<SearchIndex, Box<dyn Error>> {
    let mut records = SearchIndexRecords::Owned(Vec::with_capacity(packets.len()));
    let mut offsets = SearchIndexOffsets::Owned(Vec::with_capacity(packets.len() + 1));
    let mut text = Vec::new();
    let mut packet_offset = 0;
    offsets.push(0);
    for packet in packets {
        records.push(SearchIndexRecord {
            packet_offset,
            packet_len: packet.data.len().try_into()?,
        });
        packet_offset += size_of::<u32>() as u64 + packet.data.len() as u64;
        text.extend_from_slice(&text::normalized_search_text(
            &packet.data,
            searchable_kinds,
        )?);
        offsets.push(text.len() as u64);
    }
    Ok(SearchIndex {
        fingerprint: events_fingerprint(packets),
        searchable_kinds_hash: text::searchable_kinds_hash(searchable_kinds),
        records,
        offsets,
        bloom: SearchBloom::Owned(search_bloom_bits(&text)),
        text: SearchIndexText::Owned(text),
    })
}

pub(crate) fn write_search_index_atomic(
    path: &Path,
    packets: &[EventPacket],
    searchable_kinds: Option<&[u32]>,
) -> Result<(), Box<dyn Error>> {
    let tmp_path = tmp_search_index_path(path);
    let index = build_search_index(packets, searchable_kinds)?;
    match write_built_search_index(&tmp_path, &index) {
        Ok(()) => {}
        Err(err) => {
            let _ = remove_file_if_exists(&tmp_path);
            return Err(err);
        }
    }
    fs::rename(&tmp_path, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

pub(crate) fn write_built_search_index(
    path: &Path,
    index: &SearchIndex,
) -> Result<(), Box<dyn Error>> {
    write_search_index_parts(
        path,
        index.fingerprint,
        index.searchable_kinds_hash,
        index.records.as_slice(),
        index.offsets.as_slice(),
        index.bloom.as_slice(),
        index.text.as_slice(),
    )
}

pub(crate) fn read_search_index_for_events(
    events_path: &Path,
    searchable_kinds: Option<&[u32]>,
) -> Result<SearchIndex, Box<dyn Error>> {
    let expected_searchable_kinds_hash = text::searchable_kinds_hash(searchable_kinds);
    let search_path = search_index_path(events_path);

    read_search_index_trusted(events_path, &search_path, expected_searchable_kinds_hash)
}

pub(crate) fn search_sidecar_is_current(
    events_path: &Path,
    searchable_kinds: Option<&[u32]>,
) -> Result<bool, Box<dyn Error>> {
    match fs::metadata(search_index_path(events_path)) {
        Ok(_) => Ok(read_search_index_for_events(events_path, searchable_kinds).is_ok()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn search_bloom_may_match(
    events_path: &Path,
    filter: &Filter,
    searchable_kinds: Option<&[u32]>,
) -> Result<bool, Box<dyn Error>> {
    let Some(query) = &filter.search else {
        return Ok(true);
    };
    let grams = search_bloom_query_grams(query);
    if grams.is_empty() {
        return Ok(true);
    }

    let expected_searchable_kinds_hash = text::searchable_kinds_hash(searchable_kinds);
    let path = search_index_path(events_path);
    let bloom = read_search_bloom_trusted(events_path, &path, expected_searchable_kinds_hash)?;

    Ok(grams.iter().all(|gram| bloom.contains(gram.as_bytes())))
}

#[cfg(test)]
pub(crate) fn read_search_index(
    path: &Path,
    expected: EventsFingerprint,
    expected_searchable_kinds_hash: u64,
) -> Result<SearchIndex, Box<dyn Error>> {
    read_search_index_inner(path, Some(expected), expected_searchable_kinds_hash)
}

fn read_search_index_trusted(
    events_path: &Path,
    path: &Path,
    expected_searchable_kinds_hash: u64,
) -> Result<SearchIndex, Box<dyn Error>> {
    let index = read_search_index_inner(path, None, expected_searchable_kinds_hash)?;
    if index.fingerprint.bytes != std::fs::metadata(events_path)?.len() {
        return Err("search index packet length mismatch".into());
    }
    Ok(index)
}

fn read_search_bloom_trusted(
    events_path: &Path,
    path: &Path,
    expected_searchable_kinds_hash: u64,
) -> Result<SearchBloom, Box<dyn Error>> {
    let file = File::open(path)?;
    // The mapping is read-only and only the fixed-size Bloom section is used.
    let mmap = Arc::new(unsafe { Mmap::map(&file)? });
    let mut position = 0;

    if read_bytes(&mmap, &mut position, SEARCH_INDEX_MAGIC.len())? != SEARCH_INDEX_MAGIC {
        return Err("invalid search index magic".into());
    }

    let count = read_u64_from(&mmap, &mut position)? as usize;
    let bytes = read_u64_from(&mmap, &mut position)?;
    read_u64_from(&mmap, &mut position)?;
    let searchable_kinds_hash = read_u64_from(&mmap, &mut position)?;

    if bytes != std::fs::metadata(events_path)?.len() {
        return Err("search index packet length mismatch".into());
    }
    if searchable_kinds_hash != expected_searchable_kinds_hash {
        return Err("search index searchable kinds mismatch".into());
    }

    position = checked_advance(position, count, SEARCH_INDEX_RECORD_BYTES)?;
    position = checked_advance(position, count + 1, size_of::<u64>())?;
    let bloom_offset = position;
    position = checked_advance(position, SEARCH_BLOOM_BYTES, 1)?;
    read_bytes(&mmap, &mut position, 0)?;

    Ok(SearchBloom::Mapped {
        mmap,
        offset: bloom_offset,
    })
}

fn read_search_index_inner(
    path: &Path,
    expected: Option<EventsFingerprint>,
    expected_searchable_kinds_hash: u64,
) -> Result<SearchIndex, Box<dyn Error>> {
    let file = File::open(path)?;
    // The mapping is read-only and the index is validated before it is used.
    let mmap = Arc::new(unsafe { Mmap::map(&file)? });
    let mut position = 0;

    if read_bytes(&mmap, &mut position, SEARCH_INDEX_MAGIC.len())? != SEARCH_INDEX_MAGIC {
        return Err("invalid search index magic".into());
    }

    let count = read_u64_from(&mmap, &mut position)? as usize;
    let bytes = read_u64_from(&mmap, &mut position)?;
    let hash = read_u64_from(&mmap, &mut position)?;
    let searchable_kinds_hash = read_u64_from(&mmap, &mut position)?;
    let fingerprint = EventsFingerprint {
        count: count as u64,
        bytes,
        hash,
    };

    let records_offset = position;
    position = checked_advance(position, count, SEARCH_INDEX_RECORD_BYTES)?;
    read_bytes(&mmap, &mut position, 0)?;

    let offsets_offset = position;
    position = checked_advance(position, count + 1, size_of::<u64>())?;
    read_bytes(&mmap, &mut position, 0)?;
    let bloom_offset = position;
    position = checked_advance(position, SEARCH_BLOOM_BYTES, 1)?;
    read_bytes(&mmap, &mut position, 0)?;
    let text_len = mmap.len().saturating_sub(position);
    let records = SearchIndexRecords::Mapped {
        mmap: Arc::clone(&mmap),
        offset: records_offset,
        len: count,
    };
    let offsets = SearchIndexOffsets::Mapped {
        mmap: Arc::clone(&mmap),
        offset: offsets_offset,
        len: count + 1,
    };

    if offsets.len() == 0 || offsets.get(0) != 0 || offsets.last() != Some(text_len as u64) {
        return Err("invalid search index offsets".into());
    }
    if !offsets.is_monotonic() {
        return Err("non-monotonic search index offsets".into());
    }
    if expected.is_some_and(|expected| fingerprint != expected) {
        return Err("search index fingerprint mismatch".into());
    }
    if searchable_kinds_hash != expected_searchable_kinds_hash {
        return Err("search index searchable kinds mismatch".into());
    }
    validate_search_index_records(&records, fingerprint.bytes)?;

    Ok(SearchIndex {
        fingerprint,
        searchable_kinds_hash,
        records,
        offsets,
        bloom: SearchBloom::Mapped {
            mmap: Arc::clone(&mmap),
            offset: bloom_offset,
        },
        text: SearchIndexText::Mapped {
            mmap,
            offset: position,
        },
    })
}

pub(crate) fn events_fingerprint(packets: &[EventPacket]) -> EventsFingerprint {
    let mut fingerprint = EventsFingerprint {
        count: packets.len() as u64,
        bytes: 0,
        hash: FNV_OFFSET_BASIS,
    };

    for packet in packets {
        let len = packet.data.len() as u32;
        fingerprint.bytes += size_of::<u32>() as u64 + packet.data.len() as u64;
        update_hash(&mut fingerprint.hash, &len.to_le_bytes());
        update_hash(&mut fingerprint.hash, &packet.data);
    }

    fingerprint
}

pub(crate) fn read_event_packet_at(
    file: &mut File,
    record: SearchIndexRecord,
) -> Result<EventPacket, Box<dyn Error>> {
    file.seek(SeekFrom::Start(record.packet_offset))?;

    let len = read_u32(file)?;
    if len != record.packet_len {
        return Err("search index packet length does not match events file".into());
    }

    let mut data = vec![0; len as usize];
    file.read_exact(&mut data)?;

    EventPacket::from_data(data)
}

fn search_index_term_matches(index: &SearchIndex, term: &[u8]) -> BTreeSet<usize> {
    if term.is_empty() {
        return (0..index.offsets.len().saturating_sub(1)).collect();
    }

    let finder = Finder::new(term);
    let mut matches = BTreeSet::new();
    for position in finder.find_iter(index.text.as_slice()) {
        if let Some(event_index) = search_index_event_at(&index.offsets, position as u64)
            && (position + term.len()) as u64 <= index.offsets.get(event_index + 1)
        {
            matches.insert(event_index);
        }
    }
    matches
}

fn search_index_event_at(offsets: &SearchIndexOffsets, position: u64) -> Option<usize> {
    if offsets.len() < 2 {
        return None;
    }

    let mut low = 0;
    let mut high = offsets.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if offsets.get(middle) <= position {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    let insertion = low;
    let index = insertion.checked_sub(1)?;
    if index + 1 < offsets.len() {
        Some(index)
    } else {
        None
    }
}

fn write_search_index_parts(
    path: &Path,
    fingerprint: EventsFingerprint,
    searchable_kinds_hash: u64,
    records: &[SearchIndexRecord],
    offsets: &[u64],
    bloom: &[u8],
    text: &[u8],
) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    file.write_all(SEARCH_INDEX_MAGIC)?;
    file.write_all(&fingerprint.count.to_le_bytes())?;
    file.write_all(&fingerprint.bytes.to_le_bytes())?;
    file.write_all(&fingerprint.hash.to_le_bytes())?;
    file.write_all(&searchable_kinds_hash.to_le_bytes())?;
    for record in records {
        file.write_all(&record.packet_offset.to_le_bytes())?;
        file.write_all(&record.packet_len.to_le_bytes())?;
    }
    for offset in offsets {
        file.write_all(&offset.to_le_bytes())?;
    }
    file.write_all(bloom)?;
    file.write_all(text)?;
    file.sync_all()?;
    Ok(())
}

fn tmp_search_index_path(path: &Path) -> PathBuf {
    let mut tmp_path = path.to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".tmp");
            name
        })
        .unwrap_or_else(|| "search.tmp".into());
    tmp_path.set_file_name(file_name);
    tmp_path
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) => File::open(parent)?.sync_all(),
        None => Ok(()),
    }
}

#[derive(Clone)]
enum SearchBloom {
    Owned(Vec<u8>),
    Mapped { mmap: Arc<Mmap>, offset: usize },
}

impl SearchBloom {
    fn empty() -> Self {
        Self::Owned(vec![0; SEARCH_BLOOM_BYTES])
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bits) => bits,
            Self::Mapped { mmap, offset } => &mmap[*offset..*offset + SEARCH_BLOOM_BYTES],
        }
    }

    fn contains(&self, gram: &[u8]) -> bool {
        let bits = self.as_slice();
        let (hash1, hash2) = bloom_hashes(gram);
        (0..SEARCH_BLOOM_HASHES).all(|index| {
            let bit = hash1.wrapping_add(index.wrapping_mul(hash2)) % SEARCH_BLOOM_BITS;
            bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0
        })
    }
}

fn search_bloom_bits(text: &[u8]) -> Vec<u8> {
    let mut bits = vec![0; SEARCH_BLOOM_BYTES];
    for gram in search_bloom_text_grams(text) {
        insert_bloom_gram(&mut bits, gram.as_bytes());
    }
    bits
}

fn insert_bloom_gram(bits: &mut [u8], gram: &[u8]) {
    let (hash1, hash2) = bloom_hashes(gram);
    for index in 0..SEARCH_BLOOM_HASHES {
        let bit = hash1.wrapping_add(index.wrapping_mul(hash2)) % SEARCH_BLOOM_BITS;
        bits[(bit / 8) as usize] |= 1 << (bit % 8);
    }
}

fn bloom_hashes(bytes: &[u8]) -> (u64, u64) {
    let mut hash1 = FNV_OFFSET_BASIS;
    update_hash(&mut hash1, bytes);

    let mut hash2 = FNV_OFFSET_BASIS ^ 0x9e37_79b9_7f4a_7c15;
    for byte in bytes.iter().rev() {
        hash2 ^= u64::from(*byte);
        hash2 = hash2.wrapping_mul(FNV_PRIME);
    }
    (hash1, hash2 | 1)
}

fn search_bloom_query_grams(query: &str) -> Vec<String> {
    let mut grams = Vec::new();
    for term in text::search_terms(query) {
        append_search_bloom_grams(&term, &mut grams);
    }
    grams.sort();
    grams.dedup();
    grams
}

fn search_bloom_text_grams(text: &[u8]) -> Vec<String> {
    let Ok(text) = std::str::from_utf8(text) else {
        return Vec::new();
    };
    let mut grams = Vec::new();
    for term in text.split(' ').filter(|term| !term.is_empty()) {
        append_search_bloom_grams(term, &mut grams);
    }
    grams
}

fn append_search_bloom_grams(term: &str, grams: &mut Vec<String>) {
    let chars = term.chars().collect::<Vec<_>>();
    if chars.is_empty() {
        return;
    }
    if chars.len() == 1 {
        grams.push(chars[0].to_string());
        return;
    }
    for window in chars.windows(2) {
        grams.push(window.iter().collect());
    }
    for window in chars.windows(3) {
        grams.push(window.iter().collect());
    }
}

fn validate_search_index_records(
    records: &SearchIndexRecords,
    events_bytes: u64,
) -> Result<(), Box<dyn Error>> {
    let mut expected_offset = 0;
    for index in 0..records.len() {
        let record = records.get(index);
        if record.packet_offset != expected_offset {
            return Err("search index packet offset mismatch".into());
        }
        expected_offset += size_of::<u32>() as u64 + u64::from(record.packet_len);
    }
    if expected_offset != events_bytes {
        return Err("search index packet length mismatch".into());
    }
    Ok(())
}

fn read_u32(file: &mut File) -> io::Result<u32> {
    let mut bytes = [0; size_of::<u32>()];
    file.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_bytes<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    len: usize,
) -> Result<&'a [u8], Box<dyn Error>> {
    let end = position
        .checked_add(len)
        .ok_or("search index position overflow")?;
    let Some(slice) = bytes.get(*position..end) else {
        return Err("truncated search index".into());
    };
    *position = end;
    Ok(slice)
}

fn checked_advance(
    position: usize,
    count: usize,
    item_size: usize,
) -> Result<usize, Box<dyn Error>> {
    let bytes = count
        .checked_mul(item_size)
        .ok_or("search index section size overflow")?;
    position
        .checked_add(bytes)
        .ok_or_else(|| "search index position overflow".into())
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    let mut value = [0; size_of::<u64>()];
    value.copy_from_slice(
        bytes
            .get(offset..offset + size_of::<u64>())
            .expect("validated search index u64 is in bounds"),
    );
    u64::from_le_bytes(value)
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    let mut value = [0; size_of::<u32>()];
    value.copy_from_slice(
        bytes
            .get(offset..offset + size_of::<u32>())
            .expect("validated search index u32 is in bounds"),
    );
    u32::from_le_bytes(value)
}

fn read_u64_from(bytes: &[u8], position: &mut usize) -> Result<u64, Box<dyn Error>> {
    let mut value = [0; size_of::<u64>()];
    value.copy_from_slice(read_bytes(bytes, position, size_of::<u64>())?);
    Ok(u64::from_le_bytes(value))
}

fn update_hash(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}
