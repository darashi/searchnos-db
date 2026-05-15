use std::error::Error;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use ndb::NdbNote;

use crate::nostr::Filter;

use super::{SECONDS_PER_DAY, text};

#[derive(Clone)]
pub(crate) struct EventPacket {
    pub(crate) created_at: u64,
    pub(crate) id: [u8; 32],
    pub(crate) pubkey: [u8; 32],
    pub(crate) kind: u32,
    pub(crate) data: Vec<u8>,
}

impl EventPacket {
    pub(crate) fn from_data(data: Vec<u8>) -> Result<Self, Box<dyn Error>> {
        let note = NdbNote::from_bytes(&data)?;
        Ok(Self {
            created_at: note.created_at(),
            id: *note.id(),
            pubkey: *note.pubkey(),
            kind: note.kind(),
            data,
        })
    }

    pub(crate) fn unix_day(&self) -> u64 {
        self.created_at / SECONDS_PER_DAY
    }

    pub(crate) fn matches_filter_without_search(
        &self,
        filter: &Filter,
    ) -> Result<bool, Box<dyn Error>> {
        let note = NdbNote::from_bytes(&self.data)?;

        Ok(matches_ids(filter, &note)
            && matches_authors(filter, &note)
            && matches_kinds(filter, &note)
            && matches_since(filter, &note)
            && matches_until(filter, &note)
            && matches_tags(filter, &note)?)
    }

    pub(crate) fn matches_filter(
        &self,
        filter: &Filter,
        searchable_kinds: Option<&[u32]>,
    ) -> Result<bool, Box<dyn Error>> {
        Ok(self.matches_filter_without_search(filter)?
            && matches_search(filter, &self.data, searchable_kinds)?)
    }
}

fn matches_ids(filter: &Filter, note: &NdbNote<'_>) -> bool {
    filter
        .ids
        .as_ref()
        .is_none_or(|ids| ids.is_empty() || ids.iter().any(|id| id.as_bytes() == note.id()))
}

fn matches_authors(filter: &Filter, note: &NdbNote<'_>) -> bool {
    filter.authors.as_ref().is_none_or(|authors| {
        authors.is_empty()
            || authors
                .iter()
                .any(|author| author.as_bytes() == note.pubkey())
    })
}

fn matches_kinds(filter: &Filter, note: &NdbNote<'_>) -> bool {
    filter.kinds.as_ref().is_none_or(|kinds| {
        kinds.is_empty() || kinds.iter().any(|kind| kind.as_u32() == note.kind())
    })
}

fn matches_since(filter: &Filter, note: &NdbNote<'_>) -> bool {
    filter
        .since
        .is_none_or(|since| note.created_at() >= since.as_u64())
}

fn matches_until(filter: &Filter, note: &NdbNote<'_>) -> bool {
    filter
        .until
        .is_none_or(|until| note.created_at() <= until.as_u64())
}

fn matches_tags(filter: &Filter, note: &NdbNote<'_>) -> Result<bool, Box<dyn Error>> {
    for (tag_name, values) in &filter.generic_tags {
        if values.is_empty() {
            continue;
        }

        let mut found = false;
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
            if !tag_name.is_ascii() || name.as_bytes() != [*tag_name as u8] {
                continue;
            }

            let Some(value) = elements.next() else {
                continue;
            };
            if values.contains(&text::tag_element_to_string(value?)?) {
                found = true;
                break;
            }
        }

        if !found {
            return Ok(false);
        }
    }

    Ok(true)
}

fn matches_search(
    filter: &Filter,
    data: &[u8],
    searchable_kinds: Option<&[u32]>,
) -> Result<bool, Box<dyn Error>> {
    let Some(search) = filter.search.as_ref() else {
        return Ok(true);
    };
    let terms = text::search_terms(search);
    if terms.is_empty() {
        return Ok(true);
    }

    let text = text::normalized_search_text(data, searchable_kinds)?;
    Ok(terms.iter().all(|term| {
        text.windows(term.len())
            .any(|window| window == term.as_bytes())
    }))
}

pub(crate) fn read_event_packets_from_path(
    path: &Path,
) -> Result<Vec<EventPacket>, Box<dyn Error>> {
    match File::open(path) {
        Ok(mut file) => {
            read_event_packets(&mut file).map_err(|err| error_with_path("read events", path, err))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(error_with_path("open events", path, err)),
    }
}

pub(crate) fn error_with_path(
    action: &'static str,
    path: &Path,
    err: impl Into<Box<dyn Error>>,
) -> Box<dyn Error> {
    io::Error::other(format!("{action} {}: {}", path.display(), err.into())).into()
}

pub(crate) fn read_event_packets(file: &mut File) -> Result<Vec<EventPacket>, Box<dyn Error>> {
    file.seek(SeekFrom::Start(0))?;

    let mut packets = Vec::new();
    while let Some(packet) = read_event_packet(file)? {
        packets.push(packet);
    }
    Ok(packets)
}

pub(crate) fn read_event_packet(file: &mut File) -> Result<Option<EventPacket>, Box<dyn Error>> {
    let mut len_bytes = [0; size_of::<u32>()];
    match file.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }

    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut data = vec![0; len];
    file.read_exact(&mut data)?;

    Ok(Some(EventPacket::from_data(data)?))
}

pub(crate) fn write_packet(file: &mut File, data: &[u8]) -> io::Result<()> {
    let len = u32::try_from(data.len())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(data)
}
