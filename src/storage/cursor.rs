use std::collections::BTreeSet;
use std::error::Error;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::nostr::Filter;

use super::event::{EventPacket, error_with_path, read_event_packet};
use super::query::{compare_packets, filter_has_search_terms};
use super::search::{SearchIndex, read_event_packet_at, read_search_index_for_events};
use super::text;
use super::visibility::{VisibilityIndex, VisibilityStore};

pub(crate) struct PacketCursor {
    source: PacketCursorSource,
    pending: Option<EventPacket>,
}

enum PacketCursorSource {
    Buffered {
        packets: Vec<EventPacket>,
        position: usize,
    },
    PartitionScan {
        file: File,
        filter: Filter,
        visibility: Option<VisibilityIndex>,
        visibility_store: Option<Arc<VisibilityStore>>,
    },
    PartitionSearch {
        file: File,
        index: SearchIndex,
        event_index: usize,
        search_terms: Vec<String>,
        filter: Filter,
        searchable_kinds: Option<Vec<u32>>,
        visibility: Option<VisibilityIndex>,
        visibility_store: Option<Arc<VisibilityStore>>,
    },
    Merged {
        cursors: Vec<PacketCursor>,
        limit: Option<usize>,
        seen: BTreeSet<[u8; 32]>,
        emitted: usize,
    },
}

impl PacketCursor {
    pub(crate) fn buffered(packets: Vec<EventPacket>) -> Option<Self> {
        if packets.is_empty() {
            None
        } else {
            Some(Self {
                source: PacketCursorSource::Buffered {
                    packets,
                    position: 0,
                },
                pending: None,
            })
        }
    }

    pub(crate) fn partition(
        path: &Path,
        filter: &Filter,
        searchable_kinds: Option<&[u32]>,
        visibility: Option<&VisibilityIndex>,
        visibility_store: Option<Arc<VisibilityStore>>,
    ) -> Result<Option<Self>, Box<dyn Error>> {
        if filter_has_search_terms(filter) {
            let index = read_search_index_for_events(path, searchable_kinds)
                .map_err(|err| error_with_path("read search index for events", path, err))?;
            let search_terms = filter
                .search
                .as_ref()
                .map_or_else(Vec::new, |query| text::search_terms(query));
            if search_terms.is_empty() || index.record_count() == 0 {
                return Ok(None);
            }
            let file = File::open(path).map_err(|err| error_with_path("open events", path, err))?;
            return Ok(Some(Self {
                source: PacketCursorSource::PartitionSearch {
                    file,
                    index,
                    event_index: 0,
                    search_terms,
                    filter: filter.clone(),
                    searchable_kinds: searchable_kinds.map(<[_]>::to_vec),
                    visibility: visibility.cloned(),
                    visibility_store,
                },
                pending: None,
            }));
        }

        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(error_with_path("open events", path, err)),
        };
        Ok(Some(Self {
            source: PacketCursorSource::PartitionScan {
                file,
                filter: filter.clone(),
                visibility: visibility.cloned(),
                visibility_store,
            },
            pending: None,
        }))
    }

    pub(crate) fn merged(cursors: Vec<PacketCursor>, limit: Option<usize>) -> Option<Self> {
        if cursors.is_empty() || limit == Some(0) {
            None
        } else {
            Some(Self {
                source: PacketCursorSource::Merged {
                    cursors,
                    limit,
                    seen: BTreeSet::new(),
                    emitted: 0,
                },
                pending: None,
            })
        }
    }

    fn peek(&mut self) -> Result<Option<&EventPacket>, Box<dyn Error>> {
        if self.pending.is_none() {
            self.pending = self.next_packet()?;
        }
        Ok(self.pending.as_ref())
    }

    pub(crate) fn pop(&mut self) -> Result<Option<EventPacket>, Box<dyn Error>> {
        if let Some(packet) = self.pending.take() {
            return Ok(Some(packet));
        }
        self.next_packet()
    }

    fn next_packet(&mut self) -> Result<Option<EventPacket>, Box<dyn Error>> {
        match &mut self.source {
            PacketCursorSource::Buffered { packets, position } => {
                let Some(packet) = packets.get(*position).cloned() else {
                    return Ok(None);
                };
                *position += 1;
                Ok(Some(packet))
            }
            PacketCursorSource::PartitionScan {
                file,
                filter,
                visibility,
                visibility_store,
            } => loop {
                let Some(packet) = read_event_packet(file)? else {
                    return Ok(None);
                };
                if !packet.matches_filter_without_search(filter)? {
                    continue;
                }
                if packet_is_visible(&packet, visibility.as_ref(), visibility_store.as_deref())? {
                    return Ok(Some(packet));
                }
            },
            PacketCursorSource::PartitionSearch {
                file,
                index,
                event_index,
                search_terms,
                filter,
                searchable_kinds,
                visibility,
                visibility_store,
            } => {
                while *event_index < index.record_count() {
                    let index_position = *event_index;
                    *event_index += 1;
                    if !index.record_matches_terms(index_position, search_terms) {
                        continue;
                    }
                    let packet = read_event_packet_at(file, index.record(index_position))?;
                    if !packet.matches_filter(filter, searchable_kinds.as_deref())? {
                        continue;
                    }
                    if packet_is_visible(&packet, visibility.as_ref(), visibility_store.as_deref())?
                    {
                        return Ok(Some(packet));
                    }
                }
                Ok(None)
            }
            PacketCursorSource::Merged {
                cursors,
                limit,
                seen,
                emitted,
            } => next_merged_packet(cursors, *limit, seen, emitted),
        }
    }
}

fn packet_is_visible(
    packet: &EventPacket,
    visibility: Option<&VisibilityIndex>,
    visibility_store: Option<&VisibilityStore>,
) -> Result<bool, Box<dyn Error>> {
    if let Some(visibility) = visibility
        && !visibility.is_visible(packet)?
    {
        return Ok(false);
    }
    if let Some(store) = visibility_store
        && !store.is_visible(packet)?
    {
        return Ok(false);
    }
    Ok(true)
}

fn next_merged_packet(
    cursors: &mut [PacketCursor],
    limit: Option<usize>,
    seen: &mut BTreeSet<[u8; 32]>,
    emitted: &mut usize,
) -> Result<Option<EventPacket>, Box<dyn Error>> {
    while limit.is_none_or(|limit| *emitted < limit) {
        let Some(cursor_index) = best_cursor_index(cursors)? else {
            return Ok(None);
        };
        let packet = cursors[cursor_index]
            .pop()?
            .expect("cursor selected from non-empty peek");
        if seen.insert(packet.id) {
            *emitted += 1;
            return Ok(Some(packet));
        }
    }
    Ok(None)
}

fn best_cursor_index(cursors: &mut [PacketCursor]) -> Result<Option<usize>, Box<dyn Error>> {
    let mut best: Option<usize> = None;
    for index in 0..cursors.len() {
        let Some(packet) = cursors[index].peek()?.cloned() else {
            continue;
        };
        if best.is_none_or(|best_index| {
            compare_packets(&packet, cursors[best_index].pending.as_ref().unwrap()).is_lt()
        }) {
            best = Some(index);
        }
    }
    Ok(best)
}

pub(crate) fn best_day_cursor_index(
    cursors: &mut [(usize, PacketCursor)],
) -> Result<Option<usize>, Box<dyn Error>> {
    let mut best: Option<usize> = None;
    for index in 0..cursors.len() {
        let Some(packet) = cursors[index].1.peek()?.cloned() else {
            continue;
        };
        if best.is_none_or(|best_index| {
            compare_packets(&packet, cursors[best_index].1.pending.as_ref().unwrap()).is_lt()
        }) {
            best = Some(index);
        }
    }
    Ok(best)
}
