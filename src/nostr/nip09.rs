use super::nip01::event::tag::Tag;
use super::{event_tag, nip01::EventId};

#[derive(Debug, Clone, Default)]
pub struct EventDeletionRequest {
    ids: Vec<EventId>,
    reason: Option<String>,
}

impl EventDeletionRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: EventId) -> Self {
        self.ids.push(id);
        self
    }

    pub fn ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = EventId>,
    {
        self.ids.extend(ids);
        self
    }

    pub fn into_components(self) -> (Vec<Tag>, Option<String>) {
        let mut tags = Vec::with_capacity(self.ids.len());
        for id in self.ids {
            tags.push(event_tag(id));
        }
        (tags, self.reason)
    }
}
