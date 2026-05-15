use std::error::Error;
use std::io;
use std::sync::{Condvar, Mutex};

pub(crate) struct SidecarUpdateQueue {
    state: Mutex<SidecarUpdateState>,
    available: Condvar,
}

#[derive(Default)]
struct SidecarUpdateState {
    active: bool,
    pending_compactions: u64,
}

enum SidecarUpdateKind {
    Compaction,
    Reindex,
}

pub(crate) struct SidecarUpdateGuard<'a> {
    queue: &'a SidecarUpdateQueue,
}

impl SidecarUpdateQueue {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SidecarUpdateState::default()),
            available: Condvar::new(),
        }
    }

    pub(crate) fn acquire_compaction(&self) -> Result<SidecarUpdateGuard<'_>, Box<dyn Error>> {
        self.acquire(SidecarUpdateKind::Compaction)
    }

    pub(crate) fn acquire_reindex(&self) -> Result<SidecarUpdateGuard<'_>, Box<dyn Error>> {
        self.acquire(SidecarUpdateKind::Reindex)
    }

    fn acquire(&self, kind: SidecarUpdateKind) -> Result<SidecarUpdateGuard<'_>, Box<dyn Error>> {
        let mut state = self
            .state
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?;
        if matches!(kind, SidecarUpdateKind::Compaction) {
            state.pending_compactions += 1;
        }

        while state.active
            || (matches!(kind, SidecarUpdateKind::Reindex) && state.pending_compactions > 0)
        {
            state = self
                .available
                .wait(state)
                .map_err(|err| io::Error::other(err.to_string()))?;
        }

        if matches!(kind, SidecarUpdateKind::Compaction) {
            state.pending_compactions -= 1;
        }
        state.active = true;
        Ok(SidecarUpdateGuard { queue: self })
    }
}

impl Drop for SidecarUpdateGuard<'_> {
    fn drop(&mut self) {
        let Ok(mut state) = self.queue.state.lock() else {
            return;
        };
        state.active = false;
        self.queue.available.notify_all();
    }
}
