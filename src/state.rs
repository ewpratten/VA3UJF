// Live status of every part of the hotspot, shared with the web UI

use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

const HEARD_HISTORY: usize = 15;

#[derive(Clone, Default, Serialize)]
pub struct LinkStatus {
    pub target: Option<String>,
    pub connected: bool,
}

#[derive(Clone, Serialize)]
pub struct Transmission {
    pub source: &'static str,
    pub callsign: String,
    pub started_at: u64,
    pub seconds: u64,
}

#[derive(Clone, Serialize)]
pub struct ScheduledRun {
    pub name: String,
    pub next_at: i64,
}

#[derive(Clone, Serialize)]
pub struct AllstarLink {
    pub node: String,
    pub mode: &'static str,
    pub outbound: bool,
    pub connected: bool,
    pub keyed: bool,
}

#[derive(Clone, Default, Serialize)]
pub struct AllstarStatus {
    pub node: u32,
    pub links: Vec<AllstarLink>,

    // None when the node isn't set up to register
    pub registered: Option<bool>,
    pub schedule: Vec<ScheduledRun>,
}

#[derive(Clone, Default, Serialize)]
pub struct Snapshot {
    pub modem_connected: bool,
    pub modem_firmware: String,
    pub link: LinkStatus,
    pub current: Option<Transmission>,
    pub heard: Vec<Transmission>,
    pub allstar: Option<AllstarStatus>,

    // None when the feature isn't configured
    pub aprs_connected: Option<bool>,
    pub vocoder_connected: Option<bool>,
}

// The snapshot has one writer at a time, and anything that needs to react to a change can subscribe to it
#[derive(Clone)]
pub struct Shared(Arc<watch::Sender<Snapshot>>);


impl Default for Shared {
    fn default() -> Self {
        Shared(Arc::new(watch::channel(Snapshot::default()).0))
    }
}

impl Shared {
    pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.0.subscribe()
    }
}

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs())
}

pub fn update(shared: &Shared, change: impl FnOnce(&mut Snapshot)) {
    shared.0.send_modify(change);
}

pub fn read(shared: &Shared) -> Snapshot {
    shared.0.borrow().clone()
}

pub fn finish_transmission(snapshot: &mut Snapshot) {
    if let Some(mut finished) = snapshot.current.take() {
        finished.seconds = unix_now().saturating_sub(finished.started_at);
        snapshot.heard.insert(0, finished);
        snapshot.heard.truncate(HEARD_HISTORY);
    }
}
