// Live status of every part of the hotspot, shared with the web UI

use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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

#[derive(Clone, Default, Serialize)]
pub struct AllstarStatus {
    pub node: u32,
    pub links: Vec<String>,
    pub error: Option<String>,
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

pub type Shared = Arc<Mutex<Snapshot>>;

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs())
}

// Update the snapshot without every caller handling a poisoned lock
pub fn update(shared: &Shared, change: impl FnOnce(&mut Snapshot)) {
    change(&mut shared.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
}

pub fn read(shared: &Shared) -> Snapshot {
    shared.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
}

pub fn finish_transmission(snapshot: &mut Snapshot) {
    if let Some(mut finished) = snapshot.current.take() {
        finished.seconds = unix_now().saturating_sub(finished.started_at);
        snapshot.heard.insert(0, finished);
        snapshot.heard.truncate(HEARD_HISTORY);
    }
}
