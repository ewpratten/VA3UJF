// Live status of every part of the hotspot, shared with the web UI

use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

const HEARD_HISTORY: usize = 15;

// One party reachable through a link, such as a node linked into AllStar
#[derive(Clone, Serialize)]
pub struct PeerStatus {
    pub id: String,
    pub mode: &'static str,
    pub outbound: bool,
    pub connected: bool,
    pub keyed: bool,
}

#[derive(Clone, Serialize)]
pub struct LinkStatus {
    pub name: &'static str,
    pub kind: &'static str,

    // What the link is aimed at, if it has one place it's pointed. Set while connecting too
    pub target: Option<String>,
    pub connected: bool,
    pub peers: Vec<PeerStatus>,

    // How the link describes itself out loud, and anything wrong with it worth saying
    pub spoken: String,
    pub warnings: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct VocoderStatus {
    pub name: &'static str,
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

    // None when the node isn't set up to register
    pub registered: Option<bool>,
    pub schedule: Vec<ScheduledRun>,
}

#[derive(Clone, Default, Serialize)]
pub struct Snapshot {
    pub modem_connected: bool,
    pub modem_firmware: String,
    pub links: Vec<LinkStatus>,
    pub current: Option<Transmission>,
    pub heard: Vec<Transmission>,

    // Management details that only AllStar has, beside its link
    pub allstar: Option<AllstarStatus>,

    // None when the feature isn't configured
    pub aprs_connected: Option<bool>,
    pub vocoders: Vec<VocoderStatus>,
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

impl LinkStatus {
    pub fn new(name: &'static str, kind: &'static str) -> Self {
        LinkStatus { name, kind, target: None, connected: false, peers: Vec::new(), spoken: String::new(), warnings: Vec::new() }
    }
}

// The named link's status, added the first time it's asked for
pub fn link_mut<'a>(snapshot: &'a mut Snapshot, name: &'static str, kind: &'static str) -> &'a mut LinkStatus {
    let index = snapshot.links.iter().position(|link| link.name == name).unwrap_or_else(|| {
        snapshot.links.push(LinkStatus::new(name, kind));
        snapshot.links.len() - 1
    });
    &mut snapshot.links[index]
}

pub fn set_vocoder(snapshot: &mut Snapshot, name: &'static str, connected: bool) {
    match snapshot.vocoders.iter_mut().find(|vocoder| vocoder.name == name) {
        Some(vocoder) => vocoder.connected = connected,
        None => snapshot.vocoders.push(VocoderStatus { name, connected }),
    }
}

// Whether anyone is keyed up on RF or through a link
pub fn channel_is_quiet(snapshot: &Snapshot) -> bool {
    snapshot.current.is_none() && snapshot.links.iter().all(|link| link.peers.iter().all(|peer| !peer.keyed))
}
