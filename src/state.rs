// Live status of every part of the hotspot, shared with the web UI

use crate::dprs::Fix;
use crate::live::Event;
use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, watch};

const HEARD_HISTORY: usize = 15;

// How far back the traffic history goes, which is how long a station stays on the flow chart after it's heard
pub const TRAFFIC_WINDOW_SECONDS: u64 = 300;
const TRAFFIC_HISTORY_LIMIT: usize = 300;
const PEER_LIMIT: usize = 200;
const EVENT_BACKLOG: usize = 256;

// One party reachable through a link, such as a node linked into AllStar
#[derive(Clone, Serialize)]
pub struct PeerStatus {
    pub id: String,
    pub mode: &'static str,
    pub outbound: bool,
    pub connected: bool,

    // Either end is transmitting, as opposed to the peer itself talking
    pub keyed: bool,
    pub talking: bool,
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

    // Whether the sender is a station to keep in the peer list and the traffic history, rather than the repeater speaking for itself or for the parties a link reports on its own
    #[serde(skip)]
    pub tracked: bool,
}

// A station that's been heard
#[derive(Clone, Serialize)]
pub struct Peer {
    pub callsign: String,
    pub via: &'static str,
    pub talking: bool,
    pub last_heard: u64,
    pub transmissions: u32,
    pub seconds: u64,
    pub fix: Option<Fix>,

    // The latest text message sent along with its voice
    pub message: String,

    // How its last RF transmission measured up
    pub ber_percent: Option<f32>,
    pub dbm: Option<f32>,

    // For peers reached through a link, whether the link has them right now. None for stations heard directly
    pub linked: Option<bool>,

    // For peers reached through a link, when they started the transmission that's going on now
    pub talking_since: Option<u64>,
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

    // Every transmission of the last few minutes. It's sent to the page once, when it connects, and then kept up to date by events
    #[serde(skip)]
    pub history: Vec<Transmission>,
    pub peers: Vec<Peer>,

    // Management details that only AllStar has, beside its link
    pub allstar: Option<AllstarStatus>,

    // None when the feature isn't configured
    pub aprs_connected: Option<bool>,
    pub vocoders: Vec<VocoderStatus>,
}

// The snapshot has one writer at a time, and anything that needs to react to a change can subscribe to it. Events are for what's happening right now, and are gone once they've been seen
#[derive(Clone)]
pub struct Shared(Arc<Inner>);

struct Inner {
    snapshot: watch::Sender<Snapshot>,
    events: broadcast::Sender<Event>,
}

impl Default for Shared {
    fn default() -> Self {
        Shared(Arc::new(Inner { snapshot: watch::channel(Snapshot::default()).0, events: broadcast::channel(EVENT_BACKLOG).0 }))
    }
}

impl Shared {
    pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.0.snapshot.subscribe()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.0.events.subscribe()
    }

    // Nobody listening is fine
    pub fn emit(&self, event: Event) {
        let _ = self.0.events.send(event);
    }
}

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs())
}

pub fn update(shared: &Shared, change: impl FnOnce(&mut Snapshot)) {
    shared.0.snapshot.send_modify(change);
}

pub fn read(shared: &Shared) -> Snapshot {
    shared.0.snapshot.borrow().clone()
}

// Room for one more peer, dropping the one that's been quiet longest if the list is full
fn make_room_for_peer(snapshot: &mut Snapshot) {
    if snapshot.peers.len() >= PEER_LIMIT
        && let Some(oldest) = snapshot.peers.iter().enumerate().filter(|(_, peer)| !peer.talking).min_by_key(|(_, peer)| peer.last_heard).map(|(index, _)| index)
    {
        snapshot.peers.remove(oldest);
    }
}

fn new_peer(callsign: &str, via: &'static str, linked: Option<bool>) -> Peer {
    Peer { callsign: callsign.to_string(), via, talking: false, last_heard: unix_now(), transmissions: 0, seconds: 0, fix: None, message: String::new(), ber_percent: None, dbm: None, linked, talking_since: None }
}

// A station keyed up, which adds it to the peer list if it's new
pub fn peer_keyed_up(snapshot: &mut Snapshot, callsign: &str, via: &'static str) {
    let now = unix_now();
    if !snapshot.peers.iter().any(|peer| peer.callsign == callsign) {
        make_room_for_peer(snapshot);
        snapshot.peers.push(new_peer(callsign, via, None));
    }
    let peer = snapshot.peers.iter_mut().find(|peer| peer.callsign == callsign).expect("the peer was just added");
    (peer.via, peer.talking, peer.last_heard, peer.transmissions, peer.message) = (via, true, now, peer.transmissions + 1, String::new());
}

// Keep a transmission in the history for the traffic chart, for as long as the chart looks back
fn record_traffic(snapshot: &mut Snapshot, transmission: &Transmission) {
    snapshot.history.push(transmission.clone());
    let oldest = unix_now().saturating_sub(TRAFFIC_WINDOW_SECONDS);
    snapshot.history.retain(|earlier| earlier.started_at + earlier.seconds >= oldest);
    let excess = snapshot.history.len().saturating_sub(TRAFFIC_HISTORY_LIMIT);
    snapshot.history.drain(..excess);
}

// Keep the peers a link says it has in step with the peer list. These are only known by what the link reports, so it's their keying that's followed. Returns the transmissions that just finished, which are also kept for the traffic chart
pub fn sync_link_peers(snapshot: &mut Snapshot, kind: &'static str, reported: &[PeerStatus]) -> Vec<Transmission> {
    let now = unix_now();
    let mut finished = Vec::new();
    for status in reported {
        if !snapshot.peers.iter().any(|peer| peer.callsign == status.id && peer.via == kind) {
            make_room_for_peer(snapshot);
            snapshot.peers.push(new_peer(&status.id, kind, Some(true)));
        }
    }
    for peer in snapshot.peers.iter_mut().filter(|peer| peer.via == kind && peer.linked.is_some()) {
        let status = reported.iter().find(|status| status.id == peer.callsign);
        let (linked, talking) = status.map_or((false, false), |status| (status.connected, status.connected && status.talking));
        peer.linked = Some(linked);
        if talking && peer.talking_since.is_none() {
            (peer.talking_since, peer.transmissions) = (Some(now), peer.transmissions + 1);
        } else if !talking && let Some(since) = peer.talking_since.take() {
            let seconds = now.saturating_sub(since);
            peer.seconds += seconds;
            finished.push(Transmission { source: kind, callsign: peer.callsign.clone(), started_at: since, seconds, tracked: true });
        }
        if talking || peer.talking {
            peer.last_heard = now;
        }
        peer.talking = talking;
    }
    for transmission in &finished {
        record_traffic(snapshot, transmission);
    }
    finished
}

pub fn peer_mut<'a>(snapshot: &'a mut Snapshot, callsign: &str) -> Option<&'a mut Peer> {
    snapshot.peers.iter_mut().find(|peer| peer.callsign == callsign)
}

pub fn finish_transmission(snapshot: &mut Snapshot) -> Option<Transmission> {
    let mut finished = snapshot.current.take()?;
    finished.seconds = unix_now().saturating_sub(finished.started_at);
    if let Some(peer) = peer_mut(snapshot, &finished.callsign).filter(|_| finished.tracked) {
        (peer.talking, peer.last_heard, peer.seconds) = (false, unix_now(), peer.seconds + finished.seconds);
    }
    snapshot.heard.insert(0, finished.clone());
    snapshot.heard.truncate(HEARD_HISTORY);
    if finished.tracked {
        record_traffic(snapshot, &finished);
    }
    Some(finished)
}

// The current transmission is over. Anyone watching live is told, if it was from a station
pub fn end_transmission(shared: &Shared) {
    let mut ended = None;
    update(shared, |snapshot| ended = finish_transmission(snapshot));
    if let Some(transmission) = ended.filter(|transmission| transmission.tracked) {
        shared.emit(Event::Transmission { source: transmission.source, callsign: transmission.callsign, started_at: transmission.started_at, seconds: transmission.seconds });
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

#[cfg(test)]
mod tests {
    use super::*;

    fn status(id: &str, connected: bool, talking: bool) -> PeerStatus {
        PeerStatus { id: id.to_string(), mode: "transceive", outbound: true, connected, keyed: talking, talking }
    }

    #[test]
    fn peers_reached_through_a_link_follow_what_the_link_reports() {
        let mut snapshot = Snapshot::default();
        sync_link_peers(&mut snapshot, "allstar", &[status("54321", true, false)]);
        assert_eq!((snapshot.peers.len(), snapshot.peers[0].linked, snapshot.peers[0].talking), (1, Some(true), false));

        // Keying up counts a transmission, and the peer is talking until it stops
        assert!(sync_link_peers(&mut snapshot, "allstar", &[status("54321", true, true)]).is_empty());
        sync_link_peers(&mut snapshot, "allstar", &[status("54321", true, true)]);
        assert_eq!((snapshot.peers[0].talking, snapshot.peers[0].transmissions, snapshot.peers[0].talking_since.is_some()), (true, 1, true));
        let finished = sync_link_peers(&mut snapshot, "allstar", &[status("54321", true, false)]);
        assert_eq!((snapshot.peers[0].talking, snapshot.peers[0].transmissions), (false, 1));

        // The transmission is reported on its own, and kept for the traffic chart, as coming from that node
        assert_eq!(finished.iter().map(|transmission| (transmission.source, transmission.callsign.as_str())).collect::<Vec<_>>(), [("allstar", "54321")]);
        assert_eq!(snapshot.history.len(), 1);

        // A peer that's gone stays known, but isn't linked
        sync_link_peers(&mut snapshot, "allstar", &[]);
        assert_eq!((snapshot.peers.len(), snapshot.peers[0].linked), (1, Some(false)));
    }

    #[test]
    fn stations_heard_directly_are_left_alone_by_a_links_sync() {
        let mut snapshot = Snapshot::default();
        peer_keyed_up(&mut snapshot, "N0TEST/ID51", "rf");
        sync_link_peers(&mut snapshot, "allstar", &[]);
        assert_eq!((snapshot.peers[0].talking, snapshot.peers[0].linked), (true, None));
    }
}
