// The node itself: IAX2 links to other nodes, the audio between them and the hotspot, and who may link in

use super::directory::{is_node_number, Directory};
use super::iax2::{self, command, control, ie, Datagram, Event, FullFrame, Ies, Session, FORMAT_ULAW, PROTOCOL_VERSION};
use super::{network, ulaw, Audio, Command, LinkMode};
use crate::config::{Allstar, Station};
use crate::reflector;
use crate::state::{self, AllstarLink, Shared};
use anyhow::Context;
use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};

const TICK: Duration = Duration::from_millis(20);
const SAMPLES_PER_FRAME: usize = 160;
const MAX_INBOUND_LINKS: usize = 32;
const MAX_RETRIES: u32 = 5;
const RETRY_DELAY: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DISCONNECT_DELAY: Duration = Duration::from_millis(200);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
const NEWKEY_WINDOW: Duration = Duration::from_secs(2);
const REDUNDANT_INTERVAL: Duration = Duration::from_secs(2);
const REDUNDANT_TIMEOUT: Duration = Duration::from_secs(10);
const LINK_LIST_INTERVAL: Duration = Duration::from_secs(30);
const LINK_LIST_SOON: Duration = Duration::from_millis(200);

// A link that goes quiet without a key-up is treated as unkeyed after this long. App_rpt's own 50 ms assumes its jitter buffer
const RX_LINGER: Duration = Duration::from_millis(300);
const TALKER_TIMEOUT: Duration = Duration::from_millis(500);
const CALL_TOKEN_LIFETIME_SECONDS: u64 = 10;

const NEWKEY: &str = "!NEWKEY!";
const NEWKEY_ONE: &str = "!NEWKEY1!";
const DISCONNECT: &str = "!!DISCONNECT!!";

// How a link is keyed. Nodes that send !NEWKEY1! key by their voice. Older ones key with explicit key and unkey frames
#[derive(Clone, Copy, PartialEq, Debug)]
enum NewKey {
    NotAllowed,
    Allowed,
    AllowedRedundant,
}

#[derive(Clone, PartialEq, Debug)]
enum Talker {
    Nobody,
    Link(String),
    DStar,
}

struct Link {
    node: String,
    mode: LinkMode,
    outbound: bool,
    permanent: bool,
    wanted: bool,
    session: Option<Session>,
    up: bool,
    resolving: bool,
    retries: u32,
    attempt_started: Instant,
    redial_at: Option<Instant>,
    hangup_at: Option<Instant>,
    closing_since: Option<Instant>,
    newkey: NewKey,
    newkey_deadline: Option<Instant>,
    rx_keyed: bool,
    rx_last_heard: Instant,
    rx_refresh_deadline: Option<Instant>,
    rx_samples: Vec<i16>,
    tx_keyed: bool,
    tx_refreshed: Instant,
    remote_list: String,
    next_list: Instant,
}

impl Link {
    fn new(node: &str, mode: LinkMode, outbound: bool, permanent: bool, now: Instant) -> Self {
        Link {
            node: node.to_string(),
            mode,
            outbound,
            permanent,
            wanted: true,
            session: None,
            up: false,
            resolving: false,
            retries: 0,
            attempt_started: now,
            redial_at: None,
            hangup_at: None,
            closing_since: None,
            newkey: NewKey::NotAllowed,
            newkey_deadline: None,
            rx_keyed: false,
            rx_last_heard: now,
            rx_refresh_deadline: None,
            rx_samples: Vec::new(),
            tx_keyed: false,
            tx_refreshed: now,
            remote_list: String::new(),
            next_list: now,
        }
    }

    fn mode_letter(&self) -> char {
        match (self.up, self.mode) {
            (false, _) => 'C',
            (true, LinkMode::Monitor) => 'R',
            (true, LinkMode::Transceive) => 'T',
        }
    }
}

// Work that finished off to the side and needs the node's attention
enum Internal {
    Resolved { node: String, addresses: anyhow::Result<Vec<SocketAddr>> },
    Verified { address: SocketAddr, frame: FullFrame, calling: String, verdict: Result<(), String> },
}

// Call tokens prove a caller can receive at the address it claims, before any state is kept for it
struct Tokens {
    keys: std::collections::hash_map::RandomState,
}

impl Tokens {
    fn tag(&self, address: SocketAddr, time: u64) -> u64 {
        let mut hasher = self.keys.build_hasher();
        address.hash(&mut hasher);
        time.hash(&mut hasher);
        hasher.finish()
    }

    fn issue(&self, address: SocketAddr) -> Vec<u8> {
        let time = state::unix_now();
        format!("{time}?{:016x}", self.tag(address, time)).into_bytes()
    }

    fn valid(&self, token: &[u8], address: SocketAddr) -> bool {
        let Some((time, tag)) = std::str::from_utf8(token).ok().and_then(|text| text.split_once('?')) else { return false };
        let (Ok(time), Ok(tag)) = (time.parse::<u64>(), u64::from_str_radix(tag, 16)) else { return false };
        let now = state::unix_now();
        tag == self.tag(address, time) && time <= now && now - time < CALL_TOKEN_LIFETIME_SECONDS
    }
}

struct Node {
    allstar: Allstar,
    shared: Shared,
    socket: Arc<UdpSocket>,
    directory: Arc<Directory>,
    internal: mpsc::Sender<Internal>,
    to_bridge: mpsc::Sender<Audio>,
    reflector_commands: mpsc::Sender<reflector::Command>,
    links: Vec<Link>,
    talker: Talker,
    talker_heard: Instant,
    next_call: u16,
    tokens: Tokens,
    verifying: HashSet<(SocketAddr, u16)>,
    parked_reflector: Option<String>,
}

fn pcm_to_bytes(samples: &[i16]) -> Vec<u8> {
    samples.iter().flat_map(|sample| sample.to_le_bytes()).collect()
}

fn adjust_listed_mode(through: char, entry: &str) -> String {
    let mut characters = entry.chars();
    let mode = characters.next().unwrap_or('T');
    let adjusted = match (through, mode) {
        ('R', 'T') => 'R',
        ('C', 'T' | 'R') => 'C',
        _ => mode,
    };
    format!("{adjusted}{}", characters.as_str())
}

impl Node {
    fn index_of(&self, node: &str) -> Option<usize> {
        self.links.iter().position(|link| link.node == node)
    }

    fn own_node(&self) -> String {
        self.allstar.node.to_string()
    }

    // Send what every session has queued, and forget sessions that are finished
    async fn flush(&mut self) {
        for index in 0..self.links.len() {
            let Some(session) = self.links[index].session.as_mut() else { continue };
            let (address, outbox, finished) = (session.addr, session.take_outbox(), session.destroyed);
            for bytes in outbox {
                if let Err(error) = self.socket.send_to(&bytes, address).await {
                    log::debug!("IAX2 send to {address} failed: {error}");
                }
            }
            if finished {
                self.links[index].session = None;
            }
        }
        let before = self.links.len();
        self.links.retain(|link| !(link.closing_since.is_some() && link.session.is_none()));
        if self.links.len() != before {
            self.after_links_changed(Instant::now());
        }
    }

    fn reply_without_session(&self, address: SocketAddr, received: &FullFrame, command: u8, ies: Ies) {
        let frame = FullFrame { source_call: 1, dest_call: received.source_call, retransmission: false, timestamp: received.timestamp, oseq: 0, iseq: received.oseq.wrapping_add(1), frame_type: iax2::frame_type::IAX, subclass: command as u64, payload: ies.to_bytes() };
        let _ = self.socket.try_send_to(&frame.to_bytes(), address);
    }

    fn reject_without_session(&self, address: SocketAddr, received: &FullFrame, cause: &str) {
        log::warn!("Refusing a call from {address}: {cause}");
        self.reply_without_session(address, received, command::REJECT, Ies::default().add_text(ie::CAUSE, cause));
    }

    fn allocate_call(&mut self) -> u16 {
        loop {
            self.next_call = if self.next_call >= 32766 { 2 } else { self.next_call + 1 };
            if !self.links.iter().any(|link| link.session.as_ref().is_some_and(|session| session.local_call == self.next_call)) {
                return self.next_call;
            }
        }
    }

    fn publish(&self) {
        let links: Vec<AllstarLink> = self
            .links
            .iter()
            .map(|link| AllstarLink { node: link.node.clone(), mode: if link.mode == LinkMode::Monitor { "monitor" } else { "transceive" }, outbound: link.outbound, connected: link.up, keyed: link.rx_keyed || link.tx_keyed })
            .collect();
        state::update(&self.shared, |snapshot| {
            if let Some(status) = snapshot.allstar.as_mut() {
                status.links = links;
            }
        });
    }

    // ---- Packets in ----

    fn handle_datagram(&mut self, data: &[u8], address: SocketAddr, now: Instant) {
        let Some(datagram) = iax2::parse(data) else { return };
        match datagram {
            Datagram::Mini { source_call, timestamp, payload } => {
                if let Some(index) = self.links.iter().position(|link| link.session.as_ref().is_some_and(|session| session.addr == address && session.peer_call == source_call)) {
                    let events = self.links[index].session.as_mut().map(|session| session.handle_mini(now, timestamp, payload)).unwrap_or_default();
                    let node = self.links[index].node.clone();
                    self.process_events(&node, events, now);
                }
            }
            Datagram::Full(frame) => {
                let found = self.links.iter().position(|link| {
                    link.session.as_ref().is_some_and(|session| session.addr == address && if frame.dest_call != 0 { session.local_call == frame.dest_call } else { session.peer_call == frame.source_call })
                });
                match found {
                    Some(index) => {
                        let node = self.links[index].node.clone();
                        let events = self.links[index].session.as_mut().map(|session| session.handle_full(now, frame)).unwrap_or_default();
                        self.process_events(&node, events, now);
                    }
                    None => self.handle_unknown(frame, address),
                }
            }
        }
    }

    // Frames that belong to no call: new calls, pokes, and strays
    fn handle_unknown(&mut self, frame: FullFrame, address: SocketAddr) {
        let is_iax = frame.frame_type == iax2::frame_type::IAX;
        let command = frame.subclass as u8;
        if is_iax && command == command::POKE && frame.dest_call == 0 {
            self.reply_without_session(address, &frame, command::PONG, Ies::default());
        } else if is_iax && command == command::NEW && frame.dest_call == 0 {
            self.handle_new(frame, address);
        } else if !(is_iax && matches!(command, command::INVAL | command::TXCNT | command::TXACC)) {
            log::debug!("IAX2 frame for an unknown call from {address}");
            let invalid = FullFrame { source_call: frame.dest_call & 0x7FFF, dest_call: frame.source_call, retransmission: false, timestamp: 0, oseq: 0, iseq: 0, frame_type: iax2::frame_type::IAX, subclass: command::INVAL as u64, payload: Vec::new() };
            let _ = self.socket.try_send_to(&invalid.to_bytes(), address);
        }
    }

    // A node is calling in. Check it's who it says it is, which takes a DNS lookup, before accepting
    fn handle_new(&mut self, frame: FullFrame, address: SocketAddr) {
        let Some(ies) = Ies::parse(&frame.payload) else { return };
        match ies.get(ie::CALLTOKEN) {
            Some([]) => {
                let token = self.tokens.issue(address);
                return self.reply_without_session(address, &frame, command::CALLTOKEN, Ies::default().add(ie::CALLTOKEN, &token));
            }
            Some(token) if !self.tokens.valid(token, address) => return self.reject_without_session(address, &frame, "Call token is invalid"),
            _ => {}
        }
        let (called, calling) = (ies.get_text(ie::CALLED_NUMBER).unwrap_or_default(), ies.get_text(ie::CALLING_NUMBER).unwrap_or_default());
        if called != self.own_node() {
            return self.reject_without_session(address, &frame, "No such node here");
        }
        if !is_node_number(&calling) || calling == called {
            return self.reject_without_session(address, &frame, "Caller isn't a node");
        }
        if ies.get_codec(ie::CAPABILITY2, ie::CAPABILITY).is_none_or(|capability| capability & FORMAT_ULAW == 0) {
            return self.reject_without_session(address, &frame, "No common codec");
        }
        if self.links.iter().filter(|link| link.session.is_some()).count() >= MAX_INBOUND_LINKS {
            return self.reject_without_session(address, &frame, "Too many links");
        }
        if !self.verifying.insert((address, frame.source_call)) {
            return;
        }
        let (directory, internal) = (self.directory.clone(), self.internal.clone());
        tokio::spawn(async move {
            let verdict = match directory.lookup(&calling).await {
                Ok(addresses) if addresses.iter().any(|candidate| candidate.ip() == address.ip()) => Ok(()),
                Ok(_) => Err(format!("{address} isn't where node {calling} is registered")),
                Err(error) => Err(format!("couldn't find node {calling}: {error:#}")),
            };
            let _ = internal.send(Internal::Verified { address, frame, calling, verdict }).await;
        });
    }

    fn accept_inbound(&mut self, frame: FullFrame, address: SocketAddr, node: &str, now: Instant) {
        let call = self.allocate_call();
        let mut session = Session::new(call, frame.source_call, address, now);

        // Sequence and acknowledge the NEW, then accept and answer it
        session.handle_full(now, frame);
        session.start(FORMAT_ULAW);
        session.send_iax(now, command::ACCEPT, Ies::default().add_u32(ie::FORMAT, FORMAT_ULAW as u32).add_versioned_u64(ie::FORMAT2, FORMAT_ULAW));
        session.send_control(now, control::ANSWER);

        // A node calling again replaces its stale link
        if let Some(index) = self.index_of(node) {
            let mut old = self.links.remove(index);
            if self.talker == Talker::Link(node.to_string()) {
                self.end_talker(now);
            }
            if let Some(old_session) = old.session.as_mut() {
                old_session.hangup(now, "Replaced by a new link");
                for bytes in old_session.take_outbox() {
                    let _ = self.socket.try_send_to(&bytes, old_session.addr);
                }
            }
        }
        let mut link = Link::new(node, LinkMode::Transceive, false, false, now);
        link.session = Some(session);
        self.links.push(link);
        log::info!("AllStar: node {node} linked in from {address}");
        self.link_up(node, now);
    }

    // ---- Links ----

    fn handle_command(&mut self, command: Command, now: Instant) {
        match command {
            Command::Link { node, mode, permanent } => {
                if node == self.own_node() {
                    return log::warn!("AllStar: node {node} can't link to itself");
                }
                if let Some(index) = self.index_of(&node) {
                    let link = &mut self.links[index];
                    (link.mode, link.permanent, link.wanted) = (mode, permanent, true);
                    return log::info!("AllStar: already linked to {node}");
                }
                log::info!("AllStar: linking to {node}{}", if permanent { " permanently" } else { "" });
                self.links.push(Link::new(&node, mode, true, permanent, now));
                self.begin_dial(&node, now);
                self.publish();
            }
            Command::Unlink { node } => {
                let nodes: Vec<String> = match node {
                    Some(node) => vec![node],
                    None => self.links.iter().map(|link| link.node.clone()).collect(),
                };
                for node in nodes {
                    self.disconnect(&node, now);
                }
            }
        }
    }

    // Tell the far end we're done, give it a moment to read that, then hang up
    fn disconnect(&mut self, node: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        let link = &mut self.links[index];
        link.wanted = false;
        link.redial_at = None;
        if link.up {
            log::info!("AllStar: disconnecting {node}");
            if let Some(session) = link.session.as_mut() {
                session.send_text(now, DISCONNECT);
            }
            link.hangup_at = Some(now + DISCONNECT_DELAY);
        } else if link.session.is_some() {
            self.close(node, now);
        } else {
            self.links.remove(index);
            self.publish();
        }
    }

    fn close(&mut self, node: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        if self.talker == Talker::Link(node.to_string()) {
            self.end_talker(now);
        }
        let link = &mut self.links[index];
        link.up = false;
        link.hangup_at = None;
        link.rx_keyed = false;
        link.tx_keyed = false;
        link.closing_since = Some(now);
        if let Some(session) = link.session.as_mut() {
            session.hangup(now, "Disconnected");
        }
        self.after_links_changed(now);
    }

    fn begin_dial(&mut self, node: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        let link = &mut self.links[index];
        (link.resolving, link.attempt_started, link.redial_at) = (true, now, None);
        let (directory, internal, node) = (self.directory.clone(), self.internal.clone(), node.to_string());
        tokio::spawn(async move {
            let addresses = directory.lookup(&node).await;
            let _ = internal.send(Internal::Resolved { node, addresses }).await;
        });
    }

    fn handle_internal(&mut self, internal: Internal, now: Instant) {
        match internal {
            Internal::Resolved { node, addresses } => {
                let Some(index) = self.index_of(&node) else { return };
                self.links[index].resolving = false;
                match addresses.map(|addresses| addresses.into_iter().next()) {
                    Ok(Some(address)) if self.links[index].wanted => self.start_call(&node, address, now),
                    Ok(Some(_)) => {}
                    Ok(None) => self.link_lost(&node, "no address", now),
                    Err(error) => self.link_lost(&node, &format!("{error:#}"), now),
                }
            }
            Internal::Verified { address, frame, calling, verdict } => {
                self.verifying.remove(&(address, frame.source_call));
                match verdict {
                    Ok(()) => self.accept_inbound(frame, address, &calling, now),
                    Err(reason) => self.reject_without_session(address, &frame, &reason),
                }
            }
        }
    }

    fn start_call(&mut self, node: &str, address: SocketAddr, now: Instant) {
        let call = self.allocate_call();
        let Some(index) = self.index_of(node) else { return };
        let mut session = Session::new(call, 0, address, now);
        let elements = Ies::default()
            .add_u16(ie::VERSION, PROTOCOL_VERSION)
            .add_text(ie::CALLED_NUMBER, node)
            .add_text(ie::CALLING_NUMBER, &self.allstar.node.to_string())
            .add_text(ie::USERNAME, "radio")
            .add_u32(ie::FORMAT, FORMAT_ULAW as u32)
            .add_versioned_u64(ie::FORMAT2, FORMAT_ULAW)
            .add_u32(ie::CAPABILITY, FORMAT_ULAW as u32)
            .add_versioned_u64(ie::CAPABILITY2, FORMAT_ULAW);
        session.start_call(now, elements);
        log::info!("AllStar: calling node {node} at {address}");
        self.links[index].session = Some(session);
    }

    // The far end answered, so the link is usable. Both ends announce they key by voice, within a couple of seconds
    fn link_up(&mut self, node: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        let link = &mut self.links[index];
        (link.up, link.retries) = (true, 0);
        link.newkey = NewKey::NotAllowed;
        link.newkey_deadline = Some(now + NEWKEY_WINDOW);
        if let Some(session) = link.session.as_mut() {
            session.send_text(now, NEWKEY_ONE);
        }
        log::info!("AllStar: linked to node {node} ({})", if link.outbound { "outbound" } else { "inbound" });
        self.kick_link_lists(now);
        self.after_links_changed(now);
    }

    // The call ended without anyone asking. Outbound and permanent links dial again, and the rest are dropped
    fn link_lost(&mut self, node: &str, reason: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        if self.links[index].rx_keyed {
            self.rx_unkey(node, now);
        }
        let link = &mut self.links[index];
        let was_up = link.up;
        (link.up, link.tx_keyed, link.rx_keyed, link.session, link.resolving) = (false, false, false, None, false);
        link.remote_list.clear();
        let redial = link.wanted && (link.outbound || link.permanent) && (link.permanent || link.retries < MAX_RETRIES);
        if redial {
            link.retries += 1;
            link.redial_at = Some(now + RETRY_DELAY);
            log::warn!("AllStar: lost node {node} ({reason}), trying again in {}s", RETRY_DELAY.as_secs());
        } else {
            self.links.remove(index);
            if was_up || reason != "disconnected" {
                log::info!("AllStar: node {node} is no longer linked ({reason})");
            }
        }
        self.after_links_changed(now);
    }

    // The links changed, so the web UI, the stats and the reflector all want to know
    fn after_links_changed(&mut self, _now: Instant) {
        self.publish();
        if !self.allstar.park_reflector {
            return;
        }
        let any_up = self.links.iter().any(|link| link.up);
        let reflector = state::read(&self.shared).link.target;
        if any_up && self.parked_reflector.is_none() {
            if let Some(target) = reflector {
                log::info!("AllStar: leaving {target} while the node has links");
                self.parked_reflector = Some(target);
                let _ = self.reflector_commands.try_send(reflector::Command::Unlink);
            }
        } else if !any_up && reflector.is_none() {
            if let Some((name, module)) = self.parked_reflector.take().and_then(|target| target.rsplit_once(' ').and_then(|(name, module)| Some((name.to_string(), module.chars().next()?)))) {
                log::info!("AllStar: no links left, returning to {name} {module}");
                let _ = self.reflector_commands.try_send(reflector::Command::Link { name, module });
            }
        } else if !any_up {
            self.parked_reflector = None;
        }
    }

    // ---- What links tell us ----

    fn process_events(&mut self, node: &str, events: Vec<Event>, now: Instant) {
        for event in events {
            if self.index_of(node).is_none() {
                return;
            }
            match event {
                Event::Iax { command: command::ACCEPT, ies } => {
                    let format = ies.get_codec(ie::FORMAT2, ie::FORMAT).unwrap_or(FORMAT_ULAW);
                    if format != FORMAT_ULAW {
                        self.hang_up_with(node, "Unsupported codec", now);
                    } else if let Some(session) = self.link_session(node) {
                        session.start(format);
                    }
                }
                Event::Iax { command: command::AUTHREQ, .. } => self.hang_up_with(node, "Authentication isn't supported", now),
                Event::Iax { command: command::REJECT, ies } => {
                    let cause = ies.get_text(ie::CAUSE).unwrap_or_default();
                    self.link_lost(node, &format!("refused: {cause}"), now);
                }
                Event::Iax { command: command::HANGUP, .. } | Event::Control(control::HANGUP) => self.link_lost(node, "disconnected", now),
                Event::Iax { .. } => {}
                Event::Control(control::ANSWER) => {
                    if !self.links[self.index_of(node).unwrap_or(0)].up {
                        self.link_up(node, now);
                    }
                }
                Event::Control(control::RADIO_KEY) => self.handle_radio_key(node, now),
                Event::Control(control::RADIO_UNKEY) => {
                    self.refresh_redundant_receive(node, now);
                    self.rx_unkey(node, now);
                }
                Event::Control(_) | Event::Cng => {}
                Event::Text(text) => self.handle_text(node, &text, now),
                Event::Voice { format, payload } => {
                    if format == FORMAT_ULAW {
                        self.handle_voice(node, &payload, now);
                    }
                }
                Event::Timeout => self.link_lost(node, "no response", now),
            }
        }
    }

    fn link_session(&mut self, node: &str) -> Option<&mut Session> {
        let index = self.index_of(node)?;
        self.links[index].session.as_mut()
    }

    fn hang_up_with(&mut self, node: &str, cause: &str, now: Instant) {
        log::warn!("AllStar: hanging up on node {node}: {cause}");
        if let Some(index) = self.index_of(node) {
            self.links[index].wanted = self.links[index].wanted && self.links[index].outbound && self.links[index].permanent;
        }
        self.close(node, now);
    }

    fn refresh_redundant_receive(&mut self, node: &str, now: Instant) {
        if let Some(index) = self.index_of(node)
            && self.links[index].newkey == NewKey::AllowedRedundant
        {
            self.links[index].rx_refresh_deadline = Some(now + REDUNDANT_TIMEOUT);
        }
    }

    fn handle_radio_key(&mut self, node: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        if self.links[index].newkey == NewKey::NotAllowed {
            return;
        }
        self.refresh_redundant_receive(node, now);
        self.rx_key(node, now);
    }

    fn handle_text(&mut self, node: &str, text: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        let link = &mut self.links[index];
        match text {
            DISCONNECT => {
                log::info!("AllStar: node {node} is disconnecting");
                link.wanted = false;
                link.hangup_at = Some(now + DISCONNECT_DELAY);
            }
            NEWKEY_ONE => (link.newkey, link.newkey_deadline) = (NewKey::NotAllowed, None),
            NEWKEY => {
                if link.newkey == NewKey::Allowed || (link.newkey == NewKey::NotAllowed && link.newkey_deadline.is_some()) {
                    (link.newkey, link.newkey_deadline, link.rx_refresh_deadline) = (NewKey::AllowedRedundant, None, Some(now + REDUNDANT_TIMEOUT));
                    if let Some(session) = link.session.as_mut() {
                        session.send_text(now, NEWKEY);
                    }
                }
            }
            _ if text.starts_with("L ") || text == "L" => link.remote_list = text[1..].trim().to_string(),
            // Telemetry, messages, DTMF commands and the rest are for nodes with a radio and a command set. Commands from the network are never run
            _ => log::trace!("AllStar: ignoring {text:?} from node {node}"),
        }
    }

    fn rx_key(&mut self, node: &str, _now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        if !self.links[index].rx_keyed {
            self.links[index].rx_keyed = true;
            log::info!("AllStar: node {node} keyed");
            self.publish();
        }
    }

    fn rx_unkey(&mut self, node: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        if self.links[index].rx_keyed {
            self.links[index].rx_keyed = false;
            self.links[index].rx_samples.clear();
            log::info!("AllStar: node {node} unkeyed");
            if self.talker == Talker::Link(node.to_string()) {
                self.end_talker(now);
            }
            self.publish();
        }
    }

    fn handle_voice(&mut self, node: &str, payload: &[u8], now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        let link = &mut self.links[index];
        link.rx_last_heard = now;
        if link.newkey == NewKey::NotAllowed && !link.rx_keyed {
            self.rx_key(node, now);
        }
        let link = &mut self.links[index];
        if !link.rx_keyed {
            return;
        }
        link.rx_samples.extend(ulaw::decode_frame(payload));
        while self.links[index].rx_samples.len() >= SAMPLES_PER_FRAME {
            let frame: Vec<i16> = self.links[index].rx_samples.drain(..SAMPLES_PER_FRAME).collect();
            self.route_link_audio(node, &frame, now);
        }
    }

    // ---- Audio ----

    // Whoever keys first holds the channel until they finish, as on the hotspot
    fn take_channel(&mut self, talker: Talker, now: Instant) -> bool {
        if self.talker == Talker::Nobody {
            self.talker = talker.clone();
        }
        let holds = self.talker == talker;
        if holds {
            self.talker_heard = now;
        }
        holds
    }

    fn route_link_audio(&mut self, source: &str, frame: &[i16], now: Instant) {
        if !self.take_channel(Talker::Link(source.to_string()), now) {
            return;
        }
        let _ = self.to_bridge.try_send(Audio::Voice(pcm_to_bytes(frame)));
        self.broadcast(frame, Some(source), now);
    }

    fn route_dstar_audio(&mut self, frame: &[i16], now: Instant) {
        if self.take_channel(Talker::DStar, now) {
            self.broadcast(frame, None, now);
        }
    }

    // Send audio to every transceive link but the one it came from
    fn broadcast(&mut self, frame: &[i16], except: Option<&str>, now: Instant) {
        let bytes = ulaw::encode_frame(frame);
        for link in &mut self.links {
            if !link.up || link.mode != LinkMode::Transceive || except == Some(link.node.as_str()) {
                continue;
            }
            let Some(session) = link.session.as_mut() else { continue };
            if !link.tx_keyed {
                (link.tx_keyed, link.tx_refreshed) = (true, now);
                if link.newkey != NewKey::NotAllowed {
                    session.send_control(now, control::RADIO_KEY);
                }
            }
            session.send_voice(now, FORMAT_ULAW, &bytes);
        }
    }

    // The talker is done. Links that were being sent audio get an unkey, and the bridge an end marker if the audio came from a link
    fn end_talker(&mut self, now: Instant) {
        if let Talker::Link(_) = self.talker {
            let _ = self.to_bridge.try_send(Audio::End);
        }
        self.talker = Talker::Nobody;
        for link in &mut self.links {
            if link.tx_keyed {
                link.tx_keyed = false;
                if let Some(session) = link.session.as_mut() {
                    session.send_control(now, control::RADIO_UNKEY);
                    session.send_cng(now);
                }
            }
        }
    }

    fn handle_bridge_audio(&mut self, audio: Audio, now: Instant) {
        match audio {
            Audio::Voice(bytes) => {
                let frame: Vec<i16> = bytes.chunks_exact(2).map(|pair| i16::from_le_bytes([pair[0], pair[1]])).collect();
                self.route_dstar_audio(&frame, now);
            }
            Audio::End => {
                if self.talker == Talker::DStar {
                    self.end_talker(now);
                }
            }
        }
    }

    // ---- Link lists ----

    // The links this node has beyond the one the list is for, with the nodes behind each, flattened
    fn link_list(&self, target: &str) -> String {
        let mut entries = Vec::new();
        for link in &self.links {
            if link.node == target || link.node.starts_with('0') {
                continue;
            }
            let letter = link.mode_letter();
            entries.push(format!("{letter}{}", link.node));
            entries.extend(link.remote_list.split(',').filter(|entry| !entry.is_empty()).map(|entry| adjust_listed_mode(letter, entry)));
        }
        format!("L {}", entries.join(","))
    }

    // New links show up in everyone's lists within a fraction of a second
    fn kick_link_lists(&mut self, now: Instant) {
        for link in &mut self.links {
            link.next_list = link.next_list.min(now + LINK_LIST_SOON);
        }
    }

    // ---- Time ----

    fn tick(&mut self, now: Instant) {
        let nodes: Vec<String> = self.links.iter().map(|link| link.node.clone()).collect();
        for node in nodes {
            self.tick_link(&node, now);
        }
        if self.talker == Talker::DStar && now.duration_since(self.talker_heard) > TALKER_TIMEOUT {
            self.end_talker(now);
        }
    }

    fn tick_link(&mut self, node: &str, now: Instant) {
        let Some(index) = self.index_of(node) else { return };
        let events = self.links[index].session.as_mut().map(|session| session.poll(now)).unwrap_or_default();
        self.process_events(node, events, now);
        let Some(index) = self.index_of(node) else { return };
        let link = &mut self.links[index];

        if link.closing_since.is_some_and(|since| now.duration_since(since) > CLOSE_TIMEOUT) {
            link.session = None;
            return;
        }
        if link.hangup_at.is_some_and(|at| now >= at) {
            return self.close(node, now);
        }
        if link.redial_at.is_some_and(|at| now >= at) && link.wanted {
            return self.begin_dial(node, now);
        }
        if !link.up && link.session.is_some() && link.closing_since.is_none() && now.duration_since(link.attempt_started) > CONNECT_TIMEOUT {
            return self.hang_up_with_retry(node, "no answer", now);
        }

        // Nodes that never say how they key are assumed to use key and unkey frames
        if link.newkey_deadline.is_some_and(|deadline| now >= deadline) {
            if link.up && link.newkey == NewKey::NotAllowed {
                log::warn!("AllStar: node {node} never said how it keys, expecting key and unkey frames");
                link.newkey = NewKey::Allowed;
                link.newkey_deadline = None;
                return self.rx_unkey(node, now);
            }
            link.newkey_deadline = if link.up { None } else { Some(now + NEWKEY_WINDOW) };
        }
        if link.newkey == NewKey::NotAllowed && link.rx_keyed && now.duration_since(link.rx_last_heard) > RX_LINGER {
            return self.rx_unkey(node, now);
        }
        if link.newkey == NewKey::AllowedRedundant {
            if link.rx_keyed && link.rx_refresh_deadline.is_some_and(|deadline| now >= deadline) {
                log::info!("AllStar: node {node} stopped refreshing its key");
                return self.rx_unkey(node, now);
            }
            if now.duration_since(link.tx_refreshed) >= REDUNDANT_INTERVAL {
                link.tx_refreshed = now;
                let state = if link.tx_keyed { control::RADIO_KEY } else { control::RADIO_UNKEY };
                if let Some(session) = link.session.as_mut() {
                    session.send_control(now, state);
                }
            }
        }
        if link.up && now >= link.next_list {
            link.next_list = now + LINK_LIST_INTERVAL;
            let list = self.link_list(node);
            if let Some(session) = self.link_session(node) {
                session.send_text(now, &list);
            }
        }
    }

    // A call that never connected is hung up on, then retried like any other loss
    fn hang_up_with_retry(&mut self, node: &str, reason: &str, now: Instant) {
        if let Some(session) = self.link_session(node) {
            session.hangup(now, "No answer");
        }
        self.link_lost(node, reason, now);
    }
}

pub async fn run(allstar: Allstar, _station: Station, shared: Shared, reflector_commands: mpsc::Sender<reflector::Command>, mut from_bridge: mpsc::Receiver<Audio>, to_bridge: mpsc::Sender<Audio>, mut commands: mpsc::Receiver<Command>) -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind(("0.0.0.0", allstar.iax_port)).await.with_context(|| format!("couldn't bind IAX2 port {}", allstar.iax_port))?);
    let directory = Arc::new(Directory::new(&allstar.dns_domain, &allstar.nodes)?);
    log::info!("AllStar node {} listening for IAX2 on port {}", allstar.node, allstar.iax_port);
    if let Some(registration) = allstar.registration.clone() {
        tokio::spawn(network::register(allstar.node, allstar.iax_port, registration, shared.clone()));
    }
    if let Some(statpost) = allstar.statpost.clone() {
        tokio::spawn(network::statpost(allstar.node, statpost, shared.clone()));
    }

    let (internal, mut internal_receiver) = mpsc::channel(64);
    let now = Instant::now();
    let mut node = Node {
        allstar,
        shared,
        socket: socket.clone(),
        directory,
        internal,
        to_bridge,
        reflector_commands,
        links: Vec::new(),
        talker: Talker::Nobody,
        talker_heard: now,
        next_call: 1,
        tokens: Tokens { keys: Default::default() },
        verifying: HashSet::new(),
        parked_reflector: None,
    };
    let mut tick = interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut buffer = vec![0u8; 2048];
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                if let Ok((length, address)) = received {
                    let data = buffer[..length].to_vec();
                    node.handle_datagram(&data, address, Instant::now());
                }
            }
            command = commands.recv() => match command {
                Some(command) => node.handle_command(command, Instant::now()),
                None => return Ok(()),
            },
            Some(internal) = internal_receiver.recv() => node.handle_internal(internal, Instant::now()),
            audio = from_bridge.recv() => match audio {
                Some(audio) => node.handle_bridge_audio(audio, Instant::now()),
                None => return Ok(()),
            },
            _ = tick.tick() => node.tick(Instant::now()),
        }
        node.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allstar::testing::*;

    #[test]
    fn listed_modes_are_downgraded_through_weaker_links() {
        assert_eq!(adjust_listed_mode('T', "T2000"), "T2000");
        assert_eq!(adjust_listed_mode('R', "T2000"), "R2000");
        assert_eq!(adjust_listed_mode('R', "R2000"), "R2000");
        assert_eq!(adjust_listed_mode('C', "T2000"), "C2000");
        assert_eq!(adjust_listed_mode('C', "R2000"), "C2000");
    }

    #[tokio::test]
    async fn two_nodes_link_over_iax2_and_pass_audio_both_ways() {
        let mut first = start_node(10001, 24569, &[(10002, "127.0.0.1:24570".into())]);
        let mut second = start_node(10002, 24570, &[(10001, "127.0.0.1:24569".into())]);
        first.handle.run_action(&crate::config::Action { link: Some("10002".into()), ..Default::default() }).await.unwrap();
        wait_for("both nodes to show the link", || linked(&first, "10002") && linked(&second, "10001")).await;

        // Audio from the first node's bridge reaches the second node's bridge, and the transmission ends cleanly
        transmit(&first.to_node, 30).await;
        let heard = heard_frames(&mut second.from_node, Duration::from_secs(3)).await.expect("the transmission to end");
        assert!((25..=30).contains(&heard), "second node heard {heard} of 30 frames");

        // And back the other way, over the same link
        transmit(&second.to_node, 30).await;
        let heard = heard_frames(&mut first.from_node, Duration::from_secs(3)).await.expect("the transmission to end");
        assert!((25..=30).contains(&heard), "first node heard {heard} of 30 frames");

        // Unlinking from one end drops it at both
        first.handle.run_action(&crate::config::Action { unlink: Some("10002".into()), ..Default::default() }).await.unwrap();
        wait_for("both nodes to drop the link", || has_no_links(&first) && has_no_links(&second)).await;
    }

    #[test]
    fn call_tokens_only_work_for_the_address_they_were_issued_to() {
        let tokens = Tokens { keys: Default::default() };
        let (first, second): (SocketAddr, SocketAddr) = ("192.0.2.1:4569".parse().unwrap(), "192.0.2.2:4569".parse().unwrap());
        let token = tokens.issue(first);
        assert!(tokens.valid(&token, first));
        assert!(!tokens.valid(&token, second));
        assert!(!tokens.valid(b"garbage", first));
        assert!(!tokens.valid(format!("{}?{:016x}", state::unix_now() - 60, 0).as_bytes(), first));
    }
}
