// The node itself: calls to and from EchoLink stations, the audio between them and the hotspot, and who may call in

use super::directory::Directory;
use super::protocol::{self, Control, Decoder, Encoder, GSM_FRAMES_PER_PACKET};
use super::Command;
use crate::allstar::mixer::{mix, Buffer};
use crate::codec::{Audio, SAMPLES_PER_FRAME};
use crate::config::Echolink;
use crate::live::Event as LiveEvent;
use crate::speech::{self, Speaker};
use crate::state::{self, PeerStatus, Shared};
use anyhow::Context;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};

const TICK: Duration = Duration::from_millis(20);
const MAX_LINKS: usize = 16;
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(50);
const CALL_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const MAX_CALL_ATTEMPTS: u32 = 5;

// A station that stops sending audio is treated as unkeyed after this long, since there's no end-of-transmission marker
const RX_LINGER: Duration = Duration::from_millis(300);
const TALKER_TIMEOUT: Duration = Duration::from_millis(500);

// Where audio in the mix comes from. Links are named by their place in the list for the length of one mixing pass
#[derive(Clone, Copy, PartialEq, Debug)]
enum Source {
    Link(usize),
    DStar,
}

struct Link {
    callsign: String,
    name: String,

    // Where the station's audio goes. Its control port is the next one up
    address: SocketAddr,
    outbound: bool,
    up: bool,
    attempts: u32,
    last_sent: Instant,
    last_heard: Instant,
    decoder: Decoder,
    rx_keyed: bool,
    rx_last_heard: Instant,
    rx_buffer: Buffer,
    encoder: Encoder,
    tx_keyed: bool,
    tx_pending: Vec<[u8; 33]>,
    tx_sequence: u16,
}

impl Link {
    fn new(callsign: &str, address: SocketAddr, outbound: bool, now: Instant) -> Self {
        Link {
            callsign: callsign.to_string(),
            name: String::new(),
            address,
            outbound,
            up: false,
            attempts: 0,
            last_sent: now,
            last_heard: now,
            decoder: Decoder::default(),
            rx_keyed: false,
            rx_last_heard: now,
            rx_buffer: Buffer::default(),
            encoder: Encoder::default(),
            tx_keyed: false,
            tx_pending: Vec::new(),
            tx_sequence: 0,
        }
    }
}

// Work that finished off to the side and needs the node's attention
enum Internal {
    Resolved { target: String, station: anyhow::Result<(String, SocketAddr)> },
    Verified { callsign: String, name: String, ip: IpAddr, verdict: Result<SocketAddr, String> },
}

// How the node's links sound when read out
fn spoken_links(callsign: &str, peers: &[PeerStatus]) -> String {
    let linked: Vec<String> = peers.iter().filter(|peer| peer.connected).map(|peer| peer.id.replace('-', "")).collect();
    let node = speech::spelled(&callsign.replace('-', ""));
    match linked.len() {
        0 => format!("Echo link node {node} has no links."),
        1 => format!("Echo link node {node} is linked to station {}.", speech::spoken_list(&linked)),
        _ => format!("Echo link node {node} is linked to stations {}.", speech::spoken_list(&linked)),
    }
}

struct Node {
    echolink: Echolink,
    name: String,
    shared: Shared,
    audio_socket: Arc<UdpSocket>,
    control_socket: Arc<UdpSocket>,
    directory: Arc<Directory>,
    internal: mpsc::Sender<Internal>,
    to_bridge: mpsc::Sender<Audio>,
    speaker: Option<Speaker>,
    links: Vec<Link>,
    dstar_buffer: Buffer,
    dstar_keyed: bool,
    dstar_heard: Instant,
    bridge_active: bool,
    verifying: HashSet<IpAddr>,
}

fn control_address(audio_address: SocketAddr) -> SocketAddr {
    SocketAddr::new(audio_address.ip(), audio_address.port() + 1)
}

impl Node {
    fn index_of_ip(&self, ip: IpAddr) -> Option<usize> {
        self.links.iter().position(|link| link.address.ip() == ip)
    }

    fn index_of_station(&self, station: &str) -> Option<usize> {
        self.links.iter().position(|link| link.callsign.eq_ignore_ascii_case(station))
    }

    fn send_control(&self, link: &Link, packet: &[u8]) {
        if let Err(error) = self.control_socket.try_send_to(packet, control_address(link.address)) {
            log::debug!("EchoLink send to {} failed: {error}", link.address);
        }
    }

    fn send_hello(&mut self, index: usize, now: Instant) {
        let packet = protocol::hello(&self.echolink.callsign, &self.name);
        self.links[index].last_sent = now;
        self.send_control(&self.links[index], &packet);
    }

    fn publish(&self) {
        let peers: Vec<PeerStatus> = self.links.iter().map(|link| PeerStatus { id: link.callsign.clone(), mode: "transceive", outbound: link.outbound, connected: link.up, keyed: link.rx_keyed || link.tx_keyed, talking: link.rx_keyed }).collect();
        let spoken = spoken_links(&self.echolink.callsign, &peers);
        let mut finished = Vec::new();
        state::update(&self.shared, |snapshot| {
            finished = state::sync_link_peers(snapshot, super::LINK_KIND, &peers);
            let status = state::link_mut(snapshot, super::LINK_NAME, super::LINK_KIND);
            (status.connected, status.peers, status.spoken) = (peers.iter().any(|peer| peer.connected), peers, spoken);
        });
        for transmission in finished {
            self.shared.emit(LiveEvent::Transmission { source: transmission.source, callsign: transmission.callsign, started_at: transmission.started_at, seconds: transmission.seconds });
        }
    }

    // ---- Packets in ----

    fn handle_control(&mut self, data: &[u8], address: SocketAddr, now: Instant) {
        let Some(control) = protocol::parse_control(data) else { return };
        match (control, self.index_of_ip(address.ip())) {
            (Control::Hello { name, .. }, Some(index)) => {
                self.links[index].last_heard = now;
                self.links[index].name = name;
                if !self.links[index].up {
                    self.link_up(index, now);
                }
            }
            (Control::Bye, Some(index)) => self.link_ended(index, "disconnected"),
            (Control::Hello { callsign, name }, None) => self.handle_new(callsign, name, address.ip()),
            (Control::Bye, None) => {}
        }
    }

    // A station is calling in. Check it's where the directory says it is, which can take a lookup, before accepting
    fn handle_new(&mut self, callsign: String, name: String, ip: IpAddr) {
        if callsign.eq_ignore_ascii_case(&self.echolink.callsign) || self.links.len() >= MAX_LINKS || !self.verifying.insert(ip) {
            return;
        }
        let (directory, internal) = (self.directory.clone(), self.internal.clone());
        tokio::spawn(async move {
            let verdict = directory.verify(&callsign, ip).await;
            let _ = internal.send(Internal::Verified { callsign, name, ip, verdict }).await;
        });
    }

    fn handle_audio_packet(&mut self, data: &[u8], address: SocketAddr, now: Instant) {
        let Some(index) = self.index_of_ip(address.ip()).filter(|index| self.links[*index].up) else { return };
        let Some(frames) = protocol::parse_voice(data) else { return };
        let link = &mut self.links[index];
        (link.last_heard, link.rx_last_heard) = (now, now);
        for frame in frames {
            if let Some(samples) = link.decoder.decode(frame) {
                link.rx_buffer.push(samples);
            }
        }
        if !link.rx_keyed {
            link.rx_keyed = true;
            log::info!("EchoLink: station {} keyed", link.callsign);
            self.publish();
        }
    }

    // ---- Links ----

    fn handle_command(&mut self, command: Command, now: Instant) {
        match command {
            Command::Link { target } => {
                if self.index_of_station(&target).is_some() {
                    return log::info!("EchoLink: already linked to {target}");
                }
                log::info!("EchoLink: linking to {target}");
                let (directory, internal) = (self.directory.clone(), self.internal.clone());
                tokio::spawn(async move {
                    let station = directory.find(&target).await;
                    let _ = internal.send(Internal::Resolved { target, station }).await;
                });
            }
            Command::Unlink { target } => {
                let stations: Vec<String> = match target {
                    Some(target) => vec![target],
                    None => self.links.iter().map(|link| link.callsign.clone()).collect(),
                };
                for station in stations {
                    self.disconnect(&station, now);
                }
            }
        }
    }

    fn handle_internal(&mut self, internal: Internal, now: Instant) {
        match internal {
            Internal::Resolved { target, station } => match station {
                Ok((callsign, address)) if self.index_of_ip(address.ip()).is_none() => {
                    log::info!("EchoLink: calling {callsign} at {address}");
                    self.links.push(Link::new(&callsign, address, true, now));
                    let index = self.links.len() - 1;
                    self.links[index].attempts = 1;
                    self.send_hello(index, now);
                    self.publish();
                }
                Ok((callsign, _)) => log::info!("EchoLink: already linked to {callsign}"),
                Err(error) => log::warn!("EchoLink: can't link to {target}: {error:#}"),
            },
            Internal::Verified { callsign, name, ip, verdict } => {
                self.verifying.remove(&ip);
                match verdict {
                    Ok(address) if self.index_of_ip(ip).is_none() => self.accept_inbound(&callsign, &name, address, now),
                    Ok(_) => {}
                    Err(reason) => log::warn!("EchoLink: refusing a call from {ip}: {reason}"),
                }
            }
        }
    }

    fn accept_inbound(&mut self, callsign: &str, name: &str, address: SocketAddr, now: Instant) {
        let mut link = Link::new(callsign, address, false, now);
        link.name = name.to_string();
        self.links.push(link);
        let index = self.links.len() - 1;
        self.send_hello(index, now);
        log::info!("EchoLink: station {callsign} linked in from {address}");
        self.link_up(index, now);
    }

    fn link_up(&mut self, index: usize, now: Instant) {
        let link = &mut self.links[index];
        (link.up, link.last_heard) = (true, now);
        log::info!("EchoLink: linked to {} ({})", link.callsign, if link.outbound { "outbound" } else { "inbound" });
        speech::say(&self.speaker, format!("Station {} connected.", speech::spelled(&link.callsign.replace('-', ""))));
        self.publish();
    }

    // Tell the station we're done and forget it
    fn disconnect(&mut self, station: &str, _now: Instant) {
        let Some(index) = self.index_of_station(station) else { return };
        log::info!("EchoLink: disconnecting {}", self.links[index].callsign);
        self.send_control(&self.links[index], &protocol::bye());
        self.link_ended(index, "disconnected");
    }

    // The call is over, however it ended
    fn link_ended(&mut self, index: usize, reason: &str) {
        let link = self.links.remove(index);
        if link.up {
            speech::say(&self.speaker, format!("Station {} disconnected.", speech::spelled(&link.callsign.replace('-', ""))));
        }
        log::info!("EchoLink: station {} is no longer linked ({reason})", link.callsign);
        self.publish();
    }

    // ---- Audio ----

    // One pass of the mixer: every source that's talking gives up a frame, and each destination gets all of them but its own
    fn mix_tick(&mut self) {
        let mut sources: Vec<(Source, Option<Vec<i16>>)> = Vec::new();
        for (index, link) in self.links.iter_mut().enumerate() {
            let keyed = link.rx_keyed;
            let frame = link.rx_buffer.next(keyed);
            if link.rx_buffer.active(keyed) {
                sources.push((Source::Link(index), frame));
            }
        }
        let dstar_frame = self.dstar_buffer.next(self.dstar_keyed);
        if self.dstar_buffer.active(self.dstar_keyed) {
            sources.push((Source::DStar, dstar_frame));
        }

        // Each station hears everyone else, a packet at a time, and the end of what it hears is sent as a short last packet
        let mut packets: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
        for (index, link) in self.links.iter_mut().enumerate().filter(|(_, link)| link.up) {
            let others: Vec<&(Source, Option<Vec<i16>>)> = sources.iter().filter(|(source, _)| *source != Source::Link(index)).collect();
            if others.is_empty() {
                if link.tx_keyed {
                    link.tx_keyed = false;
                    let silence = vec![0i16; SAMPLES_PER_FRAME];
                    while !link.tx_pending.is_empty() {
                        link.tx_pending.push(link.encoder.encode(&silence));
                        if link.tx_pending.len() == GSM_FRAMES_PER_PACKET {
                            packets.push((link.address, protocol::voice_packet(link.tx_sequence, &std::mem::take(&mut link.tx_pending))));
                            link.tx_sequence = link.tx_sequence.wrapping_add(1);
                        }
                    }
                }
                continue;
            }
            link.tx_keyed = true;
            let frames: Vec<&Vec<i16>> = others.iter().filter_map(|(_, frame)| frame.as_ref()).collect();
            link.tx_pending.push(link.encoder.encode(&mix(&frames)));
            if link.tx_pending.len() == GSM_FRAMES_PER_PACKET {
                packets.push((link.address, protocol::voice_packet(link.tx_sequence, &std::mem::take(&mut link.tx_pending))));
                link.tx_sequence = link.tx_sequence.wrapping_add(1);
            }
        }
        for (address, packet) in packets {
            if let Err(error) = self.audio_socket.try_send_to(&packet, address) {
                log::debug!("EchoLink audio to {address} failed: {error}");
            }
        }

        // The hotspot hears all the stations, and is told when the last one stops
        let link_frames: Vec<&Vec<i16>> = sources.iter().filter(|(source, _)| matches!(source, Source::Link(_))).filter_map(|(_, frame)| frame.as_ref()).collect();
        if sources.iter().any(|(source, _)| matches!(source, Source::Link(_))) {
            self.bridge_active = true;
            if !link_frames.is_empty() {
                let _ = self.to_bridge.try_send(Audio::Voice(mix(&link_frames)));
            }
        } else if self.bridge_active {
            self.bridge_active = false;
            let _ = self.to_bridge.try_send(Audio::End);
        }
    }

    fn handle_bridge_audio(&mut self, audio: Audio, now: Instant) {
        match audio {
            Audio::Voice(samples) => {
                self.dstar_keyed = true;
                self.dstar_heard = now;
                self.dstar_buffer.push(samples);
            }
            Audio::End => self.dstar_keyed = false,
        }
    }

    // ---- Time ----

    fn tick(&mut self, now: Instant) {
        let mut index = 0;
        while index < self.links.len() {
            if !self.tick_link(index, now) {
                index += 1;
            }
        }
        if self.dstar_keyed && now.duration_since(self.dstar_heard) > TALKER_TIMEOUT {
            self.dstar_keyed = false;
        }
        self.mix_tick();
    }

    // Returns whether the link ended
    fn tick_link(&mut self, index: usize, now: Instant) -> bool {
        let link = &self.links[index];
        if !link.up {
            if now.duration_since(link.last_sent) >= CALL_RETRY_INTERVAL {
                if link.attempts >= MAX_CALL_ATTEMPTS {
                    self.link_ended(index, "no answer");
                    return true;
                }
                self.links[index].attempts += 1;
                self.send_hello(index, now);
            }
            return false;
        }
        if now.duration_since(link.last_heard) > CONNECTION_TIMEOUT {
            self.link_ended(index, "no response");
            return true;
        }
        if now.duration_since(link.last_sent) >= KEEP_ALIVE_INTERVAL {
            self.send_hello(index, now);
        }
        let link = &mut self.links[index];
        if link.rx_keyed && now.duration_since(link.rx_last_heard) > RX_LINGER {
            link.rx_keyed = false;
            log::info!("EchoLink: station {} unkeyed", link.callsign);
            self.publish();
        }
        false
    }
}

pub async fn run(echolink: Echolink, directory: Arc<Directory>, shared: Shared, speaker: Option<Speaker>, mut from_bridge: mpsc::Receiver<Audio>, to_bridge: mpsc::Sender<Audio>, mut commands: mpsc::Receiver<Command>) -> anyhow::Result<()> {
    let audio_socket = Arc::new(UdpSocket::bind(("0.0.0.0", echolink.udp_port)).await.with_context(|| format!("couldn't bind EchoLink audio port {}", echolink.udp_port))?);
    let control_socket = Arc::new(UdpSocket::bind(("0.0.0.0", echolink.udp_port + 1)).await.with_context(|| format!("couldn't bind EchoLink control port {}", echolink.udp_port + 1))?);
    log::info!("EchoLink node {} listening on ports {} and {}", echolink.callsign, echolink.udp_port, echolink.udp_port + 1);

    let (internal, mut internal_receiver) = mpsc::channel(64);
    let now = Instant::now();
    let mut node = Node {
        name: echolink.name.clone().unwrap_or_else(|| echolink.callsign.clone()),
        echolink,
        shared,
        audio_socket: audio_socket.clone(),
        control_socket: control_socket.clone(),
        directory,
        internal,
        to_bridge,
        speaker,
        links: Vec::new(),
        dstar_buffer: Buffer::default(),
        dstar_keyed: false,
        dstar_heard: now,
        bridge_active: false,
        verifying: HashSet::new(),
    };
    node.publish();
    let mut tick = interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let (mut audio_buffer, mut control_buffer) = (vec![0u8; 2048], vec![0u8; 2048]);
    loop {
        tokio::select! {
            received = audio_socket.recv_from(&mut audio_buffer) => {
                if let Ok((length, address)) = received {
                    let data = audio_buffer[..length].to_vec();
                    node.handle_audio_packet(&data, address, Instant::now());
                }
            }
            received = control_socket.recv_from(&mut control_buffer) => {
                if let Ok((length, address)) = received {
                    let data = control_buffer[..length].to_vec();
                    node.handle_control(&data, address, Instant::now());
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allstar::testing::{heard_frames, transmit, wait_for};

    struct TestNode {
        commands: mpsc::Sender<Command>,
        shared: Shared,
        to_node: mpsc::Sender<Audio>,
        from_node: mpsc::Receiver<Audio>,
    }

    // A node on a local port that reaches each station at a fixed "host:port"
    fn start_node(callsign: &str, port: u16, stations: &[(&str, u16)]) -> TestNode {
        let station_table: String = stations.iter().map(|(station, port)| format!("\"{station}\" = \"127.0.0.1:{port}\"\n")).collect();
        let echolink: Echolink = toml::from_str(&format!("callsign = \"{callsign}\"\nambeserver = \"127.0.0.1:2460\"\nudp_port = {port}\n[stations]\n{station_table}")).unwrap();
        let shared = Shared::default();
        let (commands, command_receiver) = mpsc::channel(16);
        let (to_node, from_bridge) = mpsc::channel(256);
        let (to_bridge, from_node) = mpsc::channel(256);
        tokio::spawn(run(echolink.clone(), Arc::new(Directory::new(&echolink)), shared.clone(), None, from_bridge, to_bridge, command_receiver));
        TestNode { commands, shared, to_node, from_node }
    }

    fn linked(node: &TestNode, station: &str) -> bool {
        state::read(&node.shared).links.iter().any(|link| link.peers.iter().any(|peer| peer.id == station && peer.connected))
    }

    fn has_no_links(node: &TestNode) -> bool {
        state::read(&node.shared).links.iter().all(|link| link.peers.is_empty())
    }

    #[tokio::test]
    async fn two_nodes_link_and_pass_audio_both_ways() {
        let mut first = start_node("N0AAA-L", 25198, &[("N0BBB-L", 25298)]);
        let mut second = start_node("N0BBB-L", 25298, &[("N0AAA-L", 25198)]);
        first.commands.send(Command::Link { target: "n0bbb-l".into() }).await.unwrap();
        wait_for("both nodes to show the link", || linked(&first, "N0BBB-L") && linked(&second, "N0AAA-L")).await;

        // Audio from the first node's bridge reaches the second node's, and the transmission ends cleanly
        transmit(&first.to_node, 30).await;
        let heard = heard_frames(&mut second.from_node, Duration::from_secs(3)).await.expect("the transmission to end");
        assert!((25..=40).contains(&heard), "second node heard {heard} of 30 frames");

        // And the other way
        transmit(&second.to_node, 20).await;
        assert!(heard_frames(&mut first.from_node, Duration::from_secs(3)).await.expect("the transmission to end") >= 15);

        // Either side can hang up, and the other follows
        first.commands.send(Command::Unlink { target: None }).await.unwrap();
        wait_for("both nodes to drop the link", || has_no_links(&first) && has_no_links(&second)).await;
    }

    #[tokio::test]
    async fn a_station_the_directory_doesnt_know_is_refused() {
        let node = start_node("N0CCC-L", 25398, &[]);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.send_to(&protocol::hello("N0ZZZ-L", "Intruder"), "127.0.0.1:25399").await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(has_no_links(&node));
    }

    #[tokio::test]
    async fn linking_to_a_station_that_doesnt_answer_gives_up() {
        let node = start_node("N0DDD-L", 25498, &[("N0SIL-L", 25598)]);
        node.commands.send(Command::Link { target: "N0SIL-L".into() }).await.unwrap();
        wait_for("the call to show", || state::read(&node.shared).links.iter().any(|link| link.peers.iter().any(|peer| !peer.connected))).await;
        assert!(!linked(&node, "N0SIL-L"));
        node.commands.send(Command::Unlink { target: Some("N0SIL-L".into()) }).await.unwrap();
        wait_for("the call to be dropped", || has_no_links(&node)).await;
    }
}
