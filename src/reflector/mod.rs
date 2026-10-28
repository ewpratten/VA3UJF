// Reflector links over DCS and DExtra, as an outgoing client of a single reflector at a time

mod dcs;
mod dextra;
mod packets;
pub mod park;
mod protocol;

use crate::config::{Config, Reflector};
use crate::hosts::{self, Directory};
use crate::router::Port;
use crate::speech::{self, Speaker};
use crate::state::{self, Shared};
use crate::dstar::{pad, Frame, Header, END_SLOW_DATA, FRAMES_PER_SUPERFRAME};
use packets::{END_AMBE, HEADER_SEQUENCE, LAST_FRAME_FLAG};
use protocol::{implementation, Endpoint, ReflectorProtocol, Transmit};
use anyhow::{bail, Context};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

// How the link is named in the status and the transmission log
pub const LINK_NAME: &str = "reflector";
pub const LINK_KIND: &str = "reflector";
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const LINK_TIMEOUT: Duration = Duration::from_secs(60);
const STREAM_TIMEOUT: Duration = Duration::from_secs(1);
pub enum Command {
    Link { name: String, module: char },
    Unlink,
}

struct Active {
    reflector: Reflector,
    protocol: Box<dyn ReflectorProtocol>,
    module: u8,
    socket: UdpSocket,
    destination: SocketAddr,
    connected: bool,
    last_heard: Instant,
    last_connect: Option<Instant>,
    last_poll: Instant,
}

// Stream bookkeeping for each direction
#[derive(Default)]
struct Streams {
    transmit: Transmit,
    receive_id: Option<u16>,
    receive_last: Option<Instant>,
}

impl Active {
    fn target_text(&self) -> String {
        format!("{} {}", self.reflector.name, self.module as char)
    }

    fn endpoint<'a>(&'a self, config: &'a Config) -> Endpoint<'a> {
        Endpoint { callsign: &config.station.callsign, local_module: config.station.module.as_bytes()[0], reflector: &self.reflector.name, module: self.module }
    }

    async fn send(&self, packet: &[u8]) {
        if let Err(error) = self.socket.send_to(packet, self.destination).await {
            log::debug!("Reflector send failed: {error}");
        }
    }
}

async fn open(directory: &Directory, name: &str, module: char) -> anyhow::Result<Active> {
    let reflector = hosts::find(directory, name).with_context(|| format!("unknown reflector {name}"))?;
    let protocol = implementation(reflector.protocol);
    let port = reflector.port.unwrap_or(protocol.default_port());
    let destination = lookup_host((reflector.address.as_str(), port)).await?.find(SocketAddr::is_ipv4).with_context(|| format!("couldn't resolve {}", reflector.address))?;
    if !module.is_ascii_uppercase() {
        bail!("reflector module must be a letter");
    }
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    log::info!("Linking {} {module} at {destination} over {:?}", reflector.name, reflector.protocol);
    let now = Instant::now();
    Ok(Active { reflector, protocol, module: module as u8, socket, destination, connected: false, last_heard: now, last_connect: None, last_poll: now })
}

fn transmit_voice(active: &Active, transmit: &mut Transmit, ambe: [u8; 9], slow_data: [u8; 3], end: bool) -> Option<Vec<u8>> {
    transmit.header.as_ref()?;
    let sequence = if end { transmit.sequence | LAST_FRAME_FLAG } else { transmit.sequence };
    let packet = active.protocol.voice_packet(transmit, sequence, ambe, slow_data);
    transmit.sequence = (transmit.sequence + 1) % FRAMES_PER_SUPERFRAME;
    transmit.count += 1;
    Some(packet)
}

// Turn a frame heading out to the reflector into packets, updating the transmit stream
async fn transmit(active: &Active, config: &Config, streams: &mut Streams, frame: Frame) {
    let transmit = &mut streams.transmit;
    match frame {
        Frame::Header(mut header) => {
            header.repeater_1 = pad::<8>(&format!("{:<7}{}", config.station.callsign, config.station.module));
            header.repeater_2 = pad::<8>(&format!("{:<7}{}", active.reflector.name, active.module as char));
            header.your_call = pad::<8>("CQCQCQ");
            transmit.id = (transmit.id.wrapping_mul(25173).wrapping_add(13849)) | 1;
            transmit.sequence = 0;
            transmit.count = 0;
            transmit.header = Some(header);
            for packet in active.protocol.header_packets(transmit) {
                active.send(&packet).await;
            }
        }
        Frame::Voice { ambe, slow_data, .. } => {
            if let Some(packet) = transmit_voice(active, transmit, ambe, slow_data, false) {
                active.send(&packet).await;
            }
        }
        Frame::End => {
            if let Some(packet) = transmit_voice(active, transmit, END_AMBE, END_SLOW_DATA, true) {
                active.send(&packet).await;
            }
            transmit.header = None;
        }
    }
}

// Turn a packet from the reflector into frames for the router
async fn receive(active: &mut Active, config: &Config, streams: &mut Streams, packet: &[u8], from_reflector: &mpsc::Sender<Frame>, speaker: &Option<Speaker>) -> anyhow::Result<()> {
    active.last_heard = Instant::now();
    let length = packet.len();
    let is_ack = length == 14 && &packet[10..13] == b"ACK";
    let is_nak = length == 14 && &packet[10..13] == b"NAK";
    if is_nak {
        log::error!("{} refused the link", active.target_text());
        active.connected = false;
        return Ok(());
    }
    if is_ack {
        if !active.connected {
            log::info!("Linked to {}", active.target_text());
            speech::say(speaker, format!("Linked to reflector {}.", speech::spoken_reflector(&active.target_text())));
        }
        active.connected = true;
        return Ok(());
    }
    if !active.connected {
        return Ok(());
    }

    // Keepalives are answered in kind
    if let Some(wants_reply) = active.protocol.keepalive(packet) {
        if wants_reply {
            let reply = active.protocol.poll_packet(&active.endpoint(config));
            active.send(&reply).await;
        }
        return Ok(());
    }

    let Some(protocol::Voice { stream_id, header, sequence, ambe, slow_data }) = active.protocol.decode_voice(packet) else { return Ok(()) };
    if sequence == HEADER_SEQUENCE {
        return Ok(forward_header(streams, stream_id, header, from_reflector).await);
    }

    // DCS carries the header in every voice packet, so a new id starts a stream
    if streams.receive_id != Some(stream_id) {
        forward_header(streams, stream_id, header, from_reflector).await;
    }
    streams.receive_last = Some(Instant::now());
    if sequence & LAST_FRAME_FLAG != 0 {
        streams.receive_id = None;
        streams.receive_last = None;
        from_reflector.send(Frame::End).await?;
    } else if streams.receive_id == Some(stream_id) {
        from_reflector.send(Frame::Voice { sequence, ambe, slow_data }).await?;
    }
    Ok(())
}

async fn forward_header(streams: &mut Streams, stream_id: u16, header: Option<Header>, from_reflector: &mpsc::Sender<Frame>) {
    if streams.receive_id == Some(stream_id) {
        return;
    }
    if let Some(header) = header {
        log::debug!("Reflector stream {stream_id:04x} from {}", header.my_call_text());
        streams.receive_id = Some(stream_id);
        streams.receive_last = Some(Instant::now());
        let _ = from_reflector.send(Frame::Header(header)).await;
    }
}

fn publish(shared: &Shared, active: Option<&Active>) {
    let target = active.map(Active::target_text);
    let connected = active.is_some_and(|link| link.connected);
    let spoken = match (&target, connected) {
        (Some(target), true) => format!("Linked to reflector {}.", speech::spoken_reflector(target)),
        (Some(target), false) => format!("Linking to reflector {}.", speech::spoken_reflector(target)),
        (None, _) => "Not linked to a reflector.".to_string(),
    };
    state::update(shared, |snapshot| {
        let status = state::link_mut(snapshot, LINK_NAME, LINK_KIND);
        (status.target, status.connected, status.spoken) = (target, connected, spoken);
    });
}

// Run the reflector link manager, taking link and unlink commands, and bridging frames in both directions
pub async fn run(config: Config, directory: Directory, mut commands: mpsc::Receiver<Command>, port: Port, shared: Shared, speaker: Option<Speaker>) {
    let Port { to_router: from_reflector, from_router: mut to_reflector } = port;
    let mut active: Option<Active> = None;
    let mut streams = Streams::default();
    let mut tick = interval(Duration::from_millis(250));
    let mut buffer = vec![0u8; 2048];
    publish(&shared, None);

    // Link the startup reflector, written as "NAME M"
    if let Some(startup) = &config.startup_reflector {
        let (name, module) = startup.split_once(' ').unwrap_or((startup, "A"));
        match open(&directory, name, module.chars().next().unwrap_or('A')).await {
            Ok(link) => active = Some(link),
            Err(error) => log::error!("Couldn't link startup reflector: {error:#}"),
        }
    }

    loop {
        // Wait on the socket only while linked
        let socket_ready = async {
            match &active {
                Some(link) => link.socket.recv_from(&mut buffer).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            received = socket_ready => {
                let Ok((length, source)) = received else { continue };
                let packet = buffer[..length].to_vec();
                if let Some(link) = active.as_mut() {
                    if source == link.destination {
                        if let Err(error) = receive(link, &config, &mut streams, &packet, &from_reflector, &speaker).await {
                            log::debug!("Bad reflector packet: {error}");
                        }
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { return };

                // Politely drop the old link before changing anything
                if let Some(link) = active.take() {
                    let packet = link.protocol.connect_packet(&link.endpoint(&config), true);
                    link.send(&packet).await;
                    log::info!("Unlinked from {}", link.target_text());
                    if link.connected {
                        speech::say(&speaker, format!("Unlinked from reflector {}.", speech::spoken_reflector(&link.target_text())));
                    }
                }
                if let Command::Link { name, module } = command {
                    match open(&directory, &name, module).await {
                        Ok(link) => active = Some(link),
                        Err(error) => log::error!("Couldn't link {name}: {error:#}"),
                    }
                }
            }
            frame = to_reflector.recv() => {
                let Some(frame) = frame else { return };
                if let Some(link) = active.as_ref().filter(|link| link.connected) {
                    transmit(link, &config, &mut streams, frame).await;
                }
            }
            _ = tick.tick() => {
                if let Some(link) = active.as_mut() {
                    let now = Instant::now();

                    // Keep dialing until the reflector answers, and again if it goes quiet
                    if link.connected && now.duration_since(link.last_heard) > LINK_TIMEOUT {
                        log::warn!("{} stopped answering", link.target_text());
                        link.connected = false;
                    }
                    if !link.connected && link.last_connect.is_none_or(|sent| now.duration_since(sent) >= CONNECT_RETRY_INTERVAL) {
                        link.last_connect = Some(now);
                        let packet = link.protocol.connect_packet(&link.endpoint(&config), false);
                        link.send(&packet).await;
                    }
                    if link.connected && now.duration_since(link.last_poll) >= link.protocol.poll_interval() {
                        link.last_poll = now;
                        let packet = link.protocol.poll_packet(&link.endpoint(&config));
                        link.send(&packet).await;
                    }

                    // A stream that goes quiet without an end marker gets one
                    if streams.receive_last.is_some_and(|heard| now.duration_since(heard) > STREAM_TIMEOUT) {
                        streams.receive_id = None;
                        streams.receive_last = None;
                        let _ = from_reflector.send(Frame::End).await;
                    }
                }
            }
        }
        publish(&shared, active.as_ref());
    }
}
