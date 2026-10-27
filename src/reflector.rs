// Reflector links over DCS and DExtra, as an outgoing client of a single reflector at a time

use crate::config::{Config, Protocol, Reflector};
use crate::hosts::{self, Directory};
use crate::state::{self, LinkStatus, Shared};
use crate::dstar::{pad, Frame, Header, END_SLOW_DATA, FRAMES_PER_SUPERFRAME};
use anyhow::{bail, Context};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

const DCS_PORT: u16 = 30051;
const DEXTRA_PORT: u16 = 30001;
const DCS_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEXTRA_POLL_INTERVAL: Duration = Duration::from_secs(10);
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const LINK_TIMEOUT: Duration = Duration::from_secs(60);
const STREAM_TIMEOUT: Duration = Duration::from_secs(1);
const END_AMBE: [u8; 9] = [0x55, 0x55, 0x55, 0x55, 0xC8, 0x7A, 0x00, 0x00, 0x00];
const DCS_CONNECT_HTML: &str = "<table border=\"0\" width=\"95%\"><tr><td width=\"4%\"><img border=\"0\" src=hotspot.jpg></td><td width=\"96%\"><font size=\"2\"><b>HOTSPOT</b> va3ujf</font></td></tr></table>";

pub enum Command {
    Link { name: String, module: char },
    Unlink,
}

struct Active {
    reflector: Reflector,
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
    transmit_id: u16,
    transmit_sequence: u8,
    transmit_count: u32,
    transmit_header: Option<Header>,
    receive_id: Option<u16>,
    receive_last: Option<Instant>,
}

impl Active {
    fn protocol(&self) -> Protocol {
        self.reflector.protocol
    }

    fn target_text(&self) -> String {
        format!("{} {}", self.reflector.name, self.module as char)
    }

    // The 11 byte connect/unlink prefix both protocols share: station callsign, local module, remote module, NUL
    fn connect_prefix(&self, config: &Config, remote_module: u8) -> Vec<u8> {
        let mut packet = pad::<8>(&config.station.callsign).to_vec();
        packet[7] = b' ';
        packet.extend([config.station.module.as_bytes()[0], remote_module, 0]);
        packet
    }

    fn connect_packet(&self, config: &Config, unlink: bool) -> Vec<u8> {
        let remote_module = if unlink { b' ' } else { self.module };
        let mut packet = self.connect_prefix(config, remote_module);
        if self.protocol() == Protocol::Dcs {
            packet.extend(pad::<8>(&self.reflector.name));
            if !unlink {
                let mut html = DCS_CONNECT_HTML.as_bytes().to_vec();
                html.resize(500, 0);
                packet.extend(html);
            }
        }
        packet
    }

    fn poll_packet(&self, config: &Config) -> Vec<u8> {
        let mut own = pad::<8>(&config.station.callsign);
        own[7] = config.station.module.as_bytes()[0];
        match self.protocol() {
            Protocol::Dcs => {
                let mut reflector = pad::<8>(&self.reflector.name);
                reflector[7] = self.module;
                [&own[..], &[0], &reflector[..]].concat()
            }
            Protocol::Dextra => [&own[..], &[0]].concat(),
        }
    }

    async fn send(&self, packet: &[u8]) {
        if let Err(error) = self.socket.send_to(packet, self.destination).await {
            log::debug!("Reflector send failed: {error}");
        }
    }
}

async fn open(directory: &Directory, name: &str, module: char) -> anyhow::Result<Active> {
    let reflector = hosts::find(directory, name).with_context(|| format!("unknown reflector {name}"))?;
    let port = reflector.port.unwrap_or(if reflector.protocol == Protocol::Dcs { DCS_PORT } else { DEXTRA_PORT });
    let destination = lookup_host((reflector.address.as_str(), port)).await?.find(SocketAddr::is_ipv4).with_context(|| format!("couldn't resolve {}", reflector.address))?;
    if !module.is_ascii_uppercase() {
        bail!("reflector module must be a letter");
    }
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    log::info!("Linking {} {module} at {destination} over {:?}", reflector.name, reflector.protocol);
    let now = Instant::now();
    Ok(Active { reflector, module: module as u8, socket, destination, connected: false, last_heard: now, last_connect: None, last_poll: now })
}

fn transmit_voice(active: &Active, streams: &mut Streams, ambe: [u8; 9], slow_data: [u8; 3], end: bool) -> Option<Vec<u8>> {
    let header = streams.transmit_header.as_ref()?;
    let sequence = if end { streams.transmit_sequence | 0x40 } else { streams.transmit_sequence };
    let packet = match active.protocol() {
        Protocol::Dextra => [b"DSVT\x20\x00\x00\x00\x20\x00\x00\x00", &streams.transmit_id.to_le_bytes()[..], &[sequence], &ambe, &slow_data].concat(),
        Protocol::Dcs => {
            let mut packet = vec![0u8; 100];
            packet[0..4].copy_from_slice(b"0001");
            packet[4..43].copy_from_slice(&header.body());
            packet[43..45].copy_from_slice(&streams.transmit_id.to_le_bytes());
            packet[45] = sequence;
            packet[46..55].copy_from_slice(&ambe);
            packet[55..58].copy_from_slice(&slow_data);
            packet[58..61].copy_from_slice(&streams.transmit_count.to_le_bytes()[..3]);
            packet[61] = 0x01;
            packet[63] = 0x21;
            packet
        }
    };
    streams.transmit_sequence = (streams.transmit_sequence + 1) % FRAMES_PER_SUPERFRAME;
    streams.transmit_count += 1;
    Some(packet)
}

// Turn a frame heading out to the reflector into packets, updating the transmit stream
async fn transmit(active: &Active, config: &Config, streams: &mut Streams, frame: Frame) {
    match frame {
        Frame::Header(mut header) => {
            header.repeater_1 = pad::<8>(&format!("{:<7}{}", config.station.callsign, config.station.module));
            header.repeater_2 = pad::<8>(&format!("{:<7}{}", active.reflector.name, active.module as char));
            header.your_call = pad::<8>("CQCQCQ");
            streams.transmit_id = (streams.transmit_id.wrapping_mul(25173).wrapping_add(13849)) | 1;
            streams.transmit_sequence = 0;
            streams.transmit_count = 0;
            if active.protocol() == Protocol::Dextra {
                let packet = [b"DSVT\x10\x00\x00\x00\x20\x00\x00\x00", &streams.transmit_id.to_le_bytes()[..], &[0x80], &header.to_bytes()].concat();
                for _ in 0..2 {
                    active.send(&packet).await;
                }
            }
            streams.transmit_header = Some(header);
        }
        Frame::Voice { ambe, slow_data, .. } => {
            if let Some(packet) = transmit_voice(active, streams, ambe, slow_data, false) {
                active.send(&packet).await;
            }
        }
        Frame::End => {
            if let Some(packet) = transmit_voice(active, streams, END_AMBE, END_SLOW_DATA, true) {
                active.send(&packet).await;
            }
            streams.transmit_header = None;
        }
    }
}

// Turn a packet from the reflector into frames for the modem.
async fn receive(active: &mut Active, config: &Config, streams: &mut Streams, packet: &[u8], from_reflector: &mpsc::Sender<Frame>) -> anyhow::Result<()> {
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
        }
        active.connected = true;
        return Ok(());
    }
    if !active.connected {
        return Ok(());
    }

    // Keepalives are answered in kind
    let is_poll = match active.protocol() {
        Protocol::Dcs => length == 22 || length == 17,
        Protocol::Dextra => length == 9,
    };
    if is_poll {
        if length != 17 {
            let reply = active.poll_packet(config);
            active.send(&reply).await;
        }
        return Ok(());
    }

    // Voice, as (stream id, header, sequence, ambe, slow data)
    let parsed = match active.protocol() {
        Protocol::Dextra if packet.starts_with(b"DSVT") && length == 56 && packet[14] == 0x80 => {
            Some((u16::from_le_bytes([packet[12], packet[13]]), Header::from_body(&packet[15..54]), 0x80, [0; 9], [0; 3]))
        }
        Protocol::Dextra if packet.starts_with(b"DSVT") && length == 27 => {
            Some((u16::from_le_bytes([packet[12], packet[13]]), None, packet[14], packet[15..24].try_into()?, packet[24..27].try_into()?))
        }
        Protocol::Dcs if packet.starts_with(b"0001") && length == 100 => {
            Some((u16::from_le_bytes([packet[43], packet[44]]), Header::from_body(&packet[4..43]), packet[45], packet[46..55].try_into()?, packet[55..58].try_into()?))
        }
        _ => None,
    };
    let Some((stream_id, header, sequence, ambe, slow_data)) = parsed else { return Ok(()) };
    if sequence == 0x80 {
        return Ok(forward_header(streams, stream_id, header, from_reflector).await);
    }

    // DCS carries the header in every voice packet, so a new id starts a stream
    if streams.receive_id != Some(stream_id) {
        forward_header(streams, stream_id, header, from_reflector).await;
    }
    streams.receive_last = Some(Instant::now());
    if sequence & 0x40 != 0 {
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

// Run the reflector link manager, taking link and unlink commands, and bridging frames in both directions
pub async fn run(config: Config, directory: Directory, mut commands: mpsc::Receiver<Command>, from_reflector: mpsc::Sender<Frame>, mut to_reflector: mpsc::Receiver<Frame>, shared: Shared) {
    let mut active: Option<Active> = None;
    let mut streams = Streams::default();
    let mut tick = interval(Duration::from_millis(250));
    let mut buffer = vec![0u8; 2048];

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
                        if let Err(error) = receive(link, &config, &mut streams, &packet, &from_reflector).await {
                            log::debug!("Bad reflector packet: {error}");
                        }
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { return };

                // Politely drop the old link before changing anything
                if let Some(link) = active.take() {
                    let packet = link.connect_packet(&config, true);
                    link.send(&packet).await;
                    log::info!("Unlinked from {}", link.target_text());
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
                        let packet = link.connect_packet(&config, false);
                        link.send(&packet).await;
                    }
                    let poll_interval = if link.protocol() == Protocol::Dcs { DCS_POLL_INTERVAL } else { DEXTRA_POLL_INTERVAL };
                    if link.connected && now.duration_since(link.last_poll) >= poll_interval {
                        link.last_poll = now;
                        let packet = link.poll_packet(&config);
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
        let link = LinkStatus { target: active.as_ref().map(Active::target_text), connected: active.as_ref().is_some_and(|link| link.connected) };
        state::update(&shared, |snapshot| snapshot.link = link);
    }
}
