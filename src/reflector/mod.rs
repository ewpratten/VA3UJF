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
use crate::dstar::{pad, Frame, Header, END_SLOW_DATA, FRAMES_PER_SUPERFRAME, NULL_SLOW_DATA, SILENT_AMBE, SYNC_SLOW_DATA};
use packets::{END_AMBE, HEADER_SEQUENCE, LAST_FRAME_FLAG};
use protocol::{implementation, Endpoint, ReflectorProtocol, Transmit};
use anyhow::{bail, Context};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::{interval, timeout, Instant};

// How the link is named in the status and the transmission log
pub const LINK_NAME: &str = "reflector";
pub const LINK_KIND: &str = "reflector";
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const REFUSED_RETRY_LIMIT: Duration = Duration::from_secs(300);
const LINK_TIMEOUT: Duration = Duration::from_secs(60);
const STREAM_TIMEOUT: Duration = Duration::from_secs(1);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

// A gap this many frames or wider is a late or repeated packet, not lost ones
const MAX_LOST_FRAMES: u8 = 10;
const SEQUENCE_MASK: u8 = 0x1F;
const REMEMBERED_FINISHED_STREAMS: usize = 4;
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
    connect_retry: Duration,
    last_poll: Instant,
}

// Stream bookkeeping for each direction. Streams from the reflector are passed on one at a time, and the header of one that
// opens meanwhile is kept so it can follow
#[derive(Default)]
struct Streams {
    transmit: Transmit,
    receive_id: Option<u16>,
    receive_last: Option<Instant>,
    receive_next_sequence: u8,
    waiting_header: Option<(u16, Header)>,
    finished_ids: VecDeque<u16>,
}

impl Streams {
    fn finish_receive(&mut self) {
        if let Some(stream_id) = self.receive_id.take() {
            if self.finished_ids.len() == REMEMBERED_FINISHED_STREAMS {
                self.finished_ids.pop_front();
            }
            self.finished_ids.push_back(stream_id);
        }
        self.receive_last = None;
    }
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
    let addresses = timeout(LOOKUP_TIMEOUT, lookup_host((reflector.address.as_str(), port))).await.with_context(|| format!("timed out resolving {}", reflector.address))??;
    let destination = addresses.into_iter().find(SocketAddr::is_ipv4).with_context(|| format!("couldn't resolve {}", reflector.address))?;
    if !module.is_ascii_uppercase() {
        bail!("reflector module must be a letter");
    }
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    log::info!("Linking {} {module} at {destination} over {:?}", reflector.name, reflector.protocol);
    let now = Instant::now();
    Ok(Active { reflector, protocol, module: module as u8, socket, destination, connected: false, last_heard: now, last_connect: None, connect_retry: CONNECT_RETRY_INTERVAL, last_poll: now })
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
            let mut stream_id = [0u8; 2];
            getrandom::fill(&mut stream_id).expect("the system has no source of randomness");
            transmit.id = u16::from_le_bytes(stream_id) | 1;
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
    // A refusal is asked again less and less often
    if is_nak {
        log::error!("{} refused the link, asking again in {:?}", active.target_text(), active.connect_retry);
        active.connected = false;
        active.last_connect = Some(Instant::now());
        active.connect_retry = (active.connect_retry * 2).min(REFUSED_RETRY_LIMIT);
        return Ok(());
    }
    if is_ack {
        if !active.connected {
            log::info!("Linked to {}", active.target_text());
            speech::say(speaker, format!("Linked to reflector {}.", speech::spoken_reflector(&active.target_text())));
        }
        active.connected = true;
        active.connect_retry = CONNECT_RETRY_INTERVAL;
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

    match active.protocol.decode_voice(packet) {
        Some(voice) => receive_voice(streams, voice, from_reflector).await,
        None => Ok(()),
    }
}

// Pass one stream at a time on to the router, ignoring others and anything late from one that's finished
async fn receive_voice(streams: &mut Streams, voice: protocol::Voice, from_reflector: &mpsc::Sender<Frame>) -> anyhow::Result<()> {
    let protocol::Voice { stream_id, header, sequence, ambe, slow_data } = voice;
    if streams.finished_ids.contains(&stream_id) {
        return Ok(());
    }
    if streams.receive_id.is_some_and(|current| current != stream_id) {
        if let (HEADER_SEQUENCE, Some(header)) = (sequence, header) {
            streams.waiting_header = Some((stream_id, header));
        }
        return Ok(());
    }

    // A stream starts from its own header, or one kept from when it opened while another was running
    if streams.receive_id.is_none() {
        let waiting = streams.waiting_header.filter(|(waiting_id, _)| *waiting_id == stream_id).map(|(_, header)| header);
        let Some(header) = header.or(waiting) else { return Ok(()) };
        if sequence & LAST_FRAME_FLAG != 0 {
            return Ok(());
        }
        log::debug!("Reflector stream {stream_id:04x} from {}", header.my_call_text());
        streams.receive_id = Some(stream_id);
        streams.receive_next_sequence = if sequence == HEADER_SEQUENCE { 0 } else { sequence & SEQUENCE_MASK };
        streams.waiting_header = None;
        from_reflector.send(Frame::Header(header)).await?;
    }
    streams.receive_last = Some(Instant::now());
    let position = sequence & SEQUENCE_MASK;
    if sequence == HEADER_SEQUENCE || position >= FRAMES_PER_SUPERFRAME {
        return Ok(());
    }

    // Lost frames are filled with silence so the radio keeps time, and late or repeated ones are dropped
    let missing = (position + FRAMES_PER_SUPERFRAME - streams.receive_next_sequence) % FRAMES_PER_SUPERFRAME;
    if missing >= MAX_LOST_FRAMES {
        return Ok(());
    }
    for lost in 0..missing {
        let lost_position = (streams.receive_next_sequence + lost) % FRAMES_PER_SUPERFRAME;
        from_reflector.send(radio_voice(lost_position, SILENT_AMBE, NULL_SLOW_DATA)).await?;
    }
    streams.receive_next_sequence = (position + 1) % FRAMES_PER_SUPERFRAME;
    if sequence & LAST_FRAME_FLAG != 0 {
        streams.finish_receive();
        from_reflector.send(Frame::End).await?;
    } else {
        from_reflector.send(radio_voice(position, ambe, slow_data)).await?;
    }
    Ok(())
}

// A frame of voice for the radio, with the sync the radio needs at the start of each superframe
fn radio_voice(position: u8, ambe: [u8; 9], slow_data: [u8; 3]) -> Frame {
    Frame::Voice { sequence: position, ambe, slow_data: if position == 0 { SYNC_SLOW_DATA } else { slow_data } }
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

                // A stream cut off by the change is ended for the radio, and nothing carries over to the next link
                if streams.receive_id.is_some() {
                    let _ = from_reflector.send(Frame::End).await;
                }
                streams = Streams::default();
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
                    if !link.connected && link.last_connect.is_none_or(|sent| now.duration_since(sent) >= link.connect_retry) {
                        link.last_connect = Some(now);
                        let packet = link.protocol.connect_packet(&link.endpoint(&config), false);
                        link.send(&packet).await;
                    }
                    if link.connected && now.duration_since(link.last_poll) >= link.protocol.poll_interval() {
                        link.last_poll = now;
                        let packet = link.protocol.poll_packet(&link.endpoint(&config));
                        link.send(&packet).await;
                    }
                }

                // A stream that goes quiet without an end marker gets one
                if streams.receive_last.is_some_and(|heard| heard.elapsed() > STREAM_TIMEOUT) {
                    streams.finish_receive();
                    let _ = from_reflector.send(Frame::End).await;
                }
            }
        }
        publish(&shared, active.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(my_call: &str) -> Header {
        Header { flags: [0; 3], repeater_2: pad("DCS405 A"), repeater_1: pad("VA3UJF B"), your_call: pad("CQCQCQ"), my_call: pad(my_call), my_suffix: pad("") }
    }

    // A DCS packet, which carries its header in every frame
    fn dcs(stream_id: u16, my_call: &str, sequence: u8) -> protocol::Voice {
        protocol::Voice { stream_id, header: Some(header(my_call)), sequence, ambe: [sequence; 9], slow_data: [7; 3] }
    }

    async fn heard(streams: &mut Streams, packets: Vec<protocol::Voice>) -> Vec<Frame> {
        let (sender, mut receiver) = mpsc::channel(64);
        for packet in packets {
            receive_voice(streams, packet, &sender).await.unwrap();
        }
        drop(sender);
        let mut frames = Vec::new();
        while let Some(frame) = receiver.recv().await {
            frames.push(frame);
        }
        frames
    }

    #[tokio::test]
    async fn a_second_stream_waits_and_stray_ends_are_ignored() {
        let mut streams = Streams::default();
        let frames = heard(&mut streams, vec![dcs(1, "FIRST", 0), dcs(2, "SECOND", 0), dcs(2, "SECOND", 1 | LAST_FRAME_FLAG), dcs(1, "FIRST", 1), dcs(1, "FIRST", 2 | LAST_FRAME_FLAG), dcs(1, "FIRST", 2)]).await;
        assert_eq!(frames.len(), 4, "{frames:?}");
        assert!(matches!(&frames[0], Frame::Header(header) if header.my_call_text() == "FIRST"));
        assert!(matches!(frames[1..], [Frame::Voice { sequence: 0, .. }, Frame::Voice { sequence: 1, .. }, Frame::End]));
    }

    #[tokio::test]
    async fn a_dextra_stream_that_opened_meanwhile_follows_on() {
        let mut streams = Streams::default();
        let dextra_header = |stream_id| protocol::Voice { stream_id, header: Some(header("SECOND")), sequence: HEADER_SEQUENCE, ambe: [0; 9], slow_data: [0; 3] };
        let dextra_voice = |stream_id, sequence| protocol::Voice { stream_id, header: None, sequence, ambe: [1; 9], slow_data: [2; 3] };
        let frames = heard(&mut streams, vec![dextra_header(1), dextra_voice(1, 0), dextra_header(2), dextra_voice(1, 1 | LAST_FRAME_FLAG), dextra_voice(2, 3)]).await;
        assert!(matches!(frames.last(), Some(Frame::Voice { sequence: 3, .. })), "{frames:?}");
        assert!(matches!(&frames[frames.len() - 2], Frame::Header(header) if header.my_call_text() == "SECOND"));
    }

    #[tokio::test]
    async fn lost_frames_are_filled_and_each_superframe_starts_with_sync() {
        let mut streams = Streams::default();
        let frames = heard(&mut streams, vec![dcs(1, "FIRST", 18), dcs(1, "FIRST", 2), dcs(1, "FIRST", 1)]).await;
        let voice: Vec<(u8, [u8; 9], [u8; 3])> = frames.iter().filter_map(|frame| match frame {
            Frame::Voice { sequence, ambe, slow_data } => Some((*sequence, *ambe, *slow_data)),
            _ => None,
        }).collect();
        assert_eq!(voice.iter().map(|(sequence, ..)| *sequence).collect::<Vec<_>>(), [18, 19, 20, 0, 1, 2]);
        assert_eq!((voice[1].1, voice[1].2), (SILENT_AMBE, NULL_SLOW_DATA));
        assert_eq!(voice[3].2, SYNC_SLOW_DATA);
        assert_eq!(voice[5].1, [2; 9]);
    }

    #[tokio::test]
    async fn voice_without_a_header_starts_nothing() {
        let mut streams = Streams::default();
        let frames = heard(&mut streams, vec![protocol::Voice { stream_id: 9, header: None, sequence: 4, ambe: [1; 9], slow_data: [2; 3] }]).await;
        assert!(frames.is_empty() && streams.receive_last.is_none());
    }
}
