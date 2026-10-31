// The node itself: a client of one M17 reflector at a time, and the Codec 2 voice between that reflector and the hotspot's audio

use super::protocol::{self, Control, StreamPacket, DEFAULT_PORT, LAST_FRAME_FLAG};
use super::{Command, LINK_KIND, LINK_NAME};
use crate::codec::{Audio, SAMPLES_PER_FRAME};
use crate::config::{Station, M17};
use crate::live::Event as LiveEvent;
use crate::speech::{self, Speaker};
use crate::state::{self, PeerStatus, Shared};
use anyhow::{bail, Context};
use codec2::{Codec2, Codec2Mode};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};
use zerocopy::IntoBytes;

const TICK: Duration = Duration::from_millis(20);
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);

// Reflectors ping every few seconds, so a link that has gone this long without a word is dead
const LINK_TIMEOUT: Duration = Duration::from_secs(30);

// A stream that stops without its last frame is treated as ended after this long
const STREAM_TIMEOUT: Duration = Duration::from_secs(1);
const CODEC_FRAME_BYTES: usize = 8;
const FRAMES_PER_PACKET: usize = 2;

struct Link {
    name: String,
    module: u8,
    address: SocketAddr,
    connected: bool,
    last_heard: Instant,
    last_connect: Option<Instant>,
}

impl Link {
    fn target_text(&self) -> String {
        format!("{} {}", self.name, self.module as char)
    }
}

// The stream being heard from the reflector, of which only one is passed on at a time
struct Heard {
    stream_id: u16,
    last_heard: Instant,
}

// The stream being sent to the reflector, with the Codec 2 frames waiting for a partner to fill a packet
struct Sending {
    stream_id: u16,
    frame_number: u16,
    pending: Vec<[u8; CODEC_FRAME_BYTES]>,
}

struct Node {
    callsign: String,
    reflectors: HashMap<String, String>,
    shared: Shared,
    socket: Arc<UdpSocket>,
    to_bridge: mpsc::Sender<Audio>,
    speaker: Option<Speaker>,
    link: Option<Link>,
    heard: Option<Heard>,
    sending: Option<Sending>,

    // Audio from the reflector, played out at the pace it was spoken
    playout: VecDeque<Audio>,
    encoder: Codec2,
    decoder: Codec2,
}

fn encode_frame(encoder: &mut Codec2, samples: &[i16]) -> [u8; CODEC_FRAME_BYTES] {
    let mut frame = [0; CODEC_FRAME_BYTES];
    encoder.encode(&mut frame, samples);
    frame
}

impl Node {
    async fn send(&self, packet: &[u8]) {
        if let Some(link) = &self.link
            && let Err(error) = self.socket.send_to(packet, link.address).await
        {
            log::debug!("M17 send failed: {error}");
        }
    }

    async fn link(&mut self, name: &str, module: char) -> anyhow::Result<()> {
        if !module.is_ascii_alphabetic() {
            bail!("reflector module must be a letter");
        }
        let (known_name, address) = self.reflectors.iter().find(|(known, _)| known.eq_ignore_ascii_case(name)).with_context(|| format!("unknown M17 reflector {name}"))?;
        let address = match address.rsplit_once(':') {
            Some((_, port)) if port.parse::<u16>().is_ok() => address.clone(),
            _ => format!("{address}:{DEFAULT_PORT}"),
        };
        let destination = lookup_host(address.as_str()).await?.find(SocketAddr::is_ipv4).with_context(|| format!("couldn't resolve {address}"))?;
        let module = module.to_ascii_uppercase() as u8;
        log::info!("Linking {known_name} {} at {destination}", module as char);
        self.link = Some(Link { name: known_name.clone(), module, address: destination, connected: false, last_heard: Instant::now(), last_connect: None });
        Ok(())
    }

    // Politely drop the link, and let the bridge know if a stream was cut short
    async fn unlink(&mut self) {
        let Some(link) = self.link.as_ref() else { return };
        let (target, connected) = (link.target_text(), link.connected);
        self.send(&protocol::disconnect_packet(&self.callsign)).await;
        self.link = None;
        log::info!("Unlinked from {target}");
        if connected {
            speech::say(&self.speaker, format!("Unlinked from M17 reflector {}.", speech::spoken_reflector(&target)));
        }
        if self.heard.take().is_some() {
            self.playout.push_back(Audio::End);
        }
    }

    // ---- Packets in ----

    async fn receive(&mut self, packet: &[u8], source: SocketAddr) {
        let Some(link) = self.link.as_mut().filter(|link| link.address == source) else { return };
        link.last_heard = Instant::now();
        let Some(control) = protocol::parse_control(packet) else {
            if link.connected && let Some(stream) = StreamPacket::parse(packet).filter(StreamPacket::is_voice_3200) {
                self.receive_voice(stream);
            }
            return;
        };
        match control {
            Control::Ack if !link.connected => {
                link.connected = true;
                let target = link.target_text();
                log::info!("Linked to {target}");
                speech::say(&self.speaker, format!("Linked to M17 reflector {}.", speech::spoken_reflector(&target)));
            }
            Control::Nack => {
                log::error!("{} refused the link", link.target_text());
                self.link = None;
            }

            // The reflector let go of us, so dial again
            Control::Disconnect => link.connected = false,
            Control::Ping => self.send(&protocol::pong_packet(&self.callsign)).await,
            Control::Ack | Control::Pong => {}
        }
    }

    fn receive_voice(&mut self, stream: StreamPacket) {
        let stream_id = stream.stream_id.get();

        // Whoever is already talking keeps the channel, and the hotspot's own transmission comes first
        if self.sending.is_some() || self.heard.as_ref().is_some_and(|heard| heard.stream_id != stream_id) {
            return;
        }
        match self.heard.as_mut() {
            Some(heard) => heard.last_heard = Instant::now(),
            None => {
                log::info!("M17 -> D-STAR: {}", protocol::decode_address(stream.source));
                self.heard = Some(Heard { stream_id, last_heard: Instant::now() });
            }
        }
        for frame in stream.payload.chunks_exact(CODEC_FRAME_BYTES) {
            let mut samples = vec![0; SAMPLES_PER_FRAME];
            self.decoder.decode(&mut samples, frame);
            self.playout.push_back(Audio::Voice(samples));
        }
        if stream.is_last() {
            self.heard = None;
            self.playout.push_back(Audio::End);
        }
    }

    // ---- Audio in ----

    async fn transmit(&mut self, audio: Audio) {
        let on_air = self.link.as_ref().is_some_and(|link| link.connected) && self.heard.is_none();
        match audio {
            Audio::Voice(samples) if on_air && samples.len() == SAMPLES_PER_FRAME => {
                let frame = encode_frame(&mut self.encoder, &samples);
                let sending = self.sending.get_or_insert_with(|| {
                    let mut stream_id = [0; 2];
                    getrandom::fill(&mut stream_id).expect("the system has no source of randomness");
                    log::info!("D-STAR -> M17: start of transmission");
                    Sending { stream_id: u16::from_be_bytes(stream_id) | 1, frame_number: 0, pending: Vec::new() }
                });
                sending.pending.push(frame);
                if sending.pending.len() == FRAMES_PER_PACKET {
                    self.send_voice(false).await;
                }
            }
            Audio::End if self.sending.is_some() => {
                log::info!("D-STAR -> M17: end of transmission");
                self.send_voice(true).await;
            }
            _ => {}
        }
    }

    // Send what's waiting as one packet, topping a short last packet up with silence
    async fn send_voice(&mut self, last: bool) {
        let Some(mut sending) = self.sending.take() else { return };
        while sending.pending.len() < FRAMES_PER_PACKET {
            sending.pending.push(encode_frame(&mut self.encoder, &[0; SAMPLES_PER_FRAME]));
        }
        let payload: [u8; CODEC_FRAME_BYTES * FRAMES_PER_PACKET] = sending.pending.concat().try_into().expect("two frames fill a packet");
        sending.pending.clear();
        let frame_number = if last { sending.frame_number | LAST_FRAME_FLAG } else { sending.frame_number };
        let packet = StreamPacket::voice(sending.stream_id, &self.callsign, frame_number, payload);
        sending.frame_number = (sending.frame_number + 1) & !LAST_FRAME_FLAG;
        self.send(packet.as_bytes()).await;
        if !last {
            self.sending = Some(sending);
        }
    }

    // ---- Timers ----

    async fn housekeeping(&mut self) {
        let now = Instant::now();
        if let Some(audio) = self.playout.pop_front() {
            let _ = self.to_bridge.send(audio).await;
        }
        if self.heard.as_ref().is_some_and(|heard| now.duration_since(heard.last_heard) > STREAM_TIMEOUT) {
            self.heard = None;
            self.playout.push_back(Audio::End);
        }

        // Keep dialing until the reflector answers, and again if it goes quiet
        let Some(link) = self.link.as_mut() else { return };
        if link.connected && now.duration_since(link.last_heard) > LINK_TIMEOUT {
            log::warn!("{} stopped answering", link.target_text());
            link.connected = false;
        }
        if !link.connected && link.last_connect.is_none_or(|sent| now.duration_since(sent) >= CONNECT_RETRY_INTERVAL) {
            link.last_connect = Some(now);
            let packet = protocol::connect_packet(&self.callsign, link.module);
            self.send(&packet).await;
        }
    }

    // ---- Status ----

    fn summary(&self) -> (Option<(String, bool)>, bool, bool) {
        (self.link.as_ref().map(|link| (link.target_text(), link.connected)), self.heard.is_some(), self.sending.is_some())
    }

    fn publish(&self) {
        let target = self.link.as_ref().map(Link::target_text);
        let connected = self.link.as_ref().is_some_and(|link| link.connected);
        let peers: Vec<PeerStatus> = target.iter().map(|target| PeerStatus { id: target.clone(), mode: "transceive", outbound: true, connected, keyed: self.heard.is_some() || self.sending.is_some(), talking: self.heard.is_some() }).collect();
        let spoken = match (&target, connected) {
            (Some(target), true) => format!("Linked to M17 reflector {}.", speech::spoken_reflector(target)),
            (Some(target), false) => format!("Linking to M17 reflector {}.", speech::spoken_reflector(target)),
            (None, _) => "Not linked to an M17 reflector.".to_string(),
        };
        let mut finished = Vec::new();
        state::update(&self.shared, |snapshot| {
            finished = state::sync_link_peers(snapshot, LINK_KIND, &peers);
            let status = state::link_mut(snapshot, LINK_NAME, LINK_KIND);
            (status.target, status.connected, status.peers, status.spoken) = (target, connected, peers, spoken);
        });
        for transmission in finished {
            self.shared.emit(LiveEvent::Transmission { source: transmission.source, callsign: transmission.callsign, started_at: transmission.started_at, seconds: transmission.seconds });
        }
    }
}

// Run the node, taking commands and audio from the bridge, and handing back the audio the reflector sends
pub async fn run(m17: M17, station: Station, shared: Shared, speaker: Option<Speaker>, mut from_bridge: mpsc::Receiver<Audio>, to_bridge: mpsc::Sender<Audio>, mut commands: mpsc::Receiver<Command>) -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    let mut node = Node {
        callsign: m17.callsign.clone().unwrap_or(station.callsign),
        reflectors: m17.reflectors,
        shared,
        socket: socket.clone(),
        to_bridge,
        speaker,
        link: None,
        heard: None,
        sending: None,
        playout: VecDeque::new(),
        encoder: Codec2::new(Codec2Mode::MODE_3200),
        decoder: Codec2::new(Codec2Mode::MODE_3200),
    };

    // Link the startup reflector, written as "NAME M"
    if let Some(startup) = &m17.startup_reflector {
        let (name, module) = startup.split_once(' ').unwrap_or((startup, "A"));
        if let Err(error) = node.link(name, module.chars().next().unwrap_or('A')).await {
            log::error!("Couldn't link startup M17 reflector: {error:#}");
        }
    }

    let mut tick = interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut buffer = vec![0u8; 2048];
    let mut published = None;
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                let Ok((length, source)) = received else { continue };
                let packet = buffer[..length].to_vec();
                node.receive(&packet, source).await;
            }
            audio = from_bridge.recv() => match audio {
                Some(audio) => node.transmit(audio).await,
                None => return Ok(()),
            },
            command = commands.recv() => match command {
                Some(Command::Link { name, module }) => {
                    node.unlink().await;
                    if let Err(error) = node.link(&name, module).await {
                        log::error!("Couldn't link {name}: {error:#}");
                    }
                }
                Some(Command::Unlink) => node.unlink().await,
                None => return Ok(()),
            },
            _ = tick.tick() => node.housekeeping().await,
        }
        if published.as_ref() != Some(&node.summary()) {
            published = Some(node.summary());
            node.publish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Reflector {
        socket: UdpSocket,
        node_address: Option<SocketAddr>,
    }

    impl Reflector {
        async fn expect(&mut self, magic: &[u8]) -> Vec<u8> {
            let mut buffer = [0u8; 2048];
            loop {
                let (length, from) = tokio::time::timeout(Duration::from_secs(3), self.socket.recv_from(&mut buffer)).await.expect("the node never sent a packet").unwrap();
                self.node_address = Some(from);
                if buffer[..length].starts_with(magic) {
                    return buffer[..length].to_vec();
                }
            }
        }

        async fn say(&self, packet: &[u8]) {
            self.socket.send_to(packet, self.node_address.unwrap()).await.unwrap();
        }
    }

    struct Fixture {
        reflector: Reflector,
        commands: mpsc::Sender<Command>,
        to_node: mpsc::Sender<Audio>,
        from_node: mpsc::Receiver<Audio>,
    }

    async fn start() -> Fixture {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let (commands, command_receiver) = mpsc::channel(4);
        let (to_node, from_bridge) = mpsc::channel(256);
        let (to_bridge, from_node) = mpsc::channel(256);
        let m17: M17 = toml::from_str(&format!("ambeserver = \"127.0.0.1:2460\"\nstartup_reflector = \"M17-TST C\"\n[reflectors]\n\"M17-TST\" = \"{address}\"\n")).unwrap();
        let station = toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0").unwrap();
        tokio::spawn(run(m17, station, Shared::default(), None, from_bridge, to_bridge, command_receiver));
        Fixture { reflector: Reflector { socket, node_address: None }, commands, to_node, from_node }
    }

    async fn linked() -> Fixture {
        let mut fixture = start().await;
        assert_eq!(fixture.reflector.expect(b"CONN").await, protocol::connect_packet("N0CALL", b'C'));
        fixture.reflector.say(b"ACKN").await;
        fixture
    }

    #[tokio::test]
    async fn the_node_dials_the_startup_reflector_and_answers_pings() {
        let mut fixture = linked().await;
        fixture.reflector.say(&[&b"PING"[..], &protocol::encode_address("M17-TST")].concat()).await;
        assert_eq!(fixture.reflector.expect(b"PONG").await, protocol::pong_packet("N0CALL"));
    }

    #[tokio::test]
    async fn a_reflector_stream_comes_out_as_audio() {
        let mut fixture = linked().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        for frame_number in [0, 1, 2 | LAST_FRAME_FLAG] {
            fixture.reflector.say(StreamPacket::voice(0x42, "W1AW", frame_number, [0; 16]).as_bytes()).await;
        }
        let mut voice = 0;
        loop {
            match tokio::time::timeout(Duration::from_secs(2), fixture.from_node.recv()).await {
                Ok(Some(Audio::Voice(samples))) => {
                    assert_eq!(samples.len(), SAMPLES_PER_FRAME);
                    voice += 1;
                }
                Ok(Some(Audio::End)) => break,
                other => panic!("the stream never ended: {other:?}"),
            }
        }
        assert_eq!(voice, 6);
    }

    #[tokio::test]
    async fn audio_goes_out_as_a_stream_that_ends_with_the_last_frame_flag() {
        let mut fixture = linked().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        for _ in 0..5 {
            fixture.to_node.send(Audio::Voice(vec![1000; SAMPLES_PER_FRAME])).await.unwrap();
        }
        fixture.to_node.send(Audio::End).await.unwrap();
        let mut frame_numbers = Vec::new();
        while !frame_numbers.last().is_some_and(|number| number & LAST_FRAME_FLAG != 0) {
            let packet = StreamPacket::parse(&fixture.reflector.expect(b"M17 ").await).expect("a stream packet");
            assert_eq!(protocol::decode_address(packet.source), "N0CALL");
            frame_numbers.push(packet.frame_number.get());
        }
        assert_eq!(frame_numbers, vec![0, 1, 2 | LAST_FRAME_FLAG]);
    }

    #[tokio::test]
    async fn unlinking_says_goodbye() {
        let mut fixture = linked().await;
        fixture.commands.send(Command::Unlink).await.unwrap();
        assert_eq!(fixture.reflector.expect(b"DISC").await, protocol::disconnect_packet("N0CALL"));
    }

    #[tokio::test]
    async fn a_refused_link_is_not_dialed_again() {
        let mut fixture = start().await;
        fixture.reflector.expect(b"CONN").await;
        fixture.reflector.say(b"NACK").await;
        let mut buffer = [0u8; 64];
        assert!(tokio::time::timeout(Duration::from_millis(2500), fixture.reflector.socket.recv_from(&mut buffer)).await.is_err());
    }
}
