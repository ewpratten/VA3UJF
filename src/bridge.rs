// Bridge between D-STAR voice and an AllStar node's USRP channel, vocoding through an AMBEserver

use crate::config::{Allstar, Station};
use crate::state::{self, Shared};
use crate::dstar::{pad, Frame, Header, FRAMES_PER_SUPERFRAME};
use anyhow::{bail, Context};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::{interval, timeout, Instant};

// AMBE-3000 packet framing, as spoken by AMBEserver
const AMBE_START_BYTE: u8 = 0x61;
const AMBE_TYPE_CONTROL: u8 = 0x00;
const AMBE_TYPE_CHANNEL: u8 = 0x01;
const AMBE_TYPE_SPEECH: u8 = 0x02;
const AMBE_REQ_RESET: [u8; 5] = [0x61, 0x00, 0x01, 0x00, 0x33];
const AMBE_REQ_PRODID: [u8; 5] = [0x61, 0x00, 0x01, 0x00, 0x30];
const AMBE_REQ_RATEP_DSTAR: [u8; 17] = [0x61, 0x00, 0x0D, 0x00, 0x0A, 0x01, 0x30, 0x07, 0x63, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x48];
const AMBE_CHANNEL_HEADER: [u8; 6] = [0x61, 0x00, 0x0B, 0x01, 0x01, 0x48];
const AMBE_SPEECH_HEADER: [u8; 6] = [0x61, 0x01, 0x42, 0x02, 0x00, 0xA0];
const AMBE_PKT_READY: u8 = 0x39;
const AMBE_PKT_PRODID: u8 = 0x30;
const AMBE_PKT_RATEP: u8 = 0x0A;
const AMBE_TIMEOUT: Duration = Duration::from_secs(3);

// USRP framing, as used by chan_usrp
const USRP_HEADER_BYTES: usize = 32;
const USRP_FRAME_BYTES: usize = 320;

// Stream housekeeping
const VOCODER_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const DSTAR_STREAM_TIMEOUT: Duration = Duration::from_secs(1);
const USRP_STREAM_TIMEOUT: Duration = Duration::from_millis(300);
const ENCODER_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

// AMBEserver forgets idle clients (60s by default), so the rate is re-sent well before that
const AMBESERVER_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

const SYNC_SLOW_DATA: [u8; 3] = [0x55, 0x2D, 0x16];
const NULL_SLOW_DATA: [u8; 3] = [0x16, 0x29, 0xF5];
const SLOW_DATA_SCRAMBLER: [u8; 3] = [0x70, 0x4F, 0x93];

// Lay the text message out over the first 8 frames of each superframe, then idle
fn build_slow_data_blocks(message: &str) -> Vec<[u8; 3]> {
    let mut text = [b' '; 20];
    for (slot, byte) in text.iter_mut().zip(message.bytes()) {
        *slot = byte;
    }
    let mut blocks = vec![SYNC_SLOW_DATA];
    for (index, chunk) in text.chunks(5).enumerate() {
        for block in [[0x40 + index as u8, chunk[0], chunk[1]], [chunk[2], chunk[3], chunk[4]]] {
            blocks.push([block[0] ^ SLOW_DATA_SCRAMBLER[0], block[1] ^ SLOW_DATA_SCRAMBLER[1], block[2] ^ SLOW_DATA_SCRAMBLER[2]]);
        }
    }
    blocks.resize(FRAMES_PER_SUPERFRAME as usize, NULL_SLOW_DATA);
    blocks
}

fn decibels_to_gain(decibels: f32) -> f32 {
    10f32.powf(decibels / 20.0)
}

fn apply_gain(samples: impl Iterator<Item = i16>, gain: f32) -> Vec<i16> {
    samples.map(|sample| (sample as f32 * gain).clamp(-32768.0, 32767.0) as i16).collect()
}

// Split a datagram into (type, payload) AMBE packets
fn split_vocoder_packets(mut buffer: &[u8]) -> Vec<(u8, &[u8])> {
    let mut packets = Vec::new();
    while let Some(start) = buffer.iter().position(|byte| *byte == AMBE_START_BYTE) {
        buffer = &buffer[start..];
        if buffer.len() < 4 {
            break;
        }
        let packet_length = 4 + (((buffer[1] as usize) << 8) | buffer[2] as usize);
        if buffer.len() < packet_length {
            break;
        }
        packets.push((buffer[3], &buffer[4..packet_length]));
        buffer = &buffer[packet_length..];
    }
    packets
}

// Wait for the AMBEserver's answer to a control request
async fn expect_control(socket: &UdpSocket, packet_id: u8) -> anyhow::Result<Vec<u8>> {
    let deadline = Instant::now() + AMBE_TIMEOUT;
    let mut buffer = [0u8; 2048];
    loop {
        let length = timeout(deadline.saturating_duration_since(Instant::now()), socket.recv(&mut buffer)).await.with_context(|| format!("no response to control packet {packet_id:#04x}"))??;
        for (kind, payload) in split_vocoder_packets(&buffer[..length]) {
            if kind == AMBE_TYPE_CONTROL && payload.first() == Some(&packet_id) {
                return Ok(payload.to_vec());
            }
        }
    }
}

// Connect and reset the chip, then switch it to the D-STAR codec
async fn open_vocoder(address: SocketAddr) -> anyhow::Result<UdpSocket> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(address).await?;
    socket.send(&AMBE_REQ_RESET).await?;
    expect_control(&socket, AMBE_PKT_READY).await?;
    socket.send(&AMBE_REQ_PRODID).await?;
    let product = expect_control(&socket, AMBE_PKT_PRODID).await?;
    socket.send(&AMBE_REQ_RATEP_DSTAR).await?;
    let ratep = expect_control(&socket, AMBE_PKT_RATEP).await?;
    if ratep.get(1) != Some(&0) {
        bail!("vocoder rejected the D-STAR rate");
    }
    log::info!("Vocoder ready: {}", String::from_utf8_lossy(&product[1..]).trim_end_matches('\0'));
    Ok(socket)
}

struct Bridge {
    allstar: Allstar,
    station: Station,
    to_gateway: mpsc::Sender<Frame>,
    shared: Shared,
    usrp: UdpSocket,
    usrp_destination: SocketAddr,
    usrp_transmit_sequence: u32,
    vocoder_address: SocketAddr,
    vocoder: Option<UdpSocket>,
    vocoder_last_attempt: Instant,
    vocoder_last_write: Instant,
    slow_data_blocks: Vec<[u8; 3]>,
    receive_gain: f32,
    transmit_gain: f32,

    // D-STAR -> AllStar stream state
    inbound_active: bool,
    inbound_last_heard: Instant,
    warned_no_vocoder_inbound: bool,

    // AllStar -> D-STAR stream state
    outbound_active: bool,
    outbound_sequence: u8,
    outbound_last_heard: Instant,
    outbound_ending_since: Option<Instant>,
    outbound_pending_encodes: u32,
    warned_no_vocoder_outbound: bool,
}

impl Bridge {
    async fn write_vocoder(&mut self, packet: &[u8]) -> bool {
        let Some(socket) = &self.vocoder else { return false };
        match socket.send(packet).await {
            Ok(_) => {
                self.vocoder_last_write = Instant::now();
                true
            }
            Err(error) => {
                log::warn!("Vocoder lost: {error}");
                self.vocoder = None;
                state::update(&self.shared, |snapshot| snapshot.vocoder_connected = Some(false));
                self.outbound_pending_encodes = 0;
                false
            }
        }
    }

    async fn send_usrp(&mut self, keyup: u32, audio: &[u8]) {
        let mut packet = vec![0u8; USRP_HEADER_BYTES];
        packet[0..4].copy_from_slice(b"USRP");
        packet[4..8].copy_from_slice(&self.usrp_transmit_sequence.to_be_bytes());
        packet[12..16].copy_from_slice(&keyup.to_be_bytes());
        packet.extend(audio);
        let _ = self.usrp.send_to(&packet, self.usrp_destination).await;
        self.usrp_transmit_sequence = self.usrp_transmit_sequence.wrapping_add(1);
    }

    // ---- D-STAR side ----

    async fn handle_gateway_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Header(header) => {
                log::info!("D-STAR -> AllStar: {}", header.my_call_text());
                self.inbound_active = true;
                self.inbound_last_heard = Instant::now();
            }
            Frame::Voice { ambe, .. } if self.inbound_active => {
                self.inbound_last_heard = Instant::now();
                if self.vocoder.is_none() {
                    if !self.warned_no_vocoder_inbound {
                        log::warn!("No vocoder attached, dropping D-STAR audio");
                        self.warned_no_vocoder_inbound = true;
                    }
                    return;
                }
                self.write_vocoder(&[&AMBE_CHANNEL_HEADER[..], &ambe[..]].concat()).await;
            }
            Frame::End if self.inbound_active => self.end_inbound().await,
            _ => {}
        }
    }

    async fn end_inbound(&mut self) {
        log::info!("D-STAR -> AllStar: end of transmission");
        self.inbound_active = false;
        self.warned_no_vocoder_inbound = false;
        self.send_usrp(0, &[]).await;
    }

    // ---- AllStar side ----

    async fn handle_usrp(&mut self, data: &[u8]) {
        if data.len() < USRP_HEADER_BYTES || &data[0..4] != b"USRP" {
            return;
        }
        let keyup = u32::from_be_bytes(data[12..16].try_into().unwrap());
        let packet_type = u32::from_be_bytes(data[20..24].try_into().unwrap());
        let audio = &data[USRP_HEADER_BYTES..];
        if packet_type != 0 {
            return;
        }

        // Unkey marks the end of the AllStar transmission
        if keyup == 0 || audio.len() != USRP_FRAME_BYTES {
            if self.outbound_active && self.outbound_ending_since.is_none() {
                self.outbound_ending_since = Some(Instant::now());
            }
            return;
        }

        // D-STAR has the channel, so drop AllStar audio
        if self.inbound_active {
            return;
        }
        if self.vocoder.is_none() {
            if !self.warned_no_vocoder_outbound {
                log::warn!("No vocoder attached, dropping AllStar audio");
                self.warned_no_vocoder_outbound = true;
            }
            return;
        }

        // Open a new D-STAR stream on the first frame
        if !self.outbound_active {
            log::info!("AllStar -> D-STAR: start of transmission");
            self.outbound_active = true;
            self.outbound_sequence = 0;
            self.outbound_pending_encodes = 0;
            let repeater_1 = format!("{:<7}{}", self.station.callsign, self.station.module);
            let repeater_2 = format!("{:<7}G", self.station.callsign);
            let header = Header { flags: [0; 3], repeater_2: pad(&repeater_2), repeater_1: pad(&repeater_1), your_call: pad("CQCQCQ"), my_call: pad(&self.station.callsign), my_suffix: pad(&self.allstar.suffix) };
            let _ = self.to_gateway.send(Frame::Header(header)).await;
        }
        self.outbound_last_heard = Instant::now();
        self.outbound_ending_since = None;

        // Hand the audio to the encoder, which answers asynchronously. AMBE wants big-endian samples
        let samples = apply_gain(audio.chunks_exact(2).map(|pair| i16::from_le_bytes([pair[0], pair[1]])), self.transmit_gain);
        let packet = [&AMBE_SPEECH_HEADER[..], &samples.iter().flat_map(|sample| sample.to_be_bytes()).collect::<Vec<u8>>()].concat();
        if self.write_vocoder(&packet).await {
            self.outbound_pending_encodes += 1;
        }
    }

    async fn handle_vocoder(&mut self, data: &[u8]) {
        for (kind, payload) in split_vocoder_packets(data) {
            // Decoded speech heads to AllStar
            if kind == AMBE_TYPE_SPEECH && payload.len() >= 2 + USRP_FRAME_BYTES {
                let samples = apply_gain(payload[2..2 + USRP_FRAME_BYTES].chunks_exact(2).map(|pair| i16::from_be_bytes([pair[0], pair[1]])), self.receive_gain);
                let audio: Vec<u8> = samples.iter().flat_map(|sample| sample.to_le_bytes()).collect();
                self.send_usrp(1, &audio).await;

            // Encoded AMBE heads to D-STAR
            } else if kind == AMBE_TYPE_CHANNEL && payload.len() >= 11 {
                self.outbound_pending_encodes = self.outbound_pending_encodes.saturating_sub(1);
                if self.outbound_active {
                    let slow_data = self.slow_data_blocks[self.outbound_sequence as usize];
                    let _ = self.to_gateway.send(Frame::Voice { sequence: self.outbound_sequence, ambe: payload[2..11].try_into().unwrap(), slow_data }).await;
                    self.outbound_sequence = (self.outbound_sequence + 1) % FRAMES_PER_SUPERFRAME;
                }
            }
        }
    }

    // ---- Timers ----

    async fn housekeeping(&mut self) {
        let now = Instant::now();

        // Keep trying to reach the vocoder
        if self.vocoder.is_none() && now.duration_since(self.vocoder_last_attempt) > VOCODER_RETRY_INTERVAL {
            self.vocoder_last_attempt = now;
            match open_vocoder(self.vocoder_address).await {
                Ok(socket) => {
                    self.vocoder = Some(socket);
                    state::update(&self.shared, |snapshot| snapshot.vocoder_connected = Some(true));
                    self.vocoder_last_write = now;
                }
                Err(error) => log::warn!("Could not initialize vocoder at {}: {error:#}", self.vocoder_address),
            }
        }

        // Keep the AMBEserver session, and the D-STAR rate it holds, alive between transmissions
        let idle = !self.inbound_active && !self.outbound_active;
        if self.vocoder.is_some() && idle && now.duration_since(self.vocoder_last_write) > AMBESERVER_KEEPALIVE_INTERVAL {
            self.write_vocoder(&AMBE_REQ_RATEP_DSTAR).await;
        }

        // Close out a D-STAR stream that lost its terminator
        if self.inbound_active && now.duration_since(self.inbound_last_heard) > DSTAR_STREAM_TIMEOUT {
            self.end_inbound().await;
        }

        // Close out an AllStar stream once it stops and the encoder has drained
        if self.outbound_active {
            if self.outbound_ending_since.is_none() && now.duration_since(self.outbound_last_heard) > USRP_STREAM_TIMEOUT {
                self.outbound_ending_since = Some(now);
            }
            if let Some(since) = self.outbound_ending_since {
                if self.outbound_pending_encodes == 0 || now.duration_since(since) > ENCODER_DRAIN_TIMEOUT {
                    log::info!("AllStar -> D-STAR: end of transmission");
                    self.outbound_active = false;
                    self.outbound_ending_since = None;
                    self.warned_no_vocoder_outbound = false;
                    let _ = self.to_gateway.send(Frame::End).await;
                }
            }
        }
    }
}

// Run the bridge, taking D-STAR frames from the gateway and handing back AllStar-originated ones
pub async fn run(allstar: Allstar, station: Station, shared: Shared, mut from_gateway: mpsc::Receiver<Frame>, to_gateway: mpsc::Sender<Frame>) -> anyhow::Result<()> {
    let usrp = UdpSocket::bind((allstar.usrp_address.as_str(), allstar.usrp_listen_port)).await.context("couldn't bind the USRP port")?;
    let usrp_destination = lookup_host((allstar.usrp_address.as_str(), allstar.usrp_send_port)).await?.next().context("couldn't resolve the USRP address")?;
    let vocoder_address = lookup_host(allstar.ambeserver.as_str()).await?.next().context("couldn't resolve the AMBEserver address")?;
    log::info!("Exchanging USRP audio via {}:{}->{}", allstar.usrp_address, allstar.usrp_listen_port, allstar.usrp_send_port);

    let now = Instant::now();
    let message = allstar.message.clone().unwrap_or_else(|| format!("{} via AllStar", station.callsign));
    let mut bridge = Bridge {
        slow_data_blocks: build_slow_data_blocks(&message),
        receive_gain: decibels_to_gain(allstar.rx_gain_db),
        transmit_gain: decibels_to_gain(allstar.tx_gain_db),
        allstar,
        station,
        to_gateway,
        shared,
        usrp,
        usrp_destination,
        usrp_transmit_sequence: 0,
        vocoder_address,
        vocoder: None,
        vocoder_last_attempt: now - VOCODER_RETRY_INTERVAL,
        vocoder_last_write: now,
        inbound_active: false,
        inbound_last_heard: now,
        warned_no_vocoder_inbound: false,
        outbound_active: false,
        outbound_sequence: 0,
        outbound_last_heard: now,
        outbound_ending_since: None,
        outbound_pending_encodes: 0,
        warned_no_vocoder_outbound: false,
    };

    let mut tick = interval(Duration::from_millis(20));
    let mut usrp_buffer = [0u8; 1024];
    let mut vocoder_buffer = [0u8; 2048];
    loop {
        let vocoder_ready = async {
            match &bridge.vocoder {
                Some(socket) => socket.recv(&mut vocoder_buffer).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            received = bridge.usrp.recv(&mut usrp_buffer) => {
                if let Ok(length) = received {
                    let data = usrp_buffer[..length].to_vec();
                    bridge.handle_usrp(&data).await;
                }
            }
            received = vocoder_ready => {
                if let Ok(length) = received {
                    let data = vocoder_buffer[..length].to_vec();
                    bridge.handle_vocoder(&data).await;
                }
            }
            frame = from_gateway.recv() => match frame {
                Some(frame) => bridge.handle_gateway_frame(frame).await,
                None => return Ok(()),
            },
            _ = tick.tick() => bridge.housekeeping().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_data_matches_the_layout_of_the_python_bridge() {
        let blocks = build_slow_data_blocks("AllStar 54207");
        assert_eq!(blocks.len(), 21);
        assert_eq!(blocks[..9], [[0x55, 0x2D, 0x16], [0x30, 0x0E, 0xFF], [0x1C, 0x1C, 0xE7], [0x31, 0x2E, 0xE1], [0x50, 0x7A, 0xA7], [0x32, 0x7D, 0xA3], [0x47, 0x6F, 0xB3], [0x33, 0x6F, 0xB3], [0x50, 0x6F, 0xB3]]);
        assert!(blocks[9..].iter().all(|block| *block == NULL_SLOW_DATA));
    }

    #[test]
    fn vocoder_packets_split_out_of_a_datagram() {
        let datagram = [0x61, 0x00, 0x02, 0x00, 0x39, 0x00, 0x61, 0x00, 0x01, 0x01, 0xAA];
        assert_eq!(split_vocoder_packets(&datagram), vec![(0x00, &[0x39, 0x00][..]), (0x01, &[0xAA][..])]);
    }

    #[test]
    fn unity_gain_leaves_samples_alone() {
        assert_eq!(apply_gain([1, -2, 32767, -32768].into_iter(), decibels_to_gain(0.0)), vec![1, -2, 32767, -32768]);
    }
}
