// IAX2 wire format and per-call reliable transport, covering what AllStar node links use (RFC 5456)

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;

pub const FORMAT_ULAW: u64 = 1 << 2;
pub const PROTOCOL_VERSION: u16 = 2;

pub mod frame_type {
    pub const VOICE: u8 = 0x02;
    pub const CONTROL: u8 = 0x04;
    pub const IAX: u8 = 0x06;
    pub const TEXT: u8 = 0x07;
    pub const CNG: u8 = 0x0A;
}

pub mod command {
    pub const NEW: u8 = 1;
    pub const PING: u8 = 2;
    pub const PONG: u8 = 3;
    pub const ACK: u8 = 4;
    pub const HANGUP: u8 = 5;
    pub const REJECT: u8 = 6;
    pub const ACCEPT: u8 = 7;
    pub const AUTHREQ: u8 = 8;
    pub const INVAL: u8 = 10;
    pub const LAGRQ: u8 = 11;
    pub const LAGRP: u8 = 12;
    pub const VNAK: u8 = 18;
    pub const TXCNT: u8 = 23;
    pub const TXACC: u8 = 24;
    pub const POKE: u8 = 30;
    pub const CALLTOKEN: u8 = 40;
}

pub mod control {
    pub const HANGUP: u8 = 1;
    pub const ANSWER: u8 = 4;
    pub const RADIO_KEY: u8 = 12;
    pub const RADIO_UNKEY: u8 = 13;
}

pub mod ie {
    pub const CALLED_NUMBER: u8 = 1;
    pub const CALLING_NUMBER: u8 = 2;
    pub const USERNAME: u8 = 6;
    pub const CAPABILITY: u8 = 8;
    pub const FORMAT: u8 = 9;
    pub const VERSION: u8 = 11;
    pub const CAUSE: u8 = 22;
    pub const CALLTOKEN: u8 = 54;
    pub const CAPABILITY2: u8 = 55;
    pub const FORMAT2: u8 = 56;
}

const FULL_HEADER_BYTES: usize = 12;
const FULL_FLAG: u16 = 0x8000;
const RETRANSMISSION_FLAG: u16 = 0x8000;
const SUBCLASS_LOG_FLAG: u8 = 0x80;

const MIN_RETRY_MS: u64 = 100;
const MAX_RETRY_MS: u64 = 10_000;
const DEFAULT_PING_TIME_MS: u64 = 1000;
const MAX_TRANSMISSIONS: u32 = 4;
const PING_INTERVAL: Duration = Duration::from_secs(21);
const LAGRQ_INTERVAL: Duration = Duration::from_secs(10);

// Voice timestamps stay on their predicted cadence unless the clock drifts this far from it
const TIMESTAMP_SKEW_MS: i64 = 160;

#[derive(Clone, Debug, PartialEq)]
pub struct FullFrame {
    pub source_call: u16,
    pub dest_call: u16,
    pub retransmission: bool,
    pub timestamp: u32,
    pub oseq: u8,
    pub iseq: u8,
    pub frame_type: u8,
    pub subclass: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Datagram {
    Full(FullFrame),
    Mini { source_call: u16, timestamp: u16, payload: Vec<u8> },
}

fn uncompress_subclass(compressed: u8) -> u64 {
    if compressed & SUBCLASS_LOG_FLAG != 0 {
        1u64 << (compressed & 0x3F).min(63)
    } else {
        compressed as u64
    }
}

fn compress_subclass(subclass: u64) -> u8 {
    if subclass < SUBCLASS_LOG_FLAG as u64 {
        subclass as u8
    } else {
        SUBCLASS_LOG_FLAG | subclass.trailing_zeros() as u8
    }
}

impl FullFrame {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(FULL_HEADER_BYTES + self.payload.len());
        bytes.extend((self.source_call | FULL_FLAG).to_be_bytes());
        bytes.extend((self.dest_call | if self.retransmission { RETRANSMISSION_FLAG } else { 0 }).to_be_bytes());
        bytes.extend(self.timestamp.to_be_bytes());
        bytes.extend([self.oseq, self.iseq, self.frame_type, compress_subclass(self.subclass)]);
        bytes.extend(&self.payload);
        bytes
    }
}

// Full frames carry the full header, mini frames are voice with the low half of the timestamp. Meta frames (trunks, video) aren't used by node links
pub fn parse(data: &[u8]) -> Option<Datagram> {
    let first = u16::from_be_bytes(data.get(0..2)?.try_into().ok()?);
    if first & FULL_FLAG != 0 {
        if data.len() < FULL_HEADER_BYTES {
            return None;
        }
        let second = u16::from_be_bytes(data[2..4].try_into().ok()?);
        return Some(Datagram::Full(FullFrame {
            source_call: first & !FULL_FLAG,
            dest_call: second & !RETRANSMISSION_FLAG,
            retransmission: second & RETRANSMISSION_FLAG != 0,
            timestamp: u32::from_be_bytes(data[4..8].try_into().ok()?),
            oseq: data[8],
            iseq: data[9],
            frame_type: data[10],
            subclass: uncompress_subclass(data[11]),
            payload: data[FULL_HEADER_BYTES..].to_vec(),
        }));
    }
    if first == 0 || data.len() < 4 {
        return None;
    }
    Some(Datagram::Mini { source_call: first, timestamp: u16::from_be_bytes(data[2..4].try_into().ok()?), payload: data[4..].to_vec() })
}

// Information elements: a type byte, a length byte, then the value
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ies(Vec<(u8, Vec<u8>)>);

impl Ies {
    pub fn parse(mut data: &[u8]) -> Option<Self> {
        let mut elements = Vec::new();
        while !data.is_empty() {
            let (&id, &length) = (data.first()?, data.get(1)?);
            let value = data.get(2..2 + length as usize)?;
            elements.push((id, value.to_vec()));
            data = &data[2 + length as usize..];
        }
        Some(Ies(elements))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.iter().flat_map(|(id, value)| [&[*id, value.len() as u8][..], value].concat()).collect()
    }

    pub fn add(mut self, id: u8, value: &[u8]) -> Self {
        self.0.push((id, value.to_vec()));
        self
    }

    pub fn add_u16(self, id: u8, value: u16) -> Self {
        self.add(id, &value.to_be_bytes())
    }

    pub fn add_u32(self, id: u8, value: u32) -> Self {
        self.add(id, &value.to_be_bytes())
    }

    // The 64-bit codec elements lead with a version byte
    pub fn add_versioned_u64(self, id: u8, value: u64) -> Self {
        self.add(id, &[&[0u8][..], &value.to_be_bytes()].concat())
    }

    pub fn add_text(self, id: u8, value: &str) -> Self {
        self.add(id, value.as_bytes())
    }

    // When an element repeats, the last one wins
    pub fn get(&self, id: u8) -> Option<&[u8]> {
        self.0.iter().rev().find(|(candidate, _)| *candidate == id).map(|(_, value)| value.as_slice())
    }

    pub fn get_text(&self, id: u8) -> Option<String> {
        self.get(id).map(|value| String::from_utf8_lossy(value).into_owned())
    }

    pub fn set(&mut self, id: u8, value: &[u8]) {
        self.0.retain(|(candidate, _)| *candidate != id);
        self.0.push((id, value.to_vec()));
    }

    // Formats come as 64 bits when the peer supports it, and 32 otherwise
    pub fn get_codec(&self, wide: u8, narrow: u8) -> Option<u64> {
        match (self.get(wide), self.get(narrow)) {
            (Some(value), _) if value.len() == 9 => Some(u64::from_be_bytes(value[1..9].try_into().ok()?)),
            (_, Some(value)) if value.len() == 4 => Some(u32::from_be_bytes(value.try_into().ok()?) as u64),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Event {
    Iax { command: u8, ies: Ies },
    Control(u8),
    Text(String),
    Voice { format: u64, payload: Vec<u8> },
    Cng,
    // The peer stopped acknowledging frames, so the call is gone
    Timeout,
}

struct Pending {
    frame: FullFrame,
    transmissions: u32,
    retry_ms: u64,
    next_attempt: Instant,
    final_frame: bool,
}

pub struct Session {
    pub local_call: u16,
    pub peer_call: u16,
    pub addr: SocketAddr,
    pub started: bool,
    pub destroyed: bool,
    outbox: Vec<Vec<u8>>,
    began: Instant,
    oseq: u8,
    iseq: u8,
    rseq: u8,
    aseq: u8,
    last_sent: u32,
    next_voice_timestamp: u32,
    voice_streaming: bool,
    last_received: u32,
    sent_voice_format: u64,
    voice_format: u64,
    ping_time_ms: u64,
    pending: Vec<Pending>,
    next_ping: Instant,
    next_lagrq: Instant,
    new_elements: Option<Ies>,
}

impl Session {
    pub fn new(local_call: u16, peer_call: u16, addr: SocketAddr, now: Instant) -> Self {
        Session {
            local_call,
            peer_call,
            addr,
            started: false,
            destroyed: false,
            outbox: Vec::new(),
            began: now,
            oseq: 0,
            iseq: 0,
            rseq: 0,
            aseq: 0,
            last_sent: 0,
            next_voice_timestamp: 0,
            voice_streaming: false,
            last_received: 0,
            sent_voice_format: 0,
            voice_format: 0,
            ping_time_ms: DEFAULT_PING_TIME_MS,
            pending: Vec::new(),
            next_ping: now + PING_INTERVAL,
            next_lagrq: now + LAGRQ_INTERVAL,
            new_elements: None,
        }
    }

    // Datagrams waiting to go to the peer
    pub fn take_outbox(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.outbox)
    }

    // Media is only delivered once the call is accepted, and the negotiated format decodes mini frames
    pub fn start(&mut self, format: u64) {
        self.started = true;
        self.voice_format = format;
    }

    pub fn destroy(&mut self) {
        self.destroyed = true;
        self.pending.clear();
    }

    fn milliseconds(&self, now: Instant) -> u32 {
        now.saturating_duration_since(self.began).as_millis() as u32
    }

    // Voice follows its own predicted cadence, and everything else slots in just after the last frame sent
    fn timestamp(&mut self, now: Instant, frame_type: u8, samples: u32) -> u32 {
        let clock = self.milliseconds(now);
        let mut timestamp = clock;
        match frame_type {
            frame_type::VOICE => {
                let frame_ms = samples / 8;
                if self.voice_streaming && (clock as i64 - self.next_voice_timestamp as i64).abs() <= TIMESTAMP_SKEW_MS {
                    timestamp = self.next_voice_timestamp;
                } else {
                    if frame_ms > 0 && !clock.is_multiple_of(frame_ms) {
                        timestamp = clock + frame_ms - clock % frame_ms;
                    }
                    self.voice_streaming = true;
                }
                self.next_voice_timestamp = timestamp + frame_ms;
            }
            frame_type::IAX => {
                if timestamp <= self.last_sent {
                    timestamp = self.last_sent + 3;
                }
            }
            _ => {
                if (timestamp as i64 - self.last_sent as i64).abs() <= TIMESTAMP_SKEW_MS {
                    timestamp = self.last_sent + 3;
                }
                if frame_type == frame_type::CNG {
                    self.voice_streaming = false;
                }
            }
        }
        self.last_sent = timestamp;
        timestamp
    }

    // Rebuild a full timestamp from a mini frame's low 16 bits, allowing for wrap-around
    fn unwrap_timestamp(&self, low: u16) -> u32 {
        let upper = self.last_received & 0xFFFF_0000;
        let full = upper | low as u32;
        let difference = full as i64 - self.last_received as i64;
        if difference < -50_000 {
            upper.wrapping_add(0x1_0000) | low as u32
        } else if difference > 50_000 {
            upper.wrapping_sub(0x1_0000) | low as u32
        } else {
            full
        }
    }

    fn retry_ms(&self) -> u64 {
        (self.ping_time_ms * 2).clamp(MIN_RETRY_MS, MAX_RETRY_MS)
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(&mut self, now: Instant, frame_type: u8, subclass: u64, payload: Vec<u8>, timestamp: u32, sequence: Option<u8>, reliable: bool, final_frame: bool) {
        let oseq = sequence.unwrap_or_else(|| {
            let next = self.oseq;
            self.oseq = self.oseq.wrapping_add(1);
            next
        });
        let frame = FullFrame { source_call: self.local_call, dest_call: self.peer_call, retransmission: false, timestamp, oseq, iseq: self.iseq, frame_type, subclass, payload };
        self.aseq = self.iseq;
        self.outbox.push(frame.to_bytes());
        if reliable {
            let retry_ms = self.retry_ms();
            self.pending.push(Pending { frame, transmissions: 1, retry_ms, next_attempt: now + Duration::from_millis(retry_ms), final_frame });
        }
    }

    // Send a frame that must be acknowledged, timestamped by its type
    fn send_reliable(&mut self, now: Instant, frame_type: u8, subclass: u64, payload: Vec<u8>, final_frame: bool) {
        if self.destroyed {
            return;
        }
        let timestamp = self.timestamp(now, frame_type, 0);
        self.emit(now, frame_type, subclass, payload, timestamp, None, true, final_frame);
    }

    pub fn send_iax(&mut self, now: Instant, command: u8, ies: Ies) {
        self.send_reliable(now, frame_type::IAX, command as u64, ies.to_bytes(), false);
    }

    // Hang up, and let the session go once the peer acknowledges
    pub fn hangup(&mut self, now: Instant, cause: &str) {
        let ies = Ies::default().add_text(ie::CAUSE, cause);
        self.send_reliable(now, frame_type::IAX, command::HANGUP as u64, ies.to_bytes(), true);
    }

    pub fn send_control(&mut self, now: Instant, subclass: u8) {
        self.send_reliable(now, frame_type::CONTROL, subclass as u64, Vec::new(), false);
    }

    // Text frames end in a NUL
    pub fn send_text(&mut self, now: Instant, text: &str) {
        self.send_reliable(now, frame_type::TEXT, 0, [text.as_bytes(), &[0]].concat(), false);
    }

    pub fn send_cng(&mut self, now: Instant) {
        self.send_reliable(now, frame_type::CNG, 0, Vec::new(), false);
    }

    // Start an outgoing call. The call token is requested with an empty element and supplied if the peer asks for it
    pub fn start_call(&mut self, now: Instant, elements: Ies) {
        let elements = elements.add(ie::CALLTOKEN, &[]);
        self.send_iax(now, command::NEW, elements.clone());
        self.new_elements = Some(elements);
    }

    pub fn send_voice(&mut self, now: Instant, format: u64, payload: &[u8]) {
        if self.destroyed {
            return;
        }
        let previous = self.last_sent;
        let timestamp = self.timestamp(now, frame_type::VOICE, payload.len() as u32);
        let upper = timestamp & 0xFFFF_0000;
        let mini = format == self.sent_voice_format && (upper == previous & 0xFFFF_0000 || upper == previous.wrapping_add(0x1_0000) & 0xFFFF_0000);
        if mini {
            let bytes = [&self.local_call.to_be_bytes()[..], &(timestamp as u16).to_be_bytes(), payload].concat();
            self.outbox.push(bytes);
            return;
        }
        self.sent_voice_format = format;
        self.emit(now, frame_type::VOICE, format, payload.to_vec(), timestamp, None, true, false);
    }

    fn send_ack(&mut self, now: Instant, echoed_timestamp: u32, peer_iseq: u8) {
        let timestamp = if echoed_timestamp == 0 { self.timestamp(now, frame_type::IAX, 0) } else { echoed_timestamp };
        self.emit(now, frame_type::IAX, command::ACK as u64, Vec::new(), timestamp, Some(peer_iseq), false, false);
    }

    fn send_vnak(&mut self, now: Instant) {
        let timestamp = self.timestamp(now, frame_type::IAX, 0);
        self.emit(now, frame_type::IAX, command::VNAK as u64, Vec::new(), timestamp, Some(self.iseq), false, false);
    }

    // Everything the peer has acknowledged can stop being retransmitted
    fn process_ack(&mut self, peer_iseq: u8) {
        let (acknowledged, outbound) = (self.rseq, self.oseq);
        let in_window = acknowledged >= outbound || (acknowledged <= peer_iseq && peer_iseq < outbound);
        let limit = if in_window { peer_iseq } else { outbound };
        if limit == outbound && outbound != peer_iseq {
            return;
        }
        let mut destroy = false;
        let mut sequence = acknowledged;
        while sequence != peer_iseq {
            self.pending.retain(|pending| {
                let matches = pending.frame.oseq == sequence;
                destroy |= matches && pending.final_frame;
                !matches
            });
            sequence = sequence.wrapping_add(1);
        }
        self.rseq = peer_iseq;
        if destroy {
            self.destroy();
        }
    }

    pub fn handle_mini(&mut self, now: Instant, low_timestamp: u16, payload: Vec<u8>) -> Vec<Event> {
        if self.destroyed {
            return Vec::new();
        }
        if self.voice_format == 0 {
            self.send_vnak(now);
            return Vec::new();
        }
        let timestamp = self.unwrap_timestamp(low_timestamp);
        self.last_received = self.last_received.max(timestamp);
        if self.started { vec![Event::Voice { format: self.voice_format, payload }] } else { Vec::new() }
    }

    pub fn handle_full(&mut self, now: Instant, frame: FullFrame) -> Vec<Event> {
        if self.destroyed {
            return Vec::new();
        }
        let is_iax = frame.frame_type == frame_type::IAX;
        let command = frame.subclass as u8;
        let acknowledgement_like = is_iax && matches!(command, command::ACK | command::INVAL | command::TXCNT | command::TXACC | command::VNAK);
        if frame.source_call != 0 && frame.source_call != self.peer_call {
            self.peer_call = frame.source_call;
        }

        // Frames out of sequence are re-acknowledged if already seen, or answered with a VNAK if one went missing
        if frame.oseq != self.iseq {
            if !acknowledgement_like {
                if self.iseq.wrapping_sub(frame.oseq) < 128 {
                    self.send_ack(now, frame.timestamp, frame.iseq);
                } else {
                    self.send_vnak(now);
                }
                return Vec::new();
            }
        } else if !acknowledgement_like {
            self.iseq = self.iseq.wrapping_add(1);
        }
        if !(is_iax && command == command::INVAL) {
            self.process_ack(frame.iseq);
            if self.destroyed {
                return Vec::new();
            }
        }
        if frame.frame_type == frame_type::VOICE && frame.subclass != self.voice_format {
            self.voice_format = frame.subclass;
        }

        if is_iax {
            if self.last_received < frame.timestamp && !matches!(command, command::ACK | command::PONG | command::LAGRP) {
                self.last_received = frame.timestamp;
            }
            let Some(ies) = Ies::parse(&frame.payload) else {
                log::warn!("Undecodable elements in IAX command {command} from {}", self.addr);
                return Vec::new();
            };
            let events = self.handle_iax(now, &frame, command, ies);
            if !self.destroyed && !acknowledgement_like && self.aseq != self.iseq {
                self.send_ack(now, frame.timestamp, frame.iseq);
            }
            return events;
        }

        if self.aseq != self.iseq {
            self.send_ack(now, frame.timestamp, frame.iseq);
        }
        if !self.started {
            return Vec::new();
        }
        self.last_received = self.last_received.max(frame.timestamp);
        match frame.frame_type {
            frame_type::VOICE => vec![Event::Voice { format: frame.subclass, payload: frame.payload }],
            frame_type::CONTROL => vec![Event::Control(frame.subclass as u8)],
            frame_type::TEXT => vec![Event::Text(String::from_utf8_lossy(&frame.payload).trim_end_matches('\0').to_string())],
            frame_type::CNG => vec![Event::Cng],
            _ => Vec::new(),
        }
    }

    fn handle_iax(&mut self, now: Instant, frame: &FullFrame, command: u8, ies: Ies) -> Vec<Event> {
        match command {
            command::ACK => {}
            command::PING => {
                let timestamp = frame.timestamp;
                self.emit(now, frame_type::IAX, command::PONG as u64, Vec::new(), timestamp, None, true, false);
            }
            command::LAGRQ => {
                let timestamp = frame.timestamp;
                self.emit(now, frame_type::IAX, command::LAGRP as u64, frame.payload.clone(), timestamp, None, true, false);
            }
            command::PONG | command::LAGRP => {
                let round_trip = self.timestamp(now, frame_type::IAX, 0).saturating_sub(frame.timestamp) as u64;
                if command == command::PONG {
                    self.ping_time_ms = round_trip.max(1);
                }
            }
            command::VNAK => {
                let sequence = frame.iseq;
                let due: Vec<Vec<u8>> = self.pending.iter().filter(|pending| pending.frame.oseq.wrapping_sub(sequence) < 128).map(|pending| pending.frame.to_bytes()).collect();
                self.outbox.extend(due);
            }
            command::INVAL => {
                self.destroy();
            }
            command::CALLTOKEN => {
                if let (Some(token), Some(elements)) = (ies.get(ie::CALLTOKEN), self.new_elements.clone()) {
                    self.resend_with_token(now, elements, token.to_vec());
                }
            }
            _ => return vec![Event::Iax { command, ies }],
        }
        Vec::new()
    }

    // The peer wants proof of our address first, so the call starts over with the token it issued
    fn resend_with_token(&mut self, now: Instant, mut elements: Ies, token: Vec<u8>) {
        elements.set(ie::CALLTOKEN, &token);
        self.pending.clear();
        (self.oseq, self.iseq, self.rseq, self.aseq, self.peer_call) = (0, 0, 0, 0, 0);
        self.send_iax(now, command::NEW, elements.clone());
        self.new_elements = Some(elements);
    }

    // Retransmit what went unacknowledged, and keep the call alive while idle
    pub fn poll(&mut self, now: Instant) -> Vec<Event> {
        if self.destroyed {
            return Vec::new();
        }
        let mut gave_up = false;
        let mut retransmissions = Vec::new();
        let iseq = self.iseq;
        self.pending.retain_mut(|pending| {
            if pending.next_attempt > now {
                return true;
            }
            if pending.transmissions >= MAX_TRANSMISSIONS {
                gave_up |= !pending.final_frame;
                return false;
            }
            pending.frame.retransmission = true;
            pending.frame.iseq = iseq;
            retransmissions.push(pending.frame.to_bytes());
            pending.transmissions += 1;
            pending.retry_ms = (pending.retry_ms * 10).min(MAX_RETRY_MS);
            pending.next_attempt = now + Duration::from_millis(pending.retry_ms);
            true
        });
        self.outbox.extend(retransmissions);
        if gave_up {
            self.destroy();
            return vec![Event::Timeout];
        }
        if self.peer_call != 0 && self.started {
            if now >= self.next_ping {
                self.next_ping = now + PING_INTERVAL;
                self.send_iax(now, command::PING, Ies::default());
            }
            if now >= self.next_lagrq {
                self.next_lagrq = now + LAGRQ_INTERVAL;
                self.send_iax(now, command::LAGRQ, Ies::default());
            }
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address() -> SocketAddr {
        "192.0.2.1:4569".parse().unwrap()
    }

    #[test]
    fn full_frames_round_trip() {
        let frame = FullFrame { source_call: 5, dest_call: 9, retransmission: true, timestamp: 1234, oseq: 3, iseq: 4, frame_type: frame_type::VOICE, subclass: FORMAT_ULAW, payload: vec![1, 2, 3] };
        assert_eq!(parse(&frame.to_bytes()), Some(Datagram::Full(frame)));
    }

    #[test]
    fn large_subclasses_are_sent_as_exponents() {
        assert_eq!(compress_subclass(1 << 15), 0x8F);
        assert_eq!(uncompress_subclass(0x8F), 1 << 15);
        assert_eq!(compress_subclass(FORMAT_ULAW), 4);
    }

    #[test]
    fn mini_frames_and_meta_frames_are_told_apart() {
        assert_eq!(parse(&[0x00, 0x07, 0x01, 0x02, 0xAA]), Some(Datagram::Mini { source_call: 7, timestamp: 0x0102, payload: vec![0xAA] }));
        assert_eq!(parse(&[0x00, 0x00, 0x01, 0x02]), None);
    }

    #[test]
    fn elements_round_trip_and_the_last_repeat_wins() {
        let ies = Ies::default().add_u16(ie::VERSION, 2).add_text(ie::CALLED_NUMBER, "1999").add_text(ie::CALLED_NUMBER, "2000").add_versioned_u64(ie::FORMAT2, FORMAT_ULAW);
        let parsed = Ies::parse(&ies.to_bytes()).unwrap();
        assert_eq!(parsed.get_text(ie::CALLED_NUMBER).as_deref(), Some("2000"));
        assert_eq!(parsed.get_codec(ie::FORMAT2, ie::FORMAT), Some(FORMAT_ULAW));
        assert_eq!(Ies::parse(&[ie::VERSION, 5, 0]), None);
    }

    // Hand every datagram one session produced to the other, as a network would
    fn deliver(from: &mut Session, to: &mut Session, now: Instant) -> Vec<Event> {
        let mut events = Vec::new();
        for bytes in from.take_outbox() {
            match parse(&bytes).unwrap() {
                Datagram::Full(frame) => events.extend(to.handle_full(now, frame)),
                Datagram::Mini { timestamp, payload, .. } => events.extend(to.handle_mini(now, timestamp, payload)),
            }
        }
        events
    }

    #[test]
    fn reliable_frames_are_acknowledged_and_stop_retransmitting() {
        let now = Instant::now();
        let mut caller = Session::new(2, 0, address(), now);
        let mut callee = Session::new(3, 2, address(), now);
        caller.start_call(now, Ies::default().add_u16(ie::VERSION, 2));
        let events = deliver(&mut caller, &mut callee, now);
        assert!(matches!(&events[..], [Event::Iax { command: command::NEW, .. }]));

        // The callee's ACCEPT carries the acknowledgement of the NEW
        callee.send_iax(now, command::ACCEPT, Ies::default());
        let events = deliver(&mut callee, &mut caller, now);
        assert!(matches!(&events[..], [Event::Iax { command: command::ACCEPT, .. }]));
        assert_eq!(caller.peer_call, 3);
        assert!(caller.pending.is_empty());

        // Without an acknowledgement the ACCEPT is sent again
        let later = now + Duration::from_secs(3);
        callee.poll(later);
        assert_eq!(callee.take_outbox().len(), 1);
    }

    #[test]
    fn voice_starts_as_a_full_frame_then_goes_out_as_mini_frames() {
        let now = Instant::now();
        let mut sender = Session::new(2, 3, address(), now);
        let mut receiver = Session::new(3, 2, address(), now);
        sender.start(FORMAT_ULAW);
        receiver.start(FORMAT_ULAW);
        for index in 0..3u64 {
            sender.send_voice(now + Duration::from_millis(index * 20), FORMAT_ULAW, &[0x7F; 160]);
        }
        let sizes: Vec<usize> = sender.outbox.iter().map(Vec::len).collect();
        assert_eq!(sizes, [12 + 160, 4 + 160, 4 + 160]);
        let events = deliver(&mut sender, &mut receiver, now);
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|event| matches!(event, Event::Voice { payload, .. } if payload.len() == 160)));
    }

    #[test]
    fn a_peer_that_never_answers_times_the_call_out() {
        let mut now = Instant::now();
        let mut caller = Session::new(2, 0, address(), now);
        caller.start_call(now, Ies::default());
        let mut timed_out = false;
        for _ in 0..40 {
            now += Duration::from_secs(5);
            timed_out |= caller.poll(now).contains(&Event::Timeout);
        }
        assert!(timed_out && caller.destroyed);
    }

    #[test]
    fn a_call_token_restarts_the_call_with_the_token() {
        let now = Instant::now();
        let mut caller = Session::new(2, 0, address(), now);
        caller.start_call(now, Ies::default().add_text(ie::CALLED_NUMBER, "1999"));
        caller.take_outbox();
        let reply = FullFrame { source_call: 1, dest_call: 2, retransmission: false, timestamp: 0, oseq: 0, iseq: 1, frame_type: frame_type::IAX, subclass: command::CALLTOKEN as u64, payload: Ies::default().add(ie::CALLTOKEN, b"123?abc").to_bytes() };
        caller.handle_full(now, reply);
        let Some(Datagram::Full(frame)) = caller.take_outbox().first().and_then(|bytes| parse(bytes)) else { panic!("no NEW resent") };
        let ies = Ies::parse(&frame.payload).unwrap();
        assert_eq!((frame.oseq, frame.dest_call, ies.get(ie::CALLTOKEN)), (0, 0, Some(&b"123?abc"[..])));
        assert_eq!(ies.get_text(ie::CALLED_NUMBER).as_deref(), Some("1999"));
    }
}
