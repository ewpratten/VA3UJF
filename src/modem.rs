// MMDVM modem driver for D-STAR, speaking the modem's UART protocol (versions 1 and 2)

use crate::config::{Modem, Station};
use crate::state::{self, Shared};
use crate::dstar::{Frame, HeaderWithCrc};
use crate::live::Event;
use anyhow::{bail, Context};
use futures_util::{SinkExt, StreamExt};
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout, Instant};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder, Framed};
use zerocopy::byteorder::{LittleEndian, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

const FRAME_START: u8 = 0xE0;
const GET_VERSION: u8 = 0x00;
const GET_STATUS: u8 = 0x01;
const SET_CONFIG: u8 = 0x02;
const SET_FREQ: u8 = 0x04;
const DSTAR_HEADER: u8 = 0x10;
const DSTAR_DATA: u8 = 0x11;
const DSTAR_LOST: u8 = 0x12;
const DSTAR_EOT: u8 = 0x13;
const ACK: u8 = 0x70;
const NAK: u8 = 0x7F;

const STATUS_INTERVAL: Duration = Duration::from_millis(250);
const STATUS_WATCHDOG: Duration = Duration::from_secs(2);
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

// Five seconds of voice waiting on the modem
const MAX_PENDING_FRAMES: usize = 250;

struct ModemFrame {
    kind: u8,
    payload: Vec<u8>,
}

// Frames are a start byte, a length that counts the whole frame, a type, and the payload. Lengths past 255 spill into a third byte
struct MmdvmCodec;

impl Decoder for MmdvmCodec {
    type Item = ModemFrame;
    type Error = std::io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<ModemFrame>, Self::Error> {
        loop {
            // Resynchronise on the next start byte after any noise
            let Some(start) = source.iter().position(|byte| *byte == FRAME_START) else {
                source.clear();
                return Ok(None);
            };
            source.advance(start);
            if source.len() < 3 {
                return Ok(None);
            }
            let (length, header_length) = if source[1] == 0 {
                if source.len() < 4 {
                    return Ok(None);
                }
                (source[2] as usize + 255, 4)
            } else {
                (source[1] as usize, 3)
            };
            if length < header_length {
                source.advance(1);
                continue;
            }
            if source.len() < length {
                return Ok(None);
            }
            let frame = source.split_to(length);
            return Ok(Some(ModemFrame { kind: frame[header_length - 1], payload: frame[header_length..].to_vec() }));
        }
    }
}

impl Encoder<ModemFrame> for MmdvmCodec {
    type Error = std::io::Error;

    fn encode(&mut self, frame: ModemFrame, destination: &mut BytesMut) -> Result<(), Self::Error> {
        let mut length = frame.payload.len() + 3;
        destination.put_u8(FRAME_START);
        if length > 255 {
            // The long form's extra length byte counts toward the frame
            length += 1;
            destination.put_slice(&[0, (length - 255) as u8]);
        } else {
            destination.put_u8(length as u8);
        }
        destination.put_u8(frame.kind);
        destination.put_slice(&frame.payload);
        Ok(())
    }
}

// Raw signal readings against what they mean in dBm, as MMDVMHost's RSSI.dat lists them. In-between values are interpolated
#[derive(Default)]
struct Calibration(BTreeMap<u16, i32>);

impl Calibration {
    fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("couldn't read {}", path.display()))?;
        let points = text
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter_map(|line| {
                let mut words = line.split_whitespace();
                Some((words.next()?.parse().ok()?, words.next()?.parse().ok()?))
            })
            .collect();
        Ok(Calibration(points))
    }

    fn dbm(&self, raw: u16) -> Option<f32> {
        let (above_raw, above_dbm) = self.0.range(raw..).next().map(|(raw, dbm)| (*raw, *dbm)).or_else(|| self.0.iter().next_back().map(|(raw, dbm)| (*raw, *dbm)))?;
        let Some((below_raw, below_dbm)) = self.0.range(..raw).next_back().map(|(raw, dbm)| (*raw, *dbm)).filter(|_| above_raw >= raw) else { return Some(above_dbm as f32) };
        let position = (raw - below_raw) as f32 / (above_raw - below_raw) as f32;
        Some((1.0 - position) * below_dbm as f32 + position * above_dbm as f32)
    }
}

// The modem appends its average signal reading to a header, and to the first frame of each superframe, when its firmware is built to
fn signal_reading(payload: &[u8], frame_length: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*payload.get(frame_length)?, *payload.get(frame_length + 1)?]))
}

struct Link {
    port: Framed<tokio_serial::SerialStream, MmdvmCodec>,
    protocol_version: u8,
}

impl Link {
    async fn send(&mut self, kind: u8, payload: &[u8]) -> anyhow::Result<()> {
        Ok(self.port.send(ModemFrame { kind, payload: payload.to_vec() }).await?)
    }

    async fn receive(&mut self) -> anyhow::Result<ModemFrame> {
        Ok(self.port.next().await.context("modem port closed")??)
    }

    // Wait for the modem's answer to a command, skipping anything unrelated
    async fn expect(&mut self, kind: u8) -> anyhow::Result<ModemFrame> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let frame = timeout(deadline.saturating_duration_since(Instant::now()), self.receive()).await.context("modem didn't answer")??;
            if frame.kind == kind {
                return Ok(frame);
            }
            if frame.kind == NAK {
                bail!("modem refused command {:#04x}", frame.payload.first().copied().unwrap_or(0));
            }
        }
    }
}

fn level_byte(percent: f32) -> u8 {
    (percent * 2.55 + 0.5) as u8
}

fn set_config_payload(protocol_version: u8, modem: &Modem) -> Vec<u8> {
    let flags = (modem.rx_invert as u8) | (modem.tx_invert as u8) << 1 | (modem.ptt_invert as u8) << 2 | 0x80;
    let tx_delay = (modem.tx_delay_ms / 10) as u8;
    let mut payload = vec![0u8; if protocol_version == 1 { 23 } else { 37 }];
    payload[0] = flags;
    payload[1] = 0x01;
    if protocol_version == 1 {
        payload[2] = tx_delay;
        payload[4] = level_byte(modem.rx_level);
        payload[5] = level_byte(modem.rx_level);
        payload[9] = level_byte(modem.tx_level);
        payload[8] = 128;
        payload[13] = 128;
        payload[14] = 128;
    } else {
        payload[3] = tx_delay;
        payload[5] = 128;
        payload[6] = 128;
        payload[7] = level_byte(modem.rx_level);
        payload[8] = level_byte(modem.rx_level);
        payload[9] = level_byte(modem.tx_level);
        payload[28] = 128;
    }
    payload
}

// The modem checks the POCSAG transmit frequency against its bands even though D-STAR never uses it, so it can't be left blank
#[derive(Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct SetFrequency {
    reserved: u8,
    rx_hz: U32<LittleEndian>,
    tx_hz: U32<LittleEndian>,
    rf_level: u8,
    pocsag_hz: U32<LittleEndian>,
}

fn set_frequency_payload(frequency_hz: u32, rf_level: f32) -> SetFrequency {
    SetFrequency { reserved: 0, rx_hz: frequency_hz.into(), tx_hz: frequency_hz.into(), rf_level: level_byte(rf_level), pocsag_hz: frequency_hz.into() }
}

async fn connect(modem: &Modem, station: &Station, shared: &Shared) -> anyhow::Result<Link> {
    let port = tokio_serial::new(&modem.port, modem.baud).open_native_async().with_context(|| format!("couldn't open {}", modem.port))?;
    let mut link = Link { port: Framed::new(port, MmdvmCodec), protocol_version: 0 };

    // Find out what the modem speaks, retrying while it boots
    sleep(Duration::from_secs(2)).await;
    for attempt in 1..=6 {
        link.send(GET_VERSION, &[]).await?;
        match link.expect(GET_VERSION).await {
            Ok(frame) => {
                link.protocol_version = *frame.payload.first().context("modem sent an empty version reply")?;
                let firmware: String = String::from_utf8_lossy(&frame.payload[1..]).chars().filter(|character| !character.is_control()).collect();
                log::info!("Modem: {firmware}");
                state::update(shared, |snapshot| snapshot.modem_firmware = firmware);
                break;
            }
            Err(error) if attempt == 6 => return Err(error),
            Err(_) => sleep(Duration::from_millis(1500)).await,
        }
    }
    if !matches!(link.protocol_version, 1 | 2) {
        bail!("unsupported MMDVM protocol version {}", link.protocol_version);
    }

    // Tune and configure for D-STAR only, simplex
    let frequency_hz = (station.frequency_mhz * 1_000_000.0) as u32;
    link.send(SET_FREQ, set_frequency_payload(frequency_hz, modem.rf_level).as_bytes()).await?;
    link.expect(ACK).await.context("SET_FREQ")?;
    link.send(SET_CONFIG, &set_config_payload(link.protocol_version, modem)).await?;
    link.expect(ACK).await.context("SET_CONFIG")?;
    Ok(link)
}

// Free D-STAR slots and the transmit flag from a status reply
fn parse_status(protocol_version: u8, payload: &[u8]) -> Option<(bool, u8)> {
    let flags_index = if protocol_version == 1 { 2 } else { 1 };
    Some((payload.get(flags_index)? & 0x01 == 0x01, *payload.get(3)?))
}

// Hand what the radio heard to the router without waiting on it, so the modem is always polled and fed
fn pass_up(from_modem: &mpsc::Sender<Frame>, frame: Frame) -> anyhow::Result<()> {
    match from_modem.try_send(frame) {
        Err(mpsc::error::TrySendError::Closed(_)) => bail!("gateway side closed"),
        Err(mpsc::error::TrySendError::Full(_)) => log::debug!("Router isn't keeping up, dropping a frame from the radio"),
        Ok(()) => {}
    }
    Ok(())
}

// What a frame takes to send: the free slots the modem must report, the slots it then uses up, and the command carrying it
fn transmit_command(frame: &Frame) -> (u8, u8, u8, Vec<u8>) {
    match frame {
        Frame::Header(header) => (5, 4, DSTAR_HEADER, header.with_crc().as_bytes().to_vec()),
        Frame::Voice { ambe, slow_data, .. } => (2, 1, DSTAR_DATA, [&ambe[..], &slow_data[..]].concat()),
        Frame::End => (2, 1, DSTAR_EOT, Vec::new()),
    }
}

// Queue a frame for the modem. Only whole streams go out, so voice that arrives without its header is dropped
fn queue_for_transmit(pending: &mut VecDeque<Frame>, in_stream: &mut bool, frame: Frame) {
    match frame {
        Frame::Header(_) => *in_stream = true,
        _ if !*in_stream => return,
        Frame::End => *in_stream = false,
        Frame::Voice { .. } => {}
    }
    if pending.len() >= MAX_PENDING_FRAMES {
        log::warn!("Modem isn't taking frames, dropping the stream it was sent");
        pending.clear();
        *in_stream = false;
        return;
    }
    pending.push_back(frame);
}

async fn session(link: &mut Link, calibration: &Calibration, shared: &Shared, from_modem: &mpsc::Sender<Frame>, to_modem: &mut mpsc::Receiver<Frame>) -> anyhow::Result<()> {
    let mut status_timer = interval(STATUS_INTERVAL);
    let mut last_status = Instant::now();
    let mut free_slots = 0u8;
    let mut pending: VecDeque<Frame> = VecDeque::new();
    let mut in_stream = false;
    loop {
        tokio::select! {
            _ = status_timer.tick() => {
                if last_status.elapsed() > STATUS_WATCHDOG {
                    bail!("modem stopped answering status requests");
                }
                link.send(GET_STATUS, &[]).await?;
            }
            outgoing = to_modem.recv() => queue_for_transmit(&mut pending, &mut in_stream, outgoing.context("gateway side closed")?),
            frame = link.receive() => {
                let frame = frame?;
                match frame.kind {
                    GET_STATUS => {
                        last_status = Instant::now();
                        if let Some((_transmitting, space)) = parse_status(link.protocol_version, &frame.payload) {
                            free_slots = space;
                        }
                    }
                    DSTAR_HEADER => {
                        if let Ok((framed, _)) = HeaderWithCrc::read_from_prefix(&frame.payload) {
                            pass_up(from_modem, Frame::Header(framed.header))?;
                            report_signal(shared, calibration, signal_reading(&frame.payload, size_of::<HeaderWithCrc>()));
                        }
                    }
                    DSTAR_DATA if frame.payload.len() >= 12 => {
                        let ambe = frame.payload[..9].try_into()?;
                        let slow_data = frame.payload[9..12].try_into()?;
                        pass_up(from_modem, Frame::Voice { sequence: 0, ambe, slow_data })?;
                        report_signal(shared, calibration, signal_reading(&frame.payload, 12));
                    }
                    DSTAR_LOST | DSTAR_EOT => pass_up(from_modem, Frame::End)?,
                    NAK => log::warn!("Modem NAKed command {:#04x}", frame.payload.first().copied().unwrap_or(0)),
                    _ => {}
                }
            }
        }

        // Feed the modem's transmit buffer as space allows
        while let Some(next) = pending.front() {
            let (needed, used, kind, payload) = transmit_command(next);
            if free_slots < needed {
                break;
            }
            link.send(kind, &payload).await?;
            free_slots -= used;
            pending.pop_front();
        }
    }
}

fn report_signal(shared: &Shared, calibration: &Calibration, raw: Option<u16>) {
    if let Some(raw) = raw {
        shared.emit(Event::Signal { raw, dbm: calibration.dbm(raw) });
    }
}

// Run the modem forever, reopening it whenever it drops out
pub async fn run(modem: Modem, station: Station, shared: Shared, from_modem: mpsc::Sender<Frame>, mut to_modem: mpsc::Receiver<Frame>) {
    let calibration = modem.rssi_file.as_deref().map(Calibration::load).transpose().unwrap_or_else(|error| {
        log::warn!("Signal levels won't be in dBm: {error:#}");
        None
    });
    let calibration = calibration.unwrap_or_default();
    loop {
        match discard_while(&mut to_modem, connect(&modem, &station, &shared)).await {
            Ok(mut link) => {
                log::info!("Modem ready on {}", modem.port);
                state::update(&shared, |snapshot| snapshot.modem_connected = true);
                if let Err(error) = session(&mut link, &calibration, &shared, &from_modem, &mut to_modem).await {
                    log::error!("Modem session ended: {error:#}");
                }
                state::update(&shared, |snapshot| snapshot.modem_connected = false);
            }
            Err(error) => log::error!("Couldn't start modem: {error:#}"),
        }
        discard_while(&mut to_modem, sleep(RECONNECT_DELAY)).await;
    }
}

// Throw away what the router sends while there's no modem to take it, so nothing stale goes out once there is
async fn discard_while<T>(to_modem: &mut mpsc::Receiver<Frame>, work: impl Future<Output = T>) -> T {
    tokio::pin!(work);
    loop {
        tokio::select! {
            output = &mut work => return output,
            frame = to_modem.recv() => if frame.is_none() {
                return work.await;
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_frequency_fills_every_field_the_modem_checks() {
        // MMDVMHost's layout: a spare byte, receive and transmit frequencies, power, then the POCSAG frequency
        let payload = set_frequency_payload(446_500_000, 50.0);
        assert_eq!(payload.as_bytes().len(), 14);
        assert_eq!(payload.reserved, 0);
        assert_eq!(payload.rx_hz.get(), 446_500_000);
        assert_eq!(payload.tx_hz.get(), 446_500_000);
        assert_eq!(payload.rf_level, 128);
        assert_eq!(payload.pocsag_hz.get(), 446_500_000);
    }

    #[test]
    fn signal_readings_are_interpolated_between_calibration_points() {
        let calibration = Calibration([(1000, -109), (1123, -100), (1134, -90)].into());
        assert_eq!(calibration.dbm(1123), Some(-100.0));
        assert!((calibration.dbm(1128).unwrap() + 95.45).abs() < 0.1);
        assert_eq!((calibration.dbm(10), calibration.dbm(2000)), (Some(-109.0), Some(-90.0)));
        assert_eq!(Calibration::default().dbm(1000), None);
    }

    #[test]
    fn readings_are_only_there_when_the_modem_sends_them() {
        assert_eq!(signal_reading(&[0; 12], 12), None);
        assert_eq!(signal_reading(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x5B], 12), Some(0x045B));
    }

    fn voice() -> Frame {
        Frame::Voice { sequence: 0, ambe: [1; 9], slow_data: [2; 3] }
    }

    fn header() -> Frame {
        Frame::Header(crate::dstar::Header { flags: [0; 3], repeater_2: [b' '; 8], repeater_1: [b' '; 8], your_call: [b' '; 8], my_call: [b' '; 8], my_suffix: [b' '; 4] })
    }

    #[test]
    fn a_header_uses_four_slots_of_the_modems_buffer() {
        let slots = |frame: Frame| {
            let (needed, used, _, _) = transmit_command(&frame);
            (needed, used)
        };
        assert_eq!((slots(header()), slots(voice()), slots(Frame::End)), ((5, 4), (2, 1), (2, 1)));
    }

    #[test]
    fn only_whole_streams_are_queued_and_the_queue_is_bounded() {
        let (mut pending, mut in_stream) = (VecDeque::new(), false);
        for frame in [voice(), Frame::End, header(), voice(), Frame::End, voice()] {
            queue_for_transmit(&mut pending, &mut in_stream, frame);
        }
        assert_eq!(pending, [header(), voice(), Frame::End]);

        queue_for_transmit(&mut pending, &mut in_stream, header());
        for _ in 0..MAX_PENDING_FRAMES {
            queue_for_transmit(&mut pending, &mut in_stream, voice());
        }
        assert!(pending.is_empty() && !in_stream);
    }

    #[tokio::test]
    async fn frames_sent_while_the_modem_is_away_are_discarded() {
        let (sender, mut receiver) = mpsc::channel(4);
        let filler = tokio::spawn(async move {
            for _ in 0..20 {
                sender.send(voice()).await.unwrap();
            }
        });
        discard_while(&mut receiver, sleep(Duration::from_millis(100))).await;
        filler.await.unwrap();
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn codec_resynchronises_and_handles_long_frames() {
        let mut codec = MmdvmCodec;
        let mut encoded = BytesMut::new();
        codec.encode(ModemFrame { kind: DSTAR_DATA, payload: vec![7; 300] }, &mut encoded).unwrap();
        let mut source = BytesMut::from(&[0x00, 0x55][..]);
        source.extend_from_slice(&encoded);
        let frame = codec.decode(&mut source).unwrap().unwrap();
        assert_eq!((frame.kind, frame.payload.len()), (DSTAR_DATA, 300));
        assert!(codec.decode(&mut source).unwrap().is_none());
    }
}
