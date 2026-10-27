// MMDVM modem driver for D-STAR, speaking the modem's UART protocol (versions 1 and 2)

use crate::config::{Modem, Station};
use crate::state::{self, Shared};
use crate::dstar::{Frame, Header, HEADER_LENGTH};
use anyhow::{bail, Context};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout, Instant};
use tokio_serial::SerialPortBuilderExt;

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

struct ModemFrame {
    kind: u8,
    payload: Vec<u8>,
}

struct Link {
    port: tokio_serial::SerialStream,
    buffer: Vec<u8>,
    protocol_version: u8,
}

impl Link {
    async fn send(&mut self, kind: u8, payload: &[u8]) -> anyhow::Result<()> {
        let length = payload.len() + 3;
        let mut packet = vec![FRAME_START];
        if length > 255 {
            packet.extend([0, (length - 255) as u8]);
        } else {
            packet.push(length as u8);
        }
        packet.push(kind);
        packet.extend(payload);
        self.port.write_all(&packet).await?;
        Ok(())
    }

    // Split one complete frame off the front of the receive buffer
    fn take_frame(&mut self) -> Option<ModemFrame> {
        loop {
            let start = self.buffer.iter().position(|byte| *byte == FRAME_START)?;
            self.buffer.drain(..start);
            if self.buffer.len() < 3 {
                return None;
            }
            let (length, header_length) = if self.buffer[1] == 0 {
                if self.buffer.len() < 4 {
                    return None;
                }
                (self.buffer[2] as usize + 255, 4)
            } else {
                (self.buffer[1] as usize, 3)
            };
            if length < header_length {
                self.buffer.drain(..1);
                continue;
            }
            if self.buffer.len() < length {
                return None;
            }
            let frame = ModemFrame { kind: self.buffer[header_length - 1], payload: self.buffer[header_length..length].to_vec() };
            self.buffer.drain(..length);
            return Some(frame);
        }
    }

    async fn receive(&mut self) -> anyhow::Result<ModemFrame> {
        loop {
            if let Some(frame) = self.take_frame() {
                return Ok(frame);
            }
            let mut chunk = [0u8; 512];
            let count = self.port.read(&mut chunk).await?;
            if count == 0 {
                bail!("modem port closed");
            }
            self.buffer.extend(&chunk[..count]);
        }
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

fn set_frequency_payload(frequency_hz: u32, rf_level: f32) -> Vec<u8> {
    let mut payload = vec![0u8; 14];
    payload[1..5].copy_from_slice(&frequency_hz.to_le_bytes());
    payload[5..9].copy_from_slice(&frequency_hz.to_le_bytes());
    payload[9] = level_byte(rf_level);
    payload
}

async fn connect(modem: &Modem, station: &Station, shared: &Shared) -> anyhow::Result<Link> {
    let port = tokio_serial::new(&modem.port, modem.baud).open_native_async().with_context(|| format!("couldn't open {}", modem.port))?;
    let mut link = Link { port, buffer: Vec::new(), protocol_version: 0 };

    // Find out what the modem speaks, retrying while it boots
    sleep(Duration::from_secs(2)).await;
    for attempt in 1..=6 {
        link.send(GET_VERSION, &[]).await?;
        match link.expect(GET_VERSION).await {
            Ok(frame) => {
                link.protocol_version = frame.payload[0];
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
    link.send(SET_FREQ, &set_frequency_payload(frequency_hz, modem.rf_level)).await?;
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

async fn session(link: &mut Link, from_modem: &mpsc::Sender<Frame>, to_modem: &mut mpsc::Receiver<Frame>) -> anyhow::Result<()> {
    let mut status_timer = interval(STATUS_INTERVAL);
    let mut last_status = Instant::now();
    let mut free_slots = 0u8;
    let mut pending: VecDeque<Frame> = VecDeque::new();
    loop {
        tokio::select! {
            _ = status_timer.tick() => {
                if last_status.elapsed() > STATUS_WATCHDOG {
                    bail!("modem stopped answering status requests");
                }
                link.send(GET_STATUS, &[]).await?;
            }
            outgoing = to_modem.recv() => pending.push_back(outgoing.context("gateway side closed")?),
            frame = link.receive() => {
                let frame = frame?;
                match frame.kind {
                    GET_STATUS => {
                        last_status = Instant::now();
                        if let Some((_transmitting, space)) = parse_status(link.protocol_version, &frame.payload) {
                            free_slots = space;
                        }
                    }
                    DSTAR_HEADER if frame.payload.len() >= HEADER_LENGTH => {
                        if let Some(header) = Header::from_body(&frame.payload[..39]) {
                            from_modem.send(Frame::Header(header)).await?;
                        }
                    }
                    DSTAR_DATA if frame.payload.len() >= 12 => {
                        let ambe = frame.payload[..9].try_into()?;
                        let slow_data = frame.payload[9..12].try_into()?;
                        from_modem.send(Frame::Voice { sequence: 0, ambe, slow_data }).await?;
                    }
                    DSTAR_LOST | DSTAR_EOT => from_modem.send(Frame::End).await?,
                    NAK => log::warn!("Modem NAKed command {:#04x}", frame.payload.first().copied().unwrap_or(0)),
                    _ => {}
                }
            }
        }

        // Feed the modem's transmit buffer as space allows
        while let Some(next) = pending.front() {
            let (needed, kind, payload) = match next {
                Frame::Header(header) => (5, DSTAR_HEADER, header.to_bytes()),
                Frame::Voice { ambe, slow_data, .. } => (2, DSTAR_DATA, [&ambe[..], &slow_data[..]].concat()),
                Frame::End => (2, DSTAR_EOT, Vec::new()),
            };
            if free_slots < needed {
                break;
            }
            link.send(kind, &payload).await?;
            free_slots -= 1;
            pending.pop_front();
        }
    }
}

// Run the modem forever, reopening it whenever it drops out
pub async fn run(modem: Modem, station: Station, shared: Shared, from_modem: mpsc::Sender<Frame>, mut to_modem: mpsc::Receiver<Frame>) {
    loop {
        match connect(&modem, &station, &shared).await {
            Ok(mut link) => {
                log::info!("Modem ready on {}", modem.port);
                state::update(&shared, |snapshot| snapshot.modem_connected = true);
                if let Err(error) = session(&mut link, &from_modem, &mut to_modem).await {
                    log::error!("Modem session ended: {error:#}");
                }
                state::update(&shared, |snapshot| snapshot.modem_connected = false);
            }
            Err(error) => log::error!("Couldn't start modem: {error:#}"),
        }
        sleep(RECONNECT_DELAY).await;
    }
}
