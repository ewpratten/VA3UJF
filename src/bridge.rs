// Bridge between D-STAR voice and plain audio, for links that don't speak D-STAR. Vocodes through an AMBEserver

use crate::codec::ambe::{Decoded, Vocoder};
use crate::codec::{apply_gain, decibels_to_gain, Audio, SAMPLES_PER_FRAME};
use crate::config::Station;
use crate::dstar::{build_slow_data_blocks, pad, Frame, Header, FRAMES_PER_SUPERFRAME};
use crate::router::Port;
use crate::state::{self, Shared};
use anyhow::Context;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

// Stream housekeeping
const VOCODER_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const DSTAR_STREAM_TIMEOUT: Duration = Duration::from_secs(1);
const AUDIO_STREAM_TIMEOUT: Duration = Duration::from_millis(300);
const ENCODER_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

pub struct Settings {
    // How the bridge is named in the log and the status page
    pub name: &'static str,
    pub ambeserver: String,

    // Shown as the "name" on radios that display it, with a text message beside it
    pub suffix: String,
    pub message: String,
    pub rx_gain_db: f32,
    pub tx_gain_db: f32,
}

struct Bridge {
    settings: Settings,
    station: Station,
    to_router: mpsc::Sender<Frame>,
    shared: Shared,
    to_audio: mpsc::Sender<Audio>,
    vocoder_address: SocketAddr,
    vocoder: Option<Vocoder>,
    vocoder_last_attempt: Instant,
    slow_data_blocks: Vec<[u8; 3]>,
    receive_gain: f32,
    transmit_gain: f32,

    // D-STAR -> audio stream state
    inbound_active: bool,
    inbound_last_heard: Instant,
    warned_no_vocoder_inbound: bool,

    // Audio -> D-STAR stream state
    outbound_active: bool,
    outbound_sequence: u8,
    outbound_last_heard: Instant,
    outbound_ending_since: Option<Instant>,
    outbound_pending_encodes: u32,
    warned_no_vocoder_outbound: bool,
}

impl Bridge {
    fn lose_vocoder(&mut self, error: std::io::Error) {
        log::warn!("Vocoder lost: {error}");
        self.vocoder = None;
        state::update(&self.shared, |snapshot| state::set_vocoder(snapshot, self.settings.name, false));
        self.outbound_pending_encodes = 0;
    }

    async fn send_audio(&mut self, audio: Audio) {
        let _ = self.to_audio.send(audio).await;
    }

    // ---- D-STAR side ----

    async fn handle_router_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Header(header) => {
                log::info!("D-STAR -> {}: {}", self.settings.name, header.my_call_text());
                self.inbound_active = true;
                self.inbound_last_heard = Instant::now();
            }
            Frame::Voice { ambe, .. } if self.inbound_active => {
                self.inbound_last_heard = Instant::now();
                let Some(vocoder) = self.vocoder.as_mut() else {
                    if !self.warned_no_vocoder_inbound {
                        log::warn!("No vocoder attached, dropping D-STAR audio");
                        self.warned_no_vocoder_inbound = true;
                    }
                    return;
                };
                if let Err(error) = vocoder.decode(&ambe).await {
                    self.lose_vocoder(error);
                }
            }
            Frame::End if self.inbound_active => self.end_inbound().await,
            _ => {}
        }
    }

    async fn end_inbound(&mut self) {
        log::info!("D-STAR -> {}: end of transmission", self.settings.name);
        self.inbound_active = false;
        self.warned_no_vocoder_inbound = false;
        self.send_audio(Audio::End).await;
    }

    // ---- Audio side ----

    async fn handle_audio(&mut self, audio: Audio) {
        // The end marker closes the transmission
        let samples = match audio {
            Audio::Voice(samples) if samples.len() == SAMPLES_PER_FRAME => samples,
            _ => {
                if self.outbound_active && self.outbound_ending_since.is_none() {
                    self.outbound_ending_since = Some(Instant::now());
                }
                return;
            }
        };

        // D-STAR has the channel, so drop this audio
        if self.inbound_active {
            return;
        }
        if self.vocoder.is_none() {
            if !self.warned_no_vocoder_outbound {
                log::warn!("No vocoder attached, dropping {} audio", self.settings.name);
                self.warned_no_vocoder_outbound = true;
            }
            return;
        }

        // Open a new D-STAR stream on the first frame
        if !self.outbound_active {
            log::info!("{} -> D-STAR: start of transmission", self.settings.name);
            self.outbound_active = true;
            self.outbound_sequence = 0;
            self.outbound_pending_encodes = 0;
            let repeater_1 = format!("{:<7}{}", self.station.callsign, self.station.module);
            let repeater_2 = format!("{:<7}G", self.station.callsign);
            let header = Header { flags: [0; 3], repeater_2: pad(&repeater_2), repeater_1: pad(&repeater_1), your_call: pad("CQCQCQ"), my_call: pad(&self.station.callsign), my_suffix: pad(&self.settings.suffix) };
            let _ = self.to_router.send(Frame::Header(header)).await;
        }
        self.outbound_last_heard = Instant::now();
        self.outbound_ending_since = None;

        // Hand the audio to the encoder, which answers asynchronously
        let samples = apply_gain(samples.into_iter(), self.transmit_gain);
        match self.vocoder.as_mut().unwrap().encode(&samples).await {
            Ok(()) => self.outbound_pending_encodes += 1,
            Err(error) => self.lose_vocoder(error),
        }
    }

    async fn handle_vocoder(&mut self, decoded: Vec<Decoded>) {
        for item in decoded {
            match item {
                // Decoded speech heads out as audio
                Decoded::Speech(samples) => {
                    let samples = apply_gain(samples.into_iter(), self.receive_gain);
                    self.send_audio(Audio::Voice(samples)).await;
                }

                // Encoded AMBE heads to D-STAR
                Decoded::Frame(ambe) => {
                    self.outbound_pending_encodes = self.outbound_pending_encodes.saturating_sub(1);
                    if self.outbound_active {
                        let slow_data = self.slow_data_blocks[self.outbound_sequence as usize];
                        let _ = self.to_router.send(Frame::Voice { sequence: self.outbound_sequence, ambe, slow_data }).await;
                        self.outbound_sequence = (self.outbound_sequence + 1) % FRAMES_PER_SUPERFRAME;
                    }
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
            match Vocoder::connect(self.vocoder_address).await {
                Ok(vocoder) => {
                    self.vocoder = Some(vocoder);
                    state::update(&self.shared, |snapshot| state::set_vocoder(snapshot, self.settings.name, true));
                }
                Err(error) => log::warn!("Could not initialize vocoder at {}: {error:#}", self.vocoder_address),
            }
        }

        // Keep the vocoder session alive between transmissions
        let idle = !self.inbound_active && !self.outbound_active;
        if let (Some(vocoder), true) = (self.vocoder.as_mut(), idle)
            && let Err(error) = vocoder.keep_alive().await
        {
            self.lose_vocoder(error);
        }

        // Close out a D-STAR stream that lost its terminator
        if self.inbound_active && now.duration_since(self.inbound_last_heard) > DSTAR_STREAM_TIMEOUT {
            self.end_inbound().await;
        }

        // Close out an audio stream once it stops and the encoder has drained
        if self.outbound_active {
            if self.outbound_ending_since.is_none() && now.duration_since(self.outbound_last_heard) > AUDIO_STREAM_TIMEOUT {
                self.outbound_ending_since = Some(now);
            }
            if let Some(since) = self.outbound_ending_since
                && (self.outbound_pending_encodes == 0 || now.duration_since(since) > ENCODER_DRAIN_TIMEOUT)
            {
                log::info!("{} -> D-STAR: end of transmission", self.settings.name);
                self.outbound_active = false;
                self.outbound_ending_since = None;
                self.warned_no_vocoder_outbound = false;
                let _ = self.to_router.send(Frame::End).await;
            }
        }
    }
}

// Run the bridge, taking D-STAR frames from the router and handing back audio, and turning audio it's given into D-STAR frames
pub async fn run(settings: Settings, station: Station, shared: Shared, port: Port, mut from_audio: mpsc::Receiver<Audio>, to_audio: mpsc::Sender<Audio>) -> anyhow::Result<()> {
    let vocoder_address = lookup_host(settings.ambeserver.as_str()).await?.next().context("couldn't resolve the AMBEserver address")?;
    let Port { to_router, mut from_router } = port;

    let now = Instant::now();
    let mut bridge = Bridge {
        slow_data_blocks: build_slow_data_blocks(&settings.message),
        receive_gain: decibels_to_gain(settings.rx_gain_db),
        transmit_gain: decibels_to_gain(settings.tx_gain_db),
        settings,
        station,
        to_router,
        shared,
        to_audio,
        vocoder_address,
        vocoder: None,
        vocoder_last_attempt: now - VOCODER_RETRY_INTERVAL,
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
    loop {
        let vocoder_ready = async {
            match &bridge.vocoder {
                Some(vocoder) => vocoder.receive().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            audio = from_audio.recv() => match audio {
                Some(audio) => bridge.handle_audio(audio).await,
                None => return Ok(()),
            },
            received = vocoder_ready => {
                if let Ok(decoded) = received {
                    bridge.handle_vocoder(decoded).await;
                }
            }
            frame = from_router.recv() => match frame {
                Some(frame) => bridge.handle_router_frame(frame).await,
                None => return Ok(()),
            },
            _ = tick.tick() => bridge.housekeeping().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ambe::fake_server;

    fn station() -> Station {
        toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0").unwrap()
    }

    fn settings(vocoder: SocketAddr) -> Settings {
        Settings { name: "test", ambeserver: vocoder.to_string(), suffix: "INFO".into(), message: "A test".into(), rx_gain_db: 0.0, tx_gain_db: 0.0 }
    }

    async fn start() -> (Shared, mpsc::Sender<Frame>, mpsc::Receiver<Frame>, mpsc::Sender<Audio>, mpsc::Receiver<Audio>) {
        let shared = Shared::default();
        let (to_bridge, from_router) = mpsc::channel(256);
        let (to_router, from_bridge) = mpsc::channel(256);
        let (audio_in, from_audio) = mpsc::channel(256);
        let (to_audio, audio_out) = mpsc::channel(256);
        let port = Port { to_router, from_router };
        tokio::spawn(run(settings(fake_server().await), station(), shared.clone(), port, from_audio, to_audio));
        for _ in 0..200 {
            if state::read(&shared).vocoders.iter().any(|vocoder| vocoder.name == "test" && vocoder.connected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (shared, to_bridge, from_bridge, audio_in, audio_out)
    }

    #[tokio::test]
    async fn audio_is_vocoded_into_a_dstar_transmission() {
        let (_shared, _to_bridge, mut from_bridge, audio_in, _audio_out) = start().await;

        // Speak ten frames, which come out as a transmission
        for _ in 0..10 {
            audio_in.send(Audio::Voice(vec![0; SAMPLES_PER_FRAME])).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        audio_in.send(Audio::End).await.unwrap();
        let mut frames = Vec::new();
        while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(3), from_bridge.recv()).await {
            let finished = frame == Frame::End;
            frames.push(frame);
            if finished {
                break;
            }
        }
        let Some(Frame::Header(header)) = frames.first() else { panic!("no header first: {frames:?}") };
        assert_eq!((header.my_call_text(), header.your_call), ("N0CALL/INFO".to_string(), pad("CQCQCQ")));
        assert_eq!(frames.iter().filter(|frame| matches!(frame, Frame::Voice { .. })).count(), 10);
        assert_eq!(frames.last(), Some(&Frame::End));
    }

    #[tokio::test]
    async fn a_dstar_transmission_comes_out_as_audio() {
        let (_shared, to_bridge, _from_bridge, _audio_in, mut audio_out) = start().await;
        let header = Header { flags: [0; 3], repeater_2: pad("N0CALL G"), repeater_1: pad("N0CALL B"), your_call: pad("CQCQCQ"), my_call: pad("N0TEST"), my_suffix: pad("") };
        to_bridge.send(Frame::Header(header)).await.unwrap();
        to_bridge.send(Frame::Voice { sequence: 0, ambe: [1; 9], slow_data: [0; 3] }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        to_bridge.send(Frame::End).await.unwrap();

        // The voice comes out as audio, and the end of the transmission follows it
        let mut voice = 0;
        loop {
            match tokio::time::timeout(Duration::from_secs(2), audio_out.recv()).await {
                Ok(Some(Audio::Voice(samples))) => {
                    assert_eq!(samples, vec![crate::codec::ambe::FAKE_SPEECH_LEVEL; SAMPLES_PER_FRAME]);
                    voice += 1;
                }
                Ok(Some(Audio::End)) => break,
                other => panic!("the transmission never ended: {other:?}"),
            }
        }
        assert_eq!(voice, 1);
    }
}
