// Measures how loud each transmission is, by decoding its voice on an AMBEserver and reading the level of the audio that comes out

use crate::codec::ambe::{Decoded, Vocoder};
use crate::live::Event;
use crate::state::Shared;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

const RETRY_INTERVAL: Duration = Duration::from_secs(5);

// Frames queue up for the vocoder, and anything past this is dropped so the meter never lags far behind the voice
const MAX_PENDING_FRAMES: usize = 8;

// A level is reported for every few frames, as the loudest of them, which makes for a handful of reports a second
const FRAMES_PER_REPORT: u32 = 5;

// Silence has no level to speak of, so it's given a floor
const FLOOR_DBFS: f32 = -90.0;

pub enum Input {
    // A new transmission, so whatever was being waited on is forgotten
    Start,
    Frame { source: &'static str, callsign: String, ambe: [u8; 9] },
    End,
}

// How far below the loudest possible sample the audio's RMS is
fn level_dbfs(samples: &[i16]) -> f32 {
    let mean_square = samples.iter().map(|sample| (*sample as f64 / 32768.0).powi(2)).sum::<f64>() / samples.len().max(1) as f64;
    (10.0 * mean_square.max(1e-9).log10() as f32).max(FLOOR_DBFS)
}

// Frames go to the vocoder in order and come back in order, so who each one is from is remembered until it does
#[derive(Default)]
struct Meter {
    waiting: VecDeque<(&'static str, String)>,
    loudest: Option<(&'static str, String, f32)>,
    frames: u32,
}

impl Meter {
    fn report(&mut self, shared: &Shared) {
        if let Some((source, callsign, dbfs)) = self.loudest.take() {
            shared.emit(Event::Level { source, callsign, dbfs });
        }
        self.frames = 0;
    }

    fn heard(&mut self, samples: &[i16], shared: &Shared) {
        let Some((source, callsign)) = self.waiting.pop_front() else { return };
        let dbfs = level_dbfs(samples);
        if self.loudest.as_ref().is_none_or(|(_, _, loudest)| dbfs > *loudest) {
            self.loudest = Some((source, callsign, dbfs));
        }
        self.frames += 1;
        if self.frames >= FRAMES_PER_REPORT {
            self.report(shared);
        }
    }

    fn forget(&mut self, shared: &Shared) {
        self.report(shared);
        self.waiting.clear();
    }
}

async fn session(vocoder: &mut Vocoder, shared: &Shared, inputs: &mut mpsc::Receiver<Input>) -> anyhow::Result<()> {
    let mut meter = Meter::default();
    let mut housekeeping = interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            input = inputs.recv() => match input {
                Some(Input::Start | Input::End) => meter.forget(shared),
                Some(Input::Frame { source, callsign, ambe }) => {
                    if meter.waiting.len() < MAX_PENDING_FRAMES {
                        vocoder.decode(&ambe).await?;
                        meter.waiting.push_back((source, callsign));
                    }
                }
                None => return Ok(()),
            },
            decoded = vocoder.receive() => {
                for item in decoded? {
                    if let Decoded::Speech(samples) = item {
                        meter.heard(&samples, shared);
                    }
                }
            }
            _ = housekeeping.tick() => vocoder.keep_alive().await?,
        }
    }
}

async fn run(ambeserver: String, shared: Shared, mut inputs: mpsc::Receiver<Input>) {
    loop {
        let connected = async {
            let address = lookup_host(ambeserver.as_str()).await?.next().ok_or_else(|| anyhow::anyhow!("couldn't resolve the AMBEserver address"))?;
            Vocoder::connect(address).await
        };
        match connected.await {
            Ok(mut vocoder) => match session(&mut vocoder, &shared, &mut inputs).await {
                Ok(()) => return,
                Err(error) => log::warn!("Level meter lost its vocoder: {error:#}"),
            },
            Err(error) => log::warn!("Level meter can't reach the vocoder at {ambeserver}: {error:#}"),
        }

        // Nothing is measured while the vocoder is away, and what's said meanwhile isn't kept
        let retry = Instant::now() + RETRY_INTERVAL;
        while Instant::now() < retry {
            tokio::select! {
                input = inputs.recv() => if input.is_none() { return },
                _ = tokio::time::sleep_until(retry) => {}
            }
        }
    }
}

// Start measuring, and return where the voice to measure goes
pub fn start(ambeserver: String, shared: Shared) -> mpsc::Sender<Input> {
    let (inputs, from_router) = mpsc::channel(64);
    tokio::spawn(run(ambeserver, shared, from_router));
    inputs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ambe::{fake_server, FAKE_SPEECH_LEVEL};

    #[test]
    fn levels_are_decibels_below_full_scale() {
        assert!((level_dbfs(&[16384; 160]) + 6.02).abs() < 0.01);
        assert!(level_dbfs(&[32767; 160]).abs() < 0.01);
        assert_eq!(level_dbfs(&[0; 160]), FLOOR_DBFS);
    }

    #[tokio::test]
    async fn voice_is_reported_with_who_it_came_from_and_how_loud_it_was() {
        let shared = Shared::default();
        let mut events = shared.subscribe_events();
        let inputs = start(fake_server().await.to_string(), shared);
        inputs.send(Input::Start).await.unwrap();
        for _ in 0..12 {
            inputs.send(Input::Frame { source: "rf", callsign: "N0TEST".into(), ambe: [1; 9] }).await.unwrap();
        }
        let Ok(Ok(Event::Level { source, callsign, dbfs })) = tokio::time::timeout(Duration::from_secs(5), events.recv()).await else { panic!("no level was reported") };
        assert_eq!((source, callsign.as_str()), ("rf", "N0TEST"));
        assert!((dbfs - 20.0 * (FAKE_SPEECH_LEVEL as f32 / 32768.0).log10()).abs() < 0.1, "{dbfs} dBFS");
    }
}
