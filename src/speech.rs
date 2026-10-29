// Spoken announcements: text is turned into speech by a Piper voice running in this process, as 8 kHz audio for a bridge to transmit

use crate::bridge;
use crate::codec::{Audio, SAMPLES_PER_FRAME};
use crate::config::{Speech, Station};
use crate::router::{Router, Scope};
use crate::state::{self, Shared, Snapshot};
use anyhow::Context;
use sherpa_onnx::{GenerationConfig, OfflineTts, OfflineTtsConfig, OfflineTtsModelConfig, OfflineTtsVitsModelConfig};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout};

// Shown as the announcement's "name" on radios that display it
const ANNOUNCEMENT_SUFFIX: &str = "INFO";

const OUTPUT_RATE: u32 = 8000;
const FRAME_INTERVAL: Duration = Duration::from_millis(20);

// Radios take a moment to open up, so the first words would be lost without some silence ahead of them
const LEAD_IN_SAMPLES: usize = OUTPUT_RATE as usize * 3 / 10;

// An announcement waits for the channel to be quiet, and gives up if it never is
const QUIET_PATIENCE: Duration = Duration::from_secs(15);
const QUIET_SETTLE: Duration = Duration::from_millis(500);

// Sinc kernel half-width, in zero crossings
const RESAMPLE_HALF_WIDTH: f64 = 12.0;

#[derive(Debug, PartialEq)]
pub enum Announcement {
    Text(String),

    // Describe the repeater as it is when the announcement is made
    Status,
}

pub type Speaker = mpsc::Sender<Announcement>;

// Queue words to be spoken, if speech is set up. A backlog means the channel is busy, so the newest words are dropped
pub fn say(speaker: &Option<Speaker>, text: String) {
    if let Some(speaker) = speaker
        && speaker.try_send(Announcement::Text(text)).is_err()
    {
        log::warn!("Too many announcements waiting, dropping one");
    }
}

// Identifiers are read one character at a time, so "DCS405" is said as letters and digits rather than a word
pub fn spelled(identifier: &str) -> String {
    identifier.chars().filter(|character| !character.is_whitespace()).map(String::from).collect::<Vec<_>>().join(" ")
}

// A lone letter is read as a word, so modules are named from the phonetic alphabet
const PHONETIC_ALPHABET: [&str; 26] = ["Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet", "Kilo", "Lima", "Mike", "November", "Oscar", "Papa", "Quebec", "Romeo", "Sierra", "Tango", "Uniform", "Victor", "Whiskey", "X-ray", "Yankee", "Zulu"];

fn module_name(module: &str) -> String {
    match module.chars().next().filter(char::is_ascii_alphabetic) {
        Some(letter) => PHONETIC_ALPHABET[(letter.to_ascii_uppercase() as u8 - b'A') as usize].to_string(),
        None => module.to_string(),
    }
}

// The letter a module was named by, which operators say as a lone letter or from the phonetic alphabet
pub fn module_letter(text: &str) -> Option<char> {
    let text = text.trim();
    if let Some(letter) = text.chars().next().filter(|letter| text.len() == 1 && letter.is_ascii_alphabetic()) {
        return Some(letter.to_ascii_uppercase());
    }
    PHONETIC_ALPHABET.iter().position(|name| name.eq_ignore_ascii_case(text)).map(|index| (b'A' + index as u8) as char)
}

// "DCS405 A" as it's said: the reflector and then its module
pub fn spoken_reflector(target: &str) -> String {
    match target.rsplit_once(' ') {
        Some((name, module)) => format!("{}, module {}", spelled(name), module_name(module)),
        None => spelled(target),
    }
}

pub fn spoken_list(nodes: &[String]) -> String {
    let spoken: Vec<String> = nodes.iter().map(|node| spelled(node)).collect();
    match spoken.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        Some((last, _)) => last.clone(),
        None => String::new(),
    }
}

// What the repeater would say if asked how it's doing
pub fn status_text(station: &Station, snapshot: &Snapshot) -> String {
    let mut sentences = vec![format!("{} Homebrew Repeater.", spelled(&station.callsign))];
    for link in &snapshot.links {
        sentences.push(link.spoken.clone());
        sentences.extend(link.warnings.iter().cloned());
    }
    sentences.join(" ")
}

// Windowed-sinc resampling, low-passed at the lower of the two rates' Nyquist so nothing aliases
pub fn resample(samples: &[i16], from_rate: u32, to_rate: u32) -> Vec<i16> {
    if from_rate == to_rate {
        return samples.to_vec();
    }
    let step = from_rate as f64 / to_rate as f64;
    let cutoff = (to_rate as f64 / from_rate as f64).min(1.0);
    let reach = RESAMPLE_HALF_WIDTH / cutoff;
    (0..(samples.len() as f64 / step) as usize)
        .map(|index| {
            let center = index as f64 * step;
            let (first, last) = ((center - reach).ceil().max(0.0) as usize, ((center + reach).floor() as usize).min(samples.len().saturating_sub(1)));
            let total: f64 = (first..=last)
                .map(|position| {
                    let distance = position as f64 - center;
                    let argument = std::f64::consts::PI * cutoff * distance;
                    let sinc = if argument == 0.0 { 1.0 } else { argument.sin() / argument };
                    let window = 0.5 * (1.0 + (std::f64::consts::PI * distance / reach).cos());
                    samples[position] as f64 * cutoff * sinc * window
                })
                .sum();
            total.round().clamp(i16::MIN as f64, i16::MAX as f64) as i16
        })
        .collect()
}

// What a Piper voice is made of on disk, as sherpa-onnx's voice packages lay it out
struct VoiceFiles {
    model: String,
    tokens: String,
    espeak_data: String,
}

fn find_voice_files(directory: &Path) -> anyhow::Result<VoiceFiles> {
    let model = std::fs::read_dir(directory)
        .with_context(|| format!("can't read the voice directory {}", directory.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "onnx"))
        .with_context(|| format!("no .onnx voice model in {}", directory.display()))?;
    let (tokens, espeak_data) = (directory.join("tokens.txt"), directory.join("espeak-ng-data"));
    anyhow::ensure!(tokens.is_file(), "no tokens.txt in {}", directory.display());
    anyhow::ensure!(espeak_data.is_dir(), "no espeak-ng-data in {}", directory.display());
    Ok(VoiceFiles { model: model.to_string_lossy().into_owned(), tokens: tokens.to_string_lossy().into_owned(), espeak_data: espeak_data.to_string_lossy().into_owned() })
}

pub struct Synthesizer {
    voice: Arc<OfflineTts>,
    speed: f32,
}

impl Synthesizer {
    // Loading a voice is slow and blocking, so it's done off the async threads
    pub async fn load(config: &Speech) -> anyhow::Result<Self> {
        let (directory, threads) = (config.voice_directory.clone(), config.threads);
        let voice = tokio::task::spawn_blocking(move || -> anyhow::Result<OfflineTts> {
            let files = find_voice_files(&directory)?;
            let voice_config = OfflineTtsConfig {
                model: OfflineTtsModelConfig { vits: OfflineTtsVitsModelConfig { model: Some(files.model), tokens: Some(files.tokens), data_dir: Some(files.espeak_data), ..Default::default() }, num_threads: threads, ..Default::default() },
                ..Default::default()
            };
            OfflineTts::create(&voice_config).with_context(|| format!("couldn't load the voice in {}", directory.display()))
        })
        .await??;
        Ok(Synthesizer { voice: Arc::new(voice), speed: config.speed })
    }

    // Speech at 8 kHz, ready for the vocoder. Synthesis is heavy work, so it runs on a blocking thread
    pub async fn synthesize(&self, text: &str) -> anyhow::Result<Vec<i16>> {
        let (voice, speed, text) = (self.voice.clone(), self.speed, text.to_string());
        tokio::task::spawn_blocking(move || {
            let audio = voice.generate_with_config(&text, &GenerationConfig { speed, ..Default::default() }, None::<fn(&[f32], f32) -> bool>).context("the voice couldn't speak that")?;
            let samples: Vec<i16> = audio.samples().iter().map(|sample| (sample * i16::MAX as f32).round().clamp(i16::MIN as f32, i16::MAX as f32) as i16).collect();
            let mut speech = vec![0; LEAD_IN_SAMPLES];
            speech.extend(resample(&samples, audio.sample_rate() as u32, OUTPUT_RATE));
            Ok(speech)
        })
        .await?
    }
}

// 20 ms frames of audio, the last one padded out with silence
fn frames(samples: &[i16]) -> Vec<Vec<i16>> {
    samples.chunks(SAMPLES_PER_FRAME).map(|chunk| chunk.iter().copied().chain(std::iter::repeat(0)).take(SAMPLES_PER_FRAME).collect()).collect()
}

// Wait for whoever is talking to finish, so an announcement never talks over anyone
async fn wait_for_quiet(shared: &Shared) -> bool {
    let mut snapshots = shared.subscribe();
    let settled = async {
        while snapshots.wait_for(state::channel_is_quiet).await.is_ok() {
            sleep(QUIET_SETTLE).await;
            if state::channel_is_quiet(&snapshots.borrow()) {
                return;
            }
        }
    };
    timeout(QUIET_PATIENCE, settled).await.is_ok()
}

// Attach the announcer to the router as a link that's only heard on the local radio, and start it. Returns where to send announcements
pub fn start(config: Speech, ambeserver: String, tx_gain_db: f32, station: Station, shared: Shared, router: &mut Router) -> Speaker {
    let (speaker, requests) = mpsc::channel(8);
    let (to_announcer, from_audio) = mpsc::channel(256);
    let (to_audio, _unused) = mpsc::channel(1);
    let settings = bridge::Settings { name: "announcements", ambeserver, suffix: ANNOUNCEMENT_SUFFIX.to_string(), message: "Repeater info".to_string(), rx_gain_db: 0.0, tx_gain_db };
    let port = router.attach("announcement", Scope::LocalOnly);
    let (bridge_station, bridge_shared) = (station.clone(), shared.clone());
    tokio::spawn(async move {
        if let Err(error) = bridge::run(settings, bridge_station, bridge_shared, port, from_audio, to_audio).await {
            log::error!("Announcement vocoder failed: {error:#}");
        }
    });
    tokio::spawn(run(config, station, shared, requests, to_announcer));
    speaker
}

// Speak each announcement in turn, handing the audio on at the pace it's heard
pub async fn run(config: Speech, station: Station, shared: Shared, mut announcements: mpsc::Receiver<Announcement>, to_announcer: mpsc::Sender<Audio>) {
    let synthesizer = match Synthesizer::load(&config).await {
        Ok(synthesizer) => synthesizer,
        Err(error) => return log::error!("Announcements are off: {error:#}"),
    };
    log::info!("Announcements ready, speaking with the voice in {}", config.voice_directory.display());
    while let Some(announcement) = announcements.recv().await {
        let text = match announcement {
            Announcement::Text(text) => text,
            Announcement::Status => status_text(&station, &state::read(&shared)),
        };
        log::info!("Announcing: {text}");
        let speech = match synthesizer.synthesize(&text).await {
            Ok(speech) => speech,
            Err(error) => {
                log::warn!("Couldn't speak {text:?}: {error:#}");
                continue;
            }
        };
        if !wait_for_quiet(&shared).await {
            log::warn!("The channel stayed busy, so {text:?} wasn't announced");
            continue;
        }
        let mut pace = interval(FRAME_INTERVAL);
        for frame in frames(&speech) {
            pace.tick().await;
            if to_announcer.send(Audio::Voice(frame)).await.is_err() {
                return;
            }
        }
        let _ = to_announcer.send(Audio::End).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::LinkStatus;

    fn station() -> Station {
        toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0").unwrap()
    }

    #[test]
    fn identifiers_are_spelled_out_and_modules_use_the_phonetic_alphabet() {
        assert_eq!(spelled("DCS405"), "D C S 4 0 5");
        assert_eq!(spelled("54207"), "5 4 2 0 7");
        assert_eq!(spoken_reflector("DCS405 A"), "D C S 4 0 5, module Alpha");
        assert_eq!(spoken_reflector("XRF757 C"), "X R F 7 5 7, module Charlie");
        assert_eq!((module_letter("b"), module_letter("Charlie"), module_letter("x-ray"), module_letter("AB"), module_letter("")), (Some('B'), Some('C'), Some('X'), None, None));
    }

    #[test]
    fn status_reads_out_each_link_and_what_is_wrong_with_it() {
        let reflector = LinkStatus { spoken: "Linked to reflector D C S 4 0 5, module Alpha.".into(), ..LinkStatus::new("reflector", "reflector") };
        let allstar = LinkStatus { spoken: "All Star node 1 2 3 4 5 has no links.".into(), warnings: vec!["The node is not registered with All Star Link.".into()], ..LinkStatus::new("allstar", "allstar") };
        let snapshot = Snapshot { links: vec![reflector, allstar], ..Default::default() };
        assert_eq!(status_text(&station(), &snapshot), "N 0 C A L L Homebrew Repeater. Linked to reflector D C S 4 0 5, module Alpha. All Star node 1 2 3 4 5 has no links. The node is not registered with All Star Link.");
    }

    #[test]
    fn resampling_keeps_a_tone_and_its_loudness_and_drops_what_would_alias() {
        let tone = |frequency: f64, rate: u32| -> Vec<i16> { (0..rate as usize).map(|index| ((index as f64 / rate as f64 * frequency * std::f64::consts::TAU).sin() * 10000.0) as i16).collect() };
        let level = |samples: &[i16]| (samples[400..samples.len() - 400].iter().map(|sample| (*sample as f64).powi(2)).sum::<f64>() / (samples.len() - 800) as f64).sqrt();

        // 1 kHz is well inside the band, and 6 kHz is above what 8 kHz audio can hold
        let kept = resample(&tone(1000.0, 22050), 22050, 8000);
        assert!((kept.len() as i64 - 8000).abs() <= 1);
        assert!((level(&kept) - 7071.0).abs() < 300.0, "kept tone came out at {}", level(&kept));
        assert!(level(&resample(&tone(6000.0, 22050), 22050, 8000)) < 300.0);
    }

    #[test]
    fn audio_is_cut_into_whole_frames() {
        let cut = frames(&[1; 400]);
        assert_eq!(cut.len(), 3);
        assert!(cut.iter().all(|frame| frame.len() == SAMPLES_PER_FRAME));
        assert_eq!(&cut[2][80..], &[0; 80]);
    }
}
