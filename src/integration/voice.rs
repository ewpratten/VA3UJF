// Speaks with the real Piper voice, from text through to the paced frames the bridge transmits

use crate::codec::Audio;
use crate::config::{Speech, Station};
use crate::speech::{self, Announcement, Synthesizer};
use crate::state::Shared;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;

// The voice the hotspot ships with, which the Dockerfile fetches the same way
const VOICE_URL: &str = "https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/vits-piper-en_US-lessac-low.tar.bz2";
const VOICE_SHA256: &str = "8fb427b8637334072ee5723d72fa418c45bfdd4b7deebeacdf2938662618c1cb";
const VOICE_DIRECTORY_VARIABLE: &str = "VA3UJF_VOICE_DIRECTORY";

fn run(program: &str, arguments: &[&str]) -> anyhow::Result<String> {
    let output = Command::new(program).args(arguments).output()?;
    anyhow::ensure!(output.status.success(), "{program} failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

// Download the voice once, check it against its checksum, and keep it for later runs
fn download_voice() -> anyhow::Result<PathBuf> {
    static DOWNLOADING: Mutex<()> = Mutex::new(());
    let _downloading = DOWNLOADING.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let cache = std::env::temp_dir().join(format!("va3ujf-test-voice-{}", &VOICE_SHA256[..12]));
    let voice = cache.join("vits-piper-en_US-lessac-low");
    if voice.join("tokens.txt").is_file() {
        return Ok(voice);
    }
    std::fs::create_dir_all(&cache)?;
    let archive = cache.join("voice.tar.bz2");
    run("curl", &["--fail", "--silent", "--show-error", "--location", "--output", archive.to_str().unwrap(), VOICE_URL])?;
    let checksum = run("openssl", &["dgst", "-sha256", "-r", archive.to_str().unwrap()])?;
    anyhow::ensure!(checksum.starts_with(VOICE_SHA256), "the voice download doesn't match its checksum: {checksum}");
    run("tar", &["-xjf", archive.to_str().unwrap(), "-C", cache.to_str().unwrap()])?;
    Ok(voice)
}

// A voice to speak with, or none if it can't be had, which skips the test the way a missing Docker daemon does
fn voice_directory() -> Option<PathBuf> {
    if std::env::var_os("VA3UJF_SKIP_CONTAINERS").is_some() {
        return None;
    }
    if let Some(directory) = std::env::var_os(VOICE_DIRECTORY_VARIABLE) {
        return Some(directory.into());
    }
    match download_voice() {
        Ok(directory) => Some(directory),
        Err(error) => {
            assert!(std::env::var_os("VA3UJF_REQUIRE_CONTAINERS").is_none(), "couldn't get the voice: {error:#}");
            eprintln!("skipping: couldn't get the voice: {error:#}");
            None
        }
    }
}

fn config(directory: PathBuf) -> Speech {
    toml::from_str(&format!("voice_directory = {:?}", directory.display().to_string())).unwrap()
}

fn level(samples: &[i16]) -> f64 {
    (samples.iter().map(|sample| (*sample as f64).powi(2)).sum::<f64>() / samples.len().max(1) as f64).sqrt()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_voice_speaks_an_announcement_as_audible_8khz_speech() {
    let Some(directory) = voice_directory() else { return };
    let samples = Synthesizer::load(&config(directory)).await.unwrap().synthesize("Node 5 4 3 2 1 connected.").await.unwrap();

    // A short sentence is a second or two of speech at 8 kHz, after the silence that lets a radio open up
    let seconds = samples.len() as f64 / 8000.0;
    assert!((1.0..8.0).contains(&seconds), "{seconds} seconds of speech");
    assert_eq!(level(&samples[..2000]), 0.0, "the lead-in isn't silent");
    assert!(level(&samples[2400..]) > 300.0, "the speech is nearly silent: {}", level(&samples[2400..]));
    assert!(samples.iter().any(|sample| sample.abs() > 3000), "the speech never gets loud");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_announcer_paces_spoken_text_and_a_status_to_the_bridge() {
    let Some(directory) = voice_directory() else { return };
    let station: Station = toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0").unwrap();
    let (requests, announcements) = mpsc::channel(4);
    let (to_bridge, mut from_announcer) = mpsc::channel(4096);
    tokio::spawn(speech::run(config(directory), station, Shared::default(), announcements, to_bridge));

    // Both kinds of announcement come out as whole frames at the pace they're heard, ending with an end marker
    for announcement in [Announcement::Text("Linked to reflector D C S 4 0 5, module Alpha.".into()), Announcement::Status] {
        requests.send(announcement).await.unwrap();
        let started = tokio::time::Instant::now();
        let mut frames = 0;
        loop {
            match tokio::time::timeout(Duration::from_secs(60), from_announcer.recv()).await.expect("audio from the announcer") {
                Some(Audio::Voice(frame)) => {
                    assert_eq!(frame.len(), 160);
                    frames += 1;
                }
                Some(Audio::End) => break,
                None => panic!("the announcer stopped"),
            }
        }
        assert!(frames > 50, "only {frames} frames of speech");

        // Real-time pacing, so the vocoder and radio aren't flooded
        assert!(started.elapsed() > Duration::from_millis(frames as u64 * 20 - 500), "{frames} frames came out in {:?}", started.elapsed());
    }
}
