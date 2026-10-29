// Voice control: operators manage the node in plain language by calling the station's own callsign over RF. What they say is
// captured off the air, transcribed, and handed to a language model that can link and unlink stations. The reply is spoken back

mod agent;
mod transcribe;

pub use agent::Controls;

use crate::codec::ambe::{Decoded, Vocoder};
use crate::config::VoiceControl;
use crate::dstar::pad;
use crate::speech::{self, spelled, Speaker};
use agent::Agent;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};
use transcribe::Transcriber;

const RETRY_INTERVAL: Duration = Duration::from_secs(5);

// Anything shorter than this is a stray key-up, and anything past the longest is cut off
const MIN_FRAMES: usize = 15;
const MAX_FRAMES: usize = 1500;

// How long to wait for the vocoder to finish what it was sent once the operator lets go
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_WAITING_UTTERANCES: usize = 4;

// What the router hands on from a transmission made to the node
pub enum Input {
    Start { caller: String },
    Frame([u8; 9]),
    End,
}

// Where the router sends transmissions that are for the node itself, which is whoever calls the station's callsign
#[derive(Clone)]
pub struct Tap {
    pub your_call: [u8; 8],
    pub inputs: mpsc::Sender<Input>,
}

struct Utterance {
    caller: String,
    samples: Vec<i16>,
}

// A callsign without the suffix radios add after it, like /ID51
fn base_callsign(callsign: &str) -> &str {
    callsign.split('/').next().unwrap_or(callsign)
}

fn is_operator(operators: &[String], caller: &str) -> bool {
    operators.iter().any(|operator| base_callsign(operator).eq_ignore_ascii_case(base_callsign(caller)))
}

struct Capture {
    caller: String,
    samples: Vec<i16>,
    frames_sent: usize,
    pending: usize,
    ended: Option<Instant>,
}

// Decode one transmission at a time on an AMBEserver, and pass it on once the operator has finished and the vocoder has caught up
async fn session(vocoder: &mut Vocoder, inputs: &mut mpsc::Receiver<Input>, utterances: &mpsc::Sender<Utterance>) -> anyhow::Result<()> {
    let mut capture: Option<Capture> = None;
    let mut housekeeping = interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            input = inputs.recv() => match input {
                Some(Input::Start { caller }) => capture = Some(Capture { caller, samples: Vec::new(), frames_sent: 0, pending: 0, ended: None }),
                Some(Input::Frame(ambe)) => {
                    if let Some(capture) = capture.as_mut().filter(|capture| capture.ended.is_none() && capture.frames_sent < MAX_FRAMES) {
                        vocoder.decode(&ambe).await?;
                        capture.frames_sent += 1;
                        capture.pending += 1;
                    }
                }
                Some(Input::End) => {
                    if let Some(capture) = capture.as_mut() {
                        capture.ended.get_or_insert(Instant::now());
                    }
                }
                None => return Ok(()),
            },
            decoded = vocoder.receive() => {
                for item in decoded? {
                    if let (Decoded::Speech(samples), Some(capture)) = (item, capture.as_mut()) {
                        capture.samples.extend(samples);
                        capture.pending = capture.pending.saturating_sub(1);
                    }
                }
            }
            _ = housekeeping.tick() => vocoder.keep_alive().await?,
        }
        if capture.as_ref().is_some_and(|capture| capture.ended.is_some_and(|ended| capture.pending == 0 || ended.elapsed() > DRAIN_TIMEOUT)) {
            let Some(finished) = capture.take() else { continue };
            if finished.frames_sent >= MIN_FRAMES && utterances.try_send(Utterance { caller: finished.caller, samples: finished.samples }).is_err() {
                log::warn!("Voice control is still busy, dropping a command");
            }
        }
    }
}

// Reconnect to the vocoder whenever it goes away
async fn listen(ambeserver: String, mut inputs: mpsc::Receiver<Input>, utterances: mpsc::Sender<Utterance>) {
    loop {
        let connected = async {
            let address = lookup_host(ambeserver.as_str()).await?.next().ok_or_else(|| anyhow::anyhow!("couldn't resolve the AMBEserver address"))?;
            Vocoder::connect(address).await
        };
        match connected.await {
            Ok(mut vocoder) => match session(&mut vocoder, &mut inputs, &utterances).await {
                Ok(()) => return,
                Err(error) => log::warn!("Voice control lost its vocoder: {error:#}"),
            },
            Err(error) => log::warn!("Voice control can't reach the vocoder at {ambeserver}: {error:#}"),
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

// Carry out each command in turn, and say how it went
async fn work(config: VoiceControl, transcriber: Transcriber, agent: Agent, speaker: Option<Speaker>, mut utterances: mpsc::Receiver<Utterance>) {
    while let Some(utterance) = utterances.recv().await {
        let spoken_caller = spelled(base_callsign(&utterance.caller));
        if !is_operator(&config.operators, &utterance.caller) {
            log::warn!("Voice control: {} isn't an operator, ignoring the command", utterance.caller);
            speech::say(&speaker, format!("{spoken_caller}, you aren't allowed to command this node."));
            continue;
        }
        let transcript = match transcriber.transcribe(utterance.samples).await {
            Ok(transcript) if !transcript.is_empty() => transcript,
            Ok(_) => {
                speech::say(&speaker, format!("{spoken_caller}, I didn't catch that."));
                continue;
            }
            Err(error) => {
                log::warn!("Voice control couldn't transcribe a command: {error:#}");
                continue;
            }
        };
        log::info!("Voice control: {} said {transcript:?}", utterance.caller);
        match agent.respond(&transcript).await {
            Ok(reply) => {
                log::info!("Voice control: replying {reply:?}");
                speech::say(&speaker, reply);
            }
            Err(error) => {
                log::warn!("Voice control couldn't act on {transcript:?}: {error:#}");
                speech::say(&speaker, format!("{spoken_caller}, I couldn't do that right now."));
            }
        }
    }
}

// Start listening, and return the tap for the router to send the node's own transmissions to
pub fn start(config: VoiceControl, ambeserver: String, callsign: &str, controls: Controls, speaker: Speaker) -> Tap {
    let (inputs, from_router) = mpsc::channel(256);
    let (to_worker, utterances) = mpsc::channel(MAX_WAITING_UTTERANCES);
    tokio::spawn(async move {
        let loaded = async { anyhow::Ok((Transcriber::load(&config).await?, Agent::connect(&config, controls).await?)) };
        match loaded.await {
            Ok((transcriber, agent)) => {
                log::info!("Voice control ready for {}", config.operators.join(", "));
                tokio::spawn(work(config, transcriber, agent, Some(speaker), utterances));
                listen(ambeserver, from_router, to_worker).await;
            }
            Err(error) => log::error!("Voice control is off: {error:#}"),
        }
    });
    Tap { your_call: pad(callsign), inputs }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ambe::{fake_server, FAKE_SPEECH_LEVEL};

    #[test]
    fn operators_are_matched_by_callsign_without_any_suffix() {
        let operators = vec!["va3zza".to_string()];
        assert!(is_operator(&operators, "VA3ZZA"));
        assert!(is_operator(&operators, "VA3ZZA/ID51"));
        assert!(!is_operator(&operators, "VA3ZZB"));
        assert!(!is_operator(&[], "VA3ZZA"));
    }

    async fn heard(frames: usize) -> Option<Utterance> {
        let mut vocoder = Vocoder::connect(fake_server().await).await.unwrap();
        let (to_session, mut inputs) = mpsc::channel(256);
        let (to_worker, mut utterances) = mpsc::channel(4);
        tokio::spawn(async move { session(&mut vocoder, &mut inputs, &to_worker).await });
        to_session.send(Input::Start { caller: "N0TEST".into() }).await.unwrap();
        for _ in 0..frames {
            to_session.send(Input::Frame([1; 9])).await.unwrap();
        }
        to_session.send(Input::End).await.unwrap();
        tokio::time::timeout(Duration::from_millis(2500), utterances.recv()).await.ok().flatten()
    }

    #[tokio::test]
    async fn a_transmission_to_the_node_is_decoded_into_one_stretch_of_audio() {
        let utterance = heard(40).await.expect("an utterance");
        assert_eq!(utterance.caller, "N0TEST");
        assert_eq!(utterance.samples.len(), 40 * 160);
        assert!(utterance.samples.iter().all(|sample| *sample == FAKE_SPEECH_LEVEL));
    }

    #[tokio::test]
    async fn a_stray_key_up_is_not_a_command() {
        assert!(heard(3).await.is_none());
    }
}
