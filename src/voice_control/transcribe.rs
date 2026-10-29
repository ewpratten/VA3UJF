// Turns what an operator said into text, with a Moonshine model running in this process

use crate::config::VoiceControl;
use crate::speech::resample;
use anyhow::Context;
use sherpa_onnx::{OfflineMoonshineModelConfig, OfflineModelConfig, OfflineRecognizer, OfflineRecognizerConfig};
use std::sync::Arc;

const MODEL_SAMPLE_RATE: u32 = 16000;
const VOCODER_SAMPLE_RATE: u32 = 8000;

pub struct Transcriber {
    recognizer: Arc<OfflineRecognizer>,
}

impl Transcriber {
    pub async fn load(config: &VoiceControl) -> anyhow::Result<Self> {
        let directory = config.stt_directory.clone();
        let threads = config.threads;
        let recognizer = tokio::task::spawn_blocking(move || {
            let file = |name: &str| -> anyhow::Result<String> {
                let path = directory.join(name);
                anyhow::ensure!(path.is_file(), "no {name} in {}", directory.display());
                Ok(path.to_string_lossy().into_owned())
            };
            let recognizer_config = OfflineRecognizerConfig {
                model_config: OfflineModelConfig {
                    moonshine: OfflineMoonshineModelConfig { encoder: Some(file("encoder_model.ort")?), merged_decoder: Some(file("decoder_model_merged.ort")?), ..Default::default() },
                    tokens: Some(file("tokens.txt")?),
                    num_threads: threads,
                    ..Default::default()
                },
                ..Default::default()
            };
            OfflineRecognizer::create(&recognizer_config).context("sherpa-onnx couldn't load the speech recognition model")
        })
        .await??;
        Ok(Transcriber { recognizer: Arc::new(recognizer) })
    }

    // The words in a stretch of the vocoder's 8 kHz audio
    pub async fn transcribe(&self, samples: Vec<i16>) -> anyhow::Result<String> {
        let recognizer = self.recognizer.clone();
        tokio::task::spawn_blocking(move || {
            let audio: Vec<f32> = resample(&samples, VOCODER_SAMPLE_RATE, MODEL_SAMPLE_RATE).iter().map(|sample| *sample as f32 / 32768.0).collect();
            let stream = recognizer.create_stream();
            stream.accept_waveform(MODEL_SAMPLE_RATE as i32, &audio);
            recognizer.decode(&stream);
            Ok(stream.get_result().map(|result| result.text.trim().to_string()).unwrap_or_default())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The model is too big to fetch on every run, so this only runs where VA3UJF_STT_DIRECTORY points at one with its test_wavs
    #[tokio::test]
    async fn speech_at_the_vocoders_sample_rate_is_transcribed() {
        let Some(directory) = std::env::var_os("VA3UJF_STT_DIRECTORY").map(std::path::PathBuf::from) else { return };
        let config: VoiceControl = toml::from_str(&format!("operators = [\"N0CALL\"]\naccount_id = \"abc\"\ntoken_file = \"/dev/null\"\nstt_directory = {:?}", directory.display().to_string())).unwrap();
        let wav = std::fs::read(directory.join("test_wavs/0.wav")).unwrap();
        let samples: Vec<i16> = wav[44..].chunks_exact(2).map(|pair| i16::from_le_bytes([pair[0], pair[1]])).collect();
        let text = Transcriber::load(&config).await.unwrap().transcribe(resample(&samples, 16000, 8000)).await.unwrap();
        assert!(text.split_whitespace().count() > 3, "heard {text:?}");
    }
}
