// Audio codecs that any link can use to talk to the router, which only carries D-STAR voice

pub mod ambe;
pub mod ulaw;

// One 20 ms frame of 8 kHz audio
pub const SAMPLES_PER_FRAME: usize = 160;

// Audio between a link and the bridge that vocodes it: one frame of samples at a time, then an end marker
pub enum Audio {
    Voice(Vec<i16>),
    End,
}

pub fn decibels_to_gain(decibels: f32) -> f32 {
    10f32.powf(decibels / 20.0)
}

pub fn apply_gain(samples: impl Iterator<Item = i16>, gain: f32) -> Vec<i16> {
    samples.map(|sample| (sample as f32 * gain).clamp(-32768.0, 32767.0) as i16).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unity_gain_leaves_samples_alone() {
        assert_eq!(apply_gain([1, -2, 32767, -32768].into_iter(), decibels_to_gain(0.0)), vec![1, -2, 32767, -32768]);
    }
}
