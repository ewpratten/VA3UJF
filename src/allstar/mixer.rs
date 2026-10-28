// Jitter buffering and mixing of the 20 ms audio frames that links and the hotspot trade

use std::collections::VecDeque;

pub const SAMPLES_PER_FRAME: usize = 160;
const PREFILL_FRAMES: usize = 2;
const MAX_BUFFERED_FRAMES: usize = 10;

// Audio from one source waits here until the mixer wants it, so network jitter doesn't turn into gaps
#[derive(Default)]
pub struct Buffer {
    frames: VecDeque<Vec<i16>>,
    playing: bool,
}

impl Buffer {
    pub fn push(&mut self, frame: Vec<i16>) {
        self.frames.push_back(frame);
        if self.frames.len() > MAX_BUFFERED_FRAMES {
            self.frames.pop_front();
        }
    }

    // Playback starts once a little audio has built up, and carries on through gaps until the source is done and drained
    pub fn next(&mut self, keyed: bool) -> Option<Vec<i16>> {
        if !self.playing && (self.frames.len() >= PREFILL_FRAMES || (!keyed && !self.frames.is_empty())) {
            self.playing = true;
        }
        if !self.playing {
            return None;
        }
        let frame = self.frames.pop_front();
        if self.frames.is_empty() && !keyed {
            self.playing = false;
        }
        frame
    }

    pub fn active(&self, keyed: bool) -> bool {
        keyed || self.playing || !self.frames.is_empty()
    }
}

// Add audio together, clipping rather than wrapping
pub fn mix(frames: &[&Vec<i16>]) -> Vec<i16> {
    (0..SAMPLES_PER_FRAME).map(|index| frames.iter().map(|frame| frame.get(index).copied().unwrap_or(0) as i32).sum::<i32>().clamp(i16::MIN as i32, i16::MAX as i32) as i16).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixing_adds_audio_and_clips_instead_of_wrapping() {
        let (quiet, loud) = (vec![1000i16; SAMPLES_PER_FRAME], vec![2000i16; SAMPLES_PER_FRAME]);
        assert_eq!(mix(&[&quiet, &loud])[0], 3000);
        let (high, low) = (vec![30000i16; SAMPLES_PER_FRAME], vec![-30000i16; SAMPLES_PER_FRAME]);
        assert_eq!(mix(&[&high, &high])[0], i16::MAX);
        assert_eq!(mix(&[&low, &low])[0], i16::MIN);
        assert_eq!(mix(&[])[0], 0);
    }

    #[test]
    fn a_buffer_waits_for_a_little_audio_then_drains_after_its_source_stops() {
        let mut buffer = Buffer::default();
        buffer.push(vec![1; SAMPLES_PER_FRAME]);
        assert!(buffer.next(true).is_none(), "one frame isn't enough to start");
        buffer.push(vec![2; SAMPLES_PER_FRAME]);
        assert_eq!(buffer.next(true).unwrap()[0], 1);

        // A gap while the source is still keyed is silence, and the buffer stays active
        assert_eq!(buffer.next(true).unwrap()[0], 2);
        assert!(buffer.next(true).is_none());
        assert!(buffer.active(true));

        // Once the source stops, what's left plays out and the buffer goes quiet
        buffer.push(vec![3; SAMPLES_PER_FRAME]);
        assert_eq!(buffer.next(false).unwrap()[0], 3);
        assert!(!buffer.active(false));
    }
}
