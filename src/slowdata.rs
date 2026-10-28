// D-STAR slow data: three bytes riding along with each voice frame, which pairs up into blocks of a type and five bytes of payload

use crate::dprs::Sentences;

const SYNC_SLOW_DATA: [u8; 3] = [0x55, 0x2D, 0x16];
const SLOW_DATA_SCRAMBLER: [u8; 3] = [0x70, 0x4F, 0x93];
const BLOCK_TYPE_MASK: u8 = 0xF0;
const BLOCK_TYPE_GPS: u8 = 0x30;
const BLOCK_TYPE_TEXT: u8 = 0x40;
const BLOCK_TYPE_HEADER: u8 = 0x50;
const BLOCK_TYPE_SQUELCH: u8 = 0xC0;
const BLOCK_TYPE_FAST_DATA: u8 = 0x80;
const TEXT_LENGTH: usize = 20;
const TEXT_BLOCK_LENGTH: usize = 5;

#[derive(Debug, PartialEq)]
pub enum Decoded {
    // The text message so far, which fills in five characters at a time
    Text(String),

    // A complete TNC-2 or NMEA line from the GPS data
    Gps(String),

    // Anything else a radio might send, as a name for it and its bytes
    Other { kind: &'static str, bytes: Vec<u8> },
}

pub struct Decoder {
    first_half: Option<[u8; 3]>,
    text: [u8; TEXT_LENGTH],
    last_text: String,
    sentences: Sentences,
}

impl Default for Decoder {
    fn default() -> Self {
        Decoder { first_half: None, text: [b' '; TEXT_LENGTH], last_text: String::new(), sentences: Sentences::default() }
    }
}

impl Decoder {
    // Each transmission starts afresh
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    // Feed one voice frame's slow data, returning whatever it completes
    pub fn push(&mut self, slow_data: [u8; 3]) -> Option<Decoded> {
        if slow_data == SYNC_SLOW_DATA {
            self.first_half = None;
            return None;
        }
        let descrambled: [u8; 3] = std::array::from_fn(|index| slow_data[index] ^ SLOW_DATA_SCRAMBLER[index]);
        let Some(first_half) = self.first_half.take() else {
            self.first_half = Some(descrambled);
            return None;
        };
        let block = [&first_half[..], &descrambled[..]].concat();
        match block[0] & BLOCK_TYPE_MASK {
            BLOCK_TYPE_GPS => self.sentences.push(&block[1..]).map(Decoded::Gps),
            BLOCK_TYPE_TEXT => self.push_text(&block),
            BLOCK_TYPE_SQUELCH => None,
            BLOCK_TYPE_HEADER => Some(Decoded::Other { kind: "header", bytes: block[1..].to_vec() }),
            BLOCK_TYPE_FAST_DATA => Some(Decoded::Other { kind: "fast data", bytes: block[1..].to_vec() }),
            _ if block.iter().all(|byte| *byte == 0x66) => None,
            _ => Some(Decoded::Other { kind: "unknown", bytes: block }),
        }
    }

    fn push_text(&mut self, block: &[u8]) -> Option<Decoded> {
        let start = (block[0] & 0x0F) as usize * TEXT_BLOCK_LENGTH;
        self.text.get_mut(start..start + TEXT_BLOCK_LENGTH)?.copy_from_slice(&block[1..]);
        let text: String = self.text.iter().map(|byte| if (0x20..0x7F).contains(byte) { *byte as char } else { ' ' }).collect::<String>().trim_end().to_string();
        if text.is_empty() || text == self.last_text {
            return None;
        }
        self.last_text = text.clone();
        Some(Decoded::Text(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dstar::build_slow_data_blocks;

    #[test]
    fn a_text_message_fills_in_as_its_blocks_arrive() {
        let mut decoder = Decoder::default();
        let heard: Vec<Decoded> = build_slow_data_blocks("Hello from the test").into_iter().filter_map(|frame| decoder.push(frame)).collect();
        assert_eq!(heard.last(), Some(&Decoded::Text("Hello from the test".into())));
        assert!(heard.len() > 1, "the message should build up: {heard:?}");

        // The same message sent again isn't news
        assert!(build_slow_data_blocks("Hello from the test").into_iter().filter_map(|frame| decoder.push(frame)).next().is_none());
    }

    #[test]
    fn idle_slow_data_says_nothing() {
        let mut decoder = Decoder::default();
        assert!(std::iter::repeat([0x16, 0x29, 0xF5]).take(40).filter_map(|frame| decoder.push(frame)).next().is_none());
    }
}
