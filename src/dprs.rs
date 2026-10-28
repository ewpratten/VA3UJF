// D-PRS position reports, collected from the slow data of RF transmissions and gated to APRS-IS

use crate::dstar::crc_ccitt;

const SYNC_SLOW_DATA: [u8; 3] = [0x55, 0x2D, 0x16];
const SLOW_DATA_SCRAMBLER: [u8; 3] = [0x70, 0x4F, 0x93];
const BLOCK_TYPE_MASK: u8 = 0xF0;
const BLOCK_TYPE_GPS: u8 = 0x30;
const SENTENCE_START: &[u8] = b"$$CRC";
const SENTENCE_END: u8 = b'\r';
const MAX_BUFFERED_BYTES: usize = 512;

// Slow data arrives three bytes per voice frame, and two frames make one block: a type and length byte, then five bytes of payload
#[derive(Default)]
pub struct Collector {
    first_half: Option<[u8; 3]>,
    buffer: Vec<u8>,
}

impl Collector {
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    // Feed one voice frame's slow data, returning the TNC-2 line once a sentence with a valid CRC completes
    pub fn push(&mut self, slow_data: [u8; 3]) -> Option<String> {
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
        if block[0] & BLOCK_TYPE_MASK != BLOCK_TYPE_GPS {
            return None;
        }
        self.buffer.extend(&block[1..]);
        if self.buffer.len() > MAX_BUFFERED_BYTES {
            log::debug!("D-PRS buffer overflowed without a sentence, discarding it");
            self.buffer.clear();
        }
        self.take_sentence()
    }

    fn take_sentence(&mut self) -> Option<String> {
        let start = self.buffer.windows(SENTENCE_START.len()).position(|window| window == SENTENCE_START)?;
        let end = start + self.buffer[start..].iter().position(|byte| *byte == SENTENCE_END)?;
        let sentence: Vec<u8> = self.buffer.drain(..=end).skip(start).map(|byte| byte & 0x7F).collect();
        parse_sentence(&sentence)
    }
}

// Check the "$$CRCxxxx," prefix against the CRC of everything after it, and return the line it carries
fn parse_sentence(sentence: &[u8]) -> Option<String> {
    if sentence.len() < 11 || sentence[9] != b',' {
        return None;
    }
    let expected = u16::from_str_radix(std::str::from_utf8(&sentence[5..9]).ok()?, 16).ok()?;
    let payload = &sentence[10..];
    if crc_ccitt(payload) != expected {
        log::debug!("D-PRS sentence failed its CRC: {}", String::from_utf8_lossy(sentence).trim_end());
        return None;
    }
    Some(String::from_utf8_lossy(payload).trim_end().to_string())
}

// Add this station's IGate to the path, as for a packet heard directly over RF. Reports that already went through an IGate are refused
pub fn gate(line: &str, igate: &str) -> Option<String> {
    let (header, body) = line.split_once(':')?;
    let hops: Vec<&str> = header.split(['>', ',']).collect();
    if hops.len() < 2 || hops.iter().any(|hop| hop.is_empty()) || body.is_empty() {
        return None;
    }
    if hops[2..].iter().any(|hop| hop.starts_with('q')) {
        return None;
    }
    Some(format!("{header},qAR,{igate}:{body}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPORT: &str = "N0CALL>API282,DSTAR*:!4500.00N/09000.00W>Test\r";

    // Lay a sentence out the way a radio sends it: sync, then scrambled GPS blocks of five bytes over two frames each
    fn slow_data_frames(payload: &str) -> Vec<[u8; 3]> {
        let crc = crc_ccitt(payload.as_bytes());
        let mut sentence = format!("$$CRC{crc:04X},{payload}").into_bytes();
        while sentence.len() % 5 != 0 {
            sentence.push(b'f');
        }
        let mut frames = vec![SYNC_SLOW_DATA];
        for chunk in sentence.chunks(5) {
            let block = [&[BLOCK_TYPE_GPS | 5][..], chunk].concat();
            for half in block.chunks(3) {
                frames.push(std::array::from_fn(|index| half[index] ^ SLOW_DATA_SCRAMBLER[index]));
            }
        }
        frames
    }

    fn collect(frames: Vec<[u8; 3]>) -> Option<String> {
        let mut collector = Collector::default();
        frames.into_iter().filter_map(|frame| collector.push(frame)).next()
    }

    #[test]
    fn crc_matches_the_icom_check_value() {
        assert_eq!(crc_ccitt(b"ABCDEF"), 0x6F5E);
    }

    #[test]
    fn collects_a_report_from_slow_data() {
        assert_eq!(collect(slow_data_frames(REPORT)).as_deref(), Some("N0CALL>API282,DSTAR*:!4500.00N/09000.00W>Test"));
    }

    #[test]
    fn rejects_a_report_with_a_bad_crc() {
        let mut frames = slow_data_frames(REPORT);
        frames[4][0] ^= 0x01;
        assert_eq!(collect(frames), None);
    }

    #[test]
    fn gating_appends_the_igate() {
        assert_eq!(gate("N0CALL>API282,DSTAR*:!4500.00N/09000.00W>", "N0CALL-10").as_deref(), Some("N0CALL>API282,DSTAR*,qAR,N0CALL-10:!4500.00N/09000.00W>"));
    }

    #[test]
    fn gating_refuses_reports_that_already_have_a_q_construct() {
        assert_eq!(gate("N0CALL>API282,DSTAR*,qAR,N0CALL:!4500.00N/09000.00W>", "N0CALL-10"), None);
        assert_eq!(gate("no header here", "N0CALL-10"), None);
    }
}
