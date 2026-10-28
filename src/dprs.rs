// D-PRS position reports, assembled from the GPS blocks of slow data and gated to APRS-IS

use crate::dstar::crc_ccitt;

const SENTENCE_START: &[u8] = b"$$CRC";
const SENTENCE_END: u8 = b'\r';
const MAX_BUFFERED_BYTES: usize = 512;

// GPS blocks carry a sentence five bytes at a time
#[derive(Default)]
pub struct Sentences {
    buffer: Vec<u8>,
}

impl Sentences {
    // Feed one GPS block's payload, returning the TNC-2 line once a sentence with a valid CRC completes
    pub fn push(&mut self, payload: &[u8]) -> Option<String> {
        self.buffer.extend(payload);
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

// A position, as the report gives it
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Fix {
    pub latitude: f64,
    pub longitude: f64,
    pub comment: String,
}

// Degrees and decimal minutes, as APRS and NMEA both write them, with the hemisphere after
fn degrees(text: &str, degree_digits: usize, negative: char) -> Option<f64> {
    let hemisphere = text.chars().last()?;
    let number = text.get(..text.len() - 1)?;
    if !number.is_ascii() || number.len() <= degree_digits || !number[degree_digits..].chars().all(|character| character.is_ascii_digit() || character == '.') {
        return None;
    }
    let value = number[..degree_digits].parse::<f64>().ok()? + number[degree_digits..].parse::<f64>().ok()? / 60.0;
    match hemisphere {
        _ if hemisphere == negative => Some(-value),
        'N' | 'E' => Some(value),
        _ => None,
    }
}

fn base91(text: &[u8]) -> Option<f64> {
    text.iter().try_fold(0.0, |total, byte| (33..=124).contains(byte).then(|| total * 91.0 + (byte - 33) as f64))
}

// An NMEA sentence: GGA or RMC, either of which says where the radio is
fn nmea_fix(sentence: &str) -> Option<Fix> {
    let fields: Vec<&str> = sentence.split('*').next()?.split(',').collect();
    let (latitude, north, longitude, east) = match (fields.first()?.get(3..)?, fields.get(6), fields.get(2)) {
        ("GGA", Some(quality), _) if *quality != "0" && !quality.is_empty() => (fields.get(2)?, fields.get(3)?, fields.get(4)?, fields.get(5)?),
        ("RMC", _, Some(&"A")) => (fields.get(3)?, fields.get(4)?, fields.get(5)?, fields.get(6)?),
        _ => return None,
    };
    Some(Fix { latitude: degrees(&format!("{latitude}{north}"), 2, 'S')?, longitude: degrees(&format!("{longitude}{east}"), 3, 'W')?, comment: String::new() })
}

// The position in a position report: a TNC-2 line carrying an uncompressed or compressed APRS position, or a bare NMEA sentence
pub fn parse_fix(line: &str) -> Option<Fix> {
    if line.starts_with('$') {
        return nmea_fix(line);
    }
    let body = line.split_once(':')?.1;
    let report = match body.chars().next()? {
        '!' | '=' => body.get(1..)?,
        '/' | '@' => body.get(8..)?,
        _ => return None,
    };
    let bytes = report.as_bytes();
    if bytes.first()?.is_ascii_digit() {
        let (latitude, longitude) = (report.get(..8)?, report.get(9..18)?);
        return Some(Fix { latitude: degrees(latitude, 2, 'S')?, longitude: degrees(longitude, 3, 'W')?, comment: report.get(19..).unwrap_or_default().trim().to_string() });
    }
    let latitude = 90.0 - base91(bytes.get(1..5)?)? / 380926.0;
    let longitude = -180.0 + base91(bytes.get(5..9)?)? / 190463.0;
    Some(Fix { latitude, longitude, comment: report.get(13..).unwrap_or_default().trim().to_string() })
}

#[cfg(test)]
pub mod testing {
    use super::*;

    const SYNC_SLOW_DATA: [u8; 3] = [0x55, 0x2D, 0x16];
    const SLOW_DATA_SCRAMBLER: [u8; 3] = [0x70, 0x4F, 0x93];
    const BLOCK_TYPE_GPS: u8 = 0x30;

    // Lay a sentence out the way a radio sends it: sync, then scrambled GPS blocks of five bytes over two frames each
    pub fn slow_data_frames(payload: &str) -> Vec<[u8; 3]> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::testing::slow_data_frames;
    use crate::slowdata::{Decoded, Decoder};

    const REPORT: &str = "N0CALL>API282,DSTAR*:!4500.00N/09000.00W>Test\r";
    fn collect(frames: Vec<[u8; 3]>) -> Option<String> {
        let mut decoder = Decoder::default();
        frames.into_iter().find_map(|frame| match decoder.push(frame) {
            Some(Decoded::Gps(line)) => Some(line),
            _ => None,
        })
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

    #[test]
    fn uncompressed_positions_are_read_in_every_hemisphere() {
        let fix = parse_fix("N0CALL>API282,DSTAR*:!4530.00N/07545.60W>Test").unwrap();
        assert_eq!((fix.latitude, fix.comment.as_str()), (45.5, "Test"));
        assert!((fix.longitude + 75.76).abs() < 1e-9);
        let fix = parse_fix("N0CALL>API282:@092345z3345.00S/15112.00E>").unwrap();
        assert_eq!((fix.latitude, fix.longitude), (-33.75, 151.2));
    }

    #[test]
    fn compressed_positions_are_read() {
        // The APRS spec's example, 49.5N 72.75W
        let fix = parse_fix("N0CALL>API282:!/5L!!<*e7>7P[").unwrap();
        assert!((fix.latitude - 49.5).abs() < 0.001 && (fix.longitude + 72.75).abs() < 0.001, "{fix:?}");
    }

    #[test]
    fn nmea_sentences_give_a_position_only_with_a_fix() {
        let fix = parse_fix("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47").unwrap();
        assert!((fix.latitude - 48.1173).abs() < 1e-4 && (fix.longitude - 11.516667).abs() < 1e-4);
        assert!(parse_fix("$GPGGA,123519,4807.038,N,01131.000,E,0,00,,,M,,M,,*47").is_none());
        assert!(parse_fix("$GPRMC,123519,V,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A").is_none());
        assert!(parse_fix("$GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A").is_some());
    }

    #[test]
    fn reports_without_a_position_give_none() {
        assert!(parse_fix("N0CALL>API282:>just a status").is_none());
        assert!(parse_fix("N0CALL>API282:!4500.00X/09000.00W>").is_none());
        assert!(parse_fix("garbage").is_none());
    }
}
