// D-PRS position reports, assembled from the GPS blocks of slow data and gated to APRS-IS

use crate::dstar::crc_ccitt;

const SENTENCE_START: &[u8] = b"$$CRC";
const SENTENCE_END: u8 = b'\r';
const NMEA_END: u8 = b'\n';
const MAX_BUFFERED_BYTES: usize = 512;
const NMEA_SYMBOL_TABLE: char = '\\';
const NMEA_SYMBOL: char = 'K';

// Path elements that mark a packet as already from the internet, or as not to be put on it
const UNGATEABLE_HOPS: [&str; 4] = ["TCPIP", "TCPXX", "NOGATE", "RFONLY"];

// GPS blocks carry a sentence five bytes at a time
#[derive(Default)]
pub struct Sentences {
    buffer: Vec<u8>,
    heard_gga: bool,
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

    // A "$$CRC" sentence runs to a carriage return, and an NMEA one, from a radio in GPS mode, to a line feed
    fn take_sentence(&mut self) -> Option<String> {
        loop {
            let start = self.buffer.iter().position(|byte| *byte == b'$')?;
            self.buffer.drain(..start);
            let prefix_length = self.buffer.len().min(SENTENCE_START.len());
            let is_crc_sentence = self.buffer[..prefix_length] == SENTENCE_START[..prefix_length];
            if is_crc_sentence && prefix_length < SENTENCE_START.len() {
                return None;
            }
            if !is_crc_sentence && self.buffer.get(1) == Some(&b'$') {
                self.buffer.remove(0);
                continue;
            }
            let terminator = if is_crc_sentence { SENTENCE_END } else { NMEA_END };
            let end = self.buffer.iter().position(|byte| *byte == terminator);
            let next_start = self.buffer.iter().skip(1).position(|byte| *byte == b'$').map(|position| position + 1);
            if let (false, Some(next_start)) = (is_crc_sentence, next_start)
                && end.is_none_or(|end| next_start < end)
            {
                self.buffer.drain(..next_start);
                continue;
            }
            let sentence: Vec<u8> = self.buffer.drain(..=end?).map(|byte| byte & 0x7F).collect();
            let line = if is_crc_sentence { parse_sentence(&sentence) } else { parse_nmea(&sentence).and_then(|line| self.wanted_nmea(line)) };
            if line.is_some() {
                return line;
            }
        }
    }

    // GGA and RMC both say where the radio is, so RMC is only passed on when a transmission has no GGA
    fn wanted_nmea(&mut self, line: String) -> Option<String> {
        match line.get(3..6)? {
            "GGA" => self.heard_gga = true,
            "RMC" if !self.heard_gga => {}
            _ => return None,
        }
        Some(line)
    }
}

// Check an NMEA sentence's XOR checksum, and return the sentence without it
fn parse_nmea(sentence: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(sentence).ok()?.trim_end();
    let (body, checksum) = text.strip_prefix('$')?.rsplit_once('*')?;
    let expected = u8::from_str_radix(checksum, 16).ok()?;
    if body.bytes().fold(0, |sum, byte| sum ^ byte) != expected {
        log::debug!("NMEA sentence failed its checksum: {text}");
        return None;
    }
    Some(format!("${body}"))
}

// The line to gate for a report heard from a station: a TNC-2 line as it is, or an NMEA position made into one from the station's callsign
pub fn aprs_line(callsign: &str, line: &str) -> Option<String> {
    if !line.starts_with('$') {
        return Some(line.to_string());
    }
    let fix = nmea_fix(line)?;
    let source = callsign.split(['/', ' ']).next().filter(|source| !source.is_empty())?;
    Some(format!("{source}>APDPRS,DSTAR*:!{}{NMEA_SYMBOL_TABLE}{}{NMEA_SYMBOL}", crate::aprs::coordinate(fix.latitude, 'N', 'S', 2), crate::aprs::coordinate(fix.longitude, 'E', 'W', 3)))
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
    if hops[2..].iter().any(|hop| hop.starts_with('q') || UNGATEABLE_HOPS.contains(&hop.trim_end_matches('*').to_ascii_uppercase().as_str())) {
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
        gps_frames(&format!("$$CRC{crc:04X},{payload}"))
    }

    // Any text as GPS blocks, with the last one padded and its length saying so
    pub fn gps_frames(text: &str) -> Vec<[u8; 3]> {
        let mut frames = vec![SYNC_SLOW_DATA];
        for chunk in text.as_bytes().chunks(5) {
            let mut padded = [b'f'; 5];
            padded[..chunk.len()].copy_from_slice(chunk);
            let block = [&[BLOCK_TYPE_GPS | chunk.len() as u8][..], &padded].concat();
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

    use super::testing::{gps_frames, slow_data_frames};
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

    const GGA: &str = "$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47";
    const RMC: &str = "$GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A";

    fn collect_all(frames: Vec<[u8; 3]>) -> Vec<String> {
        let mut decoder = Decoder::default();
        frames.into_iter().filter_map(|frame| match decoder.push(frame) {
            Some(Decoded::Gps(line)) => Some(line),
            _ => None,
        }).collect()
    }

    #[test]
    fn nmea_positions_are_collected_preferring_gga() {
        let both = collect_all(gps_frames(&format!("{GGA}\r\nVA3UJF  ,BEACON*00\r\n{RMC}\r\n")));
        assert_eq!(both, [GGA.split_once('*').unwrap().0]);
        let rmc_only = collect_all(gps_frames(&format!("{RMC}\r\n")));
        assert_eq!(rmc_only, [RMC.split_once('*').unwrap().0]);
        assert!(collect_all(gps_frames(&format!("{}\r\n", GGA.replace("*47", "*48")))).is_empty());
    }

    #[test]
    fn short_gps_blocks_carry_only_the_bytes_they_say() {
        let mut frames = gps_frames("$GPG");
        frames.extend(gps_frames(&format!("{}\r\n", &GGA[4..])).into_iter().skip(1));
        assert_eq!(collect_all(frames).len(), 1);
    }

    #[test]
    fn nmea_positions_are_gated_under_the_callsign_that_sent_them() {
        let line = collect_all(gps_frames(&format!("{GGA}\r\n"))).remove(0);
        assert_eq!(aprs_line("VA3UJF/ID51", &line).as_deref(), Some("VA3UJF>APDPRS,DSTAR*:!4807.04N\\01131.00EK"));
        assert_eq!(aprs_line("VA3UJF", "VA3UJF>API51:!4500.00N/09000.00W>").as_deref(), Some("VA3UJF>API51:!4500.00N/09000.00W>"));
    }

    #[test]
    fn gating_refuses_internet_and_rf_only_paths() {
        assert_eq!(gate("N0CALL>APRS,TCPIP*:!4500.00N/09000.00W>", "N0CALL-10"), None);
        assert_eq!(gate("N0CALL>APRS,NOGATE:!4500.00N/09000.00W>", "N0CALL-10"), None);
        assert_eq!(gate("N0CALL>APRS,RFONLY,WIDE1-1:!4500.00N/09000.00W>", "N0CALL-10"), None);
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
