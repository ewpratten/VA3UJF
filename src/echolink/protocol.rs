// EchoLink's packets: RTCP for connecting and staying connected, and RTP carrying GSM audio

use oxideav_gsm::{DecoderState, EncoderState, UnpackedFrame, FRAME_SAMPLES, GSM_BYTE_FRAME_LEN};

pub const GSM_FRAMES_PER_PACKET: usize = 4;
const RTP_HEADER_LENGTH: usize = 12;
const RTP_VERSION_BYTE: u8 = 0xC0;
const PAYLOAD_TYPE_GSM: u8 = 0x03;
const RTCP_VERSION: u8 = 3;
const RTCP_RECEIVER_REPORT: u8 = 201;
const RTCP_SENDER_REPORT: u8 = 200;
const RTCP_SDES: u8 = 202;
const RTCP_BYE: u8 = 203;
const SDES_END: u8 = 0;
const SDES_CNAME: u8 = 1;
const SDES_NAME: u8 = 2;
const SDES_EMAIL: u8 = 3;
const SDES_PHONE: u8 = 4;
const MAX_CALLSIGN_LENGTH: usize = 32;

#[derive(Debug, PartialEq)]
pub enum Control {
    Hello { callsign: String, name: String },
    Bye,
}

// Every control packet starts with an empty receiver report, which is what EchoLink clients expect
fn with_receiver_report(mut body: Vec<u8>) -> Vec<u8> {
    let mut packet = vec![RTCP_VERSION << 6, RTCP_RECEIVER_REPORT, 0, 1, 0, 0, 0, 0];
    packet.append(&mut body);
    packet
}

fn add_text(packet: &mut Vec<u8>, item: u8, text: &str) {
    let text = &text.as_bytes()[..text.len().min(255)];
    packet.extend([item, text.len() as u8]);
    packet.extend(text);
}

fn pad_and_measure(packet: &mut Vec<u8>) {
    while packet.len() % 4 != 0 {
        packet.push(0);
    }
    let words = (packet.len() / 4 - 1) as u16;
    packet[2..4].copy_from_slice(&words.to_be_bytes());
}

// Says who we are: the callsign, padded so the name starts at a fixed column, and the name
pub fn hello(callsign: &str, name: &str) -> Vec<u8> {
    let mut packet = vec![RTCP_VERSION << 6 | 1, RTCP_SDES, 0, 0, 0, 0, 0, 0];
    add_text(&mut packet, SDES_CNAME, "CALLSIGN");
    add_text(&mut packet, SDES_NAME, &format!("{callsign:<15}{name}"));
    add_text(&mut packet, SDES_EMAIL, "CALLSIGN");
    add_text(&mut packet, SDES_PHONE, "08:30");
    packet.extend([SDES_END, 0]);
    pad_and_measure(&mut packet);
    with_receiver_report(packet)
}

pub fn bye() -> Vec<u8> {
    let mut packet = vec![RTCP_VERSION << 6 | 1, RTCP_BYE, 0, 0, 0, 0, 0, 0];
    add_text_without_item(&mut packet, "jan2002");
    pad_and_measure(&mut packet);
    with_receiver_report(packet)
}

fn add_text_without_item(packet: &mut Vec<u8>, text: &str) {
    packet.push(text.len() as u8);
    packet.extend(text.as_bytes());
}

// Remote callsigns end up in logs and speech, so only what a callsign can contain gets through
fn sanitized_callsign(text: &str) -> String {
    text.chars().filter(|character| character.is_ascii_alphanumeric() || "-/*_".contains(*character)).take(MAX_CALLSIGN_LENGTH).collect()
}

// Walk the subpackets of a compound RTCP packet, which must open with a sender or receiver report
fn subpackets(data: &[u8]) -> Option<Vec<(u8, &[u8])>> {
    let valid_version = |byte: u8| matches!(byte >> 6, RTCP_VERSION | 1);
    if data.len() < 4 || !valid_version(data[0]) || data[0] & 0x20 != 0 || !matches!(data[1], RTCP_SENDER_REPORT | RTCP_RECEIVER_REPORT) {
        return None;
    }
    let mut found = Vec::new();
    let mut rest = data;
    while rest.len() >= 4 && valid_version(rest[0]) {
        let length = (u16::from_be_bytes([rest[2], rest[3]]) as usize + 1) * 4;
        if length > rest.len() {
            break;
        }
        found.push((rest[1], &rest[..length]));
        rest = &rest[length..];
    }
    Some(found)
}

// The text of one item in an SDES subpacket, whose items follow its 8-byte header
fn sdes_item(subpacket: &[u8], wanted: u8) -> Option<String> {
    let mut items = subpacket.get(8..)?;
    while items.len() >= 2 && items[0] != SDES_END {
        let length = items[1] as usize;
        let text = items.get(2..2 + length)?;
        if items[0] == wanted {
            return Some(String::from_utf8_lossy(text).into_owned());
        }
        items = &items[2 + length..];
    }
    None
}

pub fn parse_control(data: &[u8]) -> Option<Control> {
    let subpackets = subpackets(data)?;
    if subpackets.iter().any(|(kind, _)| *kind == RTCP_BYE) {
        return Some(Control::Bye);
    }
    let (_, sdes) = subpackets.iter().find(|(kind, _)| *kind == RTCP_SDES)?;
    let identity = sdes_item(sdes, SDES_NAME)?;
    let (callsign, name) = identity.split_once(char::is_whitespace).unwrap_or((&identity, ""));
    let callsign = sanitized_callsign(callsign);
    (!callsign.is_empty()).then(|| Control::Hello { callsign, name: name.trim().to_string() })
}

// Four GSM frames, 80 ms of speech, in one packet
pub fn voice_packet(sequence: u16, frames: &[[u8; GSM_BYTE_FRAME_LEN]]) -> Vec<u8> {
    let mut packet = vec![RTP_VERSION_BYTE, PAYLOAD_TYPE_GSM];
    packet.extend(sequence.to_be_bytes());
    packet.extend([0; 8]);
    packet.extend(frames.iter().flatten());
    packet
}

// The GSM frames in an audio packet. Anything else on the audio port, like text messages, isn't audio
pub fn parse_voice(data: &[u8]) -> Option<Vec<&[u8]>> {
    if data.len() < RTP_HEADER_LENGTH + GSM_BYTE_FRAME_LEN || data[0] != RTP_VERSION_BYTE || data[1] != PAYLOAD_TYPE_GSM {
        return None;
    }
    Some(data[RTP_HEADER_LENGTH..].chunks_exact(GSM_BYTE_FRAME_LEN).collect())
}

#[derive(Default)]
pub struct Encoder(EncoderState);

impl Encoder {
    pub fn encode(&mut self, samples: &[i16]) -> [u8; GSM_BYTE_FRAME_LEN] {
        let mut frame = [0i16; FRAME_SAMPLES];
        frame.iter_mut().zip(samples).for_each(|(slot, sample)| *slot = *sample);
        self.0.encode_frame(&frame).to_gsm_byte_frame()
    }
}

#[derive(Default)]
pub struct Decoder(DecoderState);

impl Decoder {
    pub fn decode(&mut self, frame: &[u8]) -> Option<Vec<i16>> {
        let frame = UnpackedFrame::from_gsm_byte_frame(frame).ok()?;
        Some(self.0.decode_frame(&frame).to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hello_is_the_receiver_report_and_source_description_clients_expect() {
        let packet = hello("N0CALL-L", "Test");
        assert_eq!(&packet[..8], &[0xC0, 201, 0, 1, 0, 0, 0, 0]);
        assert_eq!(&packet[8..12], &[0xC1, 202, 0, (packet.len() as u8 - 8) / 4 - 1]);
        assert_eq!(packet.len() % 4, 0);
        let name = b"N0CALL-L       Test";
        assert!(packet.windows(name.len()).any(|window| window == name));
    }

    #[test]
    fn a_bye_is_a_receiver_report_and_a_goodbye() {
        let packet = bye();
        assert_eq!(&packet[8..12], &[0xC1, 203, 0, 3]);
        assert_eq!(packet.len(), 24);
        assert_eq!(parse_control(&packet), Some(Control::Bye));
    }

    #[test]
    fn a_hello_is_read_back_into_a_callsign_and_name() {
        assert_eq!(parse_control(&hello("W1AW-R", "Hiram Percy")), Some(Control::Hello { callsign: "W1AW-R".into(), name: "Hiram Percy".into() }));
        assert_eq!(parse_control(&hello("*ECHOTEST*", "")), Some(Control::Hello { callsign: "*ECHOTEST*".into(), name: String::new() }));
    }

    #[test]
    fn callsigns_are_cleaned_of_anything_a_callsign_can_not_hold() {
        assert_eq!(sanitized_callsign("N0CALL$(reboot)-L"), "N0CALLreboot-L");
        assert_eq!(sanitized_callsign(&"A".repeat(100)).len(), MAX_CALLSIGN_LENGTH);
    }

    #[test]
    fn malformed_control_packets_are_ignored() {
        assert_eq!(parse_control(&[]), None);
        assert_eq!(parse_control(&[0xC0, 201, 0, 1]), None);
        assert_eq!(parse_control(&hello("N0CALL", "x")[..14]), None);
        assert_eq!(parse_control(b"oNDATA\rhello"), None);
    }

    #[test]
    fn voice_packets_carry_four_gsm_frames_after_the_rtp_header() {
        let packet = voice_packet(7, &[[1; 33], [2; 33], [3; 33], [4; 33]]);
        assert_eq!(packet.len(), 12 + 4 * 33);
        assert_eq!(&packet[..4], &[0xC0, 3, 0, 7]);
        let frames = parse_voice(&packet).unwrap();
        assert_eq!((frames.len(), frames[2][0]), (4, 3));
        assert!(parse_voice(b"oNDATA\rhello world, this is some text.......").is_none());
    }

    #[test]
    fn speech_survives_a_round_trip_through_gsm() {
        let (mut encoder, mut decoder) = (Encoder::default(), Decoder::default());
        let tone: Vec<i16> = (0..FRAME_SAMPLES * 10).map(|index| ((index as f32 * 0.3).sin() * 8000.0) as i16).collect();
        let heard: Vec<i16> = tone.chunks(FRAME_SAMPLES).flat_map(|chunk| decoder.decode(&encoder.encode(chunk)).unwrap()).collect();
        let level = |samples: &[i16]| (samples.iter().map(|sample| (*sample as f64).powi(2)).sum::<f64>() / samples.len() as f64).sqrt();
        assert!((level(&heard[FRAME_SAMPLES * 4..]) / level(&tone[FRAME_SAMPLES * 4..]) - 1.0).abs() < 0.3, "{} vs {}", level(&heard), level(&tone));
    }
}
