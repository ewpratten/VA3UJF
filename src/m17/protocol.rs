// Wire formats of the M17 reflector protocol: control packets, stream packets, and the base-40 callsign encoding

use crc::{Algorithm, Crc};
use zerocopy::byteorder::{BigEndian, U16};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

pub const DEFAULT_PORT: u16 = 17000;
const STREAM_PACKET_SIZE: usize = 54;
pub const LAST_FRAME_FLAG: u16 = 0x8000;

const STREAM_MAGIC: [u8; 4] = *b"M17 ";
const ALPHABET: &[u8; 40] = b" ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-/.";
const BROADCAST_ADDRESS: [u8; 6] = [0xFF; 6];

// A stream of Codec 2 3200 voice with no encryption, in the layout that reflectors recognise a stream by
const VOICE_3200_TYPE: u16 = 0x0005;

const CRC_M17: Algorithm<u16> = Algorithm { width: 16, poly: 0x5935, init: 0xFFFF, refin: false, refout: false, xorout: 0x0000, check: 0x772B, residue: 0x0000 };
const CRC: Crc<u16> = Crc::<u16>::new(&CRC_M17);

// What a reflector says outside of a stream
#[derive(Debug, PartialEq)]
pub enum Control {
    Ack,
    Nack,
    Disconnect,
    Ping,
    Pong,
}

// A callsign as the 48 bits it's packed into: the first character is the least significant base-40 digit
pub fn encode_address(callsign: &str) -> [u8; 6] {
    if callsign == "@ALL" {
        return BROADCAST_ADDRESS;
    }
    let mut value: u64 = 0;
    for character in callsign.bytes().take(9).collect::<Vec<u8>>().into_iter().rev() {
        let digit = ALPHABET.iter().position(|candidate| *candidate == character.to_ascii_uppercase()).unwrap_or(0);
        value = value * 40 + digit as u64;
    }
    value.to_be_bytes()[2..].try_into().unwrap()
}

pub fn decode_address(address: [u8; 6]) -> String {
    if address == BROADCAST_ADDRESS {
        return "@ALL".to_string();
    }
    let mut value = u64::from_be_bytes([0, 0, address[0], address[1], address[2], address[3], address[4], address[5]]);
    let mut callsign = String::new();
    while value > 0 {
        callsign.push(ALPHABET[(value % 40) as usize] as char);
        value /= 40;
    }
    callsign.trim_end().to_string()
}

fn addressed(magic: &[u8; 4], callsign: &str) -> Vec<u8> {
    [&magic[..], &encode_address(callsign)].concat()
}

pub fn connect_packet(callsign: &str, module: u8) -> Vec<u8> {
    [addressed(b"CONN", callsign), vec![module]].concat()
}

pub fn disconnect_packet(callsign: &str) -> Vec<u8> {
    addressed(b"DISC", callsign)
}

pub fn pong_packet(callsign: &str) -> Vec<u8> {
    addressed(b"PONG", callsign)
}

pub fn parse_control(packet: &[u8]) -> Option<Control> {
    match (packet.get(..4)?, packet.len()) {
        (b"ACKN", _) => Some(Control::Ack),
        (b"NACK", _) => Some(Control::Nack),
        (b"DISC", _) => Some(Control::Disconnect),
        (b"PING", 10) => Some(Control::Ping),
        (b"PONG", 10) => Some(Control::Pong),
        _ => None,
    }
}

// One 40 ms slice of a voice stream, carrying the whole link setup frame beside its two frames of Codec 2
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct StreamPacket {
    magic: [u8; 4],
    pub stream_id: U16<BigEndian>,
    destination: [u8; 6],
    pub source: [u8; 6],
    frame_type: U16<BigEndian>,
    meta: [u8; 14],
    pub frame_number: U16<BigEndian>,
    pub payload: [u8; 16],
    crc: U16<BigEndian>,
}

impl StreamPacket {
    pub fn voice(stream_id: u16, source: &str, frame_number: u16, payload: [u8; 16]) -> Self {
        let mut packet = Self::new_zeroed();
        packet.magic = STREAM_MAGIC;
        packet.stream_id = stream_id.into();
        packet.destination = BROADCAST_ADDRESS;
        packet.source = encode_address(source);
        packet.frame_type = VOICE_3200_TYPE.into();
        packet.frame_number = frame_number.into();
        packet.payload = payload;
        packet.crc = CRC.checksum(&packet.as_bytes()[..STREAM_PACKET_SIZE - 2]).into();
        packet
    }

    pub fn parse(packet: &[u8]) -> Option<Self> {
        Self::read_from_bytes(packet).ok().filter(|stream| stream.magic == STREAM_MAGIC)
    }

    // Reflectors pass on the type of whoever spoke, so the older layout and the one that replaced it both mean clear 3200 voice
    pub fn is_unencrypted_voice_3200(&self) -> bool {
        let frame_type = self.frame_type.get();
        match frame_type >> 12 {
            0 => frame_type & 1 == 1 && (frame_type >> 1) & 0x3 == 2 && (frame_type >> 3) & 0x3 == 0,
            nibble => nibble == 2 && (frame_type >> 9) & 0x7 == 0,
        }
    }

    pub fn is_last(&self) -> bool {
        self.frame_number.get() & LAST_FRAME_FLAG != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_matches_the_check_values() {
        assert_eq!(CRC.checksum(b""), 0xFFFF);
        assert_eq!(CRC.checksum(b"A"), 0x206E);
        assert_eq!(CRC.checksum(b"123456789"), 0x772B);
    }

    #[test]
    fn callsigns_pack_first_character_lowest() {
        assert_eq!(encode_address("A"), [0, 0, 0, 0, 0, 1]);
        assert_eq!(encode_address(" A"), [0, 0, 0, 0, 0, 40]);
        assert_eq!(encode_address("@ALL"), [0xFF; 6]);
    }

    #[test]
    fn callsigns_round_trip() {
        for callsign in ["VA3UJF", "W1AW", "N0CALL-R", "AB1CD/P", "@ALL"] {
            assert_eq!(decode_address(encode_address(callsign)), callsign);
        }
        assert_eq!(decode_address(encode_address("va3ujf")), "VA3UJF");
    }

    #[test]
    fn control_packets_have_the_sizes_reflectors_expect() {
        assert_eq!(connect_packet("VA3UJF", b'C').len(), 11);
        assert_eq!(&connect_packet("VA3UJF", b'C')[..4], b"CONN");
        assert_eq!(connect_packet("VA3UJF", b'C')[10], b'C');
        assert_eq!(disconnect_packet("VA3UJF").len(), 10);
        assert_eq!(pong_packet("VA3UJF").len(), 10);
    }

    #[test]
    fn control_packets_are_told_apart() {
        assert_eq!(parse_control(b"ACKN"), Some(Control::Ack));
        assert_eq!(parse_control(b"NACK"), Some(Control::Nack));
        assert_eq!(parse_control(b"DISC"), Some(Control::Disconnect));
        assert_eq!(parse_control(&addressed(b"PING", "W1AW")), Some(Control::Ping));
        assert_eq!(parse_control(&addressed(b"PONG", "W1AW")), Some(Control::Pong));
        assert_eq!(parse_control(b"PING"), None);
        assert_eq!(parse_control(b"XX"), None);
    }

    #[test]
    fn stream_packets_lay_out_where_reflectors_look() {
        let packet = StreamPacket::voice(0x1234, "VA3UJF", 0x8003, [7; 16]);
        let bytes = packet.as_bytes();
        assert_eq!(bytes.len(), STREAM_PACKET_SIZE);
        assert_eq!(&bytes[..6], b"M17 \x12\x34");
        assert_eq!(&bytes[6..12], [0xFF; 6]);
        assert_eq!(&bytes[12..18], encode_address("VA3UJF"));
        assert_eq!(&bytes[18..20], [0x00, 0x05]);
        assert_eq!(&bytes[34..36], [0x80, 0x03]);
        assert_eq!(&bytes[36..52], [7; 16]);
        assert_eq!(CRC.checksum(&bytes[..52]), u16::from_be_bytes([bytes[52], bytes[53]]));
        assert!(packet.is_last());
        assert_eq!(StreamPacket::parse(bytes), Some(packet));
    }

    #[test]
    fn both_type_layouts_are_voice() {
        let mut packet = StreamPacket::voice(1, "W1AW", 0, [0; 16]);
        assert!(packet.is_unencrypted_voice_3200());
        packet.frame_type = 0x2000.into();
        assert!(packet.is_unencrypted_voice_3200());
        packet.frame_type = 0x0007.into();
        assert!(!packet.is_unencrypted_voice_3200());
        packet.frame_type = 0x3000.into();
        assert!(!packet.is_unencrypted_voice_3200());
    }

    #[test]
    fn encrypted_voice_is_not_voice_we_can_play() {
        let mut packet = StreamPacket::voice(1, "W1AW", 0, [0; 16]);
        for encrypted in [0x000D, 0x0015, 0x2200, 0x2800] {
            packet.frame_type = encrypted.into();
            assert!(!packet.is_unencrypted_voice_3200(), "{encrypted:#06x}");
        }
        packet.frame_type = 0x2105.into();
        assert!(packet.is_unencrypted_voice_3200());
    }

    #[test]
    fn other_packets_are_not_streams() {
        assert_eq!(StreamPacket::parse(b"PING"), None);
        assert_eq!(StreamPacket::parse(&[0; STREAM_PACKET_SIZE]), None);
    }
}
