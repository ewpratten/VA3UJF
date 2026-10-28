// Wire formats of the DCS and DExtra reflector protocols

use crate::dstar::{pad, Header, HeaderWithCrc};
use zerocopy::byteorder::{LittleEndian, U16};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

pub const HEADER_SEQUENCE: u8 = 0x80;
pub const LAST_FRAME_FLAG: u8 = 0x40;
pub const END_AMBE: [u8; 9] = [0x55, 0x55, 0x55, 0x55, 0xC8, 0x7A, 0x00, 0x00, 0x00];

const DSVT_MAGIC: [u8; 4] = *b"DSVT";
const DSVT_HEADER_KIND: u8 = 0x10;
const DSVT_VOICE_KIND: u8 = 0x20;
const DCS_MAGIC: [u8; 4] = *b"0001";
const DCS_MARKER: u8 = 0x01;
const DCS_TEXT_LENGTH: u8 = 0x21;
const DCS_CONNECT_HTML: &str = "<table border=\"0\" width=\"95%\"><tr><td width=\"4%\"><img border=\"0\" src=hotspot.jpg></td><td width=\"96%\"><font size=\"2\"><b>HOTSPOT</b> va3ujf</font></td></tr></table>";

// The station callsign (cut to 7 characters), local module, remote module, NUL. Both protocols open a link and close it with this
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct ConnectPacket {
    pub callsign: [u8; 8],
    pub local_module: u8,
    pub remote_module: u8,
    pub terminator: u8,
}

impl ConnectPacket {
    pub fn new(callsign: &str, local_module: u8, remote_module: u8) -> Self {
        let mut callsign = pad::<8>(callsign);
        callsign[7] = b' ';
        ConnectPacket { callsign, local_module, remote_module, terminator: 0 }
    }
}

// DCS names the reflector it's calling, and a link request also carries a page describing the station
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DcsUnlinkPacket {
    pub connect: ConnectPacket,
    pub reflector: [u8; 8],
}

#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DcsLinkPacket {
    pub unlink: DcsUnlinkPacket,
    pub html: [u8; 500],
}

impl DcsLinkPacket {
    pub fn new(unlink: DcsUnlinkPacket) -> Self {
        let mut html = [0; 500];
        html[..DCS_CONNECT_HTML.len()].copy_from_slice(DCS_CONNECT_HTML.as_bytes());
        DcsLinkPacket { unlink, html }
    }
}

// Keepalives: DCS says who it's from and who it's for, DExtra only who it's from
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DextraPoll {
    pub callsign: [u8; 8],
    pub terminator: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DcsPoll {
    pub poll: DextraPoll,
    pub reflector: [u8; 8],
}

// DExtra streams are DSVT packets: this prefix, then a header or one frame of voice
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DsvtPrefix {
    pub magic: [u8; 4],
    pub kind: u8,
    pub reserved_a: [u8; 3],
    pub config: u8,
    pub reserved_b: [u8; 3],
    pub stream_id: U16<LittleEndian>,
    pub sequence: u8,
}

impl DsvtPrefix {
    fn new(kind: u8, stream_id: u16, sequence: u8) -> Self {
        DsvtPrefix { magic: DSVT_MAGIC, kind, reserved_a: [0; 3], config: 0x20, reserved_b: [0; 3], stream_id: stream_id.into(), sequence }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == DSVT_MAGIC
    }
}

#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DsvtHeaderPacket {
    pub prefix: DsvtPrefix,
    pub header: HeaderWithCrc,
}

impl DsvtHeaderPacket {
    pub fn new(stream_id: u16, header: Header) -> Self {
        DsvtHeaderPacket { prefix: DsvtPrefix::new(DSVT_HEADER_KIND, stream_id, HEADER_SEQUENCE), header: header.with_crc() }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DsvtVoicePacket {
    pub prefix: DsvtPrefix,
    pub ambe: [u8; 9],
    pub slow_data: [u8; 3],
}

impl DsvtVoicePacket {
    pub fn new(stream_id: u16, sequence: u8, ambe: [u8; 9], slow_data: [u8; 3]) -> Self {
        DsvtVoicePacket { prefix: DsvtPrefix::new(DSVT_VOICE_KIND, stream_id, sequence), ambe, slow_data }
    }
}

// DCS carries the whole header in every frame of voice
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DcsVoicePacket {
    pub magic: [u8; 4],
    pub header: Header,
    pub stream_id: U16<LittleEndian>,
    pub sequence: u8,
    pub ambe: [u8; 9],
    pub slow_data: [u8; 3],
    pub frame_count: [u8; 3],
    pub marker: u8,
    pub reserved: u8,
    pub text_length: u8,
    pub text: [u8; 36],
}

impl DcsVoicePacket {
    pub fn new(header: Header, stream_id: u16, sequence: u8, ambe: [u8; 9], slow_data: [u8; 3], frame_count: u32) -> Self {
        let mut packet = Self::new_zeroed();
        packet.magic = DCS_MAGIC;
        packet.header = header;
        packet.stream_id = stream_id.into();
        packet.sequence = sequence;
        packet.ambe = ambe;
        packet.slow_data = slow_data;
        packet.frame_count.copy_from_slice(&frame_count.to_le_bytes()[..3]);
        packet.marker = DCS_MARKER;
        packet.text_length = DCS_TEXT_LENGTH;
        packet
    }

    pub fn is_valid(&self) -> bool {
        self.magic == DCS_MAGIC
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packets_have_the_sizes_the_reflectors_expect() {
        assert_eq!(size_of::<ConnectPacket>(), 11);
        assert_eq!(size_of::<DcsUnlinkPacket>(), 19);
        assert_eq!(size_of::<DcsLinkPacket>(), 519);
        assert_eq!(size_of::<DcsPoll>(), 17);
        assert_eq!(size_of::<DsvtHeaderPacket>(), 56);
        assert_eq!(size_of::<DsvtVoicePacket>(), 27);
        assert_eq!(size_of::<DcsVoicePacket>(), 100);
    }

    #[test]
    fn dsvt_voice_lays_out_the_stream_id_and_sequence() {
        let packet = DsvtVoicePacket::new(0x1234, 0x41, [1; 9], [2; 3]);
        let bytes = packet.as_bytes();
        assert_eq!(&bytes[..12], b"DSVT\x20\x00\x00\x00\x20\x00\x00\x00");
        assert_eq!(&bytes[12..15], [0x34, 0x12, 0x41]);
    }

    #[test]
    fn dcs_voice_round_trips() {
        let header = Header { flags: [0; 3], repeater_2: pad("DCS405 A"), repeater_1: pad("VA3UJF B"), your_call: pad("CQCQCQ"), my_call: pad("VA3UJF"), my_suffix: pad("") };
        let packet = DcsVoicePacket::new(header, 7, 3, [9; 9], [8; 3], 0x010203);
        let bytes = packet.as_bytes();
        assert_eq!((bytes[58..61].to_vec(), bytes[61], bytes[63]), (vec![3, 2, 1], 0x01, 0x21));
        assert_eq!(DcsVoicePacket::read_from_bytes(bytes).unwrap(), packet);
    }
}
