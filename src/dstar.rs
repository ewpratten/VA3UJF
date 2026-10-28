// D-STAR header and voice frame types, shared by the modem driver and the reflector links

use crc::{Crc, CRC_16_IBM_SDLC};
use zerocopy::byteorder::{LittleEndian, U16};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

const CRC_X25: Crc<u16> = Crc::<u16>::new(&CRC_16_IBM_SDLC);
pub const END_SLOW_DATA: [u8; 3] = [0x55, 0x55, 0x55];
pub const FRAMES_PER_SUPERFRAME: u8 = 21;

// The 39 bytes the CRC covers, which is also the layout DCS and DExtra carry
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct Header {
    pub flags: [u8; 3],
    pub repeater_2: [u8; 8],
    pub repeater_1: [u8; 8],
    pub your_call: [u8; 8],
    pub my_call: [u8; 8],
    pub my_suffix: [u8; 4],
}

// A header followed by its CRC, as the modem and DExtra carry it
#[derive(Clone, Copy, Debug, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct HeaderWithCrc {
    pub header: Header,
    pub crc: U16<LittleEndian>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Header(Header),
    Voice { sequence: u8, ambe: [u8; 9], slow_data: [u8; 3] },
    End,
}

// Space-pad a callsign out to a fixed field width
pub fn pad<const N: usize>(text: &str) -> [u8; N] {
    let mut field = [b' '; N];
    for (slot, byte) in field.iter_mut().zip(text.bytes()) {
        *slot = byte.to_ascii_uppercase();
    }
    field
}

pub fn field_text(field: &[u8]) -> String {
    String::from_utf8_lossy(field).trim_end().to_string()
}

pub fn crc_ccitt(data: &[u8]) -> u16 {
    CRC_X25.checksum(data)
}

impl Header {
    pub fn with_crc(self) -> HeaderWithCrc {
        HeaderWithCrc { header: self, crc: crc_ccitt(self.as_bytes()).into() }
    }

    pub fn my_call_text(&self) -> String {
        let suffix = field_text(&self.my_suffix);
        let call = field_text(&self.my_call);
        if suffix.is_empty() { call } else { format!("{call}/{suffix}") }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_header() -> Header {
        Header { flags: [0; 3], repeater_2: pad("DCS405 A"), repeater_1: pad("VA3UJF B"), your_call: pad("CQCQCQ"), my_call: pad("VA3UJF"), my_suffix: pad("TEST") }
    }

    #[test]
    fn header_round_trips_through_its_wire_form() {
        let header = test_header();
        let framed = header.with_crc();
        assert_eq!(framed.as_bytes().len(), 41);
        assert_eq!(HeaderWithCrc::read_from_prefix(framed.as_bytes()).unwrap().0, framed);
        assert_eq!(framed.crc.get(), crc_ccitt(header.as_bytes()));
    }

    #[test]
    fn crc_matches_the_ccitt_check_value() {
        // CRC-16/X-25 of "123456789" is 0x906E
        assert_eq!(crc_ccitt(b"123456789"), 0x906E);
    }
}
