// D-STAR header and voice frame types, shared by the modem driver and the reflector links

pub const HEADER_LENGTH: usize = 41;
pub const END_SLOW_DATA: [u8; 3] = [0x55, 0x55, 0x55];
pub const FRAMES_PER_SUPERFRAME: u8 = 21;

#[derive(Clone, Debug, PartialEq)]
pub struct Header {
    pub flags: [u8; 3],
    pub repeater_2: [u8; 8],
    pub repeater_1: [u8; 8],
    pub your_call: [u8; 8],
    pub my_call: [u8; 8],
    pub my_suffix: [u8; 4],
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

pub fn crc_ccitt(data: &[u8]) -> [u8; 2] {
    let mut crc: u16 = 0xFFFF;
    for byte in data {
        crc ^= *byte as u16;
        for _ in 0..8 {
            crc = if crc & 1 == 1 { (crc >> 1) ^ 0x8408 } else { crc >> 1 };
        }
    }
    (!crc).to_le_bytes()
}

impl Header {
    // The 39 bytes the CRC covers, which is also the layout DCS and DExtra carry
    pub fn body(&self) -> Vec<u8> {
        [&self.flags[..], &self.repeater_2, &self.repeater_1, &self.your_call, &self.my_call, &self.my_suffix].concat()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let body = self.body();
        let crc = crc_ccitt(&body);
        [body, crc.to_vec()].concat()
    }

    pub fn from_body(body: &[u8]) -> Option<Self> {
        if body.len() < 39 {
            return None;
        }
        Some(Header {
            flags: body[0..3].try_into().ok()?,
            repeater_2: body[3..11].try_into().ok()?,
            repeater_1: body[11..19].try_into().ok()?,
            your_call: body[19..27].try_into().ok()?,
            my_call: body[27..35].try_into().ok()?,
            my_suffix: body[35..39].try_into().ok()?,
        })
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

    #[test]
    fn header_round_trips_through_its_wire_form() {
        let mut header = Header::default_for_test();
        header.flags = [0, 0, 0];
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), HEADER_LENGTH);
        assert_eq!(Header::from_body(&bytes[..39]), Some(header));
        assert_eq!(crc_ccitt(&bytes[..39]), [bytes[39], bytes[40]]);
    }

    #[test]
    fn crc_matches_the_ccitt_check_value() {
        // CRC-16/X-25 of "123456789" is 0x906E
        assert_eq!(crc_ccitt(b"123456789"), 0x906Eu16.to_le_bytes());
    }
}

#[cfg(test)]
impl Header {
    fn default_for_test() -> Self {
        Header { flags: [0; 3], repeater_2: pad("DCS405 A"), repeater_1: pad("VA3UJF B"), your_call: pad("CQCQCQ"), my_call: pad("VA3UJF"), my_suffix: pad("TEST") }
    }
}
