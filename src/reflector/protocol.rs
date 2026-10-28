// What differs between the reflector protocols, so the link itself doesn't have to know which one it's speaking

use crate::config::Protocol;
use crate::dstar::Header;
use std::time::Duration;

// The two ends of a link, as the protocols name them
pub struct Endpoint<'a> {
    pub callsign: &'a str,
    pub local_module: u8,
    pub reflector: &'a str,
    pub module: u8,
}

// A frame of voice heard from a reflector. Protocols that open a stream with a header of its own send it with a header sequence number
pub struct Voice {
    pub stream_id: u16,
    pub header: Option<Header>,
    pub sequence: u8,
    pub ambe: [u8; 9],
    pub slow_data: [u8; 3],
}

// Where a stream being sent to the reflector has got to
#[derive(Default)]
pub struct Transmit {
    pub id: u16,
    pub sequence: u8,
    pub count: u32,
    pub header: Option<Header>,
}

pub trait ReflectorProtocol: Send + Sync {
    fn default_port(&self) -> u16;
    fn poll_interval(&self) -> Duration;
    fn connect_packet(&self, endpoint: &Endpoint, unlink: bool) -> Vec<u8>;
    fn poll_packet(&self, endpoint: &Endpoint) -> Vec<u8>;

    // Whether a packet is a keepalive from the reflector, and if so, whether it wants one back
    fn keepalive(&self, packet: &[u8]) -> Option<bool>;

    // What announces a new stream. Some protocols don't, since every frame of voice says so
    fn header_packets(&self, transmit: &Transmit) -> Vec<Vec<u8>>;
    fn voice_packet(&self, transmit: &Transmit, sequence: u8, ambe: [u8; 9], slow_data: [u8; 3]) -> Vec<u8>;
    fn decode_voice(&self, packet: &[u8]) -> Option<Voice>;
}

pub fn implementation(protocol: Protocol) -> Box<dyn ReflectorProtocol> {
    match protocol {
        Protocol::Dcs => Box::new(super::dcs::Dcs),
        Protocol::Dextra => Box::new(super::dextra::Dextra),
    }
}
