// DExtra: streams are DSVT packets, opened by a header that's sent twice

use super::packets::{ConnectPacket, DextraPoll, DsvtHeaderPacket, DsvtVoicePacket, HEADER_SEQUENCE};
use super::protocol::{Endpoint, ReflectorProtocol, Transmit, Voice};
use crate::dstar::pad;
use std::time::Duration;
use zerocopy::{FromBytes, IntoBytes};

pub struct Dextra;

impl ReflectorProtocol for Dextra {
    fn default_port(&self) -> u16 {
        30001
    }

    fn poll_interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    fn connect_packet(&self, endpoint: &Endpoint, unlink: bool) -> Vec<u8> {
        let remote_module = if unlink { b' ' } else { endpoint.module };
        ConnectPacket::new(endpoint.callsign, endpoint.local_module, remote_module).as_bytes().to_vec()
    }

    fn poll_packet(&self, endpoint: &Endpoint) -> Vec<u8> {
        let mut callsign = pad::<8>(endpoint.callsign);
        callsign[7] = endpoint.local_module;
        DextraPoll { callsign, terminator: 0 }.as_bytes().to_vec()
    }

    fn keepalive(&self, packet: &[u8]) -> Option<bool> {
        (packet.len() == 9).then_some(true)
    }

    fn header_packets(&self, transmit: &Transmit) -> Vec<Vec<u8>> {
        let packet = DsvtHeaderPacket::new(transmit.id, transmit.header.expect("a stream opens with its header")).as_bytes().to_vec();
        vec![packet.clone(), packet]
    }

    fn voice_packet(&self, transmit: &Transmit, sequence: u8, ambe: [u8; 9], slow_data: [u8; 3]) -> Vec<u8> {
        DsvtVoicePacket::new(transmit.id, sequence, ambe, slow_data).as_bytes().to_vec()
    }

    fn decode_voice(&self, packet: &[u8]) -> Option<Voice> {
        match DsvtHeaderPacket::read_from_bytes(packet) {
            Ok(dsvt) if dsvt.prefix.is_valid() && dsvt.prefix.sequence == HEADER_SEQUENCE => Some(Voice { stream_id: dsvt.prefix.stream_id.get(), header: Some(dsvt.header.header), sequence: HEADER_SEQUENCE, ambe: [0; 9], slow_data: [0; 3] }),
            _ => match DsvtVoicePacket::read_from_bytes(packet) {
                Ok(dsvt) if dsvt.prefix.is_valid() => Some(Voice { stream_id: dsvt.prefix.stream_id.get(), header: None, sequence: dsvt.prefix.sequence, ambe: dsvt.ambe, slow_data: dsvt.slow_data }),
                _ => None,
            },
        }
    }
}
