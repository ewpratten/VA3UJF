// DCS: every frame of voice carries the whole header, and the reflector is named in each keepalive

use super::packets::{ConnectPacket, DcsLinkPacket, DcsPoll, DcsUnlinkPacket, DcsVoicePacket, DextraPoll};
use super::protocol::{Endpoint, ReflectorProtocol, Transmit, Voice};
use crate::dstar::pad;
use std::time::Duration;
use zerocopy::{FromBytes, IntoBytes};

pub struct Dcs;

impl ReflectorProtocol for Dcs {
    fn default_port(&self) -> u16 {
        30051
    }

    fn poll_interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    fn connect_packet(&self, endpoint: &Endpoint, unlink: bool) -> Vec<u8> {
        let remote_module = if unlink { b' ' } else { endpoint.module };
        let unlink_packet = DcsUnlinkPacket { connect: ConnectPacket::new(endpoint.callsign, endpoint.local_module, remote_module), reflector: pad(endpoint.reflector) };
        if unlink { unlink_packet.as_bytes().to_vec() } else { DcsLinkPacket::new(unlink_packet).as_bytes().to_vec() }
    }

    fn poll_packet(&self, endpoint: &Endpoint) -> Vec<u8> {
        let mut callsign = pad::<8>(endpoint.callsign);
        callsign[7] = endpoint.local_module;
        let mut reflector = pad::<8>(endpoint.reflector);
        reflector[7] = endpoint.module;
        DcsPoll { poll: DextraPoll { callsign, terminator: 0 }, reflector }.as_bytes().to_vec()
    }

    // The 17 byte form is the reflector answering ours
    fn keepalive(&self, packet: &[u8]) -> Option<bool> {
        match packet.len() {
            22 => Some(true),
            17 => Some(false),
            _ => None,
        }
    }

    fn header_packets(&self, _transmit: &Transmit) -> Vec<Vec<u8>> {
        Vec::new()
    }

    fn voice_packet(&self, transmit: &Transmit, sequence: u8, ambe: [u8; 9], slow_data: [u8; 3]) -> Vec<u8> {
        DcsVoicePacket::new(transmit.header.expect("voice is only sent inside a transmission"), transmit.id, sequence, ambe, slow_data, transmit.count).as_bytes().to_vec()
    }

    fn decode_voice(&self, packet: &[u8]) -> Option<Voice> {
        match DcsVoicePacket::read_from_bytes(packet) {
            Ok(dcs) if dcs.is_valid() => Some(Voice { stream_id: dcs.stream_id.get(), header: Some(dcs.header), sequence: dcs.sequence, ambe: dcs.ambe, slow_data: dcs.slow_data }),
            _ => None,
        }
    }
}
