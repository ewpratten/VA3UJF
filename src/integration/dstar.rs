// Links this program's reflector client into a real DStarGateway, with the test standing in for MMDVMHost and a fake ircDDB behind the gateway.
// A relay container is the repeater's address on the network, since the gateway only accepts a repeater by its IP

use super::{build_image, container_assets, containers_available, Container, Network};
use crate::config::Config;
use crate::dstar::{pad, Frame, Header, HeaderWithCrc};
use crate::reflector::{self, Command};
use crate::speech::Announcement;
use crate::{hosts, state};
use std::time::Duration;
use zerocopy::{FromBytes, IntoBytes};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

const GATEWAY_CALLSIGN: &str = "N0GATE";
const HOTSPOT_CALLSIGN: &str = "N0CALL";

// Streams only reach the gateway's repeater when they're addressed to it, so the link is named after the gateway
const REFLECTOR_NAME: &str = GATEWAY_CALLSIGN;
const FRAMES: usize = 20;

// Homebrew repeater protocol, as MMDVMHost speaks it to the gateway
fn homebrew_header(header: &Header) -> Vec<u8> {
    [&b"DSRP"[..], &[0x20, 0x12, 0x34, 0], header.with_crc().as_bytes()].concat()
}

fn homebrew_voice(sequence: u8, end: bool, ambe: [u8; 9], slow_data: [u8; 3]) -> Vec<u8> {
    [&b"DSRP"[..], &[0x21, 0x12, 0x34, sequence | if end { 0x40 } else { 0 }, 0], &ambe, &slow_data].concat()
}

fn voice_payload(index: usize) -> ([u8; 9], [u8; 3]) {
    let seed = index as u8 + 1;
    ([seed; 9], [seed ^ 0xFF, seed ^ 0xAA, seed ^ 0x55])
}

fn callsign_header(my_call: &str) -> Header {
    Header { flags: [0; 3], repeater_2: pad(&format!("{GATEWAY_CALLSIGN:<7}G")), repeater_1: pad(&format!("{GATEWAY_CALLSIGN:<7}B")), your_call: pad("CQCQCQ"), my_call: pad(my_call), my_suffix: pad("TEST") }
}

// Everything the repeater side of the gateway sent us, as headers and voice
async fn receive_homebrew(socket: &UdpSocket, patience: Duration) -> (Option<Header>, Vec<([u8; 9], [u8; 3])>) {
    let (mut header, mut voice) = (None, Vec::new());
    let mut buffer = [0u8; 512];
    while let Ok(Ok(length)) = tokio::time::timeout(patience, socket.recv(&mut buffer)).await {
        let packet = &buffer[..length];
        match packet.get(4) {
            Some(0x20) if length >= 47 => header = HeaderWithCrc::read_from_prefix(&packet[8..]).ok().map(|(framed, _)| framed.header),
            Some(0x21) if length >= 21 && packet[7] & 0x40 == 0 => voice.push((packet[9..18].try_into().unwrap(), packet[18..21].try_into().unwrap())),
            Some(0x21) => break,
            _ => {}
        }
    }
    (header, voice)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn our_reflector_client_trades_voice_with_a_real_dstargateway() {
    if !containers_available() {
        return;
    }
    let network = Network::create().unwrap();
    let ircddb_assets = container_assets("ircddb");
    build_image("va3ujf-test-ircddb", &ircddb_assets.join("Dockerfile"), &ircddb_assets).unwrap();
    let ircddb = Container::start("va3ujf-test-ircddb", &["--network", &network.name, "--network-alias", "ircddb"]).unwrap();
    ircddb.wait_for_logs("listening on 9007", Duration::from_secs(30)).unwrap();

    // The test is the repeater, reached through the relay: it sees what the gateway sends, and sends what MMDVMHost would
    let repeater = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let repeater_port = repeater.local_addr().unwrap().port().to_string();
    let relay_assets = container_assets("relay");
    build_image("va3ujf-test-relay", &relay_assets.join("Dockerfile"), &relay_assets).unwrap();
    let relay_environment = ["GATEWAY_HOST=gateway".to_string(), "HOST_ADDRESS=host.docker.internal".to_string(), format!("HOST_PORT={repeater_port}")];
    let mut relay_arguments = vec!["--network", &network.name, "--network-alias", "repeater", "--add-host", "host.docker.internal:host-gateway", "--publish", "127.0.0.1::20012/udp"];
    for variable in &relay_environment {
        relay_arguments.extend(["--env", variable]);
    }
    let relay = Container::start("va3ujf-test-relay", &relay_arguments).unwrap();
    let gateway_assets = container_assets("dstargateway");
    build_image("va3ujf-test-dstargateway", &gateway_assets.join("Dockerfile"), &gateway_assets).unwrap();
    let environment = [format!("GATEWAY_CALLSIGN={GATEWAY_CALLSIGN}"), "IRCDDB_HOST=ircddb".to_string(), "REPEATER_ADDRESS=repeater".to_string(), "REPEATER_PORT=20011".to_string()];
    let mut arguments = vec!["--network", &network.name, "--network-alias", "gateway", "--publish", "127.0.0.1::30001/udp"];
    for variable in &environment {
        arguments.extend(["--env", variable]);
    }
    let gateway = Container::start("va3ujf-test-dstargateway", &arguments).unwrap();

    // The gateway signs in to the fake ircDDB, which is how we know it's up
    ircddb.wait_for_logs("received NICK", Duration::from_secs(30)).unwrap_or_else(|error| panic!("{error:#}\nGateway logs:\n{}", gateway.logs()));
    let (dextra_port, homebrew_port) = (gateway.host_port("30001/udp").unwrap(), relay.host_port("20012/udp").unwrap());

    // Our reflector client links into the gateway as a DExtra dongle on its module B
    let config: Config = toml::from_str(&format!(
        "[station]\ncallsign = \"{HOTSPOT_CALLSIGN}\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0\n[modem]\nport = \"/dev/null\"\n[[reflector]]\nname = \"{REFLECTOR_NAME}\"\nprotocol = \"dextra\"\naddress = \"127.0.0.1\"\nport = {dextra_port}\n"
    ))
    .unwrap();
    let directory = hosts::directory(&config);
    let shared = state::Shared::default();
    let (commands, command_receiver) = mpsc::channel(4);
    let (from_reflector_sender, mut from_reflector) = mpsc::channel(256);
    let (to_reflector, to_reflector_receiver) = mpsc::channel(256);
    let (speaker, mut announcements) = mpsc::channel(8);
    tokio::spawn(reflector::run(config, directory, command_receiver, from_reflector_sender, to_reflector_receiver, shared.clone(), Some(speaker)));
    commands.send(Command::Link { name: REFLECTOR_NAME.into(), module: 'B' }).await.unwrap();
    let mut linked = false;
    for _ in 0..100 {
        linked = state::read(&shared).link.connected;
        if linked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(linked, "never linked to the gateway. Gateway logs:\n{}", gateway.logs());

    // The link is announced out loud
    assert_eq!(announcements.try_recv().ok(), Some(Announcement::Text("Linked to reflector N 0 G A T E, module Bravo.".into())));

    // The gateway announces the new link on both sides, and we want that done before our own voice goes through
    tokio::time::sleep(Duration::from_secs(5)).await;
    while from_reflector.try_recv().is_ok() {}
    let mut discard = [0u8; 512];
    while repeater.try_recv(&mut discard).is_ok() {}

    // Voice from the repeater side of the gateway reaches us
    let to_gateway = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    to_gateway.connect(format!("127.0.0.1:{homebrew_port}")).await.unwrap();
    to_gateway.send(&homebrew_header(&callsign_header("N0TEST"))).await.unwrap();
    for index in 0..FRAMES {
        let (ambe, slow_data) = voice_payload(index);
        to_gateway.send(&homebrew_voice(index as u8, false, ambe, slow_data)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    to_gateway.send(&homebrew_voice(FRAMES as u8, true, [0x55; 9], [0x55; 3])).await.unwrap();
    let mut heard_header = None;
    let mut heard_voice = Vec::new();
    while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(2), from_reflector.recv()).await {
        match frame {
            Frame::Header(header) => heard_header = Some(header),
            Frame::Voice { ambe, slow_data, .. } => heard_voice.push((ambe, slow_data)),
            Frame::End => break,
        }
    }
    let heard_header = heard_header.unwrap_or_else(|| panic!("no header came through the gateway. Gateway logs:\n{}", gateway.logs()));
    assert_eq!(heard_header.my_call_text(), "N0TEST/TEST");
    assert!(heard_voice.len() >= FRAMES - 2, "only {} of {FRAMES} voice frames came through", heard_voice.len());
    assert_eq!(heard_voice[0], voice_payload(0));

    // And our voice reaches the repeater side
    let mut header = callsign_header(HOTSPOT_CALLSIGN);
    header.repeater_1 = pad(&format!("{HOTSPOT_CALLSIGN:<7}B"));
    to_reflector.send(Frame::Header(header)).await.unwrap();
    for index in 0..FRAMES {
        let (ambe, slow_data) = voice_payload(index);
        to_reflector.send(Frame::Voice { sequence: index as u8, ambe, slow_data }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    to_reflector.send(Frame::End).await.unwrap();
    let (repeater_header, repeater_voice) = receive_homebrew(&repeater, Duration::from_secs(3)).await;
    let repeater_header = repeater_header.unwrap_or_else(|| panic!("the repeater side got no header. Gateway logs:\n{}", gateway.logs()));
    assert_eq!(repeater_header.my_call_text(), format!("{HOTSPOT_CALLSIGN}/TEST"));
    assert!(repeater_voice.len() >= FRAMES - 2, "the repeater side got only {} of {FRAMES} voice frames", repeater_voice.len());
    assert_eq!(repeater_voice[0], voice_payload(0));
}
