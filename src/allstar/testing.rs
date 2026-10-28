// Fixtures for tests that run a whole node, with the channels a bridge would hold

use super::{channel, run, Audio, Handle};
use crate::config::{Allstar, Station};
use crate::state::{self, Shared};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub const SAMPLES_PER_FRAME: usize = 160;

pub struct TestNode {
    pub handle: Handle,
    pub shared: Shared,
    pub to_node: mpsc::Sender<Audio>,
    pub from_node: mpsc::Receiver<Audio>,
}

// A node on a local port that reaches each peer at a fixed "host:port"
pub fn start_node(node: u32, port: u16, peers: &[(u32, String)]) -> TestNode {
    let peer_table: String = peers.iter().map(|(peer, address)| format!("\"{peer}\" = \"{address}\"\n")).collect();
    let allstar: Allstar = toml::from_str(&format!("node = {node}\nambeserver = \"127.0.0.1:2460\"\niax_port = {port}\npark_reflector = false\n[nodes]\n{peer_table}")).unwrap();
    let station: Station = toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0").unwrap();
    let shared = Shared::default();
    state::update(&shared, |snapshot| snapshot.allstar = Some(state::AllstarStatus { node, ..Default::default() }));
    let (handle, commands) = channel();
    let (to_node, from_bridge) = mpsc::channel(256);
    let (to_bridge, from_node) = mpsc::channel(256);
    let (reflector_commands, _unused) = mpsc::channel(4);
    tokio::spawn(run(allstar, station, shared.clone(), reflector_commands, from_bridge, to_bridge, commands));
    TestNode { handle, shared, to_node, from_node }
}

pub async fn wait_for(description: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {description}");
}

pub fn linked(node: &TestNode, peer: &str) -> bool {
    state::read(&node.shared).allstar.is_some_and(|status| status.links.iter().any(|link| link.node == peer && link.connected))
}

pub fn has_no_links(node: &TestNode) -> bool {
    state::read(&node.shared).allstar.is_some_and(|status| status.links.is_empty())
}

pub fn pcm_to_bytes(samples: &[i16]) -> Vec<u8> {
    samples.iter().flat_map(|sample| sample.to_le_bytes()).collect()
}

// 20 ms of a tone, so every frame carries something
pub fn tone_frame(phase: usize) -> Vec<u8> {
    pcm_to_bytes(&(0..SAMPLES_PER_FRAME).map(|index| (((phase * SAMPLES_PER_FRAME + index) as f32 * 0.2).sin() * 8000.0) as i16).collect::<Vec<_>>())
}

// Send a transmission at the pace of real audio, then end it
pub async fn transmit(sender: &mpsc::Sender<Audio>, frames: usize) {
    for phase in 0..frames {
        sender.send(Audio::Voice(tone_frame(phase))).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sender.send(Audio::End).await.unwrap();
}

// Count the frames of one transmission, up to its end marker
pub async fn heard_frames(receiver: &mut mpsc::Receiver<Audio>, patience: Duration) -> Option<usize> {
    let mut frames = 0;
    let deadline = Instant::now() + patience;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(600), receiver.recv()).await {
            Ok(Some(Audio::Voice(bytes))) => {
                assert_eq!(bytes.len(), SAMPLES_PER_FRAME * 2);
                frames += 1;
            }
            Ok(Some(Audio::End)) => return Some(frames),
            _ if frames > 0 => return None,
            _ => {}
        }
    }
    None
}
