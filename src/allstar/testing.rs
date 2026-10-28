// Fixtures for tests that run a whole node, with the channels a bridge would hold

use super::node::run;
use super::{channel, Handle};
use crate::codec::{Audio, SAMPLES_PER_FRAME};
use crate::speech::Speaker;
use crate::config::Allstar;
use crate::state::{self, Shared};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub struct TestNode {
    pub handle: Handle,
    pub shared: Shared,
    pub to_node: mpsc::Sender<Audio>,
    pub from_node: mpsc::Receiver<Audio>,
}

// A node on a local port that reaches each peer at a fixed "host:port"
pub fn start_node(node: u32, port: u16, peers: &[(u32, String)]) -> TestNode {
    start_node_speaking(node, port, peers, None)
}

// The same, with announcements going to a speaker
pub fn start_node_speaking(node: u32, port: u16, peers: &[(u32, String)], speaker: Option<Speaker>) -> TestNode {
    let peer_table: String = peers.iter().map(|(peer, address)| format!("\"{peer}\" = \"{address}\"\n")).collect();
    let allstar: Allstar = toml::from_str(&format!("node = {node}\nambeserver = \"127.0.0.1:2460\"\niax_port = {port}\npark_reflector = false\n[nodes]\n{peer_table}")).unwrap();
    let shared = Shared::default();
    state::update(&shared, |snapshot| snapshot.allstar = Some(state::AllstarStatus { node, ..Default::default() }));
    let (handle, commands) = channel();
    let (to_node, from_bridge) = mpsc::channel(256);
    let (to_bridge, from_node) = mpsc::channel(256);
    tokio::spawn(run(allstar, shared.clone(), speaker, from_bridge, to_bridge, commands));
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

pub fn linked(node: &TestNode, peer_id: &str) -> bool {
    state::read(&node.shared).links.iter().any(|link| link.peers.iter().any(|peer| peer.id == peer_id && peer.connected))
}

pub fn has_no_links(node: &TestNode) -> bool {
    state::read(&node.shared).links.iter().all(|link| link.peers.is_empty())
}

// 20 ms of a tone, so every frame carries something
pub fn tone_frame(phase: usize) -> Vec<i16> {
    (0..SAMPLES_PER_FRAME).map(|index| (((phase * SAMPLES_PER_FRAME + index) as f32 * 0.2).sin() * 8000.0) as i16).collect()
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
            Ok(Some(Audio::Voice(samples))) => {
                assert_eq!(samples.len(), SAMPLES_PER_FRAME);
                frames += 1;
            }
            Ok(Some(Audio::End)) => return Some(frames),
            _ if frames > 0 => return None,
            _ => {}
        }
    }
    None
}

// A frame of one steady level, so what comes out of a mix can be checked by its size
pub fn constant_frame(level: i16) -> Vec<i16> {
    vec![level; SAMPLES_PER_FRAME]
}

pub async fn transmit_level(sender: &mpsc::Sender<Audio>, level: i16, frames: usize) {
    for _ in 0..frames {
        sender.send(Audio::Voice(constant_frame(level))).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sender.send(Audio::End).await.unwrap();
}

// The levels heard during one transmission, as the average of each frame
pub async fn heard_levels(receiver: &mut mpsc::Receiver<Audio>, patience: Duration) -> Vec<i32> {
    let mut levels = Vec::new();
    let deadline = Instant::now() + patience;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(600), receiver.recv()).await {
            Ok(Some(Audio::Voice(samples))) => {
                levels.push(samples.iter().map(|sample| *sample as i32).sum::<i32>() / samples.len() as i32);
            }
            Ok(Some(Audio::End)) => break,
            _ if !levels.is_empty() => break,
            _ => {}
        }
    }
    levels
}
