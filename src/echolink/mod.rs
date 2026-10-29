// An EchoLink node implemented natively: calls to and from EchoLink stations, and the directory login that lets them find it

mod directory;
mod network;
mod node;
mod protocol;

use crate::bridge;
use crate::config::{Echolink, Station};
use crate::router::Router;
use crate::speech::Speaker;
use crate::state::{self, EcholinkStatus, Shared};
use anyhow::bail;
use std::sync::Arc;
use tokio::sync::mpsc;

// How the node is named in the status and the transmission log
pub const LINK_NAME: &str = "echolink";
pub const LINK_KIND: &str = "echolink";

const MAX_TARGET_LENGTH: usize = 32;

pub enum Command {
    Link { target: String },
    Unlink { target: Option<String> },
}

#[derive(Clone)]
pub struct Handle {
    commands: mpsc::Sender<Command>,
}

// A station is named by its callsign, or by the numeric ID the directory gives it
fn valid_target(text: &str) -> anyhow::Result<()> {
    if text.is_empty() || text.len() > MAX_TARGET_LENGTH || !text.chars().all(|character| character.is_ascii_alphanumeric() || "-/*_".contains(character)) {
        bail!("{text:?} isn't a station callsign or ID");
    }
    Ok(())
}

impl Handle {
    pub async fn link(&self, target: &str) -> anyhow::Result<()> {
        valid_target(target)?;
        self.send(Command::Link { target: target.to_string() }).await
    }

    // Unlink takes "all" to drop everything
    pub async fn unlink(&self, target: &str) -> anyhow::Result<()> {
        if target == "all" {
            return self.send(Command::Unlink { target: None }).await;
        }
        valid_target(target)?;
        self.send(Command::Unlink { target: Some(target.to_string()) }).await
    }

    async fn send(&self, command: Command) -> anyhow::Result<()> {
        self.commands.send(command).await.map_err(|_| anyhow::anyhow!("the EchoLink node isn't running"))
    }
}

// Attach the node to the router, vocoding between its audio and D-STAR, and start it along with its directory login
pub fn start(echolink: Echolink, station: Station, shared: Shared, router: &mut Router, speaker: Option<Speaker>) -> Handle {
    let (bridge_to_node, from_bridge) = mpsc::channel(256);
    let (node_to_bridge, from_node) = mpsc::channel(256);
    let (commands, node_commands) = mpsc::channel(16);
    let directory = Arc::new(directory::Directory::new(&echolink));
    state::update(&shared, |snapshot| snapshot.echolink = Some(EcholinkStatus { callsign: echolink.callsign.clone(), logged_in: directory.can_log_in().then_some(false) }));

    let settings = bridge::Settings {
        name: "echolink",
        ambeserver: echolink.ambeserver.clone(),
        suffix: echolink.suffix.clone(),
        message: echolink.message.clone().unwrap_or_else(|| format!("{} via EchoLink", station.callsign)),
        rx_gain_db: echolink.rx_gain_db,
        tx_gain_db: echolink.tx_gain_db,
    };
    let port = router.attach_bridge(LINK_KIND);
    let (bridge_shared, node_shared) = (shared.clone(), shared.clone());
    tokio::spawn(async move {
        if let Err(error) = bridge::run(settings, station, bridge_shared, port, from_node, bridge_to_node).await {
            log::error!("EchoLink bridge failed: {error:#}");
        }
    });
    if directory.can_log_in() {
        tokio::spawn(network::keep_logged_in(directory.clone(), echolink.refresh_seconds, shared));
    }
    tokio::spawn(async move {
        if let Err(error) = node::run(echolink, directory, node_shared, speaker, from_bridge, node_to_bridge, node_commands).await {
            log::error!("EchoLink node failed: {error:#}");
        }
    });
    Handle { commands }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_are_callsigns_or_ids() {
        assert!(valid_target("W1AW-R").is_ok());
        assert!(valid_target("*ECHOTEST*").is_ok());
        assert!(valid_target("12345").is_ok());
        assert!(valid_target("").is_err());
        assert!(valid_target("W1AW R").is_err());
        assert!(valid_target(&"A".repeat(40)).is_err());
    }
}
