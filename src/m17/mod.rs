// An M17 client implemented natively: links to one reflector at a time, vocoding its Codec 2 voice to and from D-STAR

mod node;
mod protocol;

use crate::bridge;
use crate::config::{Station, M17};
use crate::router::Router;
use crate::speech::Speaker;
use crate::state::Shared;
use anyhow::bail;
use tokio::sync::mpsc;

// How the node is named in the status and the transmission log
pub const LINK_NAME: &str = "m17";
pub const LINK_KIND: &str = "m17";

const MAX_NAME_LENGTH: usize = 32;

pub enum Command {
    Link { name: String, module: char },
    Unlink,
}

#[derive(Clone)]
pub struct Handle {
    commands: mpsc::Sender<Command>,
}

impl Handle {
    pub async fn link(&self, name: &str, module: char) -> anyhow::Result<()> {
        if name.is_empty() || name.len() > MAX_NAME_LENGTH || !name.chars().all(|character| character.is_ascii_alphanumeric() || "-_".contains(character)) {
            bail!("{name:?} isn't a reflector name");
        }
        self.send(Command::Link { name: name.to_string(), module }).await
    }

    pub async fn unlink(&self) -> anyhow::Result<()> {
        self.send(Command::Unlink).await
    }

    async fn send(&self, command: Command) -> anyhow::Result<()> {
        self.commands.send(command).await.map_err(|_| anyhow::anyhow!("the M17 node isn't running"))
    }
}

// Attach the node to the router, vocoding between its audio and D-STAR, and start it
pub fn start(m17: M17, station: Station, shared: Shared, router: &mut Router, speaker: Option<Speaker>) -> Handle {
    let (bridge_to_node, from_bridge) = mpsc::channel(256);
    let (node_to_bridge, from_node) = mpsc::channel(256);
    let (commands, node_commands) = mpsc::channel(16);
    let settings = bridge::Settings {
        name: LINK_NAME,
        ambeserver: m17.ambeserver.clone(),
        suffix: m17.suffix.clone(),
        message: m17.message.clone().unwrap_or_else(|| format!("{} via M17", station.callsign)),
        rx_gain_db: m17.rx_gain_db,
        tx_gain_db: m17.tx_gain_db,
    };
    let port = router.attach_bridge(LINK_KIND);
    let (bridge_station, bridge_shared, node_shared) = (station.clone(), shared.clone(), shared);
    tokio::spawn(async move {
        if let Err(error) = bridge::run(settings, bridge_station, bridge_shared, port, from_node, bridge_to_node).await {
            log::error!("M17 bridge failed: {error:#}");
        }
    });
    tokio::spawn(async move {
        if let Err(error) = node::run(m17, station, node_shared, speaker, from_bridge, node_to_bridge, node_commands).await {
            log::error!("M17 node failed: {error:#}");
        }
    });
    Handle { commands }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reflector_names_are_checked() {
        let (commands, _receiver) = mpsc::channel(4);
        let handle = Handle { commands };
        assert!(handle.link("M17-USA", 'C').await.is_ok());
        assert!(handle.link("", 'C').await.is_err());
        assert!(handle.link("M17 USA", 'C').await.is_err());
        assert!(handle.link(&"M".repeat(40), 'C').await.is_err());
    }
}
