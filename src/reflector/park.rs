// Steps off the reflector while another link has parties linked in, and goes back once they're gone

use super::{Command, LINK_KIND};
use crate::state::{Shared, Snapshot};
use tokio::sync::mpsc;

fn others_are_linked(snapshot: &Snapshot) -> bool {
    snapshot.links.iter().any(|link| link.kind != LINK_KIND && link.peers.iter().any(|peer| peer.connected))
}

fn reflector_target(snapshot: &Snapshot) -> Option<String> {
    snapshot.links.iter().find(|link| link.kind == LINK_KIND).and_then(|link| link.target.clone())
}

// A target is written "NAME M"
fn parse_target(target: &str) -> Option<Command> {
    let (name, module) = target.rsplit_once(' ')?;
    Some(Command::Link { name: name.to_string(), module: module.chars().next()? })
}

pub async fn run(shared: Shared, commands: mpsc::Sender<Command>) {
    let mut snapshots = shared.subscribe();
    let mut parked: Option<String> = None;
    loop {
        let (others_linked, reflector) = {
            let snapshot = snapshots.borrow_and_update();
            (others_are_linked(&snapshot), reflector_target(&snapshot))
        };
        if others_linked && parked.is_none() {
            if let Some(target) = reflector {
                log::info!("Leaving {target} while other links are up");
                parked = Some(target);
                let _ = commands.try_send(Command::Unlink);
            }
        } else if !others_linked && reflector.is_none() {
            if let Some(command) = parked.take().and_then(|target| parse_target(&target)) {
                log::info!("No other links left, returning to the reflector");
                let _ = commands.try_send(command);
            }
        } else if !others_linked {
            parked = None;
        }
        if snapshots.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{self, PeerStatus};
    use std::time::Duration;

    async fn next(commands: &mut mpsc::Receiver<Command>) -> Option<Command> {
        tokio::time::timeout(Duration::from_secs(1), commands.recv()).await.ok().flatten()
    }

    fn set_peer(shared: &Shared, connected: bool) {
        state::update(shared, |snapshot| {
            let peers = if connected { vec![PeerStatus { id: "54321".into(), mode: "transceive", outbound: true, connected, keyed: false }] } else { Vec::new() };
            state::link_mut(snapshot, "other", "other").peers = peers;
        });
    }

    #[tokio::test]
    async fn the_reflector_is_left_while_another_link_has_peers_and_rejoined_after() {
        let shared = Shared::default();
        let (commands, mut received) = mpsc::channel(4);
        tokio::spawn(run(shared.clone(), commands));
        state::update(&shared, |snapshot| state::link_mut(snapshot, "reflector", LINK_KIND).target = Some("DCS405 A".into()));

        set_peer(&shared, true);
        assert!(matches!(next(&mut received).await, Some(Command::Unlink)));

        // The reflector drops its target once it has unlinked
        state::update(&shared, |snapshot| state::link_mut(snapshot, "reflector", LINK_KIND).target = None);
        set_peer(&shared, false);
        assert!(matches!(next(&mut received).await, Some(Command::Link { name, module: 'A' }) if name == "DCS405"));
    }
}
