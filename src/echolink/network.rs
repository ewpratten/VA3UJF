// The node's presence on the EchoLink network: logging in on a timer so other stations can find it, and keeping the list of stations fresh

use super::directory::Directory;
use crate::state::{self, Shared};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, Instant};

const CHECK_INTERVAL: Duration = Duration::from_secs(1);
const MIN_STATUS_CHANGE_GAP: Duration = Duration::from_secs(5);

fn in_a_call(shared: &Shared) -> bool {
    state::read(shared).links.iter().any(|link| link.kind == super::LINK_KIND && link.peers.iter().any(|peer| peer.connected))
}

// Log in on a timer, as busy whenever someone is linked, and take the list of stations from the directory each time
pub async fn keep_logged_in(directory: Arc<Directory>, interval_seconds: u64, shared: Shared) {
    let period = Duration::from_secs(interval_seconds.max(1));
    let mut next_login = Instant::now();
    let mut last_login = Instant::now() - MIN_STATUS_CHANGE_GAP;
    let mut last_busy = false;
    let mut logged_in_before = None;
    let mut check = interval(CHECK_INTERVAL);
    loop {
        check.tick().await;
        let busy = in_a_call(&shared);
        let status_changed = busy != last_busy && last_login.elapsed() >= MIN_STATUS_CHANGE_GAP;
        if Instant::now() < next_login && !status_changed {
            continue;
        }
        (next_login, last_login, last_busy) = (Instant::now() + period, Instant::now(), busy);
        let outcome = match directory.log_in(busy).await {
            Ok(()) => directory.refresh().await.map(|stations| log::debug!("EchoLink: {stations} stations are online")),
            Err(error) => Err(error),
        };
        let logged_in = outcome.is_ok();
        if let Err(error) = &outcome {
            log::warn!("EchoLink: couldn't log in to the directory: {error:#}");
        } else if logged_in_before != Some(true) {
            log::info!("EchoLink: logged in to the directory");
        }
        logged_in_before = Some(logged_in);
        state::update(&shared, |snapshot| {
            if let Some(status) = snapshot.echolink.as_mut() {
                status.logged_in = Some(logged_in);
            }
            let link = state::link_mut(snapshot, super::LINK_NAME, super::LINK_KIND);
            link.warnings = if logged_in { Vec::new() } else { vec!["The node is not logged in to the EchoLink directory.".to_string()] };
        });
    }
}
