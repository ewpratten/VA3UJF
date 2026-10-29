// An AllStar node implemented natively: IAX2 links to other nodes, registration with AllStarLink, and the link schedule

mod directory;
mod iax2;
pub mod mixer;
mod network;
mod node;
mod tokens;

#[cfg(test)]
pub mod testing;

use crate::bridge;
use crate::config::{Action, Allstar, ScheduleEntry, Station};
use crate::router::Router;
use crate::speech::Speaker;
use crate::state::{self, AllstarStatus, ScheduledRun, Shared};
use anyhow::{bail, Context};
use chrono::{DateTime, Local};
use croner::Cron;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

// How the node is named in the status and the transmission log
pub const LINK_NAME: &str = "allstar";
pub const LINK_KIND: &str = "allstar";

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LinkMode {
    Monitor,
    Transceive,
}

pub enum Command {
    Link { node: String, mode: LinkMode, permanent: bool },
    Unlink { node: Option<String> },
}

#[derive(Clone)]
pub struct Handle {
    commands: mpsc::Sender<Command>,
}

pub fn channel() -> (Handle, mpsc::Receiver<Command>) {
    let (commands, receiver) = mpsc::channel(16);
    (Handle { commands }, receiver)
}

// Attach the node to the router, vocoding between its audio and D-STAR, and start it along with its schedule
pub fn start(allstar: Allstar, station: Station, shared: Shared, router: &mut Router, speaker: Option<Speaker>) -> Handle {
    let (bridge_to_node, from_bridge) = mpsc::channel(256);
    let (node_to_bridge, from_node) = mpsc::channel(256);
    let (handle, node_commands) = channel();
    state::update(&shared, |snapshot| snapshot.allstar = Some(AllstarStatus { node: allstar.node, ..Default::default() }));

    let settings = bridge::Settings {
        name: "allstar",
        ambeserver: allstar.ambeserver.clone(),
        suffix: allstar.suffix.clone(),
        message: allstar.message.clone().unwrap_or_else(|| format!("{} via AllStar", station.callsign)),
        rx_gain_db: allstar.rx_gain_db,
        tx_gain_db: allstar.tx_gain_db,
    };
    let port = router.attach_bridge(LINK_KIND);
    let (bridge_shared, node_shared, schedule_shared) = (shared.clone(), shared.clone(), shared);
    tokio::spawn(async move {
        if let Err(error) = bridge::run(settings, station, bridge_shared, port, from_node, bridge_to_node).await {
            log::error!("AllStar bridge failed: {error:#}");
        }
    });
    let (node_config, schedule_config) = (allstar.clone(), allstar);
    tokio::spawn(async move {
        if let Err(error) = node::run(node_config, node_shared, speaker, from_bridge, node_to_bridge, node_commands).await {
            log::error!("AllStar node failed: {error:#}");
        }
    });
    tokio::spawn(run_schedule(schedule_config, handle.clone(), schedule_shared));
    handle
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Link { target: String, mode: LinkMode, permanent: bool },
    Unlink { target: Option<String> },
}

fn target_node(text: &str) -> anyhow::Result<String> {
    if !directory::is_node_number(text) {
        bail!("{text:?} isn't a node number");
    }
    Ok(text.to_string())
}

pub fn step(action: &Action) -> anyhow::Result<Step> {
    match (&action.link, &action.unlink) {
        (Some(node), None) => Ok(Step::Link { target: target_node(node)?, mode: if action.monitor { LinkMode::Monitor } else { LinkMode::Transceive }, permanent: action.permanent }),
        (None, Some(node)) if node == "all" => Ok(Step::Unlink { target: None }),
        (None, Some(node)) => Ok(Step::Unlink { target: Some(target_node(node)?) }),
        _ => bail!("an action needs exactly one of link or unlink"),
    }
}

pub fn validate_schedule(schedule: &[ScheduleEntry]) -> anyhow::Result<()> {
    for entry in schedule {
        Cron::from_str(&entry.cron).with_context(|| format!("schedule {:?} has a bad cron expression", entry.name))?;
        for action in &entry.actions {
            step(action).with_context(|| format!("schedule {:?}", entry.name))?;
        }
    }
    Ok(())
}

impl Handle {
    pub async fn run_action(&self, action: &Action) -> anyhow::Result<()> {
        let command = match step(action)? {
            Step::Link { target, mode, permanent } => Command::Link { node: target, mode, permanent },
            Step::Unlink { target } => Command::Unlink { node: target },
        };
        self.commands.send(command).await.map_err(|_| anyhow::anyhow!("the AllStar node isn't running"))
    }
}

// Run each schedule entry at its cron times, publishing the upcoming runs for the web UI
pub async fn run_schedule(allstar: Allstar, handle: Handle, shared: Shared) {
    let entries: Vec<(&ScheduleEntry, Cron)> = allstar.schedule.iter().filter_map(|entry| Cron::from_str(&entry.cron).ok().map(|cron| (entry, cron))).collect();
    if entries.is_empty() {
        return;
    }
    loop {
        let now = Local::now();
        let upcoming: Vec<(usize, DateTime<Local>)> = entries.iter().enumerate().filter_map(|(index, (_, cron))| cron.find_next_occurrence(&now, false).ok().map(|time| (index, time))).collect();
        let Some(next) = upcoming.iter().map(|(_, time)| *time).min() else { return };
        let mut runs: Vec<ScheduledRun> = upcoming.iter().map(|(index, time)| ScheduledRun { name: entries[*index].0.name.clone(), next_at: time.timestamp() }).collect();
        runs.sort_by_key(|run| run.next_at);
        state::update(&shared, |snapshot| {
            if let Some(status) = snapshot.allstar.as_mut() {
                status.schedule = runs;
            }
        });

        // Sleep until the next run is due, rechecking in case the clock moved
        while let Ok(wait) = (next - Local::now()).to_std() {
            sleep(wait.min(Duration::from_secs(60))).await;
        }
        for (index, _) in upcoming.iter().filter(|(_, time)| *time == next) {
            let (entry, _) = &entries[*index];
            log::info!("AllStar schedule: {}", entry.name);
            for action in &entry.actions {
                if let Err(error) = handle.run_action(action).await {
                    log::error!("AllStar schedule {:?} failed: {error:#}", entry.name);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(node: &str) -> Action {
        Action { link: Some(node.to_string()), ..Action::default() }
    }

    #[test]
    fn link_actions_pick_the_mode_and_permanence() {
        assert_eq!(step(&Action { permanent: true, ..link("54321") }).unwrap(), Step::Link { target: "54321".into(), mode: LinkMode::Transceive, permanent: true });
        assert_eq!(step(&Action { monitor: true, ..link("54321") }).unwrap(), Step::Link { target: "54321".into(), mode: LinkMode::Monitor, permanent: false });
    }

    #[test]
    fn unlink_actions_take_a_node_or_all() {
        assert_eq!(step(&Action { unlink: Some("all".into()), ..Action::default() }).unwrap(), Step::Unlink { target: None });
        assert_eq!(step(&Action { unlink: Some("54322".into()), ..Action::default() }).unwrap(), Step::Unlink { target: Some("54322".into()) });
    }

    #[test]
    fn malformed_actions_are_rejected() {
        assert!(step(&Action::default()).is_err());
        assert!(step(&link("not-a-node")).is_err());
        assert!(step(&Action { link: Some("1".into()), unlink: Some("2".into()), ..Action::default() }).is_err());
    }

    #[test]
    fn cron_times_use_the_five_field_form() {
        let cron = Cron::from_str("55 18 * * *").unwrap();
        let from = DateTime::parse_from_rfc3339("2026-01-01T18:00:00-05:00").unwrap();
        assert_eq!(cron.find_next_occurrence(&from, false).unwrap().to_rfc3339(), "2026-01-01T18:55:00-05:00");
    }
}
