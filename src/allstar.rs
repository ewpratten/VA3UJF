// Control of the AllStar node through Asterisk's CLI, plus the link schedule that used to live in cron

use crate::config::{Action, Allstar, ScheduleEntry};
use crate::state::{self, ScheduledRun, Shared};
use anyhow::{bail, Context};
use chrono::{DateTime, Local};
use croner::Cron;
use std::str::FromStr;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::sleep;

const LINK_POLL_INTERVAL: Duration = Duration::from_secs(5);

// app_rpt's ilink functions: 2 is monitor-only, 3 is transceive, permanent links add 10, and 11 drops either kind
const ILINK_MONITOR: u32 = 2;
const ILINK_CONNECT: u32 = 3;
const ILINK_PERMANENT_OFFSET: u32 = 10;
const ILINK_DISCONNECT: u32 = 11;

#[derive(Debug, PartialEq)]
pub enum Step {
    Link { target: String, ilink: u32 },
    Unlink { target: Option<String> },
}

// AllStar reaches EchoLink nodes as 3 followed by their six-digit number
fn target_node(text: &str, echolink: bool) -> anyhow::Result<String> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("{text:?} isn't a node number");
    }
    if !echolink {
        return Ok(text.to_string());
    }
    if text.len() > 6 {
        bail!("{text} isn't an EchoLink node number");
    }
    Ok(format!("3{:06}", text.parse::<u32>()?))
}

pub fn step(action: &Action) -> anyhow::Result<Step> {
    match (&action.link, &action.unlink) {
        (Some(node), None) => {
            let base = if action.monitor { ILINK_MONITOR } else { ILINK_CONNECT };
            Ok(Step::Link { target: target_node(node, action.echolink)?, ilink: base + if action.permanent { ILINK_PERMANENT_OFFSET } else { 0 } })
        }
        (None, Some(node)) if node == "all" => Ok(Step::Unlink { target: None }),
        (None, Some(node)) => Ok(Step::Unlink { target: Some(target_node(node, action.echolink)?) }),
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

// Asterisk's control socket needs root, or membership in the asterisk group
async fn asterisk(command: &str) -> anyhow::Result<String> {
    let output = Command::new("asterisk").args(["-rx", command]).output().await.context("couldn't run asterisk")?;
    if !output.status.success() {
        bail!("asterisk failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

// Nodes linked directly to ours, from the rows under lstats' dashed header
pub async fn links(node: u32) -> anyhow::Result<Vec<String>> {
    let output = asterisk(&format!("rpt lstats {node}")).await?;
    let lines: Vec<&str> = output.lines().collect();
    let header_end = lines.iter().position(|line| line.starts_with("----")).unwrap_or(lines.len());
    Ok(lines.iter().skip(header_end + 1).filter_map(|line| line.split_whitespace().next().map(str::to_string)).collect())
}

pub async fn run_action(node: u32, action: &Action) -> anyhow::Result<()> {
    match step(action)? {
        Step::Link { target, ilink } => {
            asterisk(&format!("rpt cmd {node} ilink {ilink} {target}")).await?;
            log::info!("AllStar: connecting {node} to {target}");
        }
        Step::Unlink { target } => {
            let targets = match target {
                Some(target) => vec![target],
                None => links(node).await?,
            };
            for target in targets {
                asterisk(&format!("rpt cmd {node} ilink {ILINK_DISCONNECT} {target}")).await?;
                log::info!("AllStar: disconnecting {target}");
            }
        }
    }
    Ok(())
}

// Keep the node's link list fresh for the web UI
pub async fn poll_links(node: u32, shared: Shared) {
    loop {
        let result = links(node).await;
        state::update(&shared, |snapshot| {
            if let Some(status) = snapshot.allstar.as_mut() {
                match result {
                    Ok(links) => (status.links, status.error) = (links, None),
                    Err(error) => status.error = Some(format!("{error:#}")),
                }
            }
        });
        sleep(LINK_POLL_INTERVAL).await;
    }
}

// Run each schedule entry at its cron times, publishing the upcoming runs for the web UI
pub async fn run_schedule(allstar: Allstar, shared: Shared) {
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
                if let Err(error) = run_action(allstar.node, action).await {
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
    fn link_actions_pick_the_ilink_function() {
        assert_eq!(step(&Action { permanent: true, ..link("52685") }).unwrap(), Step::Link { target: "52685".into(), ilink: 13 });
        assert_eq!(step(&Action { monitor: true, ..link("52685") }).unwrap(), Step::Link { target: "52685".into(), ilink: 2 });
        assert_eq!(step(&Action { echolink: true, ..link("9999") }).unwrap(), Step::Link { target: "3009999".into(), ilink: 3 });
    }

    #[test]
    fn unlink_actions_take_a_node_or_all() {
        assert_eq!(step(&Action { unlink: Some("all".into()), ..Action::default() }).unwrap(), Step::Unlink { target: None });
        assert_eq!(step(&Action { unlink: Some("542490".into()), ..Action::default() }).unwrap(), Step::Unlink { target: Some("542490".into()) });
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
