// The node's presence on the AllStarLink network: registration, so other nodes can find it, and the stats posts that show its links

use crate::config::{Registration, Statpost};
use crate::state::{self, Shared, Snapshot};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::{interval, sleep, Instant};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const KEY_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const MIN_STATPOST_SECONDS: u64 = 30;
const MAX_STATPOST_SECONDS: u64 = 600;

fn client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build()?)
}

// What the registration server needs to know: the node, its password, and the port other nodes should call
fn registration_request(node: u32, iax_port: u16, password: &str) -> Value {
    json!({ "port": iax_port, "data": { "nodes": { node.to_string(): { "node": node.to_string(), "passwd": password, "remote": 0 } } } })
}

// The server answers with the address it saw us at, and a message that says whether it took
fn registration_accepted(response: &Value) -> bool {
    response.get("data").is_some_and(|data| data.to_string().contains("successfully registered"))
}

async fn register_once(client: &reqwest::Client, node: u32, iax_port: u16, registration: &Registration) -> anyhow::Result<Value> {
    let password = tokio::fs::read_to_string(&registration.password_file).await.map_err(|error| anyhow::anyhow!("couldn't read {}: {error}", registration.password_file.display()))?;
    let response = client.post(&registration.url).json(&registration_request(node, iax_port, password.trim())).send().await?.error_for_status()?;
    Ok(response.json().await?)
}

// Register on a timer, so the network keeps pointing other nodes at this one
pub async fn register(node: u32, iax_port: u16, registration: Registration, shared: Shared) {
    let client = match client() {
        Ok(client) => client,
        Err(error) => return log::error!("Can't register the node: {error:#}"),
    };
    let mut registered_before = None;
    loop {
        let outcome = register_once(&client, node, iax_port, &registration).await;
        let registered = match &outcome {
            Ok(response) if registration_accepted(response) => true,
            Ok(response) => {
                log::warn!("AllStarLink didn't accept the registration: {response}");
                false
            }
            Err(error) => {
                log::warn!("Couldn't register node {node}: {error:#}");
                false
            }
        };
        if registered_before != Some(registered) {
            if registered {
                let address = outcome.ok().and_then(|response| response.get("ipaddr").and_then(Value::as_str).map(str::to_string));
                log::info!("Registered node {node} with AllStarLink{}", address.map(|address| format!(" as {address}")).unwrap_or_default());
            }
            registered_before = Some(registered);
        }
        state::update(&shared, |snapshot| {
            if let Some(status) = snapshot.allstar.as_mut() {
                status.registered = Some(registered);
            }
        });
        sleep(Duration::from_secs(registration.interval_seconds.max(1))).await;
    }
}

// Links as AllStarLink lists them, each one mode letter and a node: T for transceive, R for monitor, C while connecting. D-STAR links ride along as T entries
fn nodes_field(snapshot: &Snapshot) -> String {
    let mut nodes: Vec<String> = snapshot
        .allstar
        .iter()
        .flat_map(|status| &status.links)
        .map(|link| format!("{}{}", if !link.connected { 'C' } else if link.mode == "monitor" { 'R' } else { 'T' }, link.node))
        .collect();
    if let Some(target) = snapshot.link.target.as_ref().filter(|_| snapshot.link.connected) {
        nodes.push(format!("T{}", target.replace(' ', "")));
    }
    nodes.join(",")
}

fn keyed_fields(snapshot: &Snapshot) -> (bool, u64) {
    let keyed = snapshot.current.is_some();
    let key_time = snapshot.current.as_ref().map(|current| state::unix_now().saturating_sub(current.started_at)).or_else(|| snapshot.heard.first().map(|heard| state::unix_now().saturating_sub(heard.started_at + heard.seconds))).unwrap_or(0);
    (keyed, key_time)
}

struct Poster {
    client: reqwest::Client,
    url: String,
    node: u32,
    sequence: u32,
    last_failed: bool,
}

impl Poster {
    async fn post(&mut self, fields: &[(&str, String)]) {
        self.sequence += 1;
        let mut query = vec![("node", self.node.to_string()), ("time", state::unix_now().to_string()), ("seqno", self.sequence.to_string())];
        query.extend(fields.iter().map(|(key, value)| (*key, value.clone())));
        let outcome = self.client.get(&self.url).query(&query).send().await.and_then(|response| response.error_for_status());
        match outcome {
            Ok(_) => self.last_failed = false,
            Err(error) if !self.last_failed => {
                log::warn!("Stats post to {} failed: {error}", self.url);
                self.last_failed = true;
            }
            Err(_) => {}
        }
    }
}

// Post the node's links and counters on a timer, and a short update whenever the receiver keys or unkeys
pub async fn statpost(node: u32, statpost: Statpost, shared: Shared) {
    let client = match client() {
        Ok(client) => client,
        Err(error) => return log::error!("Can't post stats: {error:#}"),
    };
    let mut poster = Poster { client, url: statpost.url.clone(), node, sequence: 0, last_failed: false };
    let started = Instant::now();
    let period = Duration::from_secs(statpost.interval_seconds.clamp(MIN_STATPOST_SECONDS, MAX_STATPOST_SECONDS));
    let mut next_full = Instant::now();
    let mut last_keyed = false;
    let mut check = interval(KEY_CHECK_INTERVAL);
    loop {
        check.tick().await;
        let snapshot = state::read(&shared);
        let (keyed, key_time) = keyed_fields(&snapshot);
        if Instant::now() >= next_full {
            next_full = Instant::now() + period;
            let fields = [
                ("nodes", nodes_field(&snapshot)),
                ("apprptvers", env!("CARGO_PKG_VERSION").to_string()),
                ("apprptuptime", started.elapsed().as_secs().to_string()),
                ("totalkerchunks", "0".to_string()),
                ("totalkeyups", "0".to_string()),
                ("totaltxtime", "0".to_string()),
                ("timeouts", "0".to_string()),
                ("totalexecdcommands", "0".to_string()),
                ("keyed", (keyed as u8).to_string()),
                ("keytime", key_time.to_string()),
            ];
            poster.post(&fields).await;
        } else if keyed != last_keyed {
            poster.post(&[("keyed", (keyed as u8).to_string()), ("keytime", key_time.to_string())]).await;
        }
        last_keyed = keyed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{AllstarLink, AllstarStatus, LinkStatus};

    fn link(node: &str, mode: &'static str, connected: bool) -> AllstarLink {
        AllstarLink { node: node.to_string(), mode, outbound: true, connected, keyed: false }
    }

    #[test]
    fn registration_request_matches_what_app_rpt_sends() {
        let request = registration_request(12345, 4569, "secret");
        assert_eq!(request, json!({ "port": 4569, "data": { "nodes": { "12345": { "node": "12345", "passwd": "secret", "remote": 0 } } } }));
    }

    #[test]
    fn registration_is_accepted_on_the_servers_say_so() {
        assert!(registration_accepted(&json!({ "ipaddr": "192.0.2.1", "port": 4569, "refresh": 120, "data": "Node 12345 successfully registered" })));
        assert!(!registration_accepted(&json!({ "data": "bad password" })));
        assert!(!registration_accepted(&json!({})));
    }

    #[test]
    fn stats_list_links_with_their_modes_and_the_reflector() {
        let snapshot = Snapshot {
            allstar: Some(AllstarStatus { links: vec![link("2000", "transceive", true), link("2001", "monitor", true), link("2002", "transceive", false)], ..Default::default() }),
            link: LinkStatus { target: Some("DCS999 A".to_string()), connected: true },
            ..Default::default()
        };
        assert_eq!(nodes_field(&snapshot), "T2000,R2001,C2002,TDCS999A");
    }
}
