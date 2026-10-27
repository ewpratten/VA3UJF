// Reflector directory, pulled from DStarGateway's published host file and refreshed in the background

use crate::config::{Config, Hosts, Protocol, Reflector};
use anyhow::Context;
use serde::Deserialize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::sleep;

const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_secs(3600);

pub type Directory = Arc<RwLock<Vec<Reflector>>>;

#[derive(Deserialize)]
struct Entry {
    name: String,
    reflector_type: String,
    ipv4: Option<String>,
    port: Option<u16>,
}

// The published file wraps its entries in an object, while DStarGateway's documentation shows a bare array
#[derive(Deserialize)]
#[serde(untagged)]
enum Document {
    Wrapped { reflectors: Vec<Entry> },
    Bare(Vec<Entry>),
}

// Everything the host file offers that this program can link to. D-Plus (REF) isn't spoken here, so those are left out
fn parse(text: &str) -> anyhow::Result<Vec<Reflector>> {
    let entries = match serde_json::from_str(text).context("host list isn't valid JSON")? {
        Document::Wrapped { reflectors } => reflectors,
        Document::Bare(entries) => entries,
    };
    let mut skipped_dplus = 0;
    let mut reflectors = Vec::new();
    for entry in entries {
        let protocol = match entry.reflector_type.as_str() {
            "DCS" => Protocol::Dcs,
            "XRF" => Protocol::Dextra,
            "REF" => {
                skipped_dplus += 1;
                continue;
            }
            other => {
                log::debug!("Host list: {} has unknown type {other}", entry.name);
                continue;
            }
        };
        if let Some(address) = entry.ipv4 {
            reflectors.push(Reflector { name: entry.name, protocol, address, port: entry.port });
        }
    }
    log::info!("Host list has {} linkable reflectors, and {skipped_dplus} D-Plus ones this hotspot can't link", reflectors.len());
    Ok(reflectors)
}

// Reflectors from the configuration win over the pulled list, so a local address can replace a published one
fn merge(configured: &[Reflector], pulled: Vec<Reflector>) -> Vec<Reflector> {
    let mut merged: Vec<Reflector> = pulled.into_iter().filter(|reflector| !configured.iter().any(|own| own.name.eq_ignore_ascii_case(&reflector.name))).chain(configured.iter().cloned()).collect();
    merged.sort_by(|left, right| left.name.cmp(&right.name));
    merged
}

pub fn directory(config: &Config) -> Directory {
    Arc::new(RwLock::new(merge(&config.reflectors, Vec::new())))
}

pub fn find(directory: &Directory, name: &str) -> Option<Reflector> {
    directory.read().unwrap_or_else(|poisoned| poisoned.into_inner()).iter().find(|reflector| reflector.name.eq_ignore_ascii_case(name)).cloned()
}

pub fn names(directory: &Directory) -> Vec<String> {
    directory.read().unwrap_or_else(|poisoned| poisoned.into_inner()).iter().map(|reflector| reflector.name.clone()).collect()
}

fn install(config: &Config, directory: &Directory, pulled: Vec<Reflector>) {
    *directory.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = merge(&config.reflectors, pulled);
}

async fn fetch(url: &str) -> anyhow::Result<String> {
    let client = reqwest::Client::builder().timeout(FETCH_TIMEOUT).build()?;
    Ok(client.get(url).send().await?.error_for_status()?.text().await?)
}

// Keep a copy for the next start, written through a temporary file so a crash can't leave half a list
async fn write_cache(hosts: &Hosts, text: &str) -> anyhow::Result<()> {
    if let Some(parent) = hosts.cache.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = hosts.cache.with_extension("tmp");
    tokio::fs::write(&temporary, text).await?;
    tokio::fs::rename(&temporary, &hosts.cache).await?;
    Ok(())
}

async fn refresh(config: &Config, hosts: &Hosts, directory: &Directory) -> anyhow::Result<()> {
    log::info!("Pulling host list from {}", hosts.url);
    let text = fetch(&hosts.url).await?;
    install(config, directory, parse(&text)?);
    if let Err(error) = write_cache(hosts, &text).await {
        log::warn!("Couldn't cache the host list at {}: {error:#}", hosts.cache.display());
    }
    Ok(())
}

async fn keep_fresh(config: Config, hosts: Hosts, directory: Directory, mut delay: Duration) {
    let interval = Duration::from_secs(hosts.refresh_hours.max(1) * 3600);
    loop {
        sleep(delay).await;
        delay = match refresh(&config, &hosts, &directory).await {
            Ok(()) => interval,
            Err(error) => {
                log::warn!("Couldn't refresh the host list, trying again in an hour: {error:#}");
                RETRY_DELAY
            }
        };
    }
}

// Fill the directory from the cache, or the network on a first run, then keep it fresh in the background
pub async fn start(config: &Config, directory: &Directory) {
    let Some(hosts) = config.hosts.clone() else { return };
    let cached = match tokio::fs::read_to_string(&hosts.cache).await {
        Ok(text) => parse(&text).inspect_err(|error| log::warn!("Ignoring the cached host list: {error:#}")).ok(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            log::warn!("Couldn't read the cached host list at {}: {error}", hosts.cache.display());
            None
        }
    };
    let first_delay = match cached {
        Some(pulled) => {
            log::info!("Loaded the cached host list from {}", hosts.cache.display());
            install(config, directory, pulled);
            Duration::ZERO
        }
        None => {
            if let Err(error) = refresh(config, &hosts, directory).await {
                log::warn!("Couldn't pull the host list, only configured reflectors are available: {error:#}");
            }
            Duration::from_secs(hosts.refresh_hours.max(1) * 3600)
        }
    };
    tokio::spawn(keep_fresh(config.clone(), hosts, directory.clone(), first_delay));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reflector(name: &str, address: &str) -> Reflector {
        Reflector { name: name.to_string(), protocol: Protocol::Dcs, address: address.to_string(), port: None }
    }

    #[test]
    fn parses_the_wrapped_and_bare_forms() {
        let entries = r#"[{"name":"DCS001","reflector_type":"DCS","ipv4":"dcs001.example"},{"name":"XRF757","reflector_type":"XRF","ipv4":"192.0.2.1","port":30002}]"#;
        for text in [entries.to_string(), format!(r#"{{"reflectors":{entries}}}"#)] {
            let reflectors = parse(&text).unwrap();
            assert_eq!(reflectors.len(), 2);
            assert_eq!(reflectors[1].protocol, Protocol::Dextra);
            assert_eq!(reflectors[1].port, Some(30002));
        }
    }

    #[test]
    fn leaves_out_dplus_and_entries_without_an_address() {
        let text = r#"[{"name":"REF001","reflector_type":"REF","ipv4":"192.0.2.1"},{"name":"DCS002","reflector_type":"DCS","ipv4":null},{"name":"DCS003","reflector_type":"DCS","ipv4":"192.0.2.3"}]"#;
        let reflectors = parse(text).unwrap();
        assert_eq!(reflectors.len(), 1);
        assert_eq!(reflectors[0].name, "DCS003");
    }

    #[test]
    fn configured_reflectors_override_pulled_ones() {
        let merged = merge(&[reflector("DCS999", "198.51.100.1")], vec![reflector("dcs999", "192.0.2.1"), reflector("DCS001", "192.0.2.2")]);
        assert_eq!(merged.iter().map(|reflector| reflector.address.as_str()).collect::<Vec<_>>(), ["192.0.2.2", "198.51.100.1"]);
    }
}
