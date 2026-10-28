// Station configuration, read from a single TOML file

use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Deserialize, Clone)]
pub struct Config {
    pub station: Station,
    pub modem: Modem,
    pub http_listen: Option<String>,
    pub aprs: Option<Aprs>,
    pub allstar: Option<Allstar>,
    pub hosts: Option<Hosts>,
    #[serde(default, rename = "reflector")]
    pub reflectors: Vec<Reflector>,
    pub startup_reflector: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct Station {
    pub callsign: String,
    pub module: String,
    pub frequency_mhz: f64,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Deserialize, Clone)]
pub struct Modem {
    pub port: String,
    #[serde(default = "default_baud")]
    pub baud: u32,
    #[serde(default)]
    pub tx_invert: bool,
    #[serde(default)]
    pub rx_invert: bool,
    #[serde(default)]
    pub ptt_invert: bool,
    #[serde(default = "default_tx_delay_ms")]
    pub tx_delay_ms: u32,
    #[serde(default = "default_level")]
    pub rx_level: f32,
    #[serde(default = "default_level")]
    pub tx_level: f32,
    #[serde(default = "default_level")]
    pub rf_level: f32,
}

#[derive(Deserialize, Clone)]
pub struct Aprs {
    pub server: String,
    #[serde(default = "default_beacon_minutes")]
    pub beacon_minutes: u64,
}

// An AllStar node whose radio is the hotspot, linking to other nodes over IAX2 and vocoding through an AMBEserver
#[derive(Deserialize, Clone)]
pub struct Allstar {
    pub node: u32,
    pub ambeserver: String,
    #[serde(default, rename = "schedule")]
    pub schedule: Vec<ScheduleEntry>,
    #[serde(default = "default_iax_port")]
    pub iax_port: u16,
    #[serde(default = "default_dns_domain")]
    pub dns_domain: String,

    // Nodes reached at a fixed "host:port" instead of through DNS
    #[serde(default)]
    pub nodes: HashMap<String, String>,
    pub registration: Option<Registration>,
    pub statpost: Option<Statpost>,

    // Step off the reflector while AllStar has links, and go back once they're gone
    #[serde(default = "default_park_reflector")]
    pub park_reflector: bool,
    #[serde(default = "default_suffix")]
    pub suffix: String,
    pub message: Option<String>,
    #[serde(default)]
    pub rx_gain_db: f32,
    #[serde(default)]
    pub tx_gain_db: f32,
}

// Lets other nodes find this one. The node's password is read from a file, to keep it out of the configuration
#[derive(Deserialize, Clone)]
pub struct Registration {
    pub password_file: PathBuf,
    #[serde(default = "default_registration_url")]
    pub url: String,
    #[serde(default = "default_registration_interval_seconds")]
    pub interval_seconds: u64,
}

// Posts the node's links to AllStarLink's stats
#[derive(Deserialize, Clone)]
pub struct Statpost {
    #[serde(default = "default_statpost_url")]
    pub url: String,
    #[serde(default = "default_statpost_interval_seconds")]
    pub interval_seconds: u64,
}

// Links or unlinks AllStar nodes at cron times in the host's timezone, running its actions in order
#[derive(Deserialize, Clone)]
pub struct ScheduleEntry {
    pub name: String,
    pub cron: String,
    pub actions: Vec<Action>,
}

// Exactly one of link or unlink. Unlink takes "all" to drop everything
#[derive(Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub link: Option<String>,
    pub unlink: Option<String>,
    #[serde(default)]
    pub permanent: bool,
    #[serde(default)]
    pub monitor: bool,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Dcs,
    Dextra,
}

#[derive(Deserialize, Clone)]
pub struct Reflector {
    pub name: String,
    pub protocol: Protocol,
    pub address: String,
    pub port: Option<u16>,
}

// Pulls the published reflector list, which the configured reflectors then override
#[derive(Deserialize, Clone)]
pub struct Hosts {
    #[serde(default = "default_hosts_url")]
    pub url: String,
    #[serde(default = "default_hosts_cache")]
    pub cache: PathBuf,
    #[serde(default = "default_hosts_refresh_hours")]
    pub refresh_hours: u64,
}

fn default_iax_port() -> u16 { 4569 }
fn default_dns_domain() -> String { "nodes.allstarlink.org".to_string() }
fn default_park_reflector() -> bool { true }
fn default_registration_url() -> String { "https://register.allstarlink.org/".to_string() }
fn default_registration_interval_seconds() -> u64 { 180 }
fn default_statpost_url() -> String { "http://stats.allstarlink.org/uhandler".to_string() }
fn default_statpost_interval_seconds() -> u64 { 60 }
fn default_suffix() -> String { "ASL".to_string() }
fn default_hosts_url() -> String { "https://raw.githubusercontent.com/g4klx/DStarGateway/master/Data/DStar_Hosts.json".to_string() }
fn default_hosts_cache() -> PathBuf { PathBuf::from("/var/lib/va3ujf/DStar_Hosts.json") }
fn default_hosts_refresh_hours() -> u64 { 72 }
fn default_baud() -> u32 { 115200 }
fn default_tx_delay_ms() -> u32 { 100 }
fn default_level() -> f32 { 50.0 }
fn default_beacon_minutes() -> u64 { 20 }

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&text)?;
        if let Some(allstar) = &config.allstar {
            crate::allstar::validate_schedule(&allstar.schedule)?;
        }
        Ok(config)
    }

}

#[cfg(test)]
mod tests {
    #[test]
    fn example_config_parses_and_validates() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("example.toml");
        let config = super::Config::load(&path).unwrap();
        assert_eq!(config.reflectors.len(), 1);
        assert_eq!(config.hosts.unwrap().refresh_hours, 72);
        assert_eq!(config.allstar.unwrap().schedule.len(), 2);
    }
}
