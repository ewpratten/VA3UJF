// Station configuration, read from a single TOML file

use serde::Deserialize;
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
    #[serde(default = "default_ssid")]
    pub ssid: u8,
    #[serde(default = "default_beacon_minutes")]
    pub beacon_minutes: u64,
}

// Bridge between the hotspot and an AllStar node's USRP channel, vocoding through an AMBEserver
#[derive(Deserialize, Clone)]
pub struct Allstar {
    pub node: u32,
    pub ambeserver: String,
    #[serde(default, rename = "schedule")]
    pub schedule: Vec<ScheduleEntry>,
    #[serde(default = "default_usrp_address")]
    pub usrp_address: String,
    #[serde(default = "default_usrp_listen_port")]
    pub usrp_listen_port: u16,
    #[serde(default = "default_usrp_send_port")]
    pub usrp_send_port: u16,
    #[serde(default = "default_suffix")]
    pub suffix: String,
    pub message: Option<String>,
    #[serde(default)]
    pub rx_gain_db: f32,
    #[serde(default)]
    pub tx_gain_db: f32,
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
    #[serde(default)]
    pub echolink: bool,
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

fn default_usrp_address() -> String { "127.0.0.1".to_string() }
fn default_usrp_listen_port() -> u16 { 34001 }
fn default_usrp_send_port() -> u16 { 32001 }
fn default_suffix() -> String { "ASL".to_string() }
fn default_hosts_url() -> String { "https://raw.githubusercontent.com/g4klx/DStarGateway/master/Data/DStar_Hosts.json".to_string() }
fn default_hosts_cache() -> PathBuf { PathBuf::from("/var/lib/va3ujf/DStar_Hosts.json") }
fn default_hosts_refresh_hours() -> u64 { 72 }
fn default_baud() -> u32 { 115200 }
fn default_tx_delay_ms() -> u32 { 100 }
fn default_level() -> f32 { 50.0 }
fn default_ssid() -> u8 { 10 }
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
