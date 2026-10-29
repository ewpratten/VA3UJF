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
    pub echolink: Option<Echolink>,
    pub hosts: Option<Hosts>,
    pub speech: Option<Speech>,
    pub ack: Option<Ack>,
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

    // MMDVMHost's RSSI.dat, which turns the modem's raw signal readings into dBm
    pub rssi_file: Option<PathBuf>,
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

// An EchoLink node whose radio is the hotspot, linking to EchoLink stations and conferences and vocoding through an AMBEserver
#[derive(Deserialize, Clone)]
pub struct Echolink {
    // The node's EchoLink callsign, which ends in -L for a link and -R for a repeater
    pub callsign: String,
    pub ambeserver: String,

    // Logging in lets other stations find this node and be told who's allowed to call. Without it, only the stations listed below can link
    pub password_file: Option<PathBuf>,

    // Shown to other stations, and defaults to the station's callsign
    pub name: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_echolink_servers")]
    pub servers: Vec<String>,
    #[serde(default = "default_echolink_refresh_seconds")]
    pub refresh_seconds: u64,
    #[serde(default = "default_echolink_port")]
    pub udp_port: u16,

    // Stations reached at a fixed "host:port" instead of through the directory, with the port being their audio port
    #[serde(default)]
    pub stations: HashMap<String, String>,
    #[serde(default = "default_echolink_suffix")]
    pub suffix: String,
    pub message: Option<String>,
    #[serde(default)]
    pub rx_gain_db: f32,
    #[serde(default)]
    pub tx_gain_db: f32,
}

// Spoken announcements, made by a Piper voice running inside this program and vocoded through an AMBEserver
#[derive(Deserialize, Clone)]
pub struct Speech {
    // Defaults to the AllStar section's AMBEserver
    pub ambeserver: Option<String>,

    // Holds the voice's .onnx model, its tokens.txt, and its espeak-ng-data
    #[serde(default = "default_voice_directory")]
    pub voice_directory: PathBuf,
    #[serde(default = "default_speech_speed")]
    pub speed: f32,
    #[serde(default = "default_speech_threads")]
    pub threads: i32,
}

// Answers an RF user who has finished transmitting with a short transmission of its own: an ACK, and how well they were heard
#[derive(Deserialize, Clone)]
pub struct Ack {
    #[serde(default = "default_ack_delay_ms")]
    pub delay_ms: u64,
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

fn default_voice_directory() -> PathBuf { PathBuf::from("/usr/share/va3ujf/voice") }
fn default_speech_speed() -> f32 { 1.0 }
fn default_speech_threads() -> i32 { 4 }
fn default_iax_port() -> u16 { 4569 }
fn default_dns_domain() -> String { "nodes.allstarlink.org".to_string() }
fn default_park_reflector() -> bool { true }
fn default_registration_url() -> String { "https://register.allstarlink.org/".to_string() }
fn default_registration_interval_seconds() -> u64 { 180 }
fn default_statpost_url() -> String { "http://stats.allstarlink.org/uhandler".to_string() }
fn default_statpost_interval_seconds() -> u64 { 60 }
fn default_suffix() -> String { "ASL".to_string() }
fn default_echolink_servers() -> Vec<String> { vec!["servers.echolink.org".to_string()] }
fn default_echolink_refresh_seconds() -> u64 { 300 }
fn default_echolink_port() -> u16 { 5198 }
fn default_echolink_suffix() -> String { "ECHO".to_string() }
fn default_hosts_url() -> String { "https://raw.githubusercontent.com/g4klx/DStarGateway/master/Data/DStar_Hosts.json".to_string() }
fn default_hosts_cache() -> PathBuf { PathBuf::from("/var/lib/va3ujf/DStar_Hosts.json") }
fn default_hosts_refresh_hours() -> u64 { 72 }
fn default_ack_delay_ms() -> u64 { 750 }
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
        if config.speech.as_ref().is_some_and(|speech| speech.ambeserver.is_none()) && config.allstar.is_none() {
            anyhow::bail!("speech needs an AMBEserver to vocode through: set ambeserver in [speech], or add an [allstar] section to borrow its");
        }
        Ok(config)
    }

}

#[cfg(test)]
mod tests {
    #[test]
    fn speech_without_an_ambeserver_is_refused() {
        let path = std::env::temp_dir().join(format!("va3ujf-speech-only-{}.toml", std::process::id()));
        std::fs::write(&path, "[station]\ncallsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0\n[modem]\nport = \"/dev/null\"\n[speech]\n").unwrap();
        let error = super::Config::load(&path).err().expect("speech alone to be refused");
        std::fs::remove_file(&path).ok();
        assert!(format!("{error:#}").contains("[allstar]"), "{error:#}");
    }

    #[test]
    fn speech_can_bring_its_own_ambeserver() {
        let path = std::env::temp_dir().join(format!("va3ujf-speech-own-{}.toml", std::process::id()));
        std::fs::write(&path, "[station]\ncallsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0\n[modem]\nport = \"/dev/null\"\n[speech]\nambeserver = \"127.0.0.1:2460\"\n").unwrap();
        let loaded = super::Config::load(&path);
        std::fs::remove_file(&path).ok();
        assert!(loaded.is_ok());
    }

    #[test]
    fn example_config_parses_and_validates() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("example.toml");
        let config = super::Config::load(&path).unwrap();
        assert_eq!(config.reflectors.len(), 1);
        assert_eq!(config.hosts.unwrap().refresh_hours, 72);
        assert_eq!(config.allstar.unwrap().schedule.len(), 2);
    }
}
