// The EchoLink directory: logging in so other stations can find this node, and finding the stations this node talks to

use crate::config::Echolink;
use anyhow::{bail, Context};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};
use tokio::time::timeout;

const DIRECTORY_PORT: u16 = 5200;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_REFRESH_GAP: Duration = Duration::from_secs(30);
pub const STANDARD_AUDIO_PORT: u16 = 5198;
const MAX_DESCRIPTION_LENGTH: usize = 27;

#[derive(Clone, Debug, PartialEq)]
pub struct Station {
    pub callsign: String,
    pub id: u32,
    pub address: Ipv4Addr,
}

#[derive(Default)]
struct Cache {
    fetched: Option<Instant>,
    by_callsign: HashMap<String, Station>,
    by_id: HashMap<u32, String>,
}

pub struct Directory {
    callsign: String,
    description: String,
    servers: Vec<String>,
    password_file: Option<std::path::PathBuf>,
    configured: HashMap<String, String>,
    cache: Mutex<Cache>,
}

pub fn is_station_id(text: &str) -> bool {
    !text.is_empty() && text.len() <= 9 && text.bytes().all(|byte| byte.is_ascii_digit())
}

// What to log in with: the callsign and password, then whether the node is free to be called, when that was said, and what the node is
fn login_message(callsign: &str, password: &str, busy: bool, time: &str, description: &str) -> Vec<u8> {
    let status = if busy { "BUSY3.40" } else { "ONLINE3.38" };
    let description: String = description.chars().take(MAX_DESCRIPTION_LENGTH).collect();
    let mut message = format!("l{}", callsign.to_uppercase()).into_bytes();
    message.extend([0xAC, 0xAC]);
    message.extend(format!("{password}\r{status}({time})\r{description}\r").into_bytes());
    message
}

// The directory answers a station list request with @@@, a count, four lines per station, and +++
fn parse_stations(reply: &[u8]) -> anyhow::Result<Vec<Station>> {
    let text = String::from_utf8_lossy(reply);
    let mut lines = text.lines().map(|line| line.trim_end_matches('\r'));
    if lines.next() != Some("@@@") {
        bail!("the station list doesn't start with @@@");
    }
    lines.next();
    let mut stations = Vec::new();
    while let Some(callsign) = lines.next() {
        if callsign.starts_with("+++") {
            return Ok(stations);
        }
        let (Some(data), Some(id), Some(address)) = (lines.next(), lines.next(), lines.next()) else { break };
        if callsign == " " && data.starts_with("INCORRECT PASSWORD") {
            bail!("the directory says the password is incorrect");
        }
        if let (Ok(id), Ok(address)) = (id.trim().parse(), address.trim().parse()) {
            if !matches!(callsign, "." | " ") {
                stations.push(Station { callsign: callsign.trim().to_uppercase(), id, address });
            }
        }
    }
    bail!("the station list doesn't end with +++")
}

// A reply is whole once it says OK or closes the list
fn reply_complete(reply: &[u8]) -> bool {
    reply == b"OK" || reply.ends_with(b"+++")
}

impl Directory {
    pub fn new(echolink: &Echolink) -> Self {
        let configured = echolink.stations.iter().map(|(callsign, address)| (callsign.to_uppercase(), address.clone())).collect();
        Directory { callsign: echolink.callsign.clone(), description: echolink.description.clone(), servers: echolink.servers.clone(), password_file: echolink.password_file.clone(), configured, cache: Mutex::new(Cache::default()) }
    }

    pub fn can_log_in(&self) -> bool {
        self.password_file.is_some()
    }

    // Ask every address of every directory server in turn until one answers
    async fn exchange(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut last_error = anyhow::anyhow!("no directory servers are configured");
        for server in &self.servers {
            let addresses = match lookup_host((server.as_str(), DIRECTORY_PORT)).await {
                Ok(addresses) => addresses.filter(SocketAddr::is_ipv4).collect::<Vec<_>>(),
                Err(error) => {
                    last_error = anyhow::anyhow!("couldn't look up {server}: {error}");
                    continue;
                }
            };
            for address in addresses {
                match timeout(EXCHANGE_TIMEOUT, Self::exchange_with(address, request)).await {
                    Ok(Ok(reply)) => return Ok(reply),
                    Ok(Err(error)) => last_error = error.context(format!("directory server {address}")),
                    Err(_) => last_error = anyhow::anyhow!("directory server {address} took too long"),
                }
            }
        }
        Err(last_error)
    }

    async fn exchange_with(address: SocketAddr, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(address)).await.context("connecting timed out")??;
        stream.write_all(request).await?;
        let mut reply = Vec::new();
        let mut chunk = [0u8; 8192];
        while !reply_complete(&reply) {
            match stream.read(&mut chunk).await? {
                0 => break,
                length => reply.extend(&chunk[..length]),
            }
        }
        Ok(reply)
    }

    // Tell the directory the node is here, and whether it's free for a call
    pub async fn log_in(&self, busy: bool) -> anyhow::Result<()> {
        let Some(password_file) = &self.password_file else { bail!("no password is configured") };
        let password = tokio::fs::read_to_string(password_file).await.with_context(|| format!("couldn't read {}", password_file.display()))?;
        let time = chrono::Local::now().format("%H:%M").to_string();
        let reply = self.exchange(&login_message(&self.callsign, password.trim(), busy, &time, &self.description)).await?;
        if reply != b"OK" {
            bail!("the directory replied {:?}", String::from_utf8_lossy(&reply).trim());
        }
        Ok(())
    }

    // Replace what's known of the stations with the directory's list of who is online
    pub async fn refresh(&self) -> anyhow::Result<usize> {
        let stations = parse_stations(&self.exchange(b"s").await?)?;
        let count = stations.len();
        let mut cache = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.by_id = stations.iter().map(|station| (station.id, station.callsign.clone())).collect();
        cache.by_callsign = stations.into_iter().map(|station| (station.callsign.clone(), station)).collect();
        cache.fetched = Some(Instant::now());
        Ok(count)
    }

    fn cached(&self, target: &str) -> Option<Station> {
        let cache = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let callsign = if is_station_id(target) { cache.by_id.get(&target.parse().ok()?)?.clone() } else { target.to_uppercase() };
        cache.by_callsign.get(&callsign).cloned()
    }

    // A list this old might not have a station that just came online, so a miss earns a fresh one, within reason
    async fn lookup(&self, target: &str) -> Option<Station> {
        if let Some(station) = self.cached(target) {
            return Some(station);
        }
        let stale = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).fetched.is_none_or(|fetched| fetched.elapsed() > MIN_REFRESH_GAP);
        if stale && self.can_log_in() && self.refresh().await.is_ok() {
            return self.cached(target);
        }
        None
    }

    async fn configured_address(&self, callsign: &str) -> Option<SocketAddr> {
        let address = self.configured.get(&callsign.to_uppercase())?;
        let with_port = if address.contains(':') { address.clone() } else { format!("{address}:{STANDARD_AUDIO_PORT}") };
        lookup_host(with_port).await.ok()?.find(SocketAddr::is_ipv4)
    }

    // Where a station's audio goes, from a callsign or the numeric ID the directory gave it, along with the callsign
    pub async fn find(&self, target: &str) -> anyhow::Result<(String, SocketAddr)> {
        if let Some(address) = self.configured_address(target).await {
            return Ok((target.to_uppercase(), address));
        }
        let station = self.lookup(target).await.with_context(|| format!("{target:?} isn't a station the directory knows"))?;
        Ok((station.callsign, SocketAddr::new(IpAddr::V4(station.address), STANDARD_AUDIO_PORT)))
    }

    // Stations only get through when they're where the directory says they are. That gives where to send their audio
    pub async fn verify(&self, callsign: &str, address: IpAddr) -> Result<SocketAddr, String> {
        if let Some(configured) = self.configured_address(callsign).await {
            return if configured.ip() == address { Ok(configured) } else { Err(format!("{address} isn't where {callsign} is configured")) };
        }
        match self.lookup(callsign).await {
            Some(station) if IpAddr::V4(station.address) == address => Ok(SocketAddr::new(address, STANDARD_AUDIO_PORT)),
            Some(_) => Err(format!("{address} isn't where {callsign} is listed")),
            None => Err(format!("{callsign} isn't listed in the directory")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_in_sends_the_callsign_password_status_and_description() {
        let message = login_message("n0call-l", "secret", false, "09:30", "Test node");
        assert_eq!(message, b"lN0CALL-L\xAC\xACsecret\rONLINE3.38(09:30)\rTest node\r");
        assert_eq!(&login_message("N0CALL-L", "secret", true, "09:30", "")[17..], b"\rBUSY3.40(09:30)\r\r");
    }

    #[test]
    fn a_station_list_is_read_into_stations() {
        let reply = b"@@@\n3\nW1AW-R\nARRL Newington[ON 12:00]\n12345\n192.0.2.1\n.\n\n0\n0.0.0.0\nN0CALL\nTest[BUSY 09:00]\n 678\n192.0.2.2\n+++";
        assert_eq!(parse_stations(reply).unwrap(), vec![Station { callsign: "W1AW-R".into(), id: 12345, address: "192.0.2.1".parse().unwrap() }, Station { callsign: "N0CALL".into(), id: 678, address: "192.0.2.2".parse().unwrap() }]);
    }

    #[test]
    fn a_bad_password_or_a_cut_off_list_is_an_error() {
        assert!(parse_stations(b"@@@\n1\n \nINCORRECT PASSWORD\n0\n0.0.0.0\n+++").is_err());
        assert!(parse_stations(b"@@@\n2\nW1AW-R\nARRL\n1\n192.0.2.1\n").is_err());
        assert!(parse_stations(b"hello").is_err());
    }

    #[test]
    fn stations_are_named_by_callsign_or_numeric_id() {
        assert!(is_station_id("12345"));
        assert!(!is_station_id("W1AW-R"));
        assert!(!is_station_id(""));
    }

    #[tokio::test]
    async fn configured_stations_skip_the_directory() {
        let echolink: Echolink = toml::from_str("callsign = \"N0CALL-L\"\nambeserver = \"127.0.0.1:2460\"\n[stations]\n\"W1AW-R\" = \"127.0.0.1:25198\"\n").unwrap();
        let directory = Directory::new(&echolink);
        assert_eq!(directory.find("w1aw-r").await.unwrap(), ("W1AW-R".to_string(), "127.0.0.1:25198".parse().unwrap()));
        assert!(directory.find("N0BODY").await.is_err());
        assert_eq!(directory.verify("W1AW-R", "127.0.0.1".parse().unwrap()).await, Ok("127.0.0.1:25198".parse().unwrap()));
        assert!(directory.verify("W1AW-R", "192.0.2.9".parse().unwrap()).await.is_err());
        assert!(directory.verify("N0BODY", "127.0.0.1".parse().unwrap()).await.is_err());
    }
}
