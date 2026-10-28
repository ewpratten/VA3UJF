// APRS-IS client that beacons the hotspot's fixed position and gates D-PRS reports heard over RF

use crate::config::{Aprs, Station};
use crate::dprs;
use crate::state::{self, Shared};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};

const RECONNECT_DELAY: Duration = Duration::from_secs(30);

// APRS wants degrees and decimal minutes, e.g. 4324.56N
fn coordinate(value: f64, positive: char, negative: char, degree_digits: usize) -> String {
    let degrees = value.abs().trunc();
    let minutes = (value.abs() - degrees) * 60.0;
    format!("{:0width$}{:05.2}{}", degrees as u32, minutes, if value >= 0.0 { positive } else { negative }, width = degree_digits)
}

// The well-known APRS-IS passcode hash of a callsign, ignoring any SSID
pub fn passcode(callsign: &str) -> i32 {
    let mut hash: i32 = 0x73e2;
    for (index, byte) in callsign.split('-').next().unwrap_or_default().to_ascii_uppercase().bytes().enumerate() {
        hash ^= if index % 2 == 0 { (byte as i32) << 8 } else { byte as i32 };
    }
    hash & 0x7fff
}

fn beacon_text(station: &Station) -> String {
    format!("{0}>APZVA3,TCPIP*:!{1}/{2}-{0} Homebrew Repeater\r\n", station.callsign, coordinate(station.latitude, 'N', 'S', 2), coordinate(station.longitude, 'E', 'W', 3))
}

async fn session(station: &Station, aprs: &Aprs, shared: &Shared, reports: &mut mpsc::Receiver<String>) -> anyhow::Result<()> {
    let stream = TcpStream::connect(&aprs.server).await?;
    let (reader, mut writer) = stream.into_split();
    let igate = station.callsign.clone();
    let login = format!("user {igate} pass {} vers va3ujf 0.1\r\n", passcode(&station.callsign));
    writer.write_all(login.as_bytes()).await?;
    log::info!("Connected to APRS-IS at {} as {igate}", aprs.server);
    state::update(shared, |snapshot| snapshot.aprs_connected = Some(true));

    // Beacon on a timer while draining the server's lines so the connection stays healthy
    let mut lines = BufReader::new(reader).lines();
    let mut beacon_timer = interval(Duration::from_secs(aprs.beacon_minutes * 60));
    loop {
        tokio::select! {
            _ = beacon_timer.tick() => {
                log::debug!("Beaconing position");
                writer.write_all(beacon_text(station).as_bytes()).await?;
            }
            Some(report) = reports.recv() => match dprs::gate(&report, &igate) {
                Some(packet) => {
                    log::info!("Gating D-PRS: {packet}");
                    writer.write_all(format!("{packet}\r\n").as_bytes()).await?;
                }
                None => log::warn!("Dropping unusable D-PRS report: {report}"),
            },
            line = lines.next_line() => match line? {
                Some(line) => log::debug!("APRS-IS: {line}"),
                None => anyhow::bail!("server closed the connection"),
            },
        }
    }
}

pub async fn run(station: Station, aprs: Aprs, shared: Shared, mut reports: mpsc::Receiver<String>) {
    loop {
        if let Err(error) = session(&station, &aprs, &shared, &mut reports).await {
            log::warn!("APRS-IS session ended: {error:#}");
        }
        state::update(&shared, |snapshot| snapshot.aprs_connected = Some(false));
        sleep(RECONNECT_DELAY).await;

        // Reports from while the server was away are too old to gate
        while reports.try_recv().is_ok() {}
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Station;
    #[test]
    fn beacon_is_sent_as_the_bare_callsign() {
        let station: Station = toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.5\nlongitude = -90.25").unwrap();
        assert_eq!(super::beacon_text(&station), "N0CALL>APZVA3,TCPIP*:!4530.00N/09015.00W-N0CALL Homebrew Repeater\r\n");
    }

    #[test]
    fn passcode_matches_the_known_hash() {
        assert_eq!(super::passcode("N0CALL"), 13023);
        assert_eq!(super::passcode("n0call-10"), 13023);
    }
}
