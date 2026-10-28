// Answers an RF user who has finished transmitting, with a short transmission that says ACK and how well they were heard

use crate::config::{Ack, Station};
use crate::dstar::{build_slow_data_blocks, pad, Frame, Header, SILENT_AMBE};
use crate::router::{Port, Router, Scope};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};

// The frames of a transmission arrive at the pace of real voice
const FRAME_INTERVAL: Duration = Duration::from_millis(20);

// Flag that says the repeater won't relay this one on
const RELAY_UNAVAILABLE: u8 = 0x01;

pub struct AckRequest {
    // Whoever is being answered
    pub your_call: [u8; 8],
    pub ber_percent: f32,
    pub dbm: Option<f32>,
}

// What the radio shows: "ACK BER 0.3% -87dBm", cut to the 20 characters it has room for
fn ack_text(request: &AckRequest) -> String {
    match request.dbm {
        Some(dbm) => format!("ACK BER {:.1}% {dbm:.0}dBm", request.ber_percent),
        None => format!("ACK BER {:.1}%", request.ber_percent),
    }
}

async fn transmit(port: &Port, station: &Station, request: &AckRequest) {
    let header = Header {
        flags: [RELAY_UNAVAILABLE, 0, 0],
        repeater_2: pad(&format!("{:<7}G", station.callsign)),
        repeater_1: pad(&format!("{:<7}{}", station.callsign, station.module)),
        your_call: request.your_call,
        my_call: pad(&station.callsign),
        my_suffix: pad("ACK"),
    };
    let _ = port.to_router.send(Frame::Header(header)).await;
    let mut pace = interval(FRAME_INTERVAL);
    for (sequence, slow_data) in build_slow_data_blocks(&ack_text(request)).into_iter().take(20).enumerate() {
        pace.tick().await;
        let _ = port.to_router.send(Frame::Voice { sequence: sequence as u8, ambe: SILENT_AMBE, slow_data }).await;
    }
    let _ = port.to_router.send(Frame::End).await;
}

async fn run(ack: Ack, station: Station, port: Port, mut requests: mpsc::Receiver<AckRequest>) {
    while let Some(mut request) = requests.recv().await {
        // Give the user a moment to let go of the PTT, and only answer the last of anything that piles up
        sleep(Duration::from_millis(ack.delay_ms)).await;
        while let Ok(newer) = requests.try_recv() {
            request = newer;
        }
        transmit(&port, &station, &request).await;
    }
}

// Attach the acknowledger to the router as a link that's only heard on the local radio. Returns where to send requests
pub fn start(ack: Ack, station: Station, router: &mut Router) -> mpsc::Sender<AckRequest> {
    let (requests, from_requests) = mpsc::channel(4);
    let port = router.attach("ack", Scope::LocalOnly);
    tokio::spawn(run(ack, station, port, from_requests));
    requests
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::RadioTaps;
    use crate::state::Shared;

    fn station() -> Station {
        toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0").unwrap()
    }

    #[test]
    fn the_text_fits_the_radios_display() {
        assert_eq!(ack_text(&AckRequest { your_call: [b' '; 8], ber_percent: 0.34, dbm: Some(-87.4) }), "ACK BER 0.3% -87dBm");
        assert_eq!(ack_text(&AckRequest { your_call: [b' '; 8], ber_percent: 12.0, dbm: None }), "ACK BER 12.0%");
        assert!(ack_text(&AckRequest { your_call: [b' '; 8], ber_percent: 100.0, dbm: Some(-120.0) }).len() <= 22);
    }

    #[tokio::test]
    async fn the_user_is_answered_on_the_local_radio_with_text_in_the_slow_data() {
        let (mut router, mut radio) = Router::new();
        let mut link = router.attach("test", Scope::Everyone);
        let requests = start(Ack { delay_ms: 10 }, station(), &mut router);
        tokio::spawn(router.run(RadioTaps::default(), Shared::default()));

        requests.send(AckRequest { your_call: pad("N0TEST"), ber_percent: 1.5, dbm: None }).await.unwrap();
        let mut frames = Vec::new();
        while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(2), radio.from_router.recv()).await {
            let finished = frame == Frame::End;
            frames.push(frame);
            if finished {
                break;
            }
        }
        let Some(Frame::Header(header)) = frames.first() else { panic!("no header first: {frames:?}") };
        assert_eq!((header.my_call_text(), header.your_call), ("N0CALL/ACK".to_string(), pad("N0TEST")));
        assert_eq!(frames.iter().filter(|frame| matches!(frame, Frame::Voice { .. })).count(), 20);

        // The text is in the slow data, where a radio reads it
        let mut decoder = crate::slowdata::Decoder::default();
        let text = frames.iter().find_map(|frame| match frame {
            Frame::Voice { slow_data, .. } => match decoder.push(*slow_data) {
                Some(crate::slowdata::Decoded::Text(text)) if text.contains("1.5%") => Some(text),
                _ => None,
            },
            _ => None,
        });
        assert_eq!(text.as_deref(), Some("ACK BER 1.5%"));

        // And no other link hears it
        assert!(tokio::time::timeout(Duration::from_millis(300), link.from_router.recv()).await.is_err());
    }
}
