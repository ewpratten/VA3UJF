// Arbitrates the channel between RF, the reflector, AllStar, and announcements, and fans each stream out to where it belongs

use crate::dprs;
use crate::dstar::{pad, Frame, Header};
use crate::speech::{Announcement, Speaker};
use crate::state::{self, Shared, Transmission};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

// Release the channel if a stream goes quiet without an end marker
const STREAM_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, PartialEq, Debug)]
enum Source {
    Rf,
    Reflector,
    AllStar,
    Announcement,
}

// Sent as the "your call" of a transmission, this asks the repeater to describe itself
fn is_info_request(header: &Header) -> bool {
    header.your_call == pad::<8>("       I")
}

pub struct Endpoints {
    pub from_modem: mpsc::Receiver<Frame>,
    pub to_modem: mpsc::Sender<Frame>,
    pub from_reflector: mpsc::Receiver<Frame>,
    pub to_reflector: mpsc::Sender<Frame>,
    pub bridge: Option<(mpsc::Receiver<Frame>, mpsc::Sender<Frame>)>,
    pub position_reports: Option<mpsc::Sender<String>>,
    pub announcements: Option<mpsc::Receiver<Frame>>,
    pub speaker: Option<Speaker>,
}

async fn deliver(targets: &[&mpsc::Sender<Frame>], frame: &Frame) {
    for target in targets {
        let _ = target.send(frame.clone()).await;
    }
}

pub async fn run(mut endpoints: Endpoints, shared: Shared) {
    let (mut from_bridge, to_bridge) = match endpoints.bridge.take() {
        Some((receiver, sender)) => (receiver, Some(sender)),
        None => (mpsc::channel(1).1, None),
    };
    let mut from_announcer = endpoints.announcements.take().unwrap_or_else(|| mpsc::channel(1).1);
    let mut active: Option<(Source, Instant)> = None;

    // Whether the stream being passed along is a request to the repeater, which goes no further than it
    let mut request_stream = false;
    let mut tick = interval(Duration::from_millis(250));
    let mut position_collector = dprs::Collector::default();

    loop {
        let (source, frame) = tokio::select! {
            Some(frame) = endpoints.from_modem.recv() => (Source::Rf, frame),
            Some(frame) = endpoints.from_reflector.recv() => (Source::Reflector, frame),
            Some(frame) = from_bridge.recv() => (Source::AllStar, frame),
            Some(frame) = from_announcer.recv() => (Source::Announcement, frame),
            _ = tick.tick() => {
                if active.is_some_and(|(_, heard)| heard.elapsed() > STREAM_TIMEOUT) {
                    active = None;
                    state::update(&shared, state::finish_transmission);
                }
                continue;
            }
            else => return,
        };

        // Whoever keys up first holds the channel until they finish
        let holds_channel = match (&frame, active) {
            (Frame::Header(_), None) => true,
            (_, Some((holder, _))) => holder == source,
            _ => false,
        };
        if !holds_channel {
            continue;
        }
        active = if matches!(frame, Frame::End) { None } else { Some((source, Instant::now())) };

        // Position reports ride in the slow data of what's heard directly over RF
        if source == Source::Rf {
            match &frame {
                Frame::Header(_) | Frame::End => position_collector.reset(),
                Frame::Voice { slow_data, .. } => {
                    if let (Some(report), Some(reports)) = (position_collector.push(*slow_data), &endpoints.position_reports)
                        && reports.try_send(report).is_err()
                    {
                        log::warn!("APRS-IS isn't keeping up, dropping a position report");
                    }
                }
            }
        }

        // An info request over RF is answered out loud, once whoever sent it has finished transmitting
        if let Frame::Header(header) = &frame {
            request_stream = source == Source::Rf && is_info_request(header);
            if request_stream {
                log::info!("{} asked for repeater info", header.my_call_text());
                if let Some(speaker) = &endpoints.speaker {
                    let _ = speaker.try_send(Announcement::Status);
                }
            }
        }

        // Record the transmission for the web UI
        match &frame {
            Frame::Header(header) => {
                let source_name = match source { Source::Rf => "rf", Source::Reflector => "reflector", Source::AllStar => "allstar", Source::Announcement => "announcement" };
                log::info!("Heard {} via {source_name}", header.my_call_text());
                state::update(&shared, |snapshot| snapshot.current = Some(Transmission { source: source_name, callsign: header.my_call_text(), started_at: state::unix_now(), seconds: 0 }));
            }
            Frame::End => state::update(&shared, state::finish_transmission),
            Frame::Voice { .. } => {}
        }

        if request_stream {
            continue;
        }

        // RF and the reflector both reach AllStar, and AllStar reaches both of them. Announcements are only for the local radio
        let bridge_target: Vec<&mpsc::Sender<Frame>> = to_bridge.iter().collect();
        match source {
            Source::Rf => deliver(&[&[&endpoints.to_reflector][..], &bridge_target[..]].concat(), &frame).await,
            Source::Reflector => deliver(&[&[&endpoints.to_modem][..], &bridge_target[..]].concat(), &frame).await,
            Source::AllStar => deliver(&[&endpoints.to_modem, &endpoints.to_reflector], &frame).await,
            Source::Announcement => deliver(&[&endpoints.to_modem], &frame).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn header(your_call: &str) -> Header {
        Header { flags: [0; 3], repeater_2: pad("N0CALL G"), repeater_1: pad("N0CALL B"), your_call: pad(your_call), my_call: pad("N0TEST"), my_suffix: pad("TEST") }
    }

    async fn transmit(frames: &mpsc::Sender<Frame>, your_call: &str) {
        frames.send(Frame::Header(header(your_call))).await.unwrap();
        frames.send(Frame::Voice { sequence: 0, ambe: [1; 9], slow_data: [2; 3] }).await.unwrap();
        frames.send(Frame::End).await.unwrap();
    }

    #[tokio::test]
    async fn an_info_request_is_answered_aloud_and_goes_no_further() {
        let (from_radio, from_modem) = mpsc::channel(16);
        let (to_modem, _modem_receiver) = mpsc::channel(16);
        let (_reflector_sender, from_reflector) = mpsc::channel(16);
        let (to_reflector, mut reflector_receiver) = mpsc::channel(16);
        let (speaker, mut announcements) = mpsc::channel(4);
        let endpoints = Endpoints { from_modem, to_modem, from_reflector, to_reflector, bridge: None, position_reports: None, announcements: None, speaker: Some(speaker) };
        tokio::spawn(run(endpoints, Shared::default()));

        // The request is spoken back, and none of it reaches the reflector
        transmit(&from_radio, "       I").await;
        assert_eq!(announcements.recv().await, Some(Announcement::Status));
        assert!(tokio::time::timeout(Duration::from_millis(300), reflector_receiver.recv()).await.is_err(), "the request leaked to the reflector");

        // An ordinary transmission right after it still does
        transmit(&from_radio, "CQCQCQ").await;
        assert!(matches!(tokio::time::timeout(Duration::from_secs(2), reflector_receiver.recv()).await, Ok(Some(Frame::Header(_)))));
        assert!(announcements.try_recv().is_err(), "an ordinary transmission was taken for an info request");
    }
}
