// Arbitrates the channel between RF, the reflector, and AllStar, and fans each stream out to the other two

use crate::dprs;
use crate::dstar::Frame;
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
}

pub struct Endpoints {
    pub from_modem: mpsc::Receiver<Frame>,
    pub to_modem: mpsc::Sender<Frame>,
    pub from_reflector: mpsc::Receiver<Frame>,
    pub to_reflector: mpsc::Sender<Frame>,
    pub bridge: Option<(mpsc::Receiver<Frame>, mpsc::Sender<Frame>)>,
    pub position_reports: Option<mpsc::Sender<String>>,
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
    let mut active: Option<(Source, Instant)> = None;
    let mut tick = interval(Duration::from_millis(250));
    let mut position_collector = dprs::Collector::default();

    loop {
        let (source, frame) = tokio::select! {
            Some(frame) = endpoints.from_modem.recv() => (Source::Rf, frame),
            Some(frame) = endpoints.from_reflector.recv() => (Source::Reflector, frame),
            Some(frame) = from_bridge.recv() => (Source::AllStar, frame),
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

        // Record the transmission for the web UI
        match &frame {
            Frame::Header(header) => {
                let source_name = match source { Source::Rf => "rf", Source::Reflector => "reflector", Source::AllStar => "allstar" };
                log::info!("Heard {} via {source_name}", header.my_call_text());
                state::update(&shared, |snapshot| snapshot.current = Some(Transmission { source: source_name, callsign: header.my_call_text(), started_at: state::unix_now(), seconds: 0 }));
            }
            Frame::End => state::update(&shared, state::finish_transmission),
            Frame::Voice { .. } => {}
        }

        // RF and the reflector both reach AllStar, and AllStar reaches both of them
        let bridge_target: Vec<&mpsc::Sender<Frame>> = to_bridge.iter().collect();
        match source {
            Source::Rf => deliver(&[&[&endpoints.to_reflector][..], &bridge_target[..]].concat(), &frame).await,
            Source::Reflector => deliver(&[&[&endpoints.to_modem][..], &bridge_target[..]].concat(), &frame).await,
            Source::AllStar => deliver(&[&endpoints.to_modem, &endpoints.to_reflector], &frame).await,
        }
    }
}
