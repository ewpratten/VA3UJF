// Prometheus metrics, scraped from the web server. What's true right now is read from the status as it's scraped, and what has happened is counted from the events

use crate::live::Event;
use crate::state::{self, Shared, Snapshot};
use prometheus::{Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

// For the metrics with a value or nothing, rather than one per label
const NO_LABELS: [&str; 0] = [];

// Where the sources of voice are named in the metrics, so each has a count from the first scrape
const SOURCES: [&str; 4] = ["rf", "reflector", "allstar", "echolink"];

// Bit error rates worth telling apart, as percentages
const BER_BUCKETS: [f64; 8] = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0];

pub struct Metrics {
    registry: Registry,
    modem_connected: IntGauge,
    transmitting: IntGauge,
    peers: IntGauge,
    peers_talking: IntGauge,

    // These are only given a value if the feature is set up, so they're absent otherwise
    aprs_connected: IntGaugeVec,
    allstar_registered: IntGaugeVec,
    echolink_logged_in: IntGaugeVec,
    vocoder_connected: IntGaugeVec,
    link_connected: IntGaugeVec,
    link_peers: IntGaugeVec,
    transmissions: IntCounterVec,
    transmission_seconds: IntCounterVec,
    rf_ber_percent: Histogram,
    rf_signal_dbm: GaugeVec,
    rf_signal_raw: IntGaugeVec,
}

fn register<T: prometheus::core::Collector + Clone + 'static>(registry: &Registry, metric: T) -> T {
    registry.register(Box::new(metric.clone())).expect("metric names are unique");
    metric
}

fn gauge_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    register(registry, IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric"))
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let build_info = gauge_vec(&registry, "va3ujf_build_info", "The version of the controller that's running", &["version"]);
        build_info.with_label_values(&[env!("CARGO_PKG_VERSION")]).set(1);
        let start_time = register(&registry, Gauge::new("va3ujf_start_time_seconds", "When the controller started, in Unix time").expect("valid metric"));
        start_time.set(state::unix_now() as f64);

        let transmissions = register(&registry, IntCounterVec::new(Opts::new("va3ujf_transmissions_total", "Transmissions heard, by where they came from"), &["source"]).expect("valid metric"));
        let transmission_seconds = register(&registry, IntCounterVec::new(Opts::new("va3ujf_transmission_seconds_total", "Seconds of transmissions heard, by where they came from"), &["source"]).expect("valid metric"));
        for source in SOURCES {
            transmissions.with_label_values(&[source]);
            transmission_seconds.with_label_values(&[source]);
        }
        Metrics {
            modem_connected: register(&registry, IntGauge::new("va3ujf_modem_connected", "Whether the modem is answering").expect("valid metric")),
            transmitting: register(&registry, IntGauge::new("va3ujf_transmitting", "Whether someone is on the air right now").expect("valid metric")),
            peers: register(&registry, IntGauge::new("va3ujf_peers", "Stations and nodes known").expect("valid metric")),
            peers_talking: register(&registry, IntGauge::new("va3ujf_peers_talking", "Stations and nodes talking right now").expect("valid metric")),
            aprs_connected: gauge_vec(&registry, "va3ujf_aprs_connected", "Whether APRS-IS is connected", &[]),
            allstar_registered: gauge_vec(&registry, "va3ujf_allstar_registered", "Whether the AllStar node is registered with AllStarLink", &[]),
            echolink_logged_in: gauge_vec(&registry, "va3ujf_echolink_logged_in", "Whether the EchoLink node is logged in to the directory", &[]),
            vocoder_connected: gauge_vec(&registry, "va3ujf_vocoder_connected", "Whether each AMBEserver session is up", &["name"]),
            link_connected: gauge_vec(&registry, "va3ujf_link_connected", "Whether each link is connected", &["link", "kind"]),
            link_peers: gauge_vec(&registry, "va3ujf_link_peers", "Peers connected through each link", &["link", "kind"]),
            transmissions,
            transmission_seconds,
            rf_ber_percent: register(&registry, Histogram::with_opts(HistogramOpts::new("va3ujf_rf_ber_percent", "Bit error rate of each transmission heard over RF, in percent").buckets(BER_BUCKETS.to_vec())).expect("valid metric")),
            rf_signal_dbm: register(&registry, GaugeVec::new(Opts::new("va3ujf_rf_signal_dbm", "The latest signal reading from the modem, when it's calibrated to dBm"), &[]).expect("valid metric")),
            rf_signal_raw: gauge_vec(&registry, "va3ujf_rf_signal_raw", "The latest raw signal reading from the modem", &[]),
            registry,
        }
    }

    // Count what happened
    pub fn record(&self, event: &Event) {
        match event {
            Event::Transmission { source, seconds, .. } => {
                self.transmissions.with_label_values(&[source]).inc();
                self.transmission_seconds.with_label_values(&[source]).inc_by(*seconds);
            }
            Event::Quality { ber_percent, .. } => self.rf_ber_percent.observe(*ber_percent as f64),
            Event::Signal { raw, dbm } => {
                self.rf_signal_raw.with_label_values(&NO_LABELS).set(*raw as i64);
                if let Some(dbm) = dbm {
                    self.rf_signal_dbm.with_label_values(&NO_LABELS).set(*dbm as f64);
                }
            }
            Event::Level { .. } | Event::SlowData { .. } | Event::Position { .. } => {}
        }
    }

    // Take what's true right now from the status
    pub fn update(&self, snapshot: &Snapshot) {
        self.modem_connected.set(snapshot.modem_connected as i64);
        self.transmitting.set(snapshot.current.is_some() as i64);
        self.peers.set(snapshot.peers.len() as i64);
        self.peers_talking.set(snapshot.peers.iter().filter(|peer| peer.talking).count() as i64);
        if let Some(connected) = snapshot.aprs_connected {
            self.aprs_connected.with_label_values(&NO_LABELS).set(connected as i64);
        }
        if let Some(registered) = snapshot.allstar.as_ref().and_then(|allstar| allstar.registered) {
            self.allstar_registered.with_label_values(&NO_LABELS).set(registered as i64);
        }
        if let Some(logged_in) = snapshot.echolink.as_ref().and_then(|echolink| echolink.logged_in) {
            self.echolink_logged_in.with_label_values(&NO_LABELS).set(logged_in as i64);
        }
        for vocoder in &snapshot.vocoders {
            self.vocoder_connected.with_label_values(&[vocoder.name]).set(vocoder.connected as i64);
        }
        for link in &snapshot.links {
            self.link_connected.with_label_values(&[link.name, link.kind]).set(link.connected as i64);
            self.link_peers.with_label_values(&[link.name, link.kind]).set(link.peers.iter().filter(|peer| peer.connected).count() as i64);
        }
    }

    pub fn encode(&self) -> String {
        let mut text = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut text).expect("metrics encode as text");
        String::from_utf8(text).expect("the text format is UTF-8")
    }
}

// Start counting events as they happen
pub fn start(shared: &Shared) -> Arc<Metrics> {
    let metrics = Arc::new(Metrics::new());
    let mut events = shared.subscribe_events();
    let counting = metrics.clone();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => counting.record(&event),
                Err(RecvError::Lagged(missed)) => log::warn!("Metrics missed {missed} events"),
                Err(RecvError::Closed) => return,
            }
        }
    });
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{LinkStatus, PeerStatus};

    fn lines(metrics: &Metrics) -> Vec<String> {
        metrics.encode().lines().filter(|line| !line.starts_with('#')).map(str::to_string).collect()
    }

    #[test]
    fn every_source_has_a_count_from_the_start_and_features_that_are_off_are_absent() {
        let metrics = Metrics::new();
        let lines = lines(&metrics);
        assert!(lines.contains(&"va3ujf_transmissions_total{source=\"rf\"} 0".to_string()), "{lines:?}");
        assert!(lines.contains(&"va3ujf_modem_connected 0".to_string()));
        assert!(!lines.iter().any(|line| line.starts_with("va3ujf_aprs_connected") || line.starts_with("va3ujf_allstar_registered") || line.starts_with("va3ujf_rf_signal")), "{lines:?}");
    }

    #[test]
    fn events_are_counted() {
        let metrics = Metrics::new();
        metrics.record(&Event::Transmission { source: "rf", callsign: "N0TEST".into(), started_at: 0, seconds: 12 });
        metrics.record(&Event::Transmission { source: "rf", callsign: "N0TEST".into(), started_at: 0, seconds: 8 });
        metrics.record(&Event::Transmission { source: "allstar", callsign: "54321".into(), started_at: 0, seconds: 3 });
        metrics.record(&Event::Quality { callsign: "N0TEST".into(), ber_percent: 0.3, dbm: None, seconds: 12.0 });
        metrics.record(&Event::Quality { callsign: "N0TEST".into(), ber_percent: 7.0, dbm: None, seconds: 8.0 });
        metrics.record(&Event::Signal { raw: 1100, dbm: Some(-95.0) });
        let lines = lines(&metrics);
        for expected in [
            "va3ujf_transmissions_total{source=\"rf\"} 2",
            "va3ujf_transmission_seconds_total{source=\"rf\"} 20",
            "va3ujf_transmission_seconds_total{source=\"allstar\"} 3",
            "va3ujf_rf_ber_percent_bucket{le=\"0.5\"} 1",
            "va3ujf_rf_ber_percent_bucket{le=\"10\"} 2",
            "va3ujf_rf_ber_percent_count 2",
            "va3ujf_rf_signal_raw 1100",
            "va3ujf_rf_signal_dbm -95",
        ] {
            assert!(lines.contains(&expected.to_string()), "missing {expected}\n{lines:#?}");
        }
    }

    #[test]
    fn the_status_is_read_as_it_is_scraped() {
        let metrics = Metrics::new();
        let mut snapshot = Snapshot { modem_connected: true, aprs_connected: Some(false), ..Default::default() };
        let mut allstar = LinkStatus::new("allstar", "allstar");
        allstar.connected = true;
        allstar.peers = vec![
            PeerStatus { id: "54321".into(), mode: "transceive", outbound: true, connected: true, keyed: false, talking: false },
            PeerStatus { id: "54322".into(), mode: "transceive", outbound: true, connected: false, keyed: false, talking: false },
        ];
        snapshot.links = vec![allstar];
        metrics.update(&snapshot);
        let lines = lines(&metrics);
        for expected in ["va3ujf_modem_connected 1", "va3ujf_aprs_connected 0", "va3ujf_link_connected{kind=\"allstar\",link=\"allstar\"} 1", "va3ujf_link_peers{kind=\"allstar\",link=\"allstar\"} 1"] {
            assert!(lines.contains(&expected.to_string()), "missing {expected}\n{lines:#?}");
        }
    }
}
