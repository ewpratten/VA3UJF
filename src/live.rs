// Things that happen too fast, or matter too briefly, to keep in the status snapshot. These are streamed to the web UI as they occur

use serde::Serialize;

#[derive(Clone, Serialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    // How strongly the radio is hearing whoever is transmitting. The dBm needs the modem's RSSI calibration
    Signal { raw: u16, dbm: Option<f32> },

    // How loud the voice being heard is, as dB below the loudest a sample can be. These come a few times a second while someone talks
    Level { source: &'static str, callsign: String, dbfs: f32 },

    // A piece of the slow data sent along with someone's voice
    SlowData { callsign: String, kind: &'static str, text: String },

    // Where a station says it is
    Position { callsign: String, latitude: f64, longitude: f64, comment: String },

    // A transmission that has finished, for the chart of where voice has been going
    Transmission { source: &'static str, callsign: String, started_at: u64, seconds: u64 },

    // How an RF transmission measured up once it ended
    Quality { callsign: String, ber_percent: f32, dbm: Option<f32>, seconds: f32 },
}
