// Things that happen too fast, or matter too briefly, to keep in the status snapshot. These are streamed to the web UI as they occur

use serde::Serialize;

#[derive(Clone, Serialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    // How strongly the radio is hearing whoever is transmitting. The dBm needs the modem's RSSI calibration
    Signal { raw: u16, dbm: Option<f32> },

    // A piece of the slow data sent along with someone's voice
    SlowData { callsign: String, kind: &'static str, text: String },

    // Where a station says it is
    Position { callsign: String, latitude: f64, longitude: f64, comment: String },

    // How an RF transmission measured up once it ended
    Quality { callsign: String, ber_percent: f32, dbm: Option<f32>, seconds: f32 },
}
