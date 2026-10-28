// G.711 µ-law, as carried by IAX2 and most telephony

use audio_codec_algorithms::{decode_ulaw, encode_ulaw};

pub fn decode(payload: &[u8]) -> impl Iterator<Item = i16> + '_ {
    payload.iter().copied().map(decode_ulaw)
}

pub fn encode(samples: &[i16]) -> Vec<u8> {
    samples.iter().copied().map(encode_ulaw).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_survive_a_round_trip_to_within_the_companding_error() {
        let samples = [0, 100, -100, 8000, -8000, 30000];
        for (sent, heard) in samples.iter().zip(decode(&encode(&samples))) {
            assert!((*sent as i32 - heard as i32).abs() <= (*sent as i32).abs() / 16 + 8, "{sent} came back as {heard}");
        }
    }
}
