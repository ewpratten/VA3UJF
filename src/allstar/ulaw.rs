// G.711 μ-law, the codec every AllStar node link allows

const BIAS: i32 = 0x84;
const CLIP: i32 = 32635;

pub fn encode(sample: i16) -> u8 {
    let mut magnitude = sample as i32;
    let sign = if magnitude < 0 {
        magnitude = -magnitude;
        0x80
    } else {
        0
    };
    let biased = magnitude.min(CLIP) + BIAS;
    let exponent = (31 - (biased as u32).leading_zeros() as i32 - 7).clamp(0, 7);
    let mantissa = (biased >> (exponent + 3)) & 0x0F;
    !(sign | (exponent << 4) as u8 | mantissa as u8)
}

pub fn decode(byte: u8) -> i16 {
    let byte = !byte;
    let magnitude = ((((byte & 0x0F) as i32) << 3) + BIAS) << ((byte & 0x70) >> 4);
    (if byte & 0x80 != 0 { BIAS - magnitude } else { magnitude - BIAS }) as i16
}

pub fn encode_frame(samples: &[i16]) -> Vec<u8> {
    samples.iter().copied().map(encode).collect()
}

pub fn decode_frame(bytes: &[u8]) -> Vec<i16> {
    bytes.iter().copied().map(decode).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_encodes_to_the_idle_byte() {
        assert_eq!(encode(0), 0xFF);
        assert_eq!(decode(0xFF), 0);
        assert_eq!(decode(0x7F), 0);
    }

    #[test]
    fn extremes_match_g711() {
        assert_eq!(encode(i16::MAX), 0x80);
        assert_eq!(encode(i16::MIN), 0x00);
        assert_eq!(decode(0x80), 32124);
        assert_eq!(decode(0x00), -32124);
    }

    #[test]
    fn round_trips_stay_within_the_codec_precision() {
        for sample in (-32000..32000).step_by(97) {
            let restored = decode(encode(sample as i16)) as i32;
            assert!((restored - sample).abs() <= (sample.abs() / 16).max(8), "{sample} came back as {restored}");
        }
    }
}
