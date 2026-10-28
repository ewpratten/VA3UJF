// The forward error correction in a frame of D-STAR voice, which says how many bits the radio link got wrong

use std::sync::LazyLock;

// What a frame of silence looks like on the air. It's a repeated pattern rather than real voice, so it says nothing about the link
pub const SILENCE: [u8; 9] = [0xEE, 0xC2, 0xA1, 0xC8, 0x42, 0x6E, 0x52, 0x51, 0xC3];

// The 72 bits of a frame are three interleaved codewords. A and B are protected by Golay(24,12), and C isn't
const A_BITS: [usize; 24] = [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 1, 7, 13, 19, 25, 31, 37, 43, 49, 55, 61, 67];
const B_BITS: [usize; 24] = [2, 8, 14, 20, 26, 32, 38, 44, 50, 56, 62, 68, 3, 9, 15, 21, 27, 33, 39, 45, 51, 57, 63, 69];

// Generator of the Golay(23,12) code
const GENERATOR: u32 = 0xC75;

// Errors in a codeword that can't be corrected count as this many
const UNCORRECTABLE_ERRORS: u32 = 10;

fn remainder(mut value: u32) -> u32 {
    for bit in (11..23).rev() {
        if value >> bit & 1 == 1 {
            value ^= GENERATOR << (bit - 11);
        }
    }
    value
}

fn parity(value: u32) -> u32 {
    value.count_ones() & 1
}

// A data word with its parity bits below it, and one more bit that makes the whole 24 even
fn encode(data: u32) -> u32 {
    let codeword = (data << 11) | remainder(data << 11);
    (codeword << 1) | parity(codeword)
}

// The error pattern for each syndrome. Golay is a perfect code, so every syndrome belongs to exactly one pattern of up to three bit errors
static ERROR_PATTERNS: LazyLock<[u32; 2048]> = LazyLock::new(|| {
    let mut patterns = [0; 2048];
    let mut record = |pattern: u32| patterns[remainder(pattern) as usize] = pattern;
    record(0);
    for first in 0..23 {
        record(1 << first);
        for second in first + 1..23 {
            record(1 << first | 1 << second);
            for third in second + 1..23 {
                record(1 << first | 1 << second | 1 << third);
            }
        }
    }
    patterns
});

// The scrambling sequence for the second codeword, which depends on the first one's data
fn scrambler(data: u32) -> u32 {
    let mut state = 16 * data;
    (0..24).fold(0, |bits, _| {
        state = (173 * state + 13849) & 0xFFFF;
        bits << 1 | state >> 15
    })
}

// The corrected data, and whether the codeword was believable
fn decode(codeword: u32) -> (u32, bool) {
    let syndrome = remainder(codeword >> 1);
    let corrected = codeword ^ (ERROR_PATTERNS[syndrome as usize] << 1);
    let believable = syndrome.count_ones() < 3 || parity(corrected) == 0;
    (corrected >> 12, believable)
}

fn gather(frame: &[u8; 9], positions: &[usize; 24]) -> u32 {
    positions.iter().fold(0, |word, position| word << 1 | (frame[position >> 3] >> (7 - (position & 7)) & 1) as u32)
}

// How many of a frame's bits the error correction had to fix. The frame is as the modem delivers it
pub fn bit_errors(frame: &[u8; 9]) -> u32 {
    let (received_a, received_b) = (gather(frame, &A_BITS), gather(frame, &B_BITS));
    let (data_a, valid_a) = decode(received_a);
    let scramble = scrambler(data_a);
    let (data_b, valid_b) = decode(received_b ^ scramble);
    if !valid_a || !valid_b {
        return UNCORRECTABLE_ERRORS;
    }
    (encode(data_a) ^ received_a).count_ones() + (encode(data_b) ^ scramble ^ received_b).count_ones()
}

#[cfg(test)]
pub mod testing {
    use super::*;

    // Builds the frame a radio would send for two data words, with a third codeword that's just carried along
    pub fn frame_for(data_a: u32, data_b: u32) -> [u8; 9] {
        let (a, b) = (encode(data_a), encode(data_b) ^ scrambler(data_a));
        let mut frame = [0u8; 9];
        for (bits, word) in [(&A_BITS, a), (&B_BITS, b)] {
            for (index, position) in bits.iter().enumerate() {
                frame[position >> 3] |= ((word >> (23 - index) & 1) as u8) << (7 - (position & 7));
            }
        }
        frame
    }

    // The same frame with one of its bits flipped
    pub fn damaged(mut frame: [u8; 9], position: usize) -> [u8; 9] {
        frame[position >> 3] ^= 0x80 >> (position & 7);
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::testing::frame_for;
    use super::*;

    #[test]
    fn the_codes_match_mmdvmhosts_tables() {
        assert_eq!((encode(1), encode(2)), (0x0018EB, 0x00293E));
        assert_eq!(scrambler(0), 0x42CC47);
        assert_eq!(scrambler(1), 0x19D6FE);
    }

    #[test]
    fn a_clean_frame_has_no_errors_and_each_flipped_bit_counts_once() {
        let frame = frame_for(0x5A3, 0x2C7);
        assert_eq!(bit_errors(&frame), 0);
        for position in [A_BITS[3], A_BITS[20], B_BITS[0], B_BITS[23]] {
            let mut damaged = frame;
            damaged[position >> 3] ^= 0x80 >> (position & 7);
            assert_eq!(bit_errors(&damaged), 1, "bit {position}");
        }
        let mut damaged = frame;
        for position in [A_BITS[1], A_BITS[9], B_BITS[5]] {
            damaged[position >> 3] ^= 0x80 >> (position & 7);
        }
        assert_eq!(bit_errors(&damaged), 3);
    }

    #[test]
    fn too_much_damage_counts_as_a_bad_frame() {
        let mut frame = frame_for(0x123, 0x456);
        for position in [A_BITS[0], A_BITS[2], A_BITS[4], A_BITS[6], A_BITS[8], A_BITS[10]] {
            frame[position >> 3] ^= 0x80 >> (position & 7);
        }
        assert_eq!(bit_errors(&frame), UNCORRECTABLE_ERRORS);
    }
}
