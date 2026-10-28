// AMBE vocoding through an AMBEserver: 8 kHz speech in, 9-byte D-STAR voice frames out, and the other way around

use super::SAMPLES_PER_FRAME;
use anyhow::{bail, Context};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{timeout, Instant};

// AMBE-3000 packet framing, as spoken by AMBEserver
const START_BYTE: u8 = 0x61;
const TYPE_CONTROL: u8 = 0x00;
const TYPE_CHANNEL: u8 = 0x01;
const TYPE_SPEECH: u8 = 0x02;
const REQ_RESET: [u8; 5] = [0x61, 0x00, 0x01, 0x00, 0x33];
const REQ_PRODID: [u8; 5] = [0x61, 0x00, 0x01, 0x00, 0x30];
const REQ_RATEP_DSTAR: [u8; 17] = [0x61, 0x00, 0x0D, 0x00, 0x0A, 0x01, 0x30, 0x07, 0x63, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x48];
const CHANNEL_HEADER: [u8; 6] = [0x61, 0x00, 0x0B, 0x01, 0x01, 0x48];
const SPEECH_HEADER: [u8; 6] = [0x61, 0x01, 0x42, 0x02, 0x00, 0xA0];
const PKT_READY: u8 = 0x39;
const PKT_PRODID: u8 = 0x30;
const PKT_RATEP: u8 = 0x0A;
const TIMEOUT: Duration = Duration::from_secs(3);

// AMBEserver forgets idle clients (60s by default), so the rate is re-sent well before that
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

// What the vocoder hands back
pub enum Decoded {
    Speech(Vec<i16>),
    Frame([u8; 9]),
}

pub struct Vocoder {
    socket: UdpSocket,
    last_write: Instant,
}

// Split a datagram into (type, payload) AMBE packets
fn split_packets(mut buffer: &[u8]) -> Vec<(u8, &[u8])> {
    let mut packets = Vec::new();
    while let Some(start) = buffer.iter().position(|byte| *byte == START_BYTE) {
        buffer = &buffer[start..];
        if buffer.len() < 4 {
            break;
        }
        let packet_length = 4 + (((buffer[1] as usize) << 8) | buffer[2] as usize);
        if buffer.len() < packet_length {
            break;
        }
        packets.push((buffer[3], &buffer[4..packet_length]));
        buffer = &buffer[packet_length..];
    }
    packets
}

// Wait for the AMBEserver's answer to a control request
async fn expect_control(socket: &UdpSocket, packet_id: u8) -> anyhow::Result<Vec<u8>> {
    let deadline = Instant::now() + TIMEOUT;
    let mut buffer = [0u8; 2048];
    loop {
        let length = timeout(deadline.saturating_duration_since(Instant::now()), socket.recv(&mut buffer)).await.with_context(|| format!("no response to control packet {packet_id:#04x}"))??;
        for (kind, payload) in split_packets(&buffer[..length]) {
            if kind == TYPE_CONTROL && payload.first() == Some(&packet_id) {
                return Ok(payload.to_vec());
            }
        }
    }
}

impl Vocoder {
    // Connect and reset the chip, then switch it to the D-STAR codec
    pub async fn connect(address: SocketAddr) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(address).await?;
        socket.send(&REQ_RESET).await?;
        expect_control(&socket, PKT_READY).await?;
        socket.send(&REQ_PRODID).await?;
        let product = expect_control(&socket, PKT_PRODID).await?;
        socket.send(&REQ_RATEP_DSTAR).await?;
        let ratep = expect_control(&socket, PKT_RATEP).await?;
        if ratep.get(1) != Some(&0) {
            bail!("vocoder rejected the D-STAR rate");
        }
        log::info!("Vocoder ready: {}", String::from_utf8_lossy(&product[1..]).trim_end_matches('\0'));
        Ok(Vocoder { socket, last_write: Instant::now() })
    }

    async fn write(&mut self, packet: &[u8]) -> std::io::Result<()> {
        self.socket.send(packet).await?;
        self.last_write = Instant::now();
        Ok(())
    }

    // Ask for a frame of speech to be encoded. The AMBE frame comes back through `receive`
    pub async fn encode(&mut self, samples: &[i16]) -> std::io::Result<()> {
        let packet = [&SPEECH_HEADER[..], &samples.iter().flat_map(|sample| sample.to_be_bytes()).collect::<Vec<u8>>()].concat();
        self.write(&packet).await
    }

    // Ask for a frame of AMBE to be decoded. The speech comes back through `receive`
    pub async fn decode(&mut self, frame: &[u8; 9]) -> std::io::Result<()> {
        self.write(&[&CHANNEL_HEADER[..], &frame[..]].concat()).await
    }

    // Keeps the AMBEserver session, and the D-STAR rate it holds, alive while there's nothing to vocode
    pub async fn keep_alive(&mut self) -> std::io::Result<()> {
        if self.last_write.elapsed() > KEEPALIVE_INTERVAL {
            self.write(&REQ_RATEP_DSTAR).await?;
        }
        Ok(())
    }

    // Whatever the vocoder has finished, in the order it arrived
    pub async fn receive(&self) -> std::io::Result<Vec<Decoded>> {
        let mut buffer = [0u8; 2048];
        let length = self.socket.recv(&mut buffer).await?;
        let mut decoded = Vec::new();
        for (kind, payload) in split_packets(&buffer[..length]) {
            if kind == TYPE_SPEECH && payload.len() >= 2 + SAMPLES_PER_FRAME * 2 {
                decoded.push(Decoded::Speech(payload[2..2 + SAMPLES_PER_FRAME * 2].chunks_exact(2).map(|pair| i16::from_be_bytes([pair[0], pair[1]])).collect()));
            } else if kind == TYPE_CHANNEL && payload.len() >= 11 {
                decoded.push(Decoded::Frame(payload[2..11].try_into().unwrap()));
            }
        }
        Ok(decoded)
    }
}

// What the fake AMBEserver says every decoded sample is
#[cfg(test)]
pub const FAKE_SPEECH_LEVEL: i16 = 3000;

// An AMBEserver that answers its setup requests, turns every frame of speech into one frame of AMBE, and every frame of AMBE into steady speech
#[cfg(test)]
pub async fn fake_server() -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut buffer, mut counter) = ([0u8; 2048], 0u8);
        while let Ok((length, peer)) = socket.recv_from(&mut buffer).await {
            let reply: Vec<u8> = match (buffer[3], buffer[4], length) {
                (TYPE_CONTROL, 0x33, _) => vec![0x61, 0x00, 0x02, 0x00, PKT_READY, 0x00],
                (TYPE_CONTROL, PKT_PRODID, _) => vec![0x61, 0x00, 0x06, 0x00, PKT_PRODID, b'F', b'A', b'K', b'E', 0x00],
                (TYPE_CONTROL, PKT_RATEP, _) => vec![0x61, 0x00, 0x02, 0x00, PKT_RATEP, 0x00],
                (TYPE_SPEECH, _, _) => {
                    counter = counter.wrapping_add(1);
                    [&CHANNEL_HEADER[..], &[counter; 9]].concat()
                }

                // Every frame of AMBE it's asked to decode comes back as a steady level of speech
                (TYPE_CHANNEL, _, _) => [&SPEECH_HEADER[..], &FAKE_SPEECH_LEVEL.to_be_bytes().repeat(SAMPLES_PER_FRAME)].concat(),
                _ => continue,
            };
            let _ = socket.send_to(&reply, peer).await;
        }
    });
    address
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packets_split_out_of_a_datagram() {
        let datagram = [0x61, 0x00, 0x02, 0x00, 0x39, 0x00, 0x61, 0x00, 0x01, 0x01, 0xAA];
        assert_eq!(split_packets(&datagram), vec![(0x00, &[0x39, 0x00][..]), (0x01, &[0xAA][..])]);
    }

    #[tokio::test]
    async fn speech_is_encoded_into_one_frame_of_ambe() {
        let mut vocoder = Vocoder::connect(fake_server().await).await.unwrap();
        vocoder.encode(&[0; SAMPLES_PER_FRAME]).await.unwrap();
        assert!(matches!(vocoder.receive().await.unwrap().as_slice(), [Decoded::Frame(frame)] if *frame == [1; 9]));
    }
}
