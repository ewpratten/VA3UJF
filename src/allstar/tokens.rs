// Call tokens prove a caller can receive at the address it claims, before any state is kept for it

use crate::state;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::net::SocketAddr;

const CALL_TOKEN_LIFETIME_SECONDS: u64 = 10;

pub struct Tokens {
    key: [u8; 32],
}

impl Tokens {
    pub fn new() -> Self {
        let mut key = [0; 32];
        getrandom::fill(&mut key).expect("the system has no source of randomness");
        Tokens { key }
    }

    fn mac(&self, address: SocketAddr, time: u64) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC takes keys of any length");
        mac.update(address.to_string().as_bytes());
        mac.update(&time.to_be_bytes());
        mac
    }

    pub fn issue(&self, address: SocketAddr) -> Vec<u8> {
        let time = state::unix_now();
        format!("{time}?{}", hex::encode(self.mac(address, time).finalize().into_bytes())).into_bytes()
    }

    pub fn valid(&self, token: &[u8], address: SocketAddr) -> bool {
        let Some((time, tag)) = std::str::from_utf8(token).ok().and_then(|text| text.split_once('?')) else { return false };
        let (Ok(time), Ok(tag)) = (time.parse::<u64>(), hex::decode(tag)) else { return false };
        let now = state::unix_now();
        self.mac(address, time).verify_slice(&tag).is_ok() && time <= now && now - time < CALL_TOKEN_LIFETIME_SECONDS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_tokens_only_work_for_the_address_they_were_issued_to() {
        let tokens = Tokens::new();
        let (first, second): (SocketAddr, SocketAddr) = ("192.0.2.1:4569".parse().unwrap(), "192.0.2.2:4569".parse().unwrap());
        let token = tokens.issue(first);
        assert!(tokens.valid(&token, first));
        assert!(!tokens.valid(&token, second));
        assert!(!tokens.valid(b"garbage", first));
    }

    #[test]
    fn expired_tokens_are_refused_even_with_a_genuine_tag() {
        let tokens = Tokens::new();
        let address: SocketAddr = "192.0.2.1:4569".parse().unwrap();
        let time = state::unix_now() - 60;
        let stale = format!("{time}?{}", hex::encode(tokens.mac(address, time).finalize().into_bytes()));
        assert!(!tokens.valid(stale.as_bytes(), address));
    }
}
