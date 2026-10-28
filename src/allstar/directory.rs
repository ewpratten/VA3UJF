// Finds nodes on the AllStar network, from configured addresses or the DNS records AllStarLink publishes for registered nodes

use anyhow::{bail, Context};
use hickory_resolver::proto::rr::RData;
use hickory_resolver::TokioResolver;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::net::lookup_host;

const CACHE_LIFETIME: Duration = Duration::from_secs(120);
const MAX_NODE_DIGITS: usize = 8;

pub struct Directory {
    resolver: TokioResolver,
    domain: String,
    configured: HashMap<String, String>,
    cache: Mutex<HashMap<String, (Instant, Vec<SocketAddr>)>>,
}

pub fn is_node_number(text: &str) -> bool {
    (1..=MAX_NODE_DIGITS).contains(&text.len()) && text.bytes().all(|byte| byte.is_ascii_digit())
}

impl Directory {
    // Configured nodes are "host:port" and take precedence over DNS
    pub fn new(domain: &str, configured: &HashMap<String, String>) -> anyhow::Result<Self> {
        let resolver = TokioResolver::builder_tokio().context("couldn't read the system's DNS settings")?.build()?;
        Ok(Directory { resolver, domain: domain.trim_end_matches('.').to_string(), configured: configured.clone(), cache: Mutex::new(HashMap::new()) })
    }

    // Every address a node can be reached at, best first
    pub async fn lookup(&self, node: &str) -> anyhow::Result<Vec<SocketAddr>> {
        if !is_node_number(node) {
            bail!("{node:?} isn't a node number");
        }
        if let Some(configured) = self.configured.get(node) {
            return Ok(lookup_host(configured.as_str()).await?.filter(SocketAddr::is_ipv4).collect());
        }
        if let Some((fetched, addresses)) = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).get(node)
            && fetched.elapsed() < CACHE_LIFETIME
        {
            return Ok(addresses.clone());
        }
        let addresses = self.lookup_dns(node).await?;
        self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).insert(node.to_string(), (Instant::now(), addresses.clone()));
        Ok(addresses)
    }

    // AllStarLink publishes an SRV record per registered node, pointing at a name that resolves to its address
    async fn lookup_dns(&self, node: &str) -> anyhow::Result<Vec<SocketAddr>> {
        let name = format!("_iax._udp.{node}.{}.", self.domain);
        let answers = self.resolver.srv_lookup(name.as_str()).await.with_context(|| format!("node {node} isn't in DNS"))?;
        let mut records: Vec<_> = answers.answers().iter().filter_map(|record| if let RData::SRV(srv) = &record.data { Some(srv.clone()) } else { None }).collect();
        records.sort_by_key(|srv| (srv.priority, std::cmp::Reverse(srv.weight)));
        let mut addresses = Vec::new();
        for srv in records {
            if let Ok(ips) = self.resolver.lookup_ip(srv.target.to_string()).await {
                addresses.extend(ips.iter().filter(IpAddr::is_ipv4).map(|ip| SocketAddr::new(ip, srv.port)));
            }
        }
        if addresses.is_empty() {
            bail!("node {node} has no address in DNS");
        }
        Ok(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_numbers_are_short_digit_strings() {
        assert!(is_node_number("54207"));
        assert!(is_node_number("1999"));
        assert!(!is_node_number(""));
        assert!(!is_node_number("N0CALL"));
        assert!(!is_node_number("123456789"));
    }

    #[tokio::test]
    async fn configured_nodes_skip_dns() {
        let configured = HashMap::from([("1999".to_string(), "127.0.0.1:4570".to_string())]);
        let directory = Directory::new("nodes.invalid", &configured).unwrap();
        assert_eq!(directory.lookup("1999").await.unwrap(), vec!["127.0.0.1:4570".parse::<SocketAddr>().unwrap()]);
        assert!(directory.lookup("abc").await.is_err());
    }
}
