// Links this program's node against a real AllStarLink 3 node (app_rpt on Asterisk)

use super::{build_image, container_assets, containers_available, repository_root, Container, Network};
use crate::allstar::testing::*;
use crate::config::Action;
use std::time::Duration;

const HUB: &str = "1999";
const OUR_NODE: &str = "10001";

// A hub node with no radio, which finds our node by the address it's given
struct Asl3 {
    container: Container,
}

impl Asl3 {
    fn start(our_address: &str, network: Option<&Network>) -> anyhow::Result<Self> {
        let assets = container_assets("asl3");
        build_image("va3ujf-test-asl3", &assets.join("Dockerfile"), &assets)?;
        let (our_node, our_address) = (format!("OUR_NODE={OUR_NODE}"), format!("OUR_ADDRESS={our_address}"));
        let mut arguments = vec!["--add-host", "host.docker.internal:host-gateway", "--publish", "127.0.0.1::4569/udp", "--env", &our_node, "--env", &our_address];
        if let Some(network) = network {
            arguments.extend(["--network", &network.name, "--network-alias", "asl3"]);
        }
        let container = Container::start("va3ujf-test-asl3", &arguments)?;
        container.wait_for_logs(&format!("Normal Repeater Init  {HUB}"), Duration::from_secs(60))?;
        Ok(Asl3 { container })
    }

    fn cli(&self, command: &str) -> String {
        self.container.exec(&["asterisk", "-rx", command]).unwrap_or_else(|error| format!("{error:#}"))
    }

    // How the hub sees its links, as app_rpt's own status variables
    fn links(&self) -> String {
        self.cli(&format!("rpt xnode {HUB}"))
    }
}

async fn eventually(description: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {description}");
}

// Blocking Docker calls get a thread of their own, so the node under test keeps running
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn our_node_links_to_asl3_and_trades_audio_with_it() {
    if !containers_available() {
        return;
    }
    let hub = Asl3::start("host.docker.internal:4569", None).unwrap();
    let hub_port = hub.container.host_port("4569/udp").unwrap();
    let mut ours = start_node(10001, 0, &[(HUB.parse().unwrap(), format!("127.0.0.1:{hub_port}"))]);
    ours.handle.run_action(&Action { link: Some(HUB.into()), ..Default::default() }).await.unwrap();
    wait_for("the link to come up", || linked(&ours, HUB)).await;

    // The hub counts us as a transceive link
    assert!(hub.links().contains(&format!("RPT_LINKS=1,T{OUR_NODE}")), "hub sees: {}", hub.links());

    // Our audio keys the link at the hub
    let sender = ours.to_node.clone();
    let transmission = tokio::spawn(async move { transmit(&sender, 100).await });
    let mut keyed = false;
    for _ in 0..15 {
        keyed |= hub.links().contains(&format!("{OUR_NODE}TK"));
        if keyed {
            break;
        }
    }
    transmission.await.unwrap();
    assert!(keyed, "the hub never saw {OUR_NODE} keyed: {}", hub.links());

    // And what the hub transmits comes through as audio, ending with an unkey
    assert!(hub.cli(&format!("rpt playback {HUB} /tmp/noise")).trim().is_empty());
    let heard = heard_frames(&mut ours.from_node, Duration::from_secs(10)).await.expect("the hub's transmission to end");
    assert!(heard > 100, "heard only {heard} frames from the hub");

    // Unlinking drops it at both ends
    ours.handle.run_action(&Action { unlink: Some(HUB.into()), ..Default::default() }).await.unwrap();
    wait_for("our node to drop the link", || has_no_links(&ours)).await;
    eventually("the hub to drop the link", || hub.links().contains("RPT_NUMLINKS=0")).await;
}

// This program's own image, as the real binary, on a network with the hub. The hub has to find us, and we have to check it's who it says
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn asl3_links_to_our_container_and_hangs_up_on_it() {
    if !containers_available() {
        return;
    }
    let network = Network::create().unwrap();
    let hub = Asl3::start("hotspot:4569", Some(&network)).unwrap();
    let root = repository_root();
    build_image("va3ujf-test", &root.join("subprojects/apps/va3ujf/Dockerfile"), &root).unwrap();

    let directory = std::env::temp_dir().join(format!("va3ujf-test-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let config = directory.join("config.toml");
    std::fs::write(&config, format!("http_listen = \"0.0.0.0:8080\"\n[station]\ncallsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0\n[modem]\nport = \"/dev/ttyS0\"\n[allstar]\nnode = {OUR_NODE}\nambeserver = \"127.0.0.1:2460\"\npark_reflector = false\n[allstar.nodes]\n\"{HUB}\" = \"asl3:4569\"\n")).unwrap();
    let mount = format!("{}:/etc/va3ujf.toml:ro", config.display());
    let ours = Container::start("va3ujf-test", &["--network", &network.name, "--network-alias", "hotspot", "--publish", "127.0.0.1::8080", "--volume", &mount]).unwrap();
    ours.wait_for_logs("listening for IAX2", Duration::from_secs(30)).unwrap();
    let web_port = ours.host_port("8080/tcp").unwrap();

    // The hub dials us, and we only accept because DNS (here, Docker's) agrees it's calling from where it says
    let status = || async {
        let response = reqwest::get(format!("http://127.0.0.1:{web_port}/api/status")).await.ok()?;
        response.json::<serde_json::Value>().await.ok()
    };
    hub.cli(&format!("rpt cmd {HUB} ilink 3 {OUR_NODE}"));
    let mut linked = false;
    for _ in 0..100 {
        linked = status().await.is_some_and(|status| status["allstar"]["links"].as_array().is_some_and(|links| links.iter().any(|link| link["node"] == HUB && link["connected"] == true)));
        if linked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(linked, "our node never showed the link. Logs:\n{}", ours.logs());
    assert!(hub.links().contains(&format!("RPT_LINKS=1,T{OUR_NODE}")), "hub sees: {}", hub.links());

    // When the hub disconnects, we notice and let go
    hub.cli(&format!("rpt cmd {HUB} ilink 1 {OUR_NODE}"));
    let mut dropped = false;
    for _ in 0..100 {
        dropped = status().await.is_some_and(|status| status["allstar"]["links"].as_array().is_some_and(|links| links.is_empty()));
        if dropped {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(dropped, "our node never dropped the link. Logs:\n{}", ours.logs());
    std::fs::remove_dir_all(&directory).ok();
}
