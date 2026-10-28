// Docker-backed fixtures, so tests can run against the real software this program replaces or talks to

mod asl3;
mod dstar;
mod voice;

use anyhow::{bail, Context};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

// Container tests skip themselves without a Docker daemon. These choose otherwise
const SKIP_VARIABLE: &str = "VA3UJF_SKIP_CONTAINERS";
const REQUIRE_VARIABLE: &str = "VA3UJF_REQUIRE_CONTAINERS";

pub fn containers_available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    if std::env::var_os(SKIP_VARIABLE).is_some() {
        return false;
    }
    let available = *AVAILABLE.get_or_init(|| Command::new("docker").arg("info").output().is_ok_and(|output| output.status.success()));
    if !available {
        assert!(std::env::var_os(REQUIRE_VARIABLE).is_none(), "{REQUIRE_VARIABLE} is set but there's no Docker daemon");
        eprintln!("skipping: no Docker daemon, so no containers to test against");
    }
    available
}

pub fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize().unwrap()
}

pub fn container_assets(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/containers").join(name)
}

pub fn docker(args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("docker").args(args).output().context("couldn't run docker")?;
    if !output.status.success() {
        bail!("docker {} failed: {}", args.join(" "), String::from_utf8_lossy(&output.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    format!("{prefix}-{}-{}", std::process::id(), COUNTER.fetch_add(1, Ordering::Relaxed))
}

// Builds are cached by Docker, and serialized so tests that share an image don't build it twice at once
pub fn build_image(tag: &str, dockerfile: &Path, context: &Path) -> anyhow::Result<()> {
    static BUILDS: Mutex<()> = Mutex::new(());
    let _building = BUILDS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    docker(&["build", "--quiet", "--tag", tag, "--file", dockerfile.to_str().unwrap(), context.to_str().unwrap()]).map(drop)
}

// A private network, so containers can find each other by name and be seen at their real addresses
pub struct Network {
    pub name: String,
}

impl Network {
    pub fn create() -> anyhow::Result<Self> {
        let name = unique_name("va3ujf-test-net");
        docker(&["network", "create", &name])?;
        Ok(Network { name })
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.name]);
    }
}

// A running container, removed when dropped
pub struct Container {
    name: String,
}

impl Container {
    pub fn start(image: &str, docker_arguments: &[&str]) -> anyhow::Result<Self> {
        let name = unique_name("va3ujf-test");
        let mut arguments = vec!["run", "--detach", "--name", &name];
        arguments.extend(docker_arguments);
        arguments.push(image);
        docker(&arguments)?;
        Ok(Container { name })
    }

    pub fn exec(&self, command: &[&str]) -> anyhow::Result<String> {
        let mut arguments = vec!["exec", &self.name];
        arguments.extend(command);
        docker(&arguments)
    }

    pub fn logs(&self) -> String {
        Command::new("docker").args(["logs", &self.name]).output().map(|output| format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr))).unwrap_or_default()
    }

    // The host port a published container port was given
    pub fn host_port(&self, container_port: &str) -> anyhow::Result<u16> {
        let published = docker(&["port", &self.name, container_port])?;
        published.lines().next().and_then(|line| line.rsplit(':').next()).and_then(|port| port.trim().parse().ok()).with_context(|| format!("no host port for {container_port}: {published:?}"))
    }

    pub fn wait_for_logs(&self, needle: &str, patience: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + patience;
        while Instant::now() < deadline {
            if self.logs().contains(needle) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        bail!("{needle:?} never showed up in the logs of {}:\n{}", self.name, self.logs())
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = docker(&["rm", "--force", &self.name]);
    }
}
