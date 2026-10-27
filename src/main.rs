mod allstar;
mod aprs;
mod bridge;
mod config;
mod dprs;
mod dstar;
mod hosts;
mod http;
mod modem;
mod reflector;
mod router;
mod state;

use clap::Parser;
use std::path::PathBuf;
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(about)]
struct Args {
    /// Path to the station configuration
    #[arg(long, short, default_value = "/etc/va3ujf.toml", global = true)]
    config: PathBuf,

    /// Also write the log to this file
    #[arg(long, global = true)]
    log_file: Option<PathBuf>,

    #[command(flatten)]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::InfoLevel>,
}

async fn run(config: config::Config) -> anyhow::Result<()> {
    // Frames flow between the modem, the reflector link, and the AllStar bridge through the router
    let (modem_to_router, from_modem) = mpsc::channel(256);
    let (to_modem, modem_from_router) = mpsc::channel(256);
    let (reflector_to_router, from_reflector) = mpsc::channel(256);
    let (to_reflector, reflector_from_router) = mpsc::channel(256);
    let (command_sender, command_receiver) = mpsc::channel(16);
    let shared = state::Shared::default();
    let directory = hosts::directory(&config);
    hosts::start(&config, &directory).await;
    state::update(&shared, |snapshot| {
        snapshot.aprs_connected = config.aprs.as_ref().map(|_| false);
        snapshot.vocoder_connected = config.allstar.as_ref().map(|_| false);
        snapshot.allstar = config.allstar.as_ref().map(|allstar| state::AllstarStatus { node: allstar.node, ..Default::default() });
    });

    let bridge_endpoints = config.allstar.clone().map(|allstar| {
        let (bridge_to_router, from_bridge) = mpsc::channel(256);
        let (to_bridge, bridge_from_router) = mpsc::channel(256);
        let station = config.station.clone();
        let shared = shared.clone();
        tokio::spawn(async move {
            if let Err(error) = bridge::run(allstar, station, shared, bridge_from_router, bridge_to_router).await {
                log::error!("AllStar bridge failed: {error:#}");
            }
        });
        (from_bridge, to_bridge)
    });

    tokio::spawn(modem::run(config.modem.clone(), config.station.clone(), shared.clone(), modem_to_router, modem_from_router));
    tokio::spawn(reflector::run(config.clone(), directory.clone(), command_receiver, reflector_to_router, reflector_from_router, shared.clone()));
    let position_reports = config.aprs.clone().map(|aprs_config| {
        let (to_aprs, from_router) = mpsc::channel(16);
        tokio::spawn(aprs::run(config.station.clone(), aprs_config, shared.clone(), from_router));
        to_aprs
    });
    if let Some(allstar_config) = config.allstar.clone() {
        tokio::spawn(allstar::poll_links(allstar_config.node, shared.clone()));
        tokio::spawn(allstar::run_schedule(allstar_config, shared.clone()));
    }
    let http_config = config.clone();
    let http_shared = shared.clone();
    tokio::spawn(async move {
        if let Err(error) = http::serve(http_config, directory, http_shared, command_sender).await {
            log::error!("Web UI failed: {error:#}");
        }
    });

    // Pass voice between the endpoints until told to stop
    let router = router::run(router::Endpoints { from_modem, to_modem, from_reflector, to_reflector, bridge: bridge_endpoints, position_reports }, shared);
    tokio::select! {
        _ = router => Ok(()),
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // Timestamps and sources on every line, with dependencies held to warnings so -v shows this program's own detail
    let mut logger = fern::Dispatch::new()
        .format(|out, message, record| out.finish(format_args!("{} {:<5} {} {}", chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z"), record.level(), record.target(), message)))
        .level(log::LevelFilter::Warn)
        .level_for(env!("CARGO_CRATE_NAME"), args.verbose.log_level_filter())
        .chain(std::io::stderr());
    if let Some(log_file) = &args.log_file {
        logger = logger.chain(fern::log_file(log_file)?);
    }
    logger.apply()?;

    run(config::Config::load(&args.config)?).await
}
