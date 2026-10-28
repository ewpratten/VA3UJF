mod allstar;
mod aprs;
mod bridge;
mod config;
mod dprs;
mod dstar;
mod hosts;
#[cfg(test)]
mod integration;
mod http;
mod modem;
mod reflector;
mod router;
mod speech;
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

    // Announcements are spoken by a Piper voice in this process, vocoded by the bridge, and sent out over the local radio only
    let (speaker, speech_requests) = match config.speech {
        Some(_) => {
            let (speaker, requests) = mpsc::channel(8);
            (Some(speaker), Some(requests))
        }
        None => (None, None),
    };
    let (speech_to_bridge, bridge_from_speech) = mpsc::channel(256);
    let (bridge_to_announcements, from_announcements) = mpsc::channel(256);

    // The node and the bridge trade audio directly, and the bridge trades D-STAR frames with the router
    let mut allstar_handle = None;
    let bridge_endpoints = config.allstar.clone().map(|allstar| {
        let (bridge_to_router, from_bridge) = mpsc::channel(256);
        let (to_bridge, bridge_from_router) = mpsc::channel(256);
        let (bridge_to_node, from_bridge_audio) = mpsc::channel(256);
        let (node_to_bridge, bridge_from_node) = mpsc::channel(256);
        let (handle, node_commands) = allstar::channel();
        allstar_handle = Some(handle.clone());
        let (station, bridge_shared, node_shared, schedule_shared) = (config.station.clone(), shared.clone(), shared.clone(), shared.clone());
        let (bridge_config, node_config, schedule_config) = (allstar.clone(), allstar.clone(), allstar);
        let reflector_commands = command_sender.clone();
        let node_speaker = speaker.clone();
        let node_station = station.clone();
        tokio::spawn(async move {
            if let Err(error) = bridge::run(bridge_config, station, bridge_shared, bridge_from_router, bridge_to_router, bridge_from_node, bridge_to_node, bridge_from_speech, bridge_to_announcements).await {
                log::error!("AllStar bridge failed: {error:#}");
            }
        });
        tokio::spawn(async move {
            if let Err(error) = allstar::run(node_config, node_station, node_shared, reflector_commands, node_speaker, from_bridge_audio, node_to_bridge, node_commands).await {
                log::error!("AllStar node failed: {error:#}");
            }
        });
        tokio::spawn(allstar::run_schedule(schedule_config, handle, schedule_shared));
        (from_bridge, to_bridge)
    });

    tokio::spawn(modem::run(config.modem.clone(), config.station.clone(), shared.clone(), modem_to_router, modem_from_router));
    tokio::spawn(reflector::run(config.clone(), directory.clone(), command_receiver, reflector_to_router, reflector_from_router, shared.clone(), speaker.clone()));
    let position_reports = config.aprs.clone().map(|aprs_config| {
        let (to_aprs, from_router) = mpsc::channel(16);
        tokio::spawn(aprs::run(config.station.clone(), aprs_config, shared.clone(), from_router));
        to_aprs
    });
    if let (Some(speech_config), Some(requests)) = (config.speech.clone(), speech_requests) {
        tokio::spawn(speech::run(speech_config, config.station.clone(), shared.clone(), requests, speech_to_bridge));
    }
    let http_config = config.clone();
    let http_shared = shared.clone();
    tokio::spawn(async move {
        if let Err(error) = http::serve(http_config, directory, http_shared, command_sender, allstar_handle).await {
            log::error!("Web UI failed: {error:#}");
        }
    });

    // Pass voice between the endpoints until told to stop
    let router = router::run(router::Endpoints { from_modem, to_modem, from_reflector, to_reflector, bridge: bridge_endpoints, position_reports, announcements: Some(from_announcements), speaker }, shared);
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
