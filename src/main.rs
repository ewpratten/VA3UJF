mod ack;
mod allstar;
mod aprs;
mod bridge;
mod codec;
mod config;
mod dprs;
mod dstar;
mod hosts;
#[cfg(test)]
mod integration;
mod http;
mod live;
mod modem;
mod reflector;
mod router;
mod slowdata;
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
    // Every link trades frames with the router, which arbitrates the channel between them and the radio
    let (mut router, radio) = router::Router::new();
    let (command_sender, command_receiver) = mpsc::channel(16);
    let shared = state::Shared::default();
    let directory = hosts::directory(&config);
    hosts::start(&config, &directory).await;
    state::update(&shared, |snapshot| snapshot.aprs_connected = config.aprs.as_ref().map(|_| false));

    // Announcements are spoken by a Piper voice in this process, and sent out over the local radio only
    let speaker = config.speech.clone().and_then(|speech| {
        let ambeserver = speech.ambeserver.clone().or_else(|| config.allstar.as_ref().map(|allstar| allstar.ambeserver.clone()))?;
        let tx_gain_db = config.allstar.as_ref().map_or(0.0, |allstar| allstar.tx_gain_db);
        Some(speech::start(speech, ambeserver, tx_gain_db, config.station.clone(), shared.clone(), &mut router))
    });

    // Whoever finishes a transmission over RF is answered with how well they were heard, if that's wanted
    let acks = config.ack.clone().map(|ack| ack::start(ack, config.station.clone(), &mut router));

    // The links
    let reflector_port = router.attach(reflector::LINK_KIND, router::Scope::Everyone);
    tokio::spawn(reflector::run(config.clone(), directory.clone(), command_receiver, reflector_port, shared.clone(), speaker.clone()));
    let allstar_handle = config.allstar.clone().map(|allstar| allstar::start(allstar, config.station.clone(), shared.clone(), &mut router, speaker.clone()));
    if config.allstar.as_ref().is_some_and(|allstar| allstar.park_reflector) {
        tokio::spawn(reflector::park::run(shared.clone(), command_sender.clone()));
    }

    // The radio, and what's done with what it hears
    tokio::spawn(modem::run(config.modem.clone(), config.station.clone(), shared.clone(), radio.to_router, radio.from_router));
    let position_reports = config.aprs.clone().map(|aprs_config| {
        let (to_aprs, from_router) = mpsc::channel(16);
        tokio::spawn(aprs::run(config.station.clone(), aprs_config, shared.clone(), from_router));
        to_aprs
    });
    let http_config = config.clone();
    let http_shared = shared.clone();
    tokio::spawn(async move {
        if let Err(error) = http::serve(http_config, directory, http_shared, command_sender, allstar_handle).await {
            log::error!("Web UI failed: {error:#}");
        }
    });

    // Pass voice between the radio and the links until told to stop
    tokio::select! {
        _ = router.run(router::RadioTaps::new(position_reports, speaker, acks), shared) => Ok(()),
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
