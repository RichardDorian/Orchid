use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ikebana::config::Config;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// Ikebana, the scheduler of Orchid.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Path of the configuration file.
    #[arg(short, long, default_value = "/etc/ikebana/ikebana.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(&args.config)?;
    let tls = match &config.tls {
        Some(tls) => Some(tls.load().await?),
        None => None,
    };
    let channel = orchid_client::connect(&config.labellum, tls.as_ref())?;

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            info!("shutting down");
            shutdown.cancel();
        }
    });
    info!(candidate = config.candidate(), "starting ikebana");
    ikebana::run(channel, config.candidate(), shutdown).await;
    Ok(())
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}
