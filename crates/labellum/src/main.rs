use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use labellum::config::Config;
use labellum::{Labellum, Options};
use orchid_store::{EtcdStore, EtcdTls};
use orchid_transport::server::{self, Listener};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// Labellum, the API server of Orchid.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Path of the configuration file.
    #[arg(short, long, default_value = "/etc/labellum/labellum.toml")]
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
    let etcd_tls = match &config.etcd.tls {
        Some(tls) => {
            let material = tls.load().await?;
            Some(EtcdTls {
                ca: material.ca,
                certificate: material.certificate,
                private_key: material.private_key,
            })
        }
        None => None,
    };

    let store = EtcdStore::connect(&config.etcd.endpoints, etcd_tls).await?;
    let candidate = format!(
        "{}-{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    );
    let labellum = Labellum::start(store, Options { candidate }).await?;

    let mut listeners = Vec::new();
    for address in config.listen_addresses()? {
        listeners.push(Listener::bind(&address).await?);
    }
    info!(
        mode = if tls.is_some() { "mTLS" } else { "cleartext" },
        "starting labellum"
    );
    let router = labellum.router(server::builder(tls.as_ref())?);
    server::serve(router, listeners, shutdown_signal()).await?;

    info!("shutting down");
    labellum.shutdown().await;
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
