//! Client configuration: where Labellum is and how to authenticate.

use std::path::{Path, PathBuf};

use orchid_transport::client::Connection;
use orchid_transport::tls::TlsConfig;
use serde::Deserialize;

use crate::Result;

/// URL used without configuration file nor `--server`.
const DEFAULT_URL: &str = "http://127.0.0.1:36116";

/// `~/.config/orchid/orchidctl.toml`
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// URLs of the Labellum instances.
    #[serde(default)]
    pub labellum: Vec<String>,
    /// Client certificate (`user:<name>`), enables mTLS mode.
    pub tls: Option<TlsConfig>,
}

fn default_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("orchid").join("orchidctl.toml"))
}

/// Loads the configuration. An explicit path must exist, the default one may not.
pub fn load(path: Option<&Path>) -> Result<ClientConfig> {
    let (path, explicit) = match path {
        Some(path) => (path.to_owned(), true),
        None => match default_path() {
            Some(path) => (path, false),
            None => return Ok(ClientConfig::default()),
        },
    };
    match std::fs::read_to_string(&path) {
        Ok(content) => toml::from_str(&content)
            .map_err(|e| format!("invalid configuration {}: {e}", path.display()).into()),
        Err(e) if !explicit && e.kind() == std::io::ErrorKind::NotFound => {
            Ok(ClientConfig::default())
        }
        Err(e) => Err(format!("failed to read {}: {e}", path.display()).into()),
    }
}

/// A connection to Labellum from the configuration and the `--server` flags.
pub async fn connect(path: Option<&Path>, servers: &[String]) -> Result<Connection> {
    let config = load(path)?;
    let urls = if !servers.is_empty() {
        servers.to_vec()
    } else if !config.labellum.is_empty() {
        config.labellum.clone()
    } else {
        vec![DEFAULT_URL.to_owned()]
    };
    let tls = match &config.tls {
        Some(tls) => Some(tls.load().await?),
        None => None,
    };
    Ok(orchid_client::connect(&urls, tls.as_ref())?)
}
