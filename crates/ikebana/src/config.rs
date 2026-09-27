//! Configuration file of Ikebana.

use std::path::Path;

use orchid_transport::tls::TlsConfig;
use orchid_transport::url::parse_server_urls;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid configuration: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// URLs of the Labellum instances.
    pub labellum: Vec<String>,
    /// Name in the leader election. Defaults to the hostname.
    pub candidate: Option<String>,
    /// Presence enables mTLS mode.
    pub tls: Option<TlsConfig>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&content)
    }

    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(content)?;
        parse_server_urls(&config.labellum, config.tls.is_some())
            .map_err(|e| ConfigError::Invalid(format!("labellum: {e}")))?;
        Ok(config)
    }

    pub fn candidate(&self) -> String {
        self.candidate
            .clone()
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_example_configuration() {
        let config = Config::parse(include_str!("../../../configs/ikebana.toml")).unwrap();
        assert!(config.tls.is_some());
    }

    #[test]
    fn rejects_urls_not_matching_the_mode() {
        assert!(Config::parse(r#"labellum = ["https://10.0.0.1:36116"]"#).is_err());
        assert!(Config::parse(r#"labellum = ["http://10.0.0.1:36116"]"#).is_ok());
    }
}
