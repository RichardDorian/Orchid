//! Configuration file of Labellum.

use std::path::Path;

use orchid_transport::tls::TlsConfig;
use orchid_transport::url::{ListenAddr, parse_server_urls};
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
    /// Addresses the gRPC server binds: `tcp://ip:port` or `unix://path`.
    pub listen: Vec<String>,
    pub etcd: EtcdConfig,
    /// Presence enables mTLS mode.
    pub tls: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
    /// Client certificate for etcd. Required in mTLS mode, forbidden in cleartext mode.
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
        config.listen_addresses()?;
        config.check_mode()?;
        Ok(config)
    }

    pub fn listen_addresses(&self) -> Result<Vec<ListenAddr>, ConfigError> {
        if self.listen.is_empty() {
            return Err(ConfigError::Invalid(
                "listen: at least one address is required".into(),
            ));
        }
        self.listen
            .iter()
            .map(|address| {
                address
                    .parse()
                    .map_err(|e| ConfigError::Invalid(format!("listen: {e}")))
            })
            .collect()
    }

    /// Cleartext and mTLS cannot be combined.
    fn check_mode(&self) -> Result<(), ConfigError> {
        let tls = self.tls.is_some();
        if tls != self.etcd.tls.is_some() {
            return Err(ConfigError::Invalid(
                "[tls] and [etcd.tls] must be both present (mTLS) or both absent (cleartext)"
                    .into(),
            ));
        }
        parse_server_urls(&self.etcd.endpoints, tls)
            .map_err(|e| ConfigError::Invalid(format!("etcd.endpoints: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_example_configuration() {
        let config = Config::parse(include_str!("../../../configs/labellum.toml")).unwrap();
        assert_eq!(config.listen_addresses().unwrap().len(), 2);
        assert!(config.tls.is_some());
    }

    #[test]
    fn parses_a_cleartext_configuration() {
        let config = Config::parse(
            r#"
            listen = ["tcp://0.0.0.0:36116"]
            [etcd]
            endpoints = ["http://127.0.0.1:2379"]
            "#,
        )
        .unwrap();
        assert!(config.tls.is_none());
    }

    #[test]
    fn rejects_mixed_modes() {
        let error = Config::parse(
            r#"
            listen = ["tcp://0.0.0.0:36116"]
            [etcd]
            endpoints = ["https://127.0.0.1:2379"]
            "#,
        );
        assert!(error.is_err());
    }
}
