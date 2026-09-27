//! Configuration file of Keiki.

use std::path::{Path, PathBuf};

use ipnet::IpNet;
use orchid_api::{NodeRole, Resources};
use orchid_transport::tls::TlsConfig;
use orchid_transport::url::parse_server_urls;
use serde::Deserialize;

use crate::agent::Registration;
use crate::runtime::containerd::ContainerdOptions;

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
    /// Name of the node. Defaults to the hostname in cleartext mode, to the
    /// certificate name in mTLS mode.
    pub name: Option<String>,
    pub role: NodeRole,
    /// Only applied when the node registers for the first time.
    pub schedulable: Option<bool>,
    pub pod_cidr: IpNet,
    /// Full containerd names of the runtimes of the node.
    pub runtimes: Vec<String>,
    /// Path of the containerd socket.
    pub containerd: PathBuf,
    /// containerd namespace of the pods. Agents sharing a containerd must use
    /// different namespaces.
    #[serde(default = "default_namespace")]
    pub containerd_namespace: String,
    /// Image of the pod sandboxes.
    #[serde(default = "default_pause_image")]
    pub pause_image: String,
    /// Directory of the container logs.
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
    /// URLs of the Labellum instances.
    pub labellum: Vec<String>,
    pub capacity: Resources,
    /// Presence enables mTLS mode.
    pub tls: Option<TlsConfig>,
}

fn default_namespace() -> String {
    "orchid".to_owned()
}

fn default_pause_image() -> String {
    "registry.k8s.io/pause:3.10".to_owned()
}

fn default_log_dir() -> PathBuf {
    PathBuf::from("/var/log/orchid")
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
        config.registration().info_errors()?;
        Ok(config)
    }

    pub fn registration(&self) -> Registration {
        let node = match (&self.name, &self.tls) {
            (Some(name), _) => Some(name.clone()),
            // The certificate names the node.
            (None, Some(_)) => None,
            (None, None) => Some(gethostname::gethostname().to_string_lossy().into_owned()),
        };
        Registration {
            node,
            role: self.role,
            schedulable: self.schedulable,
            runtimes: self.runtimes.clone(),
            pod_cidr: self.pod_cidr,
            capacity: self.capacity,
        }
    }

    pub fn containerd_options(&self) -> ContainerdOptions {
        ContainerdOptions {
            namespace: self.containerd_namespace.clone(),
            pause_image: self.pause_image.clone(),
            log_dir: self.log_dir.clone(),
            ..ContainerdOptions::new(self.containerd.clone())
        }
    }
}

impl Registration {
    fn info_errors(&self) -> Result<(), ConfigError> {
        let info = orchid_api::NodeInfo {
            role: self.role,
            runtimes: self.runtimes.clone(),
            pod_cidr: self.pod_cidr,
            capacity: self.capacity,
        };
        info.validate()
            .map_err(|e| ConfigError::Invalid(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_example_configuration() {
        let config = Config::parse(include_str!("../../../configs/keiki.toml")).unwrap();
        assert_eq!(config.capacity.cpu, orchid_api::MilliCpu(12_000));
        assert_eq!(config.runtimes.len(), 3);
        // mTLS without a name: the certificate names the node.
        assert_eq!(config.registration().node, None);
    }

    #[test]
    fn rejects_invalid_node_information() {
        let config = r#"
            role = "worker"
            pod_cidr = "10.244.0.5/24"
            runtimes = ["io.containerd.runc.v2"]
            containerd = "/run/containerd/containerd.sock"
            labellum = ["http://10.0.0.1:36116"]
            [capacity]
            cpu = "4"
            memory = "8Gi"
        "#;
        assert!(Config::parse(config).is_err());
    }
}
