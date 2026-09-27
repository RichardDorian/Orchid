//! OCI images: references, manifests and configurations.

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Normalizes an image reference the way Docker does:
/// `nginx` -> `docker.io/library/nginx:latest`,
/// `acme/app` -> `docker.io/acme/app:latest`,
/// `ghcr.io/acme/app` -> `ghcr.io/acme/app:latest`.
pub fn normalize(reference: &str) -> String {
    let (first, rest) = match reference.split_once('/') {
        Some((first, rest)) => (first, Some(rest)),
        None => (reference, None),
    };
    let has_domain = rest.is_some() && (first.contains(['.', ':']) || first == "localhost");
    let mut name = match (has_domain, rest) {
        (true, _) => reference.to_owned(),
        (false, Some(_)) => format!("docker.io/{reference}"),
        (false, None) => format!("docker.io/library/{reference}"),
    };
    // A tag is a ':' after the last '/', a digest contains '@'.
    let last = name.rsplit('/').next().unwrap_or_default();
    if !last.contains(':') && !name.contains('@') {
        name.push_str(":latest");
    }
    name
}

/// The OCI platform of this machine (`amd64`, `arm64`...).
pub fn architecture() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "powerpc64" => "ppc64le",
        "s390x" => "s390x",
        "riscv64" => "riscv64",
        other => other,
    }
}

pub const INDEX_MEDIA_TYPES: [&str; 2] = [
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
];

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Descriptor {
    #[serde(rename = "mediaType", default)]
    pub media_type: String,
    pub digest: String,
    #[serde(default)]
    pub platform: Option<Platform>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Platform {
    pub os: String,
    pub architecture: String,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Index {
    pub manifests: Vec<Descriptor>,
}

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub config: Descriptor,
}

/// The configuration of an image.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct ImageConfig {
    #[serde(default)]
    pub config: RunConfig,
    pub rootfs: RootFs,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct RunConfig {
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default)]
    pub cmd: Option<Vec<String>>,
    #[serde(default)]
    pub env: Option<Vec<String>>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct RootFs {
    pub diff_ids: Vec<String>,
}

impl ImageConfig {
    /// Entrypoint followed by the command.
    pub fn args(&self) -> Vec<String> {
        let config = &self.config;
        config
            .entrypoint
            .iter()
            .flatten()
            .chain(config.cmd.iter().flatten())
            .cloned()
            .collect()
    }

    /// Chain ID of the layers: the name of the unpacked snapshot of the image.
    pub fn chain_id(&self) -> Option<String> {
        let mut layers = self.rootfs.diff_ids.iter();
        let mut chain = layers.next()?.clone();
        for diff_id in layers {
            let digest = Sha256::digest(format!("{chain} {diff_id}"));
            chain = format!("sha256:{}", hex(&digest));
        }
        Some(chain)
    }
}

/// The manifest of an index matching this machine.
pub fn select_manifest(index: &Index) -> Option<&Descriptor> {
    index.manifests.iter().find(|m| {
        m.platform
            .as_ref()
            .is_some_and(|p| p.os == "linux" && p.architecture == architecture())
    })
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_references() {
        let cases = [
            ("nginx", "docker.io/library/nginx:latest"),
            ("nginx:1.27", "docker.io/library/nginx:1.27"),
            ("acme/app", "docker.io/acme/app:latest"),
            ("ghcr.io/acme/my-app:latest", "ghcr.io/acme/my-app:latest"),
            ("localhost/app", "localhost/app:latest"),
            ("registry:5000/app", "registry:5000/app:latest"),
            ("registry.k8s.io/pause:3.10", "registry.k8s.io/pause:3.10"),
            (
                "nginx@sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "docker.io/library/nginx@sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(normalize(input), expected, "{input}");
        }
    }

    #[test]
    fn computes_chain_ids() {
        let single = ImageConfig {
            rootfs: RootFs {
                diff_ids: vec!["sha256:aaaa".into()],
            },
            ..Default::default()
        };
        assert_eq!(single.chain_id().as_deref(), Some("sha256:aaaa"));

        let two = ImageConfig {
            rootfs: RootFs {
                diff_ids: vec!["sha256:aaaa".into(), "sha256:bbbb".into()],
            },
            ..Default::default()
        };
        let expected = format!("sha256:{}", hex(&Sha256::digest("sha256:aaaa sha256:bbbb")));
        assert_eq!(two.chain_id(), Some(expected));
        assert_eq!(ImageConfig::default().chain_id(), None);
    }

    #[test]
    fn parses_image_configurations() {
        let config: ImageConfig = serde_json::from_str(
            r#"{
                "architecture": "amd64",
                "config": {
                    "Entrypoint": ["/docker-entrypoint.sh"],
                    "Cmd": ["nginx", "-g", "daemon off;"],
                    "Env": ["PATH=/usr/bin"],
                    "WorkingDir": "/app"
                },
                "rootfs": {"type": "layers", "diff_ids": ["sha256:aaaa"]}
            }"#,
        )
        .unwrap();
        assert_eq!(
            config.args(),
            ["/docker-entrypoint.sh", "nginx", "-g", "daemon off;"]
        );
        assert_eq!(config.config.working_dir.as_deref(), Some("/app"));
    }

    #[test]
    fn selects_the_manifest_of_the_platform() {
        let index: Index = serde_json::from_str(&format!(
            r#"{{"manifests": [
                {{"mediaType": "m", "digest": "sha256:other", "platform": {{"os": "linux", "architecture": "s390x-nope"}}}},
                {{"mediaType": "m", "digest": "sha256:ours", "platform": {{"os": "linux", "architecture": "{}"}}}}
            ]}}"#,
            architecture()
        ))
        .unwrap();
        assert_eq!(select_manifest(&index).unwrap().digest, "sha256:ours");
    }
}
