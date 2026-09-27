//! Pod files: the format of `resources/pod.toml` and of `get pod -o toml`.
//!
//! Only the name and the spec are read, read only fields are ignored.

use std::path::Path;

use orchid_api::PodSpec;
use serde::Deserialize;

use crate::Result;

#[derive(Clone, Debug, Deserialize)]
pub struct PodManifest {
    pub name: String,
    pub spec: PodSpec,
}

pub fn read(path: &Path) -> Result<PodManifest> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    parse(&content).map_err(|e| format!("{}: {e}", path.display()).into())
}

pub fn parse(content: &str) -> Result<PodManifest, String> {
    let manifest: PodManifest = toml::from_str(content).map_err(|e| e.to_string())?;
    orchid_api::validate_name(&manifest.name).map_err(|reason| format!("name: {reason}"))?;
    manifest.spec.validate().map_err(|e| format!("spec: {e}"))?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_example_pod() {
        let manifest = parse(include_str!("../../../resources/pod.toml")).unwrap();
        assert_eq!(manifest.name, "my-app");
        assert_eq!(manifest.spec.runtime, "io.containerd.runsc.v1");
        assert_eq!(
            manifest.spec.containers[0].image,
            "ghcr.io/acme/my-app:latest"
        );
    }

    #[test]
    fn needs_a_valid_spec() {
        assert!(parse("name = \"x\"\n[spec]\ncontainer = []\n").is_err());
        assert!(parse("name = \"Bad_Name\"\n[spec]\n[[spec.container]]\nname = \"a\"\nimage = \"b\"\nresources = { cpu = \"1\", memory = \"1Mi\" }\n").is_err());
    }
}
