//! Key layout of the cluster in etcd.
//!
//! ```text
//! /orchid/config/cluster              cluster configuration
//! /orchid/leader/<election>           leader keys (controller, ikebana)
//! /orchid/cluster/pod-cidrs           map node name -> pod CIDR
//!
//! /orchid/nodes/<name>/spec           schedulable, draining
//! /orchid/nodes/<name>/info           role, runtimes, pod CIDR, capacity
//! /orchid/nodes/<name>/lease          heartbeat key (bound to a lease)
//! /orchid/nodes/<name>/status         health, usage, last heartbeat
//! /orchid/nodes/<name>/allocated      sum of resources of bound pods
//!
//! /orchid/pods/<name>/spec            user desired state
//! /orchid/pods/<name>/binding         node + attempt
//! /orchid/pods/<name>/status          phase, reason, containers
//! ```
//!
//! Object prefixes end with a `/`, so the prefix of `my-app` doesn't match `my-app-2`.

pub const ROOT: &str = "/orchid/";
pub const CLUSTER_CONFIG: &str = "/orchid/config/cluster";
pub const POD_CIDRS: &str = "/orchid/cluster/pod-cidrs";
pub const LEADERS: &str = "/orchid/leader/";
pub const NODES: &str = "/orchid/nodes/";
pub const PODS: &str = "/orchid/pods/";

/// Name of the Labellum controller election.
pub const CONTROLLER_ELECTION: &str = "controller";
/// Name of the Ikebana election.
pub const IKEBANA_ELECTION: &str = "ikebana";

pub fn leader(election: &str) -> String {
    format!("{LEADERS}{election}")
}

/// Keys of the node `name`.
pub mod node {
    use super::NODES;

    /// Prefix of every key of the node.
    pub fn prefix(name: &str) -> String {
        format!("{NODES}{name}/")
    }

    pub fn spec(name: &str) -> String {
        format!("{NODES}{name}/spec")
    }

    pub fn info(name: &str) -> String {
        format!("{NODES}{name}/info")
    }

    pub fn lease(name: &str) -> String {
        format!("{NODES}{name}/lease")
    }

    pub fn status(name: &str) -> String {
        format!("{NODES}{name}/status")
    }

    pub fn allocated(name: &str) -> String {
        format!("{NODES}{name}/allocated")
    }
}

/// Keys of the pod `name`.
pub mod pod {
    use super::PODS;

    /// Prefix of every key of the pod.
    pub fn prefix(name: &str) -> String {
        format!("{PODS}{name}/")
    }

    pub fn spec(name: &str) -> String {
        format!("{PODS}{name}/spec")
    }

    pub fn binding(name: &str) -> String {
        format!("{PODS}{name}/binding")
    }

    pub fn status(name: &str) -> String {
        format!("{PODS}{name}/status")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeKey {
    Spec,
    Info,
    Lease,
    Status,
    Allocated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PodKey {
    Spec,
    Binding,
    Status,
}

/// A parsed key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Key<'a> {
    ClusterConfig,
    PodCidrs,
    Leader(&'a str),
    Node(&'a str, NodeKey),
    Pod(&'a str, PodKey),
}

/// Parses a key of the layout. `None` for unknown keys.
pub fn parse(key: &str) -> Option<Key<'_>> {
    match key {
        CLUSTER_CONFIG => return Some(Key::ClusterConfig),
        POD_CIDRS => return Some(Key::PodCidrs),
        _ => {}
    }
    if let Some(election) = key.strip_prefix(LEADERS) {
        return (!election.is_empty() && !election.contains('/')).then_some(Key::Leader(election));
    }
    if let Some(rest) = key.strip_prefix(NODES) {
        let (name, part) = split_object(rest)?;
        let part = match part {
            "spec" => NodeKey::Spec,
            "info" => NodeKey::Info,
            "lease" => NodeKey::Lease,
            "status" => NodeKey::Status,
            "allocated" => NodeKey::Allocated,
            _ => return None,
        };
        return Some(Key::Node(name, part));
    }
    if let Some(rest) = key.strip_prefix(PODS) {
        let (name, part) = split_object(rest)?;
        let part = match part {
            "spec" => PodKey::Spec,
            "binding" => PodKey::Binding,
            "status" => PodKey::Status,
            _ => return None,
        };
        return Some(Key::Pod(name, part));
    }
    None
}

fn split_object(rest: &str) -> Option<(&str, &str)> {
    let (name, part) = rest.split_once('/')?;
    (!name.is_empty() && !part.contains('/')).then_some((name, part))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_keys() {
        assert_eq!(node::lease("phoenix"), "/orchid/nodes/phoenix/lease");
        assert_eq!(pod::binding("my-app"), "/orchid/pods/my-app/binding");
        assert_eq!(pod::prefix("my-app"), "/orchid/pods/my-app/");
        assert_eq!(leader(IKEBANA_ELECTION), "/orchid/leader/ikebana");
    }

    #[test]
    fn parses_keys() {
        assert_eq!(parse(CLUSTER_CONFIG), Some(Key::ClusterConfig));
        assert_eq!(parse(POD_CIDRS), Some(Key::PodCidrs));
        assert_eq!(
            parse(&leader("controller")),
            Some(Key::Leader("controller"))
        );
        assert_eq!(
            parse(&node::allocated("phoenix")),
            Some(Key::Node("phoenix", NodeKey::Allocated))
        );
        assert_eq!(
            parse(&pod::status("my-app")),
            Some(Key::Pod("my-app", PodKey::Status))
        );
    }

    #[test]
    fn rejects_unknown_keys() {
        for key in [
            "/orchid/pods/my-app/other",
            "/orchid/pods/my-app",
            "/orchid/pods//spec",
            "/orchid/pods/a/b/spec",
            "/orchid/leader/",
            "/other",
        ] {
            assert_eq!(parse(key), None, "{key}");
        }
    }

    #[test]
    fn keys_round_trip() {
        for name in ["a", "my-app-2"] {
            assert_eq!(
                parse(&node::spec(name)),
                Some(Key::Node(name, NodeKey::Spec))
            );
            assert_eq!(parse(&pod::spec(name)), Some(Key::Pod(name, PodKey::Spec)));
        }
    }
}
