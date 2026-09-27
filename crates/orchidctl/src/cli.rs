//! Command line definition.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Command line client of Orchid.
#[derive(Debug, Parser)]
#[command(name = "orchidctl", version)]
pub struct Cli {
    /// Client configuration file [default: ~/.config/orchid/orchidctl.toml]
    #[arg(long, global = true, env = "ORCHID_CONFIG")]
    pub config: Option<PathBuf>,

    /// URL of a Labellum instance, overrides the configuration. Can be repeated.
    #[arg(short, long = "server", global = true, value_name = "URL")]
    pub servers: Vec<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Display one or many resources.
    Get(GetArgs),
    /// Show the details of a resource.
    Describe(DescribeArgs),
    /// Create pods from files.
    Create(CreateArgs),
    /// Create pods from files, or update the ones that exist.
    Apply(ApplyArgs),
    /// Create a pod running a single container.
    Run(RunArgs),
    /// Delete resources by name or from files.
    Delete(DeleteArgs),
    /// Mark a node as unschedulable.
    Cordon(NodeArgs),
    /// Mark a node as schedulable.
    Uncordon(NodeArgs),
    /// Evict every pod of a node and mark it unschedulable.
    Drain(DrainArgs),
    /// Wait for a condition on a resource.
    Wait(WaitArgs),
    /// Change the cluster configuration.
    Set(SetArgs),
}

/// A kind of resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Pod,
    Node,
    ClusterConfig,
}

impl FromStr for Kind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "pod" | "pods" | "po" => Ok(Self::Pod),
            "node" | "nodes" | "no" => Ok(Self::Node),
            "cluster-config" | "clusterconfig" | "cc" => Ok(Self::ClusterConfig),
            _ => Err(format!(
                "unknown resource type {s:?}, expected pods, nodes or cluster-config"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// Human readable table.
    #[default]
    Table,
    /// Table with more columns.
    Wide,
    /// TOML, the format of `create -f`.
    Toml,
    Json,
    /// Only the resource names.
    Name,
}

#[derive(Debug, Args)]
pub struct GetArgs {
    /// pods, nodes or cluster-config.
    pub kind: Kind,
    /// Names of the resources, every resource if empty.
    pub names: Vec<String>,
    /// Output format.
    #[arg(short, long, value_enum, default_value_t)]
    pub output: Format,
    /// After listing, keep printing changes.
    #[arg(short, long)]
    pub watch: bool,
    /// Only the pods bound to this node.
    #[arg(long)]
    pub node: Option<String>,
}

#[derive(Debug, Args)]
pub struct DescribeArgs {
    /// pod or node.
    pub kind: Kind,
    pub name: String,
}

#[derive(Debug, Args)]
pub struct CreateArgs {
    /// Pod file, in the format of `resources/pod.toml`. Can be repeated.
    #[arg(short, long = "filename", required = true, value_name = "FILE")]
    pub files: Vec<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ApplyArgs {
    /// Pod file, in the format of `resources/pod.toml`. Can be repeated.
    #[arg(short, long = "filename", required = true, value_name = "FILE")]
    pub files: Vec<PathBuf>,
    /// Delete and create again pods whose immutable fields changed.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Name of the pod, also used for its container.
    pub name: String,
    /// OCI image of the container.
    #[arg(long)]
    pub image: String,
    /// CPU reserved for the container.
    #[arg(long, default_value = "100m")]
    pub cpu: String,
    /// Memory reserved for the container.
    #[arg(long, default_value = "64Mi")]
    pub memory: String,
    #[arg(long, value_enum, default_value_t = Restart::Always)]
    pub restart: Restart,
    #[arg(long, default_value_t = 0, allow_negative_numbers = true)]
    pub priority: i32,
    /// Full containerd runtime name, the cluster default if not set.
    #[arg(long)]
    pub runtime: Option<String>,
    /// Time between SIGTERM and SIGKILL when stopping the container.
    #[arg(long, value_parser = parse_duration)]
    pub grace_period: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Restart {
    Always,
    Failure,
    Never,
}

#[derive(Debug, Args)]
pub struct DeleteArgs {
    /// pod or node. Not needed with --filename.
    pub kind: Option<Kind>,
    pub names: Vec<String>,
    /// Delete the pods defined in a file. Can be repeated.
    #[arg(short, long = "filename", value_name = "FILE")]
    pub files: Vec<PathBuf>,
    /// Return without waiting for the pods to be gone.
    #[arg(long)]
    pub no_wait: bool,
    #[arg(long, value_parser = parse_duration, default_value = "60s")]
    pub timeout: Duration,
}

#[derive(Debug, Args)]
pub struct NodeArgs {
    pub node: String,
}

#[derive(Debug, Args)]
pub struct DrainArgs {
    pub node: String,
    /// Return without waiting for the pods to be evicted.
    #[arg(long)]
    pub no_wait: bool,
    #[arg(long, value_parser = parse_duration, default_value = "5m")]
    pub timeout: Duration,
}

#[derive(Debug, Args)]
pub struct WaitArgs {
    /// pod or node.
    pub kind: Kind,
    pub name: String,
    /// `phase=<phase>` or `delete` for pods, `condition=<condition>` for nodes.
    #[arg(long = "for", value_name = "CONDITION")]
    pub condition: String,
    #[arg(long, value_parser = parse_duration, default_value = "60s")]
    pub timeout: Duration,
}

#[derive(Debug, Args)]
pub struct SetArgs {
    /// cluster-config.
    pub kind: Kind,
    /// `key=value` pairs, e.g. `default_runtime=io.containerd.runc.v2`.
    #[arg(required = true, value_name = "KEY=VALUE")]
    pub assignments: Vec<String>,
}

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}
