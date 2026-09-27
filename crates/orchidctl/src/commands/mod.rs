//! The commands.

mod create;
mod delete;
mod describe;
mod get;
mod nodes;
mod set;
mod wait;

use std::io::Write;
use std::time::Duration;

use orchid_api::{Node, Pod};
use orchid_proto::v1 as pb;
use orchid_proto::v1::node_service_client::NodeServiceClient;
use orchid_proto::v1::pod_service_client::PodServiceClient;
use orchid_transport::client::Connection;
use tonic::Code;

use crate::cli::Command;
use crate::{Context, Result};

/// How often waiting commands check the state of the cluster.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run(command: Command, connection: &Connection, out: &mut dyn Write) -> Result {
    let ctx = Context { connection };
    match command {
        Command::Get(args) => get::run(&ctx, args, out).await,
        Command::Describe(args) => describe::run(&ctx, args, out).await,
        Command::Create(args) => create::create(&ctx, args, out).await,
        Command::Apply(args) => create::apply(&ctx, args, out).await,
        Command::Run(args) => create::run(&ctx, args, out).await,
        Command::Delete(args) => delete::run(&ctx, args, out).await,
        Command::Cordon(args) => nodes::set_schedulable(&ctx, &args.node, false, out).await,
        Command::Uncordon(args) => nodes::set_schedulable(&ctx, &args.node, true, out).await,
        Command::Drain(args) => nodes::drain(&ctx, args, out).await,
        Command::Wait(args) => wait::run(&ctx, args, out).await,
        Command::Set(args) => set::run(&ctx, args, out).await,
    }
}

impl Context<'_> {
    fn pods(&self) -> PodServiceClient<Connection> {
        PodServiceClient::new(self.connection.clone())
    }

    fn nodes(&self) -> NodeServiceClient<Connection> {
        NodeServiceClient::new(self.connection.clone())
    }

    /// The pod, `None` if it doesn't exist.
    async fn pod(&self, name: &str) -> Result<Option<Pod>> {
        match self
            .pods()
            .get_pod(pb::GetPodRequest {
                name: name.to_owned(),
            })
            .await
        {
            Ok(response) => Ok(Some(Pod::try_from(response.into_inner())?)),
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(status) => Err(status.into()),
        }
    }

    async fn list_pods(&self, node: Option<String>) -> Result<Vec<Pod>> {
        let response = self
            .pods()
            .list_pods(pb::ListPodsRequest {
                filter: node.map(|node| pb::PodFilter {
                    node: Some(node),
                    unbound: false,
                }),
            })
            .await?
            .into_inner();
        Ok(response
            .pods
            .into_iter()
            .map(Pod::try_from)
            .collect::<Result<_, _>>()?)
    }

    /// The node, `None` if it doesn't exist.
    async fn node(&self, name: &str) -> Result<Option<Node>> {
        match self
            .nodes()
            .get_node(pb::GetNodeRequest {
                name: name.to_owned(),
            })
            .await
        {
            Ok(response) => Ok(Some(Node::try_from(response.into_inner())?)),
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(status) => Err(status.into()),
        }
    }

    async fn list_nodes(&self) -> Result<Vec<Node>> {
        let response = self
            .nodes()
            .list_nodes(pb::ListNodesRequest {})
            .await?
            .into_inner();
        Ok(response
            .nodes
            .into_iter()
            .map(Node::try_from)
            .collect::<Result<_, _>>()?)
    }
}

/// Polls `check` until it returns `Some`, failing after `timeout`.
async fn poll<T, F>(timeout: Duration, what: &str, mut check: impl FnMut() -> F) -> Result<T>
where
    F: Future<Output = Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = check().await? {
            return Ok(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for {what}").into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
