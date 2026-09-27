use std::io::Write;

use orchid_api::proto::duration_to_proto;
use orchid_proto::v1 as pb;
use orchid_proto::v1::cluster_service_client::ClusterServiceClient;

use crate::cli::{Kind, SetArgs, parse_duration};
use crate::{Context, Result};

pub async fn run(ctx: &Context<'_>, args: SetArgs, out: &mut dyn Write) -> Result {
    if args.kind != Kind::ClusterConfig {
        return Err(
            "only the cluster configuration can be set, use cordon, drain or apply for the rest"
                .into(),
        );
    }
    let mut request = pb::UpdateClusterConfigRequest::default();
    for assignment in &args.assignments {
        let (key, value) = assignment
            .split_once('=')
            .ok_or_else(|| format!("expected KEY=VALUE, got {assignment:?}"))?;
        let duration = || {
            parse_duration(value)
                .map(duration_to_proto)
                .map_err(|e| format!("{key}: {e}"))
        };
        match key {
            "default_runtime" => request.default_runtime = Some(value.to_owned()),
            "node_lease_ttl" => request.node_lease_ttl = Some(duration()?),
            "pod_eviction_delay" => request.pod_eviction_delay = Some(duration()?),
            "leader_lease_ttl" => request.leader_lease_ttl = Some(duration()?),
            _ => {
                return Err(format!(
                    "unknown key {key:?}, expected default_runtime, node_lease_ttl, pod_eviction_delay or leader_lease_ttl"
                )
                .into());
            }
        }
    }
    ClusterServiceClient::new(ctx.connection.clone())
        .update_cluster_config(request)
        .await?;
    writeln!(out, "cluster-config updated")?;
    Ok(())
}
