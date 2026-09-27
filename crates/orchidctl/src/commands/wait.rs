use std::io::Write;

use orchid_api::{NodeCondition, PodPhase};

use super::poll;
use crate::cli::{Kind, WaitArgs};
use crate::{Context, Result};

enum Condition {
    Deleted,
    Phase(PodPhase),
    NodeCondition(NodeCondition),
}

fn parse(kind: Kind, condition: &str) -> Result<Condition> {
    let lower = condition.to_ascii_lowercase();
    if lower == "delete" {
        return Ok(Condition::Deleted);
    }
    match (kind, lower.split_once('=')) {
        (Kind::Pod, Some(("phase", phase))) => {
            let phase = match phase {
                "pending" => PodPhase::Pending,
                "creating" => PodPhase::Creating,
                "running" => PodPhase::Running,
                "succeeded" => PodPhase::Succeeded,
                "failed" => PodPhase::Failed,
                "terminating" => PodPhase::Terminating,
                _ => return Err(format!("unknown phase {phase:?}").into()),
            };
            Ok(Condition::Phase(phase))
        }
        (Kind::Node, Some(("condition", value))) => {
            let condition = match value {
                "ready" => NodeCondition::Ready,
                "unhealthy" => NodeCondition::Unhealthy,
                "unreachable" => NodeCondition::Unreachable,
                _ => return Err(format!("unknown node condition {value:?}").into()),
            };
            Ok(Condition::NodeCondition(condition))
        }
        _ => Err(format!(
            "invalid condition {condition:?}: expected `delete`, `phase=<phase>` for pods or `condition=<condition>` for nodes"
        )
        .into()),
    }
}

pub async fn run(ctx: &Context<'_>, args: WaitArgs, out: &mut dyn Write) -> Result {
    if args.kind == Kind::ClusterConfig {
        return Err("only pods and nodes can be waited for".into());
    }
    let condition = parse(args.kind, &args.condition)?;
    let name = &args.name;
    let what = format!("{} {name} to meet {}", kind_name(args.kind), args.condition);
    poll(args.timeout, &what, || {
        let condition = &condition;
        async move {
            let met = match (args.kind, condition) {
                (Kind::Pod, Condition::Deleted) => ctx.pod(name).await?.is_none(),
                (Kind::Pod, Condition::Phase(phase)) => ctx
                    .pod(name)
                    .await?
                    .is_some_and(|pod| pod.status.phase == *phase),
                (Kind::Node, Condition::Deleted) => ctx.node(name).await?.is_none(),
                (Kind::Node, Condition::NodeCondition(wanted)) => ctx
                    .node(name)
                    .await?
                    .is_some_and(|node| node.status.condition == *wanted),
                _ => unreachable!("conditions are checked against the kind"),
            };
            Ok(met.then_some(()))
        }
    })
    .await?;
    writeln!(out, "{}/{name} condition met", kind_name(args.kind))?;
    Ok(())
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Pod => "pod",
        Kind::Node => "node",
        Kind::ClusterConfig => "cluster-config",
    }
}
