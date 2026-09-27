use std::io::Write;

use orchid_proto::v1 as pb;

use crate::cli::DrainArgs;
use crate::{Context, Result};

pub async fn set_schedulable(
    ctx: &Context<'_>,
    node: &str,
    schedulable: bool,
    out: &mut dyn Write,
) -> Result {
    ctx.nodes()
        .update_node_spec(pb::UpdateNodeSpecRequest {
            name: node.to_owned(),
            schedulable: Some(schedulable),
            draining: None,
            revision: None,
        })
        .await?;
    let action = if schedulable {
        "uncordoned"
    } else {
        "cordoned"
    };
    writeln!(out, "node/{node} {action}")?;
    Ok(())
}

pub async fn drain(ctx: &Context<'_>, args: DrainArgs, out: &mut dyn Write) -> Result {
    let node = &args.node;
    ctx.nodes()
        .update_node_spec(pb::UpdateNodeSpecRequest {
            name: node.clone(),
            schedulable: None,
            draining: Some(true),
            revision: None,
        })
        .await?;
    writeln!(out, "node/{node} draining")?;
    if args.no_wait {
        return Ok(());
    }

    // The controller clears `draining` once no pod is bound to the node.
    let deadline = tokio::time::Instant::now() + args.timeout;
    let mut reported = None;
    loop {
        let current = ctx
            .node(node)
            .await?
            .ok_or_else(|| format!("node {node} was deleted"))?;
        if !current.spec.draining {
            break;
        }
        let remaining = ctx.list_pods(Some(node.clone())).await?.len();
        if reported != Some(remaining) {
            writeln!(out, "evicting {remaining} pod(s)")?;
            out.flush()?;
            reported = Some(remaining);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for node {node} to be drained").into());
        }
        tokio::time::sleep(super::POLL_INTERVAL).await;
    }
    writeln!(out, "node/{node} drained")?;
    Ok(())
}
