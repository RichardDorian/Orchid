use std::io::Write;
use std::time::Duration;

use orchid_proto::v1 as pb;

use super::poll;
use crate::cli::{DeleteArgs, Kind};
use crate::manifest;
use crate::{Context, Result};

pub async fn run(ctx: &Context<'_>, args: DeleteArgs, out: &mut dyn Write) -> Result {
    let (kind, mut names) = match (args.kind, args.files.is_empty()) {
        (Some(_), false) => {
            return Err("give either a resource type and names, or --filename".into());
        }
        (Some(kind), true) => (kind, args.names),
        (None, false) => (Kind::Pod, Vec::new()),
        (None, true) => return Err("give a resource type and names, or --filename".into()),
    };
    for path in &args.files {
        names.push(manifest::read(path)?.name);
    }
    if names.is_empty() {
        return Err("no resource to delete".into());
    }

    match kind {
        Kind::Pod => {
            let mut deleted = Vec::new();
            let mut failures = Vec::new();
            for name in names {
                let request = pb::DeletePodRequest {
                    name: name.clone(),
                    uid: None,
                };
                match ctx.pods().delete_pod(request).await {
                    Ok(_) => {
                        writeln!(out, "pod/{name} deleted")?;
                        deleted.push(name);
                    }
                    Err(status) => {
                        failures.push(format!("pod/{name}: {}", crate::Error::from(status)))
                    }
                }
            }
            if !args.no_wait {
                wait_gone(ctx, &deleted, args.timeout).await?;
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("\n").into())
            }
        }
        Kind::Node => {
            for name in names {
                ctx.nodes()
                    .delete_node(pb::DeleteNodeRequest {
                        name: name.clone(),
                        uid: None,
                    })
                    .await?;
                writeln!(out, "node/{name} deleted")?;
            }
            Ok(())
        }
        Kind::ClusterConfig => Err("the cluster configuration cannot be deleted".into()),
    }
}

/// Waits until none of the pods exists anymore.
pub async fn wait_gone(ctx: &Context<'_>, names: &[String], timeout: Duration) -> Result {
    poll(timeout, "the pods to be deleted", || async move {
        for name in names {
            if ctx.pod(name).await?.is_some() {
                return Ok(None);
            }
        }
        Ok(Some(()))
    })
    .await
}
