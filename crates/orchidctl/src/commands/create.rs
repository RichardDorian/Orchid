use std::io::Write;

use orchid_api::{Bytes, Container, MilliCpu, PodSpec, Resources, RestartPolicy};
use orchid_proto::v1 as pb;

use super::delete;
use crate::cli::{ApplyArgs, CreateArgs, Restart, RunArgs};
use crate::manifest::{self, PodManifest};
use crate::{Context, Result};

async fn create_pod(ctx: &Context<'_>, name: &str, spec: PodSpec) -> Result {
    ctx.pods()
        .create_pod(pb::CreatePodRequest {
            name: name.to_owned(),
            spec: Some(spec.into()),
        })
        .await?;
    Ok(())
}

/// Runs `apply` on every file, reporting every failure.
async fn each_file<F>(
    files: &[std::path::PathBuf],
    out: &mut dyn Write,
    mut apply: impl FnMut(PodManifest) -> F,
) -> Result
where
    F: Future<Output = Result<String>>,
{
    let mut failures = Vec::new();
    for path in files {
        let result = match manifest::read(path) {
            Ok(manifest) => apply(manifest).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(message) => writeln!(out, "{message}")?,
            Err(error) => failures.push(error.to_string()),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n").into())
    }
}

pub async fn create(ctx: &Context<'_>, args: CreateArgs, out: &mut dyn Write) -> Result {
    each_file(&args.files, out, |manifest| async move {
        create_pod(ctx, &manifest.name, manifest.spec)
            .await
            .map_err(|e| format!("pod/{}: {e}", manifest.name))?;
        Ok(format!("pod/{} created", manifest.name))
    })
    .await
}

pub async fn apply(ctx: &Context<'_>, args: ApplyArgs, out: &mut dyn Write) -> Result {
    let force = args.force;
    each_file(&args.files, out, |manifest| async move {
        apply_one(ctx, manifest, force).await
    })
    .await
}

async fn apply_one(ctx: &Context<'_>, manifest: PodManifest, force: bool) -> Result<String> {
    let name = manifest.name;
    let wanted = manifest.spec;
    let Some(existing) = ctx.pod(&name).await? else {
        create_pod(ctx, &name, wanted).await?;
        return Ok(format!("pod/{name} created"));
    };

    // An empty runtime means the cluster default, which the pod already has.
    let mut comparable = wanted.clone();
    if comparable.runtime.is_empty() {
        comparable.runtime.clone_from(&existing.spec.runtime);
    }
    comparable.priority = existing.spec.priority;
    if comparable == existing.spec {
        if wanted.priority == existing.spec.priority {
            return Ok(format!("pod/{name} unchanged"));
        }
        ctx.pods()
            .update_pod_priority(pb::UpdatePodPriorityRequest {
                name: name.clone(),
                priority: wanted.priority,
                revision: Some(existing.revision),
            })
            .await?;
        return Ok(format!("pod/{name} configured"));
    }

    if !force {
        return Err(format!(
            "pod/{name}: only the priority of a pod can change, use --force to delete and create it again"
        )
        .into());
    }
    ctx.pods()
        .delete_pod(pb::DeletePodRequest {
            name: name.clone(),
            uid: Some(existing.uid),
        })
        .await?;
    delete::wait_gone(
        ctx,
        std::slice::from_ref(&name),
        std::time::Duration::from_secs(120),
    )
    .await?;
    create_pod(ctx, &name, wanted).await?;
    Ok(format!("pod/{name} replaced"))
}

pub async fn run(ctx: &Context<'_>, args: RunArgs, out: &mut dyn Write) -> Result {
    let cpu: MilliCpu = args.cpu.parse().map_err(|e| format!("--cpu: {e}"))?;
    let memory: Bytes = args.memory.parse().map_err(|e| format!("--memory: {e}"))?;
    let spec = PodSpec {
        runtime: args.runtime.unwrap_or_default(),
        priority: args.priority,
        restart_policy: match args.restart {
            Restart::Always => RestartPolicy::Always,
            Restart::Failure => RestartPolicy::Failure,
            Restart::Never => RestartPolicy::Never,
        },
        termination_grace_period: args
            .grace_period
            .unwrap_or(orchid_api::DEFAULT_TERMINATION_GRACE_PERIOD),
        containers: vec![Container {
            name: args.name.clone(),
            image: args.image,
            resources: Resources::new(cpu, memory),
        }],
    };
    create_pod(ctx, &args.name, spec).await?;
    writeln!(out, "pod/{} created", args.name)?;
    Ok(())
}
