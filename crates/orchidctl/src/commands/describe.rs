use std::io::Write;

use orchid_api::{ContainerState, Node, Pod, RestartPolicy, Timestamp};

use crate::cli::{DescribeArgs, Kind};
use crate::output::{self, Table};
use crate::{Context, Result};

pub async fn run(ctx: &Context<'_>, args: DescribeArgs, out: &mut dyn Write) -> Result {
    match args.kind {
        Kind::Pod => {
            let pod = ctx
                .pod(&args.name)
                .await?
                .ok_or_else(|| format!("pod {} not found", args.name))?;
            describe_pod(&pod, out)
        }
        Kind::Node => {
            let node = ctx
                .node(&args.name)
                .await?
                .ok_or_else(|| format!("node {} not found", args.name))?;
            let pods = ctx.list_pods(Some(node.name.clone())).await?;
            describe_node(&node, &pods, out)
        }
        Kind::ClusterConfig => Err("use `orchidctl get cluster-config`".into()),
    }
}

fn field(out: &mut dyn Write, indent: usize, name: &str, value: impl std::fmt::Display) -> Result {
    let label = format!("{name}:");
    writeln!(out, "{:indent$}{label:<18}{value}", "")?;
    Ok(())
}

fn when(timestamp: Timestamp) -> String {
    format!("{timestamp} ({} ago)", output::age(timestamp))
}

fn describe_pod(pod: &Pod, out: &mut dyn Write) -> Result {
    let spec = &pod.spec;
    let status = &pod.status;
    field(out, 0, "Name", &pod.name)?;
    field(out, 0, "UID", &pod.uid)?;
    let node = match &pod.binding {
        Some(binding) if binding.node().is_some() => {
            format!("{} (attempt {})", binding.node, binding.attempt)
        }
        Some(binding) => format!("<none> (last attempt {})", binding.attempt),
        None => "<none>".to_owned(),
    };
    field(out, 0, "Node", node)?;
    field(out, 0, "Priority", spec.priority)?;
    field(out, 0, "Runtime", &spec.runtime)?;
    let policy = match spec.restart_policy {
        RestartPolicy::Always => "Always",
        RestartPolicy::Failure => "OnFailure",
        RestartPolicy::Never => "Never",
    };
    field(out, 0, "Restart policy", policy)?;
    field(
        out,
        0,
        "Grace period",
        humantime::format_duration(spec.termination_grace_period),
    )?;
    field(out, 0, "Created", when(pod.created_at))?;
    field(out, 0, "Phase", output::phase(status.phase))?;
    if !status.reason.is_empty() {
        field(out, 0, "Reason", &status.reason)?;
    }
    if !status.message.is_empty() {
        field(out, 0, "Message", &status.message)?;
    }
    field(out, 0, "Pending since", when(status.pending_since))?;
    if let Some(requested) = status.deletion_requested_at {
        field(out, 0, "Deletion", format!("requested {}", when(requested)))?;
    }

    writeln!(out, "Containers:")?;
    for container in &spec.containers {
        writeln!(out, "  {}:", container.name)?;
        field(out, 4, "Image", &container.image)?;
        let (cpu, memory) = output::resources(&container.resources);
        field(out, 4, "CPU", cpu)?;
        field(out, 4, "Memory", memory)?;
        match status.containers.iter().find(|c| c.name == container.name) {
            Some(state) => {
                let description = match (state.state, state.exit_code) {
                    (ContainerState::Running, _) => "Running".to_owned(),
                    (ContainerState::Waiting, Some(code)) => {
                        format!("Waiting (last exit code {code})")
                    }
                    (ContainerState::Waiting, None) => "Waiting".to_owned(),
                    (ContainerState::Exited, code) => {
                        format!(
                            "Exited (code {})",
                            code.map_or("?".to_owned(), |c| c.to_string())
                        )
                    }
                };
                field(out, 4, "State", description)?;
                if let Some(started) = state.started_at {
                    field(out, 4, "Started", when(started))?;
                }
                field(out, 4, "Restarts", state.restart_count)?;
            }
            None => field(out, 4, "State", "<unknown>")?,
        }
    }
    Ok(())
}

fn describe_node(node: &Node, pods: &[Pod], out: &mut dyn Write) -> Result {
    let info = &node.info;
    let status = &node.status;
    field(out, 0, "Name", &node.name)?;
    field(out, 0, "UID", &node.uid)?;
    field(
        out,
        0,
        "Role",
        output::node_row(node, crate::cli::Format::Table)[2].clone(),
    )?;
    field(out, 0, "Status", output::node_status(node))?;
    if !status.message.is_empty() {
        field(out, 0, "Message", &status.message)?;
    }
    field(out, 0, "Schedulable", node.spec.schedulable)?;
    field(out, 0, "Draining", node.spec.draining)?;
    field(out, 0, "Pod CIDR", info.pod_cidr)?;
    field(out, 0, "Runtimes", info.runtimes.join(", "))?;
    match status.last_heartbeat {
        Some(heartbeat) => field(out, 0, "Last heartbeat", when(heartbeat))?,
        None => field(out, 0, "Last heartbeat", "<none>")?,
    }
    writeln!(out, "Resources:")?;
    let mut table = Table::new(&["", "ALLOCATED", "USAGE", "CAPACITY"]);
    table.row(vec![
        "cpu".into(),
        output::cpu_usage(status.allocated.cpu, info.capacity.cpu),
        status.usage.cpu.to_string(),
        info.capacity.cpu.to_string(),
    ]);
    table.row(vec![
        "memory".into(),
        output::memory_usage(status.allocated.memory, info.capacity.memory),
        output::approximate_memory(status.usage.memory),
        info.capacity.memory.to_string(),
    ]);
    write_indented(&table, out)?;

    writeln!(out, "Pods ({}):", pods.len())?;
    if !pods.is_empty() {
        let mut table = Table::new(&["NAME", "PHASE", "CPU", "MEMORY", "AGE"]);
        for pod in pods {
            let (cpu, memory) = output::resources(&pod.spec.resources().unwrap_or_default());
            table.row(vec![
                pod.name.clone(),
                output::phase(pod.status.phase),
                cpu,
                memory,
                output::age(pod.created_at),
            ]);
        }
        write_indented(&table, out)?;
    }
    Ok(())
}

fn write_indented(table: &Table, out: &mut dyn Write) -> Result {
    let mut rendered = Vec::new();
    table.write(&mut rendered)?;
    for line in String::from_utf8_lossy(&rendered).lines() {
        writeln!(out, "  {line}")?;
    }
    Ok(())
}
