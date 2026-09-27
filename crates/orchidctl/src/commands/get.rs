use std::io::Write;

use orchid_api::{ClusterConfig, Node, Pod};
use orchid_client::informer::{self, Change, InformerEvent, NodeSource, PodSource};
use orchid_proto::v1 as pb;
use orchid_proto::v1::cluster_service_client::ClusterServiceClient;
use tokio::sync::mpsc;

use crate::cli::{Format, GetArgs, Kind};
use crate::output::{self, Table, WatchPrinter};
use crate::{Context, Result};

pub async fn run(ctx: &Context<'_>, args: GetArgs, out: &mut dyn Write) -> Result {
    if args.node.is_some() && args.kind != Kind::Pod {
        return Err("--node only applies to pods".into());
    }
    if args.watch && !matches!(args.output, Format::Table | Format::Wide) {
        return Err("--watch only supports the table outputs".into());
    }
    match args.kind {
        Kind::Pod if args.watch => watch_pods(ctx, args, out).await,
        Kind::Node if args.watch => watch_nodes(ctx, args, out).await,
        Kind::Pod => get_pods(ctx, args, out).await,
        Kind::Node => get_nodes(ctx, args, out).await,
        Kind::ClusterConfig => get_cluster_config(ctx, args, out).await,
    }
}

/// Fetches the named objects, or every object if no name is given.
async fn fetch<T, F>(
    names: &[String],
    kind: &str,
    all: impl Future<Output = Result<Vec<T>>>,
    mut one: impl FnMut(String) -> F,
) -> Result<(Vec<T>, Result)>
where
    F: Future<Output = Result<Option<T>>>,
{
    if names.is_empty() {
        return Ok((all.await?, Ok(())));
    }
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for name in names {
        match one(name.clone()).await? {
            Some(object) => found.push(object),
            None => missing.push(name.as_str()),
        }
    }
    let result = if missing.is_empty() {
        Ok(())
    } else {
        Err(format!("{kind} not found: {}", missing.join(", ")).into())
    };
    Ok((found, result))
}

async fn get_pods(ctx: &Context<'_>, args: GetArgs, out: &mut dyn Write) -> Result {
    let (pods, result) = fetch(
        &args.names,
        "pods",
        ctx.list_pods(args.node.clone()),
        |name| async move { ctx.pod(&name).await },
    )
    .await?;
    write_pods(&pods, args.output, out)?;
    result
}

pub fn write_pods(pods: &[Pod], format: Format, out: &mut dyn Write) -> Result {
    match format {
        Format::Table | Format::Wide => {
            if pods.is_empty() {
                writeln!(out, "No pods found.")?;
                return Ok(());
            }
            let mut table = Table::new(&output::pod_header(format));
            for pod in pods {
                table.row(output::pod_row(pod, format));
            }
            table.write(out)
        }
        format => {
            let names: Vec<&str> = pods.iter().map(|p| p.name.as_str()).collect();
            output::write_documents(out, "pod", &names, pods, format)
        }
    }
}

async fn get_nodes(ctx: &Context<'_>, args: GetArgs, out: &mut dyn Write) -> Result {
    let (nodes, result) = fetch(&args.names, "nodes", ctx.list_nodes(), |name| async move {
        ctx.node(&name).await
    })
    .await?;
    match args.output {
        Format::Table | Format::Wide => {
            if nodes.is_empty() {
                writeln!(out, "No nodes found.")?;
            } else {
                let mut table = Table::new(&output::node_header(args.output));
                for node in &nodes {
                    table.row(output::node_row(node, args.output));
                }
                table.write(out)?;
            }
        }
        format => {
            let names: Vec<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
            output::write_documents(out, "node", &names, &nodes, format)?;
        }
    }
    result
}

async fn get_cluster_config(ctx: &Context<'_>, args: GetArgs, out: &mut dyn Write) -> Result {
    if !args.names.is_empty() {
        return Err("the cluster configuration has no name".into());
    }
    let proto = ClusterServiceClient::new(ctx.connection.clone())
        .get_cluster_config(pb::GetClusterConfigRequest {})
        .await?
        .into_inner();
    let config = ClusterConfig::try_from(proto)?;
    match args.output {
        Format::Table | Format::Wide => {
            let mut table = Table::new(&["KEY", "VALUE"]);
            table.row(vec![
                "default_runtime".into(),
                config.default_runtime.clone(),
            ]);
            for (key, value) in [
                ("node_lease_ttl", config.node_lease_ttl),
                ("pod_eviction_delay", config.pod_eviction_delay),
                ("leader_lease_ttl", config.leader_lease_ttl),
            ] {
                table.row(vec![
                    key.into(),
                    humantime::format_duration(value).to_string(),
                ]);
            }
            table.write(out)
        }
        format => output::write_documents(out, "cluster-config", &["cluster"], &[config], format),
    }
}

async fn watch_pods(ctx: &Context<'_>, args: GetArgs, out: &mut dyn Write) -> Result {
    let filter = pb::PodFilter {
        node: args.node.clone(),
        unbound: false,
    };
    let (events, mut received) = mpsc::channel(64);
    let task = tokio::spawn(informer::run(
        PodSource::new(ctx.connection.clone(), filter),
        events,
    ));
    let header = output::pod_header(args.output);
    // Name, ready, phase ("Terminating"), restarts, node.
    let mut printer = WatchPrinter::new(&header, &[20, 5, 11, 8, 10]);
    let mut listed = false;
    let selected = |pod: &Pod| args.names.is_empty() || args.names.contains(&pod.name);
    while let Some(event) = received.recv().await {
        match event {
            // After a relist, only the changes are printed.
            InformerEvent::Synced(pods) if !listed => {
                listed = true;
                let rows = pods
                    .iter()
                    .filter(|p| selected(p))
                    .map(|p| output::pod_row(p, args.output))
                    .collect();
                printer.table(&header, rows, out)?;
            }
            InformerEvent::Synced(pods) => {
                for pod in pods.iter().filter(|p| selected(p)) {
                    printer.row(output::pod_row(pod, args.output), out)?;
                }
            }
            InformerEvent::Changed(Change::Added(pod) | Change::Modified(pod))
                if selected(&pod) =>
            {
                printer.row(output::pod_row(&pod, args.output), out)?;
            }
            InformerEvent::Changed(Change::Deleted(pod)) if selected(&pod) => {
                let mut row = output::pod_row(&pod, args.output);
                row[2] = "Deleted".to_owned();
                printer.row(row, out)?;
            }
            InformerEvent::Changed(_) => {}
        }
    }
    task.abort();
    Ok(())
}

async fn watch_nodes(ctx: &Context<'_>, args: GetArgs, out: &mut dyn Write) -> Result {
    let (events, mut received) = mpsc::channel(64);
    let task = tokio::spawn(informer::run(
        NodeSource::new(ctx.connection.clone()),
        events,
    ));
    let header = output::node_header(args.output);
    // Name, status ("Unreachable,SchedulingDisabled").
    let mut printer = WatchPrinter::new(&header, &[12, 30]);
    let mut listed = false;
    let selected = |node: &Node| args.names.is_empty() || args.names.contains(&node.name);
    while let Some(event) = received.recv().await {
        match event {
            InformerEvent::Synced(nodes) if !listed => {
                listed = true;
                let rows = nodes
                    .iter()
                    .filter(|n| selected(n))
                    .map(|n| output::node_row(n, args.output))
                    .collect();
                printer.table(&header, rows, out)?;
            }
            InformerEvent::Synced(nodes) => {
                for node in nodes.iter().filter(|n| selected(n)) {
                    printer.row(output::node_row(node, args.output), out)?;
                }
            }
            // Heartbeats modify nodes every few seconds: only visible changes are printed.
            InformerEvent::Changed(Change::Added(node) | Change::Modified(node))
                if selected(&node) =>
            {
                printer.row(output::node_row(&node, args.output), out)?;
            }
            InformerEvent::Changed(Change::Deleted(node)) if selected(&node) => {
                let mut row = output::node_row(&node, args.output);
                row[1] = "Deleted".to_owned();
                printer.row(row, out)?;
            }
            InformerEvent::Changed(_) => {}
        }
    }
    task.abort();
    Ok(())
}
