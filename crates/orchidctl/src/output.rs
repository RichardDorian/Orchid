//! Rendering of resources.

use std::io::Write;

use jiff::Timestamp;
use orchid_api::{Bytes, ContainerState, MilliCpu, Node, NodeCondition, Pod, PodPhase, Resources};
use serde::Serialize;

use crate::Result;
use crate::cli::Format;

/// A table aligned like kubectl: columns separated by 3 spaces.
pub struct Table {
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(header: &[&str]) -> Self {
        Self {
            rows: vec![header.iter().map(|h| (*h).to_owned()).collect()],
        }
    }

    pub fn row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    pub fn write(&self, out: &mut dyn Write) -> Result {
        let columns = self.rows.iter().map(Vec::len).max().unwrap_or(0);
        let widths: Vec<usize> = (0..columns)
            .map(|c| {
                self.rows
                    .iter()
                    .filter_map(|r| r.get(c))
                    .map(|v| v.chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        for row in &self.rows {
            let mut line = String::new();
            for (c, value) in row.iter().enumerate() {
                if c + 1 == row.len() {
                    line.push_str(value);
                } else {
                    line.push_str(&format!("{value:<width$}   ", width = widths[c]));
                }
            }
            writeln!(out, "{}", line.trim_end())?;
        }
        Ok(())
    }
}

/// Prints the rows of a watch: columns keep the widths of the widest value
/// seen so far, and a row is only printed when its content changed.
pub struct WatchPrinter {
    widths: Vec<usize>,
    last: std::collections::HashMap<String, Vec<String>>,
}

impl WatchPrinter {
    /// `minimum` gives the smallest width of the first columns, so that the
    /// table rarely has to grow while watching.
    pub fn new(header: &[&str], minimum: &[usize]) -> Self {
        Self {
            widths: header
                .iter()
                .enumerate()
                .map(|(c, h)| h.chars().count().max(minimum.get(c).copied().unwrap_or(0)))
                .collect(),
            last: std::collections::HashMap::new(),
        }
    }

    fn write_line(&mut self, row: &[String], out: &mut dyn Write) -> Result {
        let mut line = String::new();
        for (c, value) in row.iter().enumerate() {
            if c >= self.widths.len() {
                self.widths.push(0);
            }
            self.widths[c] = self.widths[c].max(value.chars().count());
            if c + 1 == row.len() {
                line.push_str(value);
            } else {
                line.push_str(&format!("{value:<width$}   ", width = self.widths[c]));
            }
        }
        writeln!(out, "{}", line.trim_end())?;
        out.flush()?;
        Ok(())
    }

    /// Prints the header and the initial rows.
    pub fn table(
        &mut self,
        header: &[&str],
        rows: Vec<Vec<String>>,
        out: &mut dyn Write,
    ) -> Result {
        for row in &rows {
            for (c, value) in row.iter().enumerate() {
                if c < self.widths.len() {
                    self.widths[c] = self.widths[c].max(value.chars().count());
                }
            }
        }
        let header: Vec<String> = header.iter().map(|h| (*h).to_owned()).collect();
        self.write_line(&header, out)?;
        for row in rows {
            self.last.insert(row[0].clone(), row.clone());
            self.write_line(&row, out)?;
        }
        Ok(())
    }

    /// Prints the row of an object if it changed since its last row.
    pub fn row(&mut self, row: Vec<String>, out: &mut dyn Write) -> Result {
        if self.last.get(&row[0]) == Some(&row) {
            return Ok(());
        }
        self.last.insert(row[0].clone(), row.clone());
        self.write_line(&row, out)
    }
}

/// A measured amount of memory, rounded: `12.8Gi`, `310Mi`.
pub fn approximate_memory(bytes: Bytes) -> String {
    let units = [
        ("Ti", 1u64 << 40),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ];
    for (suffix, size) in units {
        if bytes.0 >= size {
            let value = bytes.0 as f64 / size as f64;
            let rounded = if value >= 100.0 {
                value.round()
            } else {
                (value * 10.0).round() / 10.0
            };
            return if rounded.fract() == 0.0 {
                format!("{rounded:.0}{suffix}")
            } else {
                format!("{rounded:.1}{suffix}")
            };
        }
    }
    bytes.to_string()
}

/// `5s`, `3m`, `2h`, `4d`.
pub fn age(since: Timestamp) -> String {
    let seconds = Timestamp::now().duration_since(since).as_secs().max(0);
    match seconds {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s if s < 86_400 => format!("{}h", s / 3600),
        s => format!("{}d", s / 86_400),
    }
}

fn capitalized(name: &str) -> String {
    let mut chars = name.chars();
    chars
        .next()
        .map(|first| first.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

pub fn phase(phase: PodPhase) -> String {
    capitalized(&format!("{phase:?}"))
}

pub fn condition(condition: NodeCondition) -> String {
    capitalized(&format!("{condition:?}"))
}

/// Human status of a node: its condition, then `SchedulingDisabled` or `Draining`.
pub fn node_status(node: &Node) -> String {
    let mut status = condition(node.status.condition);
    if node.spec.draining {
        status.push_str(",Draining");
    } else if !node.spec.schedulable {
        status.push_str(",SchedulingDisabled");
    }
    status
}

pub fn resources(resources: &Resources) -> (String, String) {
    (resources.cpu.to_string(), resources.memory.to_string())
}

/// `300m/12 (2%)`
fn usage(used: u64, capacity: u64, display: impl Fn(u64) -> String) -> String {
    let percent = (used * 100).checked_div(capacity).unwrap_or(0);
    format!("{}/{} ({percent}%)", display(used), display(capacity))
}

pub fn cpu_usage(used: MilliCpu, capacity: MilliCpu) -> String {
    usage(used.0, capacity.0, |v| MilliCpu(v).to_string())
}

pub fn memory_usage(used: Bytes, capacity: Bytes) -> String {
    usage(used.0, capacity.0, |v| Bytes(v).to_string())
}

pub fn pod_header(format: Format) -> Vec<&'static str> {
    let mut header = vec!["NAME", "READY", "PHASE", "RESTARTS", "NODE", "AGE"];
    if format == Format::Wide {
        header.extend(["PRIORITY", "ATTEMPT", "REASON", "CPU", "MEMORY", "RUNTIME"]);
    }
    header
}

pub fn pod_row(pod: &Pod, format: Format) -> Vec<String> {
    let status = &pod.status;
    let running = status
        .containers
        .iter()
        .filter(|c| c.state == ContainerState::Running)
        .count();
    let restarts: u32 = status.containers.iter().map(|c| c.restart_count).sum();
    let mut row = vec![
        pod.name.clone(),
        format!("{running}/{}", pod.spec.containers.len()),
        phase(status.phase),
        restarts.to_string(),
        pod.node().unwrap_or("<none>").to_owned(),
        age(pod.created_at),
    ];
    if format == Format::Wide {
        let (cpu, memory) = resources(&pod.spec.resources().unwrap_or_default());
        row.extend([
            pod.spec.priority.to_string(),
            pod.binding.as_ref().map_or(0, |b| b.attempt).to_string(),
            if status.reason.is_empty() {
                "<none>".to_owned()
            } else {
                status.reason.clone()
            },
            cpu,
            memory,
            pod.spec.runtime.clone(),
        ]);
    }
    row
}

pub fn node_header(format: Format) -> Vec<&'static str> {
    let mut header = vec!["NAME", "STATUS", "ROLE", "CPU", "MEMORY"];
    if format == Format::Wide {
        header.extend([
            "POD-CIDR",
            "CPU-USAGE",
            "MEMORY-USAGE",
            "HEARTBEAT",
            "RUNTIMES",
        ]);
    }
    header
}

pub fn node_row(node: &Node, format: Format) -> Vec<String> {
    let capacity = node.info.capacity;
    let allocated = node.status.allocated;
    let role = match node.info.role {
        orchid_api::NodeRole::Worker => "worker",
        orchid_api::NodeRole::ControlPlane => "control-plane",
    };
    let mut row = vec![
        node.name.clone(),
        node_status(node),
        role.to_owned(),
        cpu_usage(allocated.cpu, capacity.cpu),
        memory_usage(allocated.memory, capacity.memory),
    ];
    if format == Format::Wide {
        row.extend([
            node.info.pod_cidr.to_string(),
            node.status.usage.cpu.to_string(),
            approximate_memory(node.status.usage.memory),
            node.status
                .last_heartbeat
                .map_or("<none>".to_owned(), |t| format!("{} ago", age(t))),
            node.info.runtimes.join(","),
        ]);
    }
    row
}

/// Writes objects as TOML, JSON or names.
pub fn write_documents<T: Serialize>(
    out: &mut dyn Write,
    kind: &str,
    names: &[&str],
    objects: &[T],
    format: Format,
) -> Result {
    match format {
        Format::Json if objects.len() == 1 => {
            writeln!(
                out,
                "{}",
                serde_json::to_string_pretty(&objects[0]).map_err(|e| e.to_string())?
            )?;
        }
        Format::Json => {
            writeln!(
                out,
                "{}",
                serde_json::to_string_pretty(objects).map_err(|e| e.to_string())?
            )?;
        }
        Format::Toml => {
            for (i, object) in objects.iter().enumerate() {
                if i > 0 {
                    writeln!(out, "\n# ---\n")?;
                }
                write!(
                    out,
                    "{}",
                    toml::to_string_pretty(object).map_err(|e| e.to_string())?
                )?;
            }
        }
        Format::Name => {
            for name in names {
                writeln!(out, "{kind}/{name}")?;
            }
        }
        Format::Table | Format::Wide => unreachable!("tables are rendered by the caller"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_columns() {
        let mut table = Table::new(&["NAME", "PHASE"]);
        table.row(vec!["my-app".into(), "Running".into()]);
        let mut out = Vec::new();
        table.write(&mut out).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "NAME     PHASE\nmy-app   Running\n"
        );
    }

    #[test]
    fn formats_ages() {
        let now = Timestamp::now();
        assert_eq!(age(now), "0s");
        assert_eq!(age(now - jiff::SignedDuration::from_secs(150)), "2m");
        assert_eq!(age(now - jiff::SignedDuration::from_hours(49)), "2d");
    }

    #[test]
    fn approximates_memory() {
        assert_eq!(approximate_memory(Bytes(13_377_440 * 1024)), "12.8Gi");
        assert_eq!(approximate_memory(Bytes(310 << 20)), "310Mi");
        assert_eq!(approximate_memory(Bytes(2 << 30)), "2Gi");
        assert_eq!(approximate_memory(Bytes(512)), "512");
    }

    #[test]
    fn keeps_watch_columns_aligned() {
        let mut printer = WatchPrinter::new(&["NAME", "PHASE"], &[]);
        let mut out = Vec::new();
        printer
            .table(
                &["NAME", "PHASE"],
                vec![vec!["web".into(), "Pending".into()]],
                &mut out,
            )
            .unwrap();
        printer
            .row(vec!["hello".into(), "Creating".into()], &mut out)
            .unwrap();
        printer
            .row(vec!["hello".into(), "Creating".into()], &mut out)
            .unwrap();
        printer
            .row(vec!["web".into(), "Running".into()], &mut out)
            .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "NAME   PHASE\nweb    Pending\nhello   Creating\nweb     Running\n"
        );
    }

    #[test]
    fn formats_usage() {
        assert_eq!(cpu_usage(MilliCpu(300), MilliCpu(12_000)), "300m/12 (2%)");
        assert_eq!(
            memory_usage(Bytes(8 << 30), Bytes(16 << 30)),
            "8Gi/16Gi (50%)"
        );
    }
}
