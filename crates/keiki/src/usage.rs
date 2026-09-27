//! Resource usage of the node, measured from `/proc`.

use orchid_api::{Bytes, MilliCpu, Resources};

/// Measures CPU usage between two samples.
#[derive(Default)]
pub struct UsageSampler {
    last: Option<CpuTimes>,
}

#[derive(Clone, Copy)]
struct CpuTimes {
    total: u64,
    idle: u64,
}

impl UsageSampler {
    /// Usage since the previous sample. CPU is zero on the first sample.
    /// Unreadable values count as zero.
    pub fn sample(&mut self) -> Resources {
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get() as u64);
        let times = std::fs::read_to_string("/proc/stat")
            .ok()
            .and_then(|s| parse_stat(&s));
        let cpu = match (self.last, times) {
            (Some(last), Some(now)) => cpu_usage(last, now, cpus),
            _ => MilliCpu::ZERO,
        };
        if times.is_some() {
            self.last = times;
        }
        let memory = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| parse_meminfo(&s))
            .unwrap_or(Bytes::ZERO);
        Resources::new(cpu, memory)
    }
}

fn cpu_usage(last: CpuTimes, now: CpuTimes, cpus: u64) -> MilliCpu {
    let total = now.total.saturating_sub(last.total);
    let idle = now.idle.saturating_sub(last.idle);
    if total == 0 {
        return MilliCpu::ZERO;
    }
    MilliCpu(total.saturating_sub(idle) * cpus * 1000 / total)
}

/// Aggregated CPU times from the first line of `/proc/stat`.
fn parse_stat(stat: &str) -> Option<CpuTimes> {
    let line = stat.lines().find(|l| l.starts_with("cpu "))?;
    let values: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    // user nice system idle iowait irq softirq steal
    let idle = values.get(3)? + values.get(4).unwrap_or(&0);
    let total = values.iter().take(8).sum();
    Some(CpuTimes { total, idle })
}

/// Used memory: `MemTotal - MemAvailable`.
fn parse_meminfo(meminfo: &str) -> Option<Bytes> {
    let field = |name: &str| {
        meminfo
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|kib| kib.parse::<u64>().ok())
    };
    let used = field("MemTotal:")?.saturating_sub(field("MemAvailable:")?);
    Some(Bytes(used * 1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_cpu_usage() {
        let last = parse_stat("cpu  100 0 100 800 0 0 0 0 0 0\ncpu0 1 2 3").unwrap();
        let now = parse_stat("cpu  200 0 200 1400 0 0 0 0 0 0").unwrap();
        // 200 busy out of 800: 25% of 4 CPUs.
        assert_eq!(cpu_usage(last, now, 4), MilliCpu(1000));
    }

    #[test]
    fn computes_memory_usage() {
        let meminfo = "MemTotal:       16000000 kB\nMemFree:  1 kB\nMemAvailable:   12000000 kB\n";
        assert_eq!(parse_meminfo(meminfo), Some(Bytes(4_000_000 * 1024)));
    }

    #[test]
    fn samples_the_machine() {
        let mut sampler = UsageSampler::default();
        assert_eq!(sampler.sample().cpu, MilliCpu::ZERO);
        sampler.sample();
    }
}
