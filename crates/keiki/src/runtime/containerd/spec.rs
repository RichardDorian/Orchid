//! OCI runtime specifications of the containers.
//!
//! Every pod has a sandbox container (the pause image) owning the network,
//! IPC and UTS namespaces. The containers of the pod join these namespaces
//! and get their own PID and mount namespaces.

use orchid_api::Resources;
use serde_json::{Value, json};

use super::image::ImageConfig;

/// CFS period used for CPU limits, in microseconds.
const CPU_PERIOD: u64 = 100_000;
/// Smallest CFS quota accepted by the kernel, in microseconds.
const MIN_CPU_QUOTA: u64 = 1_000;

const DEFAULT_PATH: &str = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Capabilities granted by default, the same as containerd and Docker.
const CAPABILITIES: [&str; 14] = [
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FSETID",
    "CAP_FOWNER",
    "CAP_MKNOD",
    "CAP_NET_RAW",
    "CAP_SETGID",
    "CAP_SETUID",
    "CAP_SETFCAP",
    "CAP_SETPCAP",
    "CAP_NET_BIND_SERVICE",
    "CAP_SYS_CHROOT",
    "CAP_KILL",
    "CAP_AUDIT_WRITE",
];

const MASKED_PATHS: [&str; 11] = [
    "/proc/acpi",
    "/proc/asound",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/sys/firmware",
    "/sys/devices/virtual/powercap",
];

const READONLY_PATHS: [&str; 5] = [
    "/proc/bus",
    "/proc/fs",
    "/proc/irq",
    "/proc/sys",
    "/proc/sysrq-trigger",
];

/// What a container is in its pod.
pub enum Role {
    /// Owns the namespaces shared by the pod.
    Sandbox,
    /// Joins the namespaces of the sandbox process.
    Container { sandbox_pid: u32 },
}

pub struct ContainerSpec<'a> {
    pub role: Role,
    pub hostname: &'a str,
    pub image: &'a ImageConfig,
    /// `None` for the sandbox, which has no limits.
    pub resources: Option<Resources>,
    pub cgroups_path: String,
}

/// Builds the OCI runtime specification (JSON).
pub fn build(spec: &ContainerSpec<'_>) -> Value {
    let image = &spec.image.config;
    let mut env: Vec<String> = image.env.clone().unwrap_or_default();
    if !env.iter().any(|e| e.starts_with("PATH=")) {
        env.push(DEFAULT_PATH.to_owned());
    }
    let (uid, gid) = user(image.user.as_deref());

    let namespaces = match spec.role {
        Role::Sandbox => json!([
            {"type": "pid"},
            {"type": "ipc"},
            {"type": "uts"},
            {"type": "mount"},
            {"type": "network"},
        ]),
        Role::Container { sandbox_pid } => json!([
            {"type": "pid"},
            {"type": "mount"},
            {"type": "ipc", "path": format!("/proc/{sandbox_pid}/ns/ipc")},
            {"type": "uts", "path": format!("/proc/{sandbox_pid}/ns/uts")},
            {"type": "network", "path": format!("/proc/{sandbox_pid}/ns/net")},
        ]),
    };

    let mut resources = json!({
        "devices": [{"allow": false, "access": "rwm"}],
    });
    if let Some(limits) = spec.resources {
        let quota = (limits.cpu.0 * CPU_PERIOD / 1000).max(MIN_CPU_QUOTA);
        resources["cpu"] = json!({"quota": quota, "period": CPU_PERIOD});
        resources["memory"] = json!({"limit": limits.memory.0});
    }

    json!({
        "ociVersion": "1.1.0",
        "process": {
            "terminal": false,
            "user": {"uid": uid, "gid": gid},
            "args": spec.image.args(),
            "env": env,
            "cwd": image.working_dir.as_deref().filter(|d| !d.is_empty()).unwrap_or("/"),
            "capabilities": {
                "bounding": CAPABILITIES,
                "effective": CAPABILITIES,
                "permitted": CAPABILITIES,
            },
            "rlimits": [{"type": "RLIMIT_NOFILE", "hard": 1_048_576, "soft": 1_048_576}],
            "noNewPrivileges": true,
        },
        "root": {"path": "rootfs", "readonly": false},
        "hostname": spec.hostname,
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc", "options": ["nosuid", "noexec", "nodev"]},
            {"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]},
            {"destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"]},
            {"destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"]},
            {"destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue", "options": ["nosuid", "noexec", "nodev"]},
            {"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": ["nosuid", "noexec", "nodev", "ro"]},
            {"destination": "/sys/fs/cgroup", "type": "cgroup", "source": "cgroup", "options": ["ro", "nosuid", "noexec", "nodev"]},
        ],
        "linux": {
            "resources": resources,
            "cgroupsPath": spec.cgroups_path,
            "namespaces": namespaces,
            "maskedPaths": MASKED_PATHS,
            "readonlyPaths": READONLY_PATHS,
        },
    })
}

/// Numeric `uid[:gid]` of the image user. User names would need the
/// `/etc/passwd` of the image: they fall back to root.
fn user(user: Option<&str>) -> (u32, u32) {
    let Some(user) = user.filter(|u| !u.is_empty()) else {
        return (0, 0);
    };
    let (uid, gid) = match user.split_once(':') {
        Some((uid, gid)) => (uid, Some(gid)),
        None => (user, None),
    };
    match (uid.parse::<u32>(), gid.map(str::parse::<u32>)) {
        (Ok(uid), None) => (uid, uid),
        (Ok(uid), Some(Ok(gid))) => (uid, gid),
        _ => {
            tracing::warn!(
                user,
                "only numeric image users are supported, running as root"
            );
            (0, 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use orchid_api::{Bytes, MilliCpu};

    use super::super::image::{RootFs, RunConfig};
    use super::*;

    fn image(user: Option<&str>) -> ImageConfig {
        ImageConfig {
            config: RunConfig {
                entrypoint: Some(vec!["/entrypoint".into()]),
                cmd: Some(vec!["serve".into()]),
                env: Some(vec!["FOO=bar".into()]),
                working_dir: Some("/app".into()),
                user: user.map(str::to_owned),
            },
            rootfs: RootFs::default(),
        }
    }

    #[test]
    fn containers_join_the_sandbox_namespaces() {
        let image = image(Some("1000:1000"));
        let spec = build(&ContainerSpec {
            role: Role::Container { sandbox_pid: 42 },
            hostname: "my-app",
            image: &image,
            resources: Some(Resources::new(MilliCpu(300), Bytes(500 << 20))),
            cgroups_path: "/orchid/uid/app".into(),
        });
        assert_eq!(spec["process"]["args"], json!(["/entrypoint", "serve"]));
        assert_eq!(spec["process"]["cwd"], "/app");
        assert_eq!(spec["process"]["user"], json!({"uid": 1000, "gid": 1000}));
        assert!(
            spec["process"]["env"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e.as_str().unwrap().starts_with("PATH="))
        );
        let namespaces = spec["linux"]["namespaces"].as_array().unwrap();
        assert!(namespaces.contains(&json!({"type": "network", "path": "/proc/42/ns/net"})));
        assert!(namespaces.contains(&json!({"type": "pid"})));
        assert_eq!(
            spec["linux"]["resources"]["cpu"],
            json!({"quota": 30_000, "period": 100_000})
        );
        assert_eq!(spec["linux"]["resources"]["memory"]["limit"], 500 << 20);
        assert_eq!(spec["hostname"], "my-app");
    }

    #[test]
    fn the_sandbox_owns_its_namespaces() {
        let image = image(None);
        let spec = build(&ContainerSpec {
            role: Role::Sandbox,
            hostname: "my-app",
            image: &image,
            resources: None,
            cgroups_path: "/orchid/uid/sandbox".into(),
        });
        let namespaces = spec["linux"]["namespaces"].as_array().unwrap();
        assert!(namespaces.iter().all(|ns| ns.get("path").is_none()));
        assert!(spec["linux"]["resources"].get("cpu").is_none());
    }

    #[test]
    fn tiny_cpu_limits_use_the_minimum_quota() {
        let image = image(None);
        let spec = build(&ContainerSpec {
            role: Role::Container { sandbox_pid: 1 },
            hostname: "p",
            image: &image,
            resources: Some(Resources::new(MilliCpu(1), Bytes(1 << 20))),
            cgroups_path: String::new(),
        });
        assert_eq!(spec["linux"]["resources"]["cpu"]["quota"], MIN_CPU_QUOTA);
    }

    #[test]
    fn parses_users() {
        assert_eq!(user(None), (0, 0));
        assert_eq!(user(Some("1000")), (1000, 1000));
        assert_eq!(user(Some("1000:50")), (1000, 50));
        assert_eq!(user(Some("nginx")), (0, 0));
    }
}
