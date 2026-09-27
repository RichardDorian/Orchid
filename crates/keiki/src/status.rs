//! Status of a pod from the state of its containers.

use std::time::Duration;

use orchid_api::{ContainerStatus, PodPhase, RestartPolicy, Timestamp};
use tokio::time::Instant;

use crate::runtime::{ContainerInfo, ContainerState};

/// First delay before restarting a container.
pub const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
/// Longest delay between two restarts.
pub const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// A container running for this long gets its backoff reset.
pub const BACKOFF_RESET: Duration = Duration::from_secs(600);

/// What Keiki remembers about a container between two checks.
#[derive(Clone, Debug)]
pub struct ContainerRecord {
    pub restart_count: u32,
    pub started_at: Option<Timestamp>,
    pub running_since: Option<Instant>,
    pub backoff: Duration,
    /// When the next restart is allowed.
    pub restart_at: Option<Instant>,
}

impl Default for ContainerRecord {
    fn default() -> Self {
        Self {
            restart_count: 0,
            started_at: None,
            running_since: None,
            backoff: INITIAL_BACKOFF,
            restart_at: None,
        }
    }
}

/// Phase of a pod whose sandbox exists.
pub fn phase(
    policy: RestartPolicy,
    containers: &[ContainerInfo],
    records: &[&ContainerRecord],
) -> PodPhase {
    let running = containers
        .iter()
        .any(|c| c.state == ContainerState::Running);
    let exits: Vec<i32> = containers
        .iter()
        .filter_map(|c| match c.state {
            ContainerState::Exited { code, .. } => Some(code),
            _ => None,
        })
        .collect();
    let all_exited = !containers.is_empty() && exits.len() == containers.len();

    if running || exits.iter().any(|code| policy.should_restart(*code)) {
        PodPhase::Running
    } else if all_exited && exits.iter().all(|code| *code == 0) {
        PodPhase::Succeeded
    } else if all_exited {
        PodPhase::Failed
    } else if records.iter().any(|r| r.started_at.is_some()) {
        PodPhase::Running
    } else {
        PodPhase::Creating
    }
}

/// API status of a container.
pub fn container_status(
    policy: RestartPolicy,
    info: &ContainerInfo,
    record: &ContainerRecord,
) -> ContainerStatus {
    let (state, exit_code) = match info.state {
        ContainerState::Created => (orchid_api::ContainerState::Waiting, None),
        ContainerState::Running => (orchid_api::ContainerState::Running, None),
        ContainerState::Exited { code, .. } if policy.should_restart(code) => {
            (orchid_api::ContainerState::Waiting, Some(code))
        }
        ContainerState::Exited { code, .. } => (orchid_api::ContainerState::Exited, Some(code)),
    };
    ContainerStatus {
        name: info.name.clone(),
        state,
        restart_count: record.restart_count,
        started_at: record.started_at,
        exit_code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(state: ContainerState) -> ContainerInfo {
        ContainerInfo {
            name: "app".into(),
            state,
        }
    }

    fn exited(code: i32) -> ContainerState {
        ContainerState::Exited { code, at: None }
    }

    #[test]
    fn computes_phases() {
        let fresh = ContainerRecord::default();
        let started = ContainerRecord {
            started_at: Some(Timestamp::UNIX_EPOCH),
            ..Default::default()
        };
        let cases = [
            (
                RestartPolicy::Always,
                vec![info(ContainerState::Created)],
                &fresh,
                PodPhase::Creating,
            ),
            (
                RestartPolicy::Always,
                vec![info(ContainerState::Running)],
                &started,
                PodPhase::Running,
            ),
            (
                RestartPolicy::Always,
                vec![info(exited(0))],
                &started,
                PodPhase::Running,
            ),
            (
                RestartPolicy::Failure,
                vec![info(exited(1))],
                &started,
                PodPhase::Running,
            ),
            (
                RestartPolicy::Failure,
                vec![info(exited(0))],
                &started,
                PodPhase::Succeeded,
            ),
            (
                RestartPolicy::Never,
                vec![info(exited(0))],
                &started,
                PodPhase::Succeeded,
            ),
            (
                RestartPolicy::Never,
                vec![info(exited(2))],
                &started,
                PodPhase::Failed,
            ),
        ];
        for (policy, containers, record, expected) in cases {
            assert_eq!(
                phase(policy, &containers, &[record]),
                expected,
                "{policy:?} {containers:?}"
            );
        }
    }

    #[test]
    fn a_running_container_keeps_the_pod_running() {
        let started = ContainerRecord {
            started_at: Some(Timestamp::UNIX_EPOCH),
            ..Default::default()
        };
        let containers = [
            ContainerInfo {
                name: "a".into(),
                state: exited(1),
            },
            ContainerInfo {
                name: "b".into(),
                state: ContainerState::Running,
            },
        ];
        assert_eq!(
            phase(RestartPolicy::Never, &containers, &[&started, &started]),
            PodPhase::Running
        );
    }

    #[test]
    fn exited_containers_waiting_for_a_restart_are_waiting() {
        let record = ContainerRecord::default();
        let status = container_status(RestartPolicy::Always, &info(exited(3)), &record);
        assert_eq!(status.state, orchid_api::ContainerState::Waiting);
        assert_eq!(status.exit_code, Some(3));
        let status = container_status(RestartPolicy::Never, &info(exited(3)), &record);
        assert_eq!(status.state, orchid_api::ContainerState::Exited);
    }
}
