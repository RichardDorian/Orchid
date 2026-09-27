//! An in-memory runtime for tests: containers run until told to exit.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use orchid_api::{Pod, Timestamp};

use super::{ContainerInfo, ContainerState, PodRef, Runtime, RuntimeError};

#[derive(Clone, Default)]
pub struct FakeRuntime {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    pods: BTreeMap<PodRef, BTreeMap<String, ContainerState>>,
    starts: BTreeMap<(String, String), u32>,
    healthy_error: Option<String>,
    create_error: Option<String>,
    creation_attempts: u32,
}

impl FakeRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes a running container exit with `code`.
    pub fn exit(&self, pod: &str, container: &str, code: i32) {
        let mut inner = self.lock();
        for (reference, containers) in &mut inner.pods {
            if reference.name == pod
                && let Some(state) = containers.get_mut(container)
            {
                *state = ContainerState::Exited {
                    code,
                    at: Some(Timestamp::now()),
                };
            }
        }
    }

    /// The pods in the runtime and the state of their containers.
    pub fn pods(&self) -> BTreeMap<PodRef, BTreeMap<String, ContainerState>> {
        self.lock().pods.clone()
    }

    /// How many times a container of the pod has been started.
    pub fn starts(&self, pod: &str, container: &str) -> u32 {
        self.lock()
            .starts
            .get(&(pod.to_owned(), container.to_owned()))
            .copied()
            .unwrap_or(0)
    }

    /// How many times a pod creation was attempted.
    pub fn creation_attempts(&self) -> u32 {
        self.lock().creation_attempts
    }

    /// Makes pod creations fail with `message`, or succeed again with `None`.
    pub fn fail_creations(&self, message: Option<&str>) {
        self.lock().create_error = message.map(str::to_owned);
    }

    /// Makes health checks fail with `message`, or succeed again with `None`.
    pub fn set_unhealthy(&self, message: Option<&str>) {
        self.lock().healthy_error = message.map(str::to_owned);
    }

    /// Adds a pod as if it had been created before, e.g. by a previous run of Keiki.
    pub fn insert(&self, reference: PodRef, containers: &[&str]) {
        let containers = containers
            .iter()
            .map(|c| ((*c).to_owned(), ContainerState::Running))
            .collect();
        self.lock().pods.insert(reference, containers);
    }
}

impl Runtime for FakeRuntime {
    async fn health(&self) -> Result<(), RuntimeError> {
        match &self.lock().healthy_error {
            Some(message) => Err(RuntimeError::Unavailable(message.clone())),
            None => Ok(()),
        }
    }

    async fn list_pods(&self) -> Result<Vec<PodRef>, RuntimeError> {
        Ok(self.lock().pods.keys().cloned().collect())
    }

    async fn create_pod(&self, pod: &Pod, attempt: u32) -> Result<(), RuntimeError> {
        let mut inner = self.lock();
        inner.creation_attempts += 1;
        if let Some(message) = &inner.create_error {
            return Err(RuntimeError::Failed(message.clone()));
        }
        let reference = PodRef {
            uid: pod.uid.clone(),
            attempt,
            name: pod.name.clone(),
        };
        let containers = pod
            .spec
            .containers
            .iter()
            .map(|c| (c.name.clone(), ContainerState::Created))
            .collect();
        inner.pods.insert(reference, containers);
        Ok(())
    }

    async fn start_container(&self, pod: &PodRef, container: &str) -> Result<(), RuntimeError> {
        let mut inner = self.lock();
        let state = inner
            .pods
            .get_mut(pod)
            .and_then(|containers| containers.get_mut(container))
            .ok_or_else(|| RuntimeError::Failed(format!("no container {container}")))?;
        *state = ContainerState::Running;
        *inner
            .starts
            .entry((pod.name.clone(), container.to_owned()))
            .or_default() += 1;
        Ok(())
    }

    async fn pod_status(&self, pod: &PodRef) -> Result<Vec<ContainerInfo>, RuntimeError> {
        let inner = self.lock();
        let containers = inner
            .pods
            .get(pod)
            .ok_or_else(|| RuntimeError::Failed(format!("no pod {}", pod.name)))?;
        Ok(containers
            .iter()
            .map(|(name, state)| ContainerInfo {
                name: name.clone(),
                state: *state,
            })
            .collect())
    }

    async fn remove_pod(&self, pod: &PodRef, _grace: Duration) -> Result<(), RuntimeError> {
        self.lock().pods.remove(pod);
        Ok(())
    }
}
