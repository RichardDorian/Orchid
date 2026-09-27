//! gRPC services.

mod agent;
mod cluster;
mod leadership;
mod nodes;
mod pods;
mod scheduler;

pub use agent::AgentApi;
pub use cluster::ClusterApi;
pub use leadership::LeadershipApi;
pub use nodes::NodeApi;
pub use pods::PodApi;
pub use scheduler::SchedulerApi;

use orchid_api::{ValidationError, ValidationErrors};
use tonic::Status;

fn invalid(errors: impl Into<ValidationErrors>) -> Status {
    Status::invalid_argument(errors.into().to_string())
}

fn invalid_prefixed(prefix: &str, errors: ValidationErrors) -> Status {
    let mut prefixed = ValidationErrors::new();
    prefixed.extend_prefixed(prefix, errors);
    invalid(prefixed)
}

fn validate_name(field: &str, name: &str) -> Result<(), Status> {
    orchid_api::validate_name(name).map_err(|reason| invalid(ValidationError::new(field, reason)))
}

/// Defines a service struct holding the shared state.
macro_rules! service {
    ($name:ident) => {
        pub struct $name<S> {
            state: std::sync::Arc<crate::State<S>>,
        }

        impl<S> $name<S> {
            pub fn new(state: std::sync::Arc<crate::State<S>>) -> Self {
                Self { state }
            }
        }
    };
}
use service;
