//! Keiki, the node agent of Orchid.
//!
//! Keiki registers its node with Labellum and keeps it alive with heartbeats.
//! It watches the pods bound to the node and runs a [`worker`] for each of
//! them, which creates the pod in the container [`runtime`], restarts its
//! containers according to the restart policy, reports its status, and
//! removes it once it is deleted or evicted.

pub mod agent;
pub mod config;
pub mod runtime;
mod status;
mod usage;
mod worker;

pub use agent::{Options, Registration, run};
