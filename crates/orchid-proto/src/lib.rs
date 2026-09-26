//! gRPC protocol definitions shared by the Orchid services.
//!
//! Every service gets both its client and its server:
//! - Labellum implements the servers,
//! - Ikebana, Keiki and the CLI use the clients.

/// Version 1 of the Orchid API (`orchid.v1` protobuf package).
pub mod v1 {
    #![allow(clippy::all, clippy::pedantic)]

    tonic::include_proto!("orchid.v1");
}

/// Well-known protobuf types (`Timestamp`, `Duration`) used by the messages.
pub use prost_types;
