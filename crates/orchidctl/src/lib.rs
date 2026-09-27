//! orchidctl, the command line client of Orchid.
//!
//! The commands write to any [`Write`] so they can be tested without a terminal.

mod cli;
mod commands;
mod config;
mod manifest;
mod output;

use std::fmt;
use std::io::Write;

pub use cli::Cli;
use orchid_transport::client::Connection;

/// An error shown to the user.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Self(message.to_owned())
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        let message = status.message();
        Self(match status.code() {
            tonic::Code::Unavailable => format!("cannot reach labellum: {message}"),
            tonic::Code::PermissionDenied => format!("permission denied: {message}"),
            _ => message.to_owned(),
        })
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<orchid_transport::TransportError> for Error {
    fn from(error: orchid_transport::TransportError) -> Self {
        Self(error.to_string())
    }
}

impl From<orchid_api::ValidationError> for Error {
    fn from(error: orchid_api::ValidationError) -> Self {
        Self(format!("invalid response from labellum: {error}"))
    }
}

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

/// Runs a command, writing its output to `out`.
pub async fn run(cli: Cli, out: &mut dyn Write) -> Result {
    let connection = config::connect(cli.config.as_deref(), &cli.servers).await?;
    commands::run(cli.command, &connection, out).await
}

/// What every command needs.
pub(crate) struct Context<'a> {
    pub connection: &'a Connection,
}
