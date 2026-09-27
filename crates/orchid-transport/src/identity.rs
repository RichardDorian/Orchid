//! Identity of a caller.
//!
//! In mTLS mode, the identity is the Common Name of the client certificate:
//!
//! | Identity | CN                  |
//! |----------|---------------------|
//! | Labellum | `labellum`          |
//! | Ikebana  | `ikebana`           |
//! | Keiki    | `keiki:<node-name>` |
//! | Users    | `user:<username>`   |
//!
//! In cleartext mode, callers are [`Identity::Anonymous`] and allowed to do everything.

use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo, UdsConnectInfo};
use tonic::{Request, Status};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Identity {
    /// Cleartext mode: no identity, everything is allowed.
    Anonymous,
    Labellum,
    Ikebana,
    /// Keiki agent of the node.
    Keiki(String),
    User(String),
}

impl Identity {
    /// Identity of the caller of `request`. `Anonymous` if the connection is
    /// not using TLS.
    pub fn of<T>(request: &Request<T>) -> Result<Self, Status> {
        let extensions = request.extensions();
        let certs = extensions
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .and_then(TlsConnectInfo::peer_certs)
            .or_else(|| {
                extensions
                    .get::<TlsConnectInfo<UdsConnectInfo>>()
                    .and_then(TlsConnectInfo::peer_certs)
            });
        let Some(certs) = certs else {
            return Ok(Self::Anonymous);
        };
        let cert = certs
            .first()
            .ok_or_else(|| Status::unauthenticated("no client certificate"))?;
        let (_, cert) = x509_parser::parse_x509_certificate(cert)
            .map_err(|_| Status::unauthenticated("invalid client certificate"))?;
        let common_name = cert
            .subject()
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok())
            .ok_or_else(|| Status::unauthenticated("client certificate without common name"))?;
        Self::from_common_name(common_name)
    }

    pub fn from_common_name(common_name: &str) -> Result<Self, Status> {
        let invalid = || Status::permission_denied(format!("unknown identity {common_name:?}"));
        match common_name {
            "labellum" => Ok(Self::Labellum),
            "ikebana" => Ok(Self::Ikebana),
            _ => {
                if let Some(node) = common_name.strip_prefix("keiki:") {
                    (!node.is_empty())
                        .then(|| Self::Keiki(node.to_owned()))
                        .ok_or_else(invalid)
                } else if let Some(user) = common_name.strip_prefix("user:") {
                    (!user.is_empty())
                        .then(|| Self::User(user.to_owned()))
                        .ok_or_else(invalid)
                } else {
                    Err(invalid())
                }
            }
        }
    }

    /// Anonymous callers (cleartext mode) and users.
    pub fn is_user(&self) -> bool {
        matches!(self, Self::Anonymous | Self::User(_))
    }

    /// Allows anonymous callers and users.
    pub fn require_user(&self) -> Result<(), Status> {
        if self.is_user() {
            Ok(())
        } else {
            Err(self.denied())
        }
    }

    /// A permission denied status for this identity.
    pub fn denied(&self) -> Status {
        Status::permission_denied(format!("{self} is not allowed to do this"))
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anonymous => f.write_str("anonymous"),
            Self::Labellum => f.write_str("labellum"),
            Self::Ikebana => f.write_str("ikebana"),
            Self::Keiki(node) => write!(f, "keiki:{node}"),
            Self::User(user) => write!(f, "user:{user}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_names() {
        assert_eq!(
            Identity::from_common_name("labellum").unwrap(),
            Identity::Labellum
        );
        assert_eq!(
            Identity::from_common_name("ikebana").unwrap(),
            Identity::Ikebana
        );
        assert_eq!(
            Identity::from_common_name("keiki:phoenix").unwrap(),
            Identity::Keiki("phoenix".into())
        );
        assert_eq!(
            Identity::from_common_name("user:alice").unwrap(),
            Identity::User("alice".into())
        );
    }

    #[test]
    fn rejects_unknown_common_names() {
        for cn in ["", "keiki:", "user:", "admin", "Keiki:phoenix"] {
            assert!(Identity::from_common_name(cn).is_err(), "{cn}");
        }
    }
}
