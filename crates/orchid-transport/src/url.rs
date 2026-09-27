//! URLs of the services.
//!
//! - Servers are reached with `http://` in cleartext mode, `https://` in mTLS
//!   mode, or `unix://` in both modes (TLS is still negotiated over the socket
//!   in mTLS mode).
//! - Servers listen on `tcp://ip:port` or `unix://path`.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::TransportError;

/// URL of a server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerUrl {
    /// `http://` or `https://` URL.
    Tcp(String),
    /// Path of a unix socket.
    Unix(PathBuf),
}

/// Parses the URLs of a server, checking them against the mode (`tls`).
///
/// A unix socket must be the only URL.
pub fn parse_server_urls(urls: &[String], tls: bool) -> Result<Vec<ServerUrl>, TransportError> {
    if urls.is_empty() {
        return Err(TransportError::InvalidUrl {
            url: String::new(),
            reason: "at least one URL is required".to_owned(),
        });
    }
    let parsed = urls
        .iter()
        .map(|url| parse_server_url(url, tls))
        .collect::<Result<Vec<_>, _>>()?;
    if parsed.len() > 1 && parsed.iter().any(|url| matches!(url, ServerUrl::Unix(_))) {
        return Err(TransportError::InvalidUrl {
            url: urls.join(", "),
            reason: "a unix socket must be the only URL".to_owned(),
        });
    }
    Ok(parsed)
}

fn parse_server_url(url: &str, tls: bool) -> Result<ServerUrl, TransportError> {
    let invalid = |reason: &str| TransportError::InvalidUrl {
        url: url.to_owned(),
        reason: reason.to_owned(),
    };
    if let Some(path) = url.strip_prefix("unix://") {
        if !path.starts_with('/') {
            return Err(invalid("the socket path must be absolute"));
        }
        return Ok(ServerUrl::Unix(PathBuf::from(path)));
    }
    let rest = if let Some(rest) = url.strip_prefix("https://") {
        if !tls {
            return Err(invalid("https:// requires mTLS mode (a [tls] section)"));
        }
        rest
    } else if let Some(rest) = url.strip_prefix("http://") {
        if tls {
            return Err(invalid("http:// is not allowed in mTLS mode, use https://"));
        }
        rest
    } else {
        return Err(invalid("expected http://, https:// or unix://"));
    };
    if rest.is_empty() || rest.starts_with('/') {
        return Err(invalid("missing host"));
    }
    Ok(ServerUrl::Tcp(url.to_owned()))
}

/// Address a server listens on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListenAddr {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl std::str::FromStr for ListenAddr {
    type Err = TransportError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = |reason: &str| TransportError::InvalidUrl {
            url: s.to_owned(),
            reason: reason.to_owned(),
        };
        if let Some(address) = s.strip_prefix("tcp://") {
            return address
                .parse()
                .map(Self::Tcp)
                .map_err(|_| invalid("expected tcp://ip:port"));
        }
        if let Some(path) = s.strip_prefix("unix://") {
            if !path.starts_with('/') {
                return Err(invalid("the socket path must be absolute"));
            }
            return Ok(Self::Unix(PathBuf::from(path)));
        }
        Err(invalid("expected tcp:// or unix://"))
    }
}

impl std::fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(address) => write!(f, "tcp://{address}"),
            Self::Unix(path) => write!(f, "unix://{}", path.display()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(urls: &[&str]) -> Vec<String> {
        urls.iter().map(|u| (*u).to_owned()).collect()
    }

    #[test]
    fn accepts_urls_matching_the_mode() {
        assert_eq!(
            parse_server_urls(
                &urls(&["http://10.0.0.1:36116", "http://10.0.0.2:36116"]),
                false
            )
            .unwrap()
            .len(),
            2
        );
        assert_eq!(
            parse_server_urls(&urls(&["https://lb.cluster:36116"]), true).unwrap(),
            [ServerUrl::Tcp("https://lb.cluster:36116".into())]
        );
        for tls in [false, true] {
            assert_eq!(
                parse_server_urls(&urls(&["unix:///run/orchid/labellum.sock"]), tls).unwrap(),
                [ServerUrl::Unix("/run/orchid/labellum.sock".into())]
            );
        }
    }

    #[test]
    fn rejects_urls_not_matching_the_mode() {
        assert!(parse_server_urls(&urls(&["https://10.0.0.1:36116"]), false).is_err());
        assert!(parse_server_urls(&urls(&["http://10.0.0.1:36116"]), true).is_err());
    }

    #[test]
    fn rejects_invalid_urls() {
        for list in [
            &[][..],
            &["grpc://10.0.0.1:36116"],
            &["http://"],
            &["unix://relative.sock"],
            &["unix:///run/a.sock", "http://10.0.0.1:36116"],
        ] {
            assert!(parse_server_urls(&urls(list), false).is_err(), "{list:?}");
        }
    }

    #[test]
    fn parses_listen_addresses() {
        assert_eq!(
            "tcp://0.0.0.0:36116".parse::<ListenAddr>().unwrap(),
            ListenAddr::Tcp("0.0.0.0:36116".parse().unwrap())
        );
        assert_eq!(
            "unix:///run/orchid/labellum.sock"
                .parse::<ListenAddr>()
                .unwrap(),
            ListenAddr::Unix("/run/orchid/labellum.sock".into())
        );
        for invalid in [
            "0.0.0.0:36116",
            "tcp://localhost:36116",
            "unix://a.sock",
            "http://0.0.0.0:1",
        ] {
            assert!(invalid.parse::<ListenAddr>().is_err(), "{invalid}");
        }
    }
}
