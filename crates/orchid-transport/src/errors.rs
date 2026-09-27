//! Machine readable error reasons.
//!
//! Some errors need to be told apart by clients although they share a status
//! code (e.g. `Bind` failures). They carry a `google.rpc.ErrorInfo` detail with
//! the [`DOMAIN`] domain and a reason.

use std::collections::HashMap;

use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

pub const DOMAIN: &str = "orchid.io";

/// A status carrying `reason`.
pub fn with_reason(code: Code, message: impl Into<String>, reason: &str) -> Status {
    Status::with_error_details(
        code,
        message,
        ErrorDetails::with_error_info(reason, DOMAIN, HashMap::<String, String>::new()),
    )
}

/// The reason carried by `status`, if any.
pub fn reason(status: &Status) -> Option<String> {
    status
        .get_details_error_info()
        .filter(|info| info.domain == DOMAIN)
        .map(|info| info.reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_reasons() {
        let status = with_reason(Code::Aborted, "stale", "BIND_FAILURE_STALE_LEADER_TOKEN");
        assert_eq!(status.code(), Code::Aborted);
        assert_eq!(
            reason(&status).as_deref(),
            Some("BIND_FAILURE_STALE_LEADER_TOKEN")
        );
        assert_eq!(reason(&Status::aborted("no details")), None);
    }
}
