use crate::Revision;

pub type Result<T, E = StoreError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The requested revision is older than the oldest revision kept in the
    /// history. Watchers must list again.
    #[error(
        "requested revision has been compacted, oldest available revision is {compact_revision}"
    )]
    Compacted { compact_revision: Revision },

    #[error("lease not found")]
    LeaseNotFound,

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("failed to decode value of {key}: {source}")]
    Decode {
        key: String,
        #[source]
        source: serde_json::Error,
    },

    /// The watch stream was closed by the store.
    #[error("watch closed: {0}")]
    WatchClosed(String),

    #[error("store error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}
