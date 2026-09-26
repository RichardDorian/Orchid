//! Storage layer of Labellum.
//!
//! [`Store`] is a small subset of the etcd v3 API: reads, transactions with
//! compare-and-swap, resumable watches and leases. It has two implementations:
//! - [`EtcdStore`], used in production,
//! - [`MemoryStore`], an in-memory implementation with the same semantics, used in tests.
//!
//! Keys are UTF-8 strings, values are opaque bytes. [`keys`] defines the key
//! layout of the cluster and [`codec`] how values are encoded.

pub mod codec;
mod error;
mod etcd;
pub mod keys;
mod memory;
mod txn;
mod watch;

use std::future::Future;
use std::time::Duration;

pub use error::{Result, StoreError};
pub use etcd::{EtcdStore, EtcdTls};
pub use memory::MemoryStore;
pub use txn::{Compare, CompareOp, CompareTarget, Op, OpResponse, Txn, TxnResponse};
pub use watch::{EventKind, Watch, WatchEvent, WatchResponse};

/// etcd revision. Every write increments the revision of the whole store by one.
pub type Revision = i64;

/// Identifier of a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LeaseId(pub i64);

/// A granted lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lease {
    pub id: LeaseId,
    /// Actual TTL, the store may round the requested TTL up.
    pub ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
    /// Revision of the write that created the key.
    pub create_revision: Revision,
    /// Revision of the last write of the key.
    pub mod_revision: Revision,
    /// Number of writes since the key was created (1 after creation).
    pub version: i64,
    /// Lease the key is attached to.
    pub lease: Option<LeaseId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetResponse {
    pub kv: Option<KeyValue>,
    /// Revision of the store when the read happened.
    pub revision: Revision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListResponse {
    /// Sorted by key.
    pub kvs: Vec<KeyValue>,
    /// Revision of the store when the read happened.
    pub revision: Revision,
}

/// Keys watched by a [`Watch`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyRange {
    /// A single key.
    Key(String),
    /// Every key starting with the prefix.
    Prefix(String),
}

impl KeyRange {
    pub fn key(key: impl Into<String>) -> Self {
        Self::Key(key.into())
    }

    pub fn prefix(prefix: impl Into<String>) -> Self {
        Self::Prefix(prefix.into())
    }

    pub fn contains(&self, key: &str) -> bool {
        match self {
            Self::Key(k) => k == key,
            Self::Prefix(prefix) => key.starts_with(prefix.as_str()),
        }
    }
}

/// A key-value store with the semantics of etcd v3.
pub trait Store: Send + Sync + 'static {
    fn get(&self, key: &str) -> impl Future<Output = Result<GetResponse>> + Send;

    /// Every key starting with `prefix`, sorted by key.
    fn list(&self, prefix: &str) -> impl Future<Output = Result<ListResponse>> + Send;

    /// Evaluates the compares, then applies the success operations if they all
    /// hold, the failure operations otherwise. Every write of the transaction
    /// shares the same revision.
    ///
    /// A key can only be written once per branch ([`StoreError::InvalidRequest`]).
    /// Putting a key with an unknown lease fails with [`StoreError::LeaseNotFound`].
    fn txn(&self, txn: Txn) -> impl Future<Output = Result<TxnResponse>> + Send;

    /// Watches `range` starting at `start_revision` (inclusive), or only new
    /// events if `None`.
    ///
    /// If `start_revision` has been compacted, the first item of the watch is
    /// [`StoreError::Compacted`].
    fn watch(
        &self,
        range: KeyRange,
        start_revision: Option<Revision>,
    ) -> impl Future<Output = Result<Watch>> + Send;

    /// Grants a lease. The TTL is rounded up to a whole number of seconds.
    fn grant_lease(&self, ttl: Duration) -> impl Future<Output = Result<Lease>> + Send;

    /// Renews a lease for its TTL. Fails with [`StoreError::LeaseNotFound`] if
    /// it expired or was revoked.
    fn keep_alive(&self, lease: LeaseId) -> impl Future<Output = Result<()>> + Send;

    /// Revokes a lease and deletes the keys attached to it.
    fn revoke_lease(&self, lease: LeaseId) -> impl Future<Output = Result<()>> + Send;

    /// Discards the history before `revision`: watches cannot start before it anymore.
    fn compact(&self, revision: Revision) -> impl Future<Output = Result<()>> + Send;

    /// Unconditionally puts a key. Returns the revision of the write.
    fn put(
        &self,
        key: impl Into<String> + Send,
        value: impl Into<Vec<u8>> + Send,
    ) -> impl Future<Output = Result<Revision>> + Send {
        let txn = Txn::new().then([Op::put(key, value)]);
        async move { Ok(self.txn(txn).await?.revision) }
    }

    /// Unconditionally deletes a key. Returns whether it existed.
    fn delete(&self, key: impl Into<String> + Send) -> impl Future<Output = Result<bool>> + Send {
        let txn = Txn::new().then([Op::delete(key)]);
        async move {
            let response = self.txn(txn).await?;
            Ok(matches!(
                response.responses.first(),
                Some(OpResponse::Delete { deleted }) if *deleted > 0
            ))
        }
    }
}
