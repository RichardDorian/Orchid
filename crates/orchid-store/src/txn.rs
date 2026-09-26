use crate::{KeyValue, LeaseId, Revision};

/// A transaction: `if compares { success } else { failure }`.
#[derive(Clone, Debug, Default)]
pub struct Txn {
    pub compares: Vec<Compare>,
    pub success: Vec<Op>,
    pub failure: Vec<Op>,
}

impl Txn {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds compares. They must all hold for the success operations to be applied.
    #[must_use]
    pub fn when(mut self, compares: impl IntoIterator<Item = Compare>) -> Self {
        self.compares.extend(compares);
        self
    }

    /// Adds operations applied if every compare holds.
    #[must_use]
    pub fn then(mut self, ops: impl IntoIterator<Item = Op>) -> Self {
        self.success.extend(ops);
        self
    }

    /// Adds operations applied if a compare doesn't hold.
    #[must_use]
    pub fn otherwise(mut self, ops: impl IntoIterator<Item = Op>) -> Self {
        self.failure.extend(ops);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Compare {
    pub key: String,
    pub op: CompareOp,
    pub target: CompareTarget,
}

/// The attribute of a key a [`Compare`] looks at. A missing key has a version,
/// create revision and mod revision of 0 and no lease. A value compare on a
/// missing key never holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompareTarget {
    Version(i64),
    CreateRevision(Revision),
    ModRevision(Revision),
    Value(Vec<u8>),
    Lease(Option<LeaseId>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
    Equal,
    NotEqual,
    Greater,
    Less,
}

impl CompareOp {
    pub(crate) fn holds<T: Ord + ?Sized>(self, actual: &T, expected: &T) -> bool {
        match self {
            Self::Equal => actual == expected,
            Self::NotEqual => actual != expected,
            Self::Greater => actual > expected,
            Self::Less => actual < expected,
        }
    }
}

impl Compare {
    pub fn new(key: impl Into<String>, op: CompareOp, target: CompareTarget) -> Self {
        Self {
            key: key.into(),
            op,
            target,
        }
    }

    /// The key doesn't exist.
    pub fn absent(key: impl Into<String>) -> Self {
        Self::version(key, CompareOp::Equal, 0)
    }

    /// The key exists.
    pub fn exists(key: impl Into<String>) -> Self {
        Self::version(key, CompareOp::Greater, 0)
    }

    /// The key has not been written since `mod_revision`. With a revision of
    /// 0, the key doesn't exist.
    pub fn unchanged(key: impl Into<String>, mod_revision: Revision) -> Self {
        Self::mod_revision(key, CompareOp::Equal, mod_revision)
    }

    pub fn version(key: impl Into<String>, op: CompareOp, version: i64) -> Self {
        Self::new(key, op, CompareTarget::Version(version))
    }

    pub fn create_revision(key: impl Into<String>, op: CompareOp, revision: Revision) -> Self {
        Self::new(key, op, CompareTarget::CreateRevision(revision))
    }

    pub fn mod_revision(key: impl Into<String>, op: CompareOp, revision: Revision) -> Self {
        Self::new(key, op, CompareTarget::ModRevision(revision))
    }

    pub fn value(key: impl Into<String>, op: CompareOp, value: impl Into<Vec<u8>>) -> Self {
        Self::new(key, op, CompareTarget::Value(value.into()))
    }

    pub fn lease(key: impl Into<String>, op: CompareOp, lease: Option<LeaseId>) -> Self {
        Self::new(key, op, CompareTarget::Lease(lease))
    }

    /// Evaluates the compare against the current value of the key.
    pub(crate) fn holds(&self, kv: Option<&KeyValue>) -> bool {
        match &self.target {
            CompareTarget::Version(v) => self.op.holds(&kv.map_or(0, |kv| kv.version), v),
            CompareTarget::CreateRevision(r) => {
                self.op.holds(&kv.map_or(0, |kv| kv.create_revision), r)
            }
            CompareTarget::ModRevision(r) => self.op.holds(&kv.map_or(0, |kv| kv.mod_revision), r),
            CompareTarget::Value(v) => kv.is_some_and(|kv| self.op.holds(kv.value.as_slice(), v)),
            CompareTarget::Lease(l) => {
                let actual = kv.and_then(|kv| kv.lease).map_or(0, |l| l.0);
                self.op.holds(&actual, &l.map_or(0, |l| l.0))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put {
        key: String,
        value: Vec<u8>,
        lease: Option<LeaseId>,
    },
    Delete {
        key: String,
    },
    /// Deletes every key starting with the prefix.
    DeletePrefix {
        prefix: String,
    },
    /// Reads a key, seeing the writes of the previous operations of the transaction.
    Get {
        key: String,
    },
}

impl Op {
    pub fn put(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
            lease: None,
        }
    }

    /// Puts a key attached to a lease: it is deleted when the lease expires.
    pub fn put_with_lease(
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        lease: LeaseId,
    ) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
            lease: Some(lease),
        }
    }

    pub fn delete(key: impl Into<String>) -> Self {
        Self::Delete { key: key.into() }
    }

    pub fn delete_prefix(prefix: impl Into<String>) -> Self {
        Self::DeletePrefix {
            prefix: prefix.into(),
        }
    }

    pub fn get(key: impl Into<String>) -> Self {
        Self::Get { key: key.into() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpResponse {
    Put,
    Delete { deleted: u64 },
    Get(Option<KeyValue>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnResponse {
    /// Whether every compare held (success operations applied).
    pub succeeded: bool,
    /// Revision of the store after the transaction: the revision of its writes
    /// if it wrote something.
    pub revision: Revision,
    /// One response per applied operation.
    pub responses: Vec<OpResponse>,
}

impl TxnResponse {
    /// The value read by the `index`-th operation, if it was a [`Op::Get`].
    pub fn get(&self, index: usize) -> Option<&KeyValue> {
        match self.responses.get(index) {
            Some(OpResponse::Get(kv)) => kv.as_ref(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(version: i64, value: &str, lease: Option<i64>) -> KeyValue {
        KeyValue {
            key: "k".into(),
            value: value.as_bytes().to_vec(),
            create_revision: 2,
            mod_revision: 5,
            version,
            lease: lease.map(LeaseId),
        }
    }

    #[test]
    fn missing_key_compares() {
        assert!(Compare::absent("k").holds(None));
        assert!(!Compare::exists("k").holds(None));
        assert!(Compare::unchanged("k", 0).holds(None));
        assert!(Compare::lease("k", CompareOp::Equal, None).holds(None));
        // A value compare never holds on a missing key, even NotEqual.
        assert!(!Compare::value("k", CompareOp::NotEqual, "x").holds(None));
    }

    #[test]
    fn existing_key_compares() {
        let kv = kv(3, "b", Some(7));
        assert!(Compare::exists("k").holds(Some(&kv)));
        assert!(Compare::unchanged("k", 5).holds(Some(&kv)));
        assert!(!Compare::unchanged("k", 4).holds(Some(&kv)));
        assert!(Compare::create_revision("k", CompareOp::Less, 3).holds(Some(&kv)));
        assert!(Compare::value("k", CompareOp::Greater, "a").holds(Some(&kv)));
        assert!(Compare::lease("k", CompareOp::Equal, Some(LeaseId(7))).holds(Some(&kv)));
        assert!(Compare::lease("k", CompareOp::NotEqual, None).holds(Some(&kv)));
    }
}
