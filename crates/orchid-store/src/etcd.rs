use std::time::Duration;

use etcd_client as etcd;
use tokio::sync::mpsc;

use crate::{
    CompareOp, CompareTarget, EventKind, GetResponse, KeyRange, KeyValue, Lease, LeaseId,
    ListResponse, Op, OpResponse, Result, Revision, Store, StoreError, Txn, TxnResponse, Watch,
    WatchEvent, WatchResponse,
};

/// [`Store`] backed by an etcd cluster.
#[derive(Clone)]
pub struct EtcdStore {
    client: etcd::Client,
}

/// Client certificate used to connect to etcd in mTLS mode. PEM encoded.
#[derive(Clone)]
pub struct EtcdTls {
    pub ca: Vec<u8>,
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
}

impl EtcdStore {
    /// Connects to etcd. Endpoints use `http://` in cleartext mode, `https://`
    /// in mTLS mode (with `tls`).
    pub async fn connect(endpoints: &[impl AsRef<str>], tls: Option<EtcdTls>) -> Result<Self> {
        let mut options = etcd::ConnectOptions::new();
        if let Some(tls) = tls {
            options = options.with_tls(
                etcd::TlsOptions::new()
                    .ca_certificate(etcd::Certificate::from_pem(tls.ca))
                    .identity(etcd::Identity::from_pem(tls.certificate, tls.private_key)),
            );
        }
        let client = etcd::Client::connect(endpoints, Some(options))
            .await
            .map_err(map_error)?;
        Ok(Self { client })
    }
}

impl Store for EtcdStore {
    async fn get(&self, key: &str) -> Result<GetResponse> {
        let response = self
            .client
            .clone()
            .get(key, None)
            .await
            .map_err(map_error)?;
        Ok(GetResponse {
            kv: response.kvs().first().map(key_value).transpose()?,
            revision: revision(response.header()),
        })
    }

    async fn list(&self, prefix: &str) -> Result<ListResponse> {
        let options = etcd::GetOptions::new().with_prefix();
        let response = self
            .client
            .clone()
            .get(prefix, Some(options))
            .await
            .map_err(map_error)?;
        Ok(ListResponse {
            kvs: response
                .kvs()
                .iter()
                .map(key_value)
                .collect::<Result<_>>()?,
            revision: revision(response.header()),
        })
    }

    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        let request = etcd::Txn::new()
            .when(txn.compares.into_iter().map(compare).collect::<Vec<_>>())
            .and_then(txn.success.into_iter().map(op).collect::<Vec<_>>())
            .or_else(txn.failure.into_iter().map(op).collect::<Vec<_>>());
        let response = self.client.clone().txn(request).await.map_err(map_error)?;

        let responses = response
            .op_responses()
            .into_iter()
            .map(|response| match response {
                etcd::TxnOpResponse::Put(_) => Ok(OpResponse::Put),
                etcd::TxnOpResponse::Delete(delete) => Ok(OpResponse::Delete {
                    deleted: u64::try_from(delete.deleted()).unwrap_or(0),
                }),
                etcd::TxnOpResponse::Get(get) => Ok(OpResponse::Get(
                    get.kvs().first().map(key_value).transpose()?,
                )),
                etcd::TxnOpResponse::Txn(_) => Err(StoreError::InvalidRequest(
                    "unexpected nested transaction response".to_owned(),
                )),
            })
            .collect::<Result<_>>()?;

        Ok(TxnResponse {
            succeeded: response.succeeded(),
            revision: revision(response.header()),
            responses,
        })
    }

    async fn watch(&self, range: KeyRange, start_revision: Option<Revision>) -> Result<Watch> {
        let (key, mut options) = match range {
            KeyRange::Key(key) => (key, etcd::WatchOptions::new()),
            KeyRange::Prefix(prefix) => (prefix, etcd::WatchOptions::new().with_prefix()),
        };
        options = options.with_prev_key();
        if let Some(revision) = start_revision.filter(|r| *r > 0) {
            options = options.with_start_revision(revision);
        }
        let stream = self
            .client
            .clone()
            .watch(key, Some(options))
            .await
            .map_err(map_error)?;

        let (responses, receiver) = mpsc::unbounded_channel();
        let (progress_requests, progress_receiver) = mpsc::unbounded_channel();
        tokio::spawn(forward_watch(stream, responses, progress_receiver));

        Ok(Watch::new(receiver, move || {
            let _ = progress_requests.send(());
        }))
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<Lease> {
        let seconds = ttl.as_secs() + u64::from(ttl.subsec_nanos() > 0);
        let seconds = i64::try_from(seconds.max(1)).unwrap_or(i64::MAX);
        let response = self
            .client
            .clone()
            .lease_grant(seconds, None)
            .await
            .map_err(map_error)?;
        Ok(Lease {
            id: LeaseId(response.id()),
            ttl: Duration::from_secs(u64::try_from(response.ttl()).unwrap_or(0)),
        })
    }

    async fn keep_alive(&self, lease: LeaseId) -> Result<()> {
        // Opening the keep alive stream sends a first keep alive request and
        // fails if the lease doesn't exist anymore.
        self.client
            .clone()
            .lease_keep_alive(lease.0)
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        self.client
            .clone()
            .lease_revoke(lease.0)
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn compact(&self, revision: Revision) -> Result<()> {
        self.client
            .clone()
            .compact(revision, None)
            .await
            .map_err(|error| match map_error(error) {
                StoreError::Backend(error) if is_revision_error(&error.to_string()) => {
                    StoreError::InvalidRequest(error.to_string())
                }
                error => error,
            })?;
        Ok(())
    }
}

/// Forwards the responses of an etcd watch to a [`Watch`], until the watch is
/// dropped or fails.
async fn forward_watch(
    stream: etcd::WatchStream,
    responses: mpsc::UnboundedSender<Result<WatchResponse>>,
    mut progress_requests: mpsc::UnboundedReceiver<()>,
) {
    // Dropping the request sender at the end cancels the etcd watch.
    let (mut requests, mut stream) = stream.split();
    loop {
        tokio::select! {
            () = responses.closed() => return,
            request = progress_requests.recv() => {
                let Some(()) = request else { return };
                if let Err(error) = requests.request_progress().await {
                    let _ = responses.send(Err(map_error(error)));
                    return;
                }
            }
            message = stream.message() => {
                let response = match message {
                    Ok(Some(response)) => response,
                    Ok(None) => {
                        let _ = responses.send(Err(StoreError::WatchClosed("stream ended".to_owned())));
                        return;
                    }
                    Err(error) => {
                        let _ = responses.send(Err(map_error(error)));
                        return;
                    }
                };

                if response.canceled() {
                    let error = if response.compact_revision() > 0 {
                        StoreError::Compacted { compact_revision: response.compact_revision() }
                    } else {
                        StoreError::WatchClosed(response.cancel_reason().to_owned())
                    };
                    let _ = responses.send(Err(error));
                    return;
                }

                if response.events().is_empty() {
                    // Either the confirmation of the watch creation, or the
                    // answer to a progress request.
                    if !response.created() {
                        let progress = WatchResponse::Progress(revision(response.header()));
                        if responses.send(Ok(progress)).is_err() {
                            return;
                        }
                    }
                    continue;
                }

                let events = response.events().iter().map(watch_event).collect::<Result<Vec<_>>>();
                let failed = events.is_err();
                if responses.send(events.map(WatchResponse::Events)).is_err() || failed {
                    return;
                }
            }
        }
    }
}

fn revision(header: Option<&etcd::ResponseHeader>) -> Revision {
    header.map_or(0, etcd::ResponseHeader::revision)
}

fn key_value(kv: &etcd::KeyValue) -> Result<KeyValue> {
    let key = kv
        .key_str()
        .map_err(|e| StoreError::Backend(Box::new(e)))?
        .to_owned();
    Ok(KeyValue {
        key,
        value: kv.value().to_vec(),
        create_revision: kv.create_revision(),
        mod_revision: kv.mod_revision(),
        version: kv.version(),
        lease: (kv.lease() != 0).then(|| LeaseId(kv.lease())),
    })
}

fn watch_event(event: &etcd::Event) -> Result<WatchEvent> {
    let kind = match event.event_type() {
        etcd::EventType::Put => EventKind::Put,
        etcd::EventType::Delete => EventKind::Delete,
    };
    let kv = event
        .kv()
        .ok_or_else(|| StoreError::WatchClosed("event without key".to_owned()))?;
    Ok(WatchEvent {
        kind,
        kv: key_value(kv)?,
        prev_kv: event.prev_kv().map(key_value).transpose()?,
    })
}

fn compare(compare: crate::Compare) -> etcd::Compare {
    let op = match compare.op {
        CompareOp::Equal => etcd::CompareOp::Equal,
        CompareOp::NotEqual => etcd::CompareOp::NotEqual,
        CompareOp::Greater => etcd::CompareOp::Greater,
        CompareOp::Less => etcd::CompareOp::Less,
    };
    let key = compare.key;
    match compare.target {
        CompareTarget::Version(version) => etcd::Compare::version(key, op, version),
        CompareTarget::CreateRevision(revision) => {
            etcd::Compare::create_revision(key, op, revision)
        }
        CompareTarget::ModRevision(revision) => etcd::Compare::mod_revision(key, op, revision),
        CompareTarget::Value(value) => etcd::Compare::value(key, op, value),
        CompareTarget::Lease(lease) => etcd::Compare::lease(key, op, lease.map_or(0, |l| l.0)),
    }
}

fn op(op: Op) -> etcd::TxnOp {
    match op {
        Op::Put { key, value, lease } => etcd::TxnOp::put(
            key,
            value,
            lease.map(|lease| etcd::PutOptions::new().with_lease(lease.0)),
        ),
        Op::Delete { key } => etcd::TxnOp::delete(key, None),
        Op::DeletePrefix { prefix } => {
            etcd::TxnOp::delete(prefix, Some(etcd::DeleteOptions::new().with_prefix()))
        }
        Op::Get { key } => etcd::TxnOp::get(key, None),
    }
}

fn map_error(error: etcd::Error) -> StoreError {
    let message = match &error {
        etcd::Error::GRpcStatus(status) => status.message(),
        etcd::Error::LeaseKeepAliveError(message) => message.as_str(),
        _ => "",
    };
    if message.contains("lease not found") {
        StoreError::LeaseNotFound
    } else if message.contains("duplicate key given in txn request") {
        StoreError::InvalidRequest(message.to_owned())
    } else {
        StoreError::Backend(Box::new(error))
    }
}

fn is_revision_error(message: &str) -> bool {
    message.contains("required revision has been compacted")
        || message.contains("required revision is a future revision")
}
