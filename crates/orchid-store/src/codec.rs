//! Encoding of the values stored in etcd.
//!
//! Values are JSON documents, so they can be inspected with `etcdctl`.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{KeyValue, StoreError};

pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    // Serializing the API types to JSON cannot fail: they only contain
    // strings, numbers and maps with string keys.
    serde_json::to_vec(value).expect("value is serializable to JSON")
}

pub fn decode<T: DeserializeOwned>(kv: &KeyValue) -> Result<T, StoreError> {
    serde_json::from_slice(&kv.value).map_err(|source| StoreError::Decode {
        key: kv.key.clone(),
        source,
    })
}
