//! Counts every storage call so tests prove costs by calls, never by timing.
use crate::{
    BlobStore, BoundedMetadataStore, CasOutcome, LocalBlobStore, MetadataLimits, MetadataOwners,
    MetadataSnapshot, MetadataSwap, ObjectLogError, native_v2::page_store::NativeStore,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    ops::Range,
    sync::{Arc, Mutex},
};

/// Calls observed since the previous `take`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::native_v2) struct Calls {
    /// Keys of every read of any kind, including listings and range reads.
    pub reads: Vec<String>,
    /// Keys of every conditional or unconditional write attempt.
    pub writes: Vec<String>,
}

pub(in crate::native_v2) struct CountingStore {
    inner: Arc<LocalBlobStore>,
    calls: Mutex<Calls>,
}
impl CountingStore {
    pub fn new(inner: Arc<LocalBlobStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            calls: Mutex::new(Calls::default()),
        })
    }
    pub fn take(&self) -> Calls {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
    fn read(&self, key: &str) {
        self.calls.lock().unwrap().reads.push(key.to_owned());
    }
    fn write(&self, key: &str) {
        self.calls.lock().unwrap().writes.push(key.to_owned());
    }
}
#[async_trait]
impl BlobStore for CountingStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        self.write(key);
        self.inner.put(key, value).await
    }
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        self.read(key);
        self.inner.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.read(key);
        self.inner.get_range(key, range).await
    }
    async fn get_range_exact(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.read(key);
        self.inner.get_range_exact(key, range).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.read(prefix);
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.write(key);
        self.inner.delete(key).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        value: Bytes,
    ) -> Result<CasOutcome, ObjectLogError> {
        self.write(key);
        self.inner.compare_and_swap(key, expected, value).await
    }
}
#[async_trait]
impl BoundedMetadataStore for CountingStore {
    async fn read_metadata(
        &self,
        key: &str,
        limits: MetadataLimits,
        owners: MetadataOwners,
    ) -> Result<MetadataSnapshot, ObjectLogError> {
        self.read(key);
        self.inner.read_metadata(key, limits, owners).await
    }
    async fn swap_metadata(
        &self,
        key: &str,
        observed: MetadataSnapshot,
        value: Bytes,
        limits: MetadataLimits,
        owners: MetadataOwners,
    ) -> Result<MetadataSwap, ObjectLogError> {
        self.write(key);
        self.inner
            .swap_metadata(key, observed, value, limits, owners)
            .await
    }
}
impl NativeStore for CountingStore {
    fn page_source_scratch(
        &self,
        key_len: usize,
        limits: MetadataLimits,
    ) -> Result<usize, ObjectLogError> {
        NativeStore::page_source_scratch(self.inner.as_ref(), key_len, limits)
    }
}
