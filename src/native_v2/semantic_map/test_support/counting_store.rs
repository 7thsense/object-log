//! Counts every storage call so tests prove costs by calls, never by timing.
use crate::{
    BlobStore, BoundedMetadataStore, CasOutcome, LocalBlobStore, MetadataLimits, MetadataOwners,
    MetadataSnapshot, MetadataSwap, ObjectLogError, native_v2::page_store::NativeStore,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

type KeyMatcher = Box<dyn Fn(&str) -> bool + Send>;

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
    swap_delay_ms: AtomicU64,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    fail_swap: Mutex<Option<KeyMatcher>>,
}
impl CountingStore {
    pub fn new(inner: Arc<LocalBlobStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            calls: Mutex::new(Calls::default()),
            swap_delay_ms: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            fail_swap: Mutex::new(None),
        })
    }
    pub fn take(&self) -> Calls {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
    /// Hold every conditional write for this long so overlapping writes are observable.
    pub fn delay_swaps(&self, millis: u64) {
        self.swap_delay_ms.store(millis, Ordering::Release);
    }
    /// Highest number of conditional writes executing at once; resets when read.
    pub fn take_max_concurrent_swaps(&self) -> usize {
        self.max_in_flight.swap(0, Ordering::AcqRel)
    }
    /// Conditional writes executing right now.
    pub fn swaps_in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
    /// Fail the next conditional write whose key matches, before any effect.
    pub fn fail_next_swap(&self, matching: impl Fn(&str) -> bool + Send + 'static) {
        *self.fail_swap.lock().unwrap() = Some(Box::new(matching));
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
        let injected = {
            let mut armed = self.fail_swap.lock().unwrap();
            if armed.as_ref().is_some_and(|matching| matching(key)) {
                armed.take();
                true
            } else {
                false
            }
        };
        let now = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight.fetch_max(now, Ordering::AcqRel);
        let delay = self.swap_delay_ms.load(Ordering::Acquire);
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        let result = if injected {
            Err(ObjectLogError::StorageUnavailable(
                "injected immutable upload failure".into(),
            ))
        } else {
            self.inner
                .swap_metadata(key, observed, value, limits, owners)
                .await
        };
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        result
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
    fn absent(&self, key: &str, retained: crate::AppendOwner) -> MetadataSnapshot {
        NativeStore::absent(self.inner.as_ref(), key, retained)
    }
}
