//! Actual immutable-write gate for deterministic cancellation/drain controls.
use crate::{
    BlobStore, BoundedMetadataStore, CasOutcome, LocalBlobStore, MetadataLimits, MetadataOwners,
    MetadataSnapshot, MetadataSwap, ObjectLogError, native_v2::page_store::NativeStore,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Semaphore;

pub(in crate::native_v2) struct GatedStore {
    inner: Arc<LocalBlobStore>,
    armed: AtomicBool,
    read_armed: AtomicBool,
    read_completed: AtomicBool,
    read_owner: std::sync::Mutex<Option<std::sync::Weak<dyn std::any::Any + Send + Sync>>>,
    reads: std::sync::Mutex<Vec<(String, usize)>>,
    entered: Semaphore,
    released: Semaphore,
}
impl GatedStore {
    pub fn new(inner: Arc<LocalBlobStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            armed: AtomicBool::new(false),
            read_armed: AtomicBool::new(false),
            read_completed: AtomicBool::new(false),
            read_owner: std::sync::Mutex::new(None),
            reads: std::sync::Mutex::new(Vec::new()),
            entered: Semaphore::new(0),
            released: Semaphore::new(0),
        })
    }
    pub fn arm_read(&self) {
        self.read_completed.store(false, Ordering::Release);
        *self.read_owner.lock().unwrap() = None;
        self.read_armed.store(true, Ordering::Release);
    }
    pub fn take_reads(&self) -> Vec<(String, usize)> {
        std::mem::take(&mut *self.reads.lock().unwrap())
    }
    pub fn armed_read_completed(&self) -> bool {
        self.read_completed.load(Ordering::Acquire)
    }
    pub fn armed_read_owner(&self) -> std::sync::Weak<dyn std::any::Any + Send + Sync> {
        self.read_owner
            .lock()
            .unwrap()
            .clone()
            .expect("armed read entered")
    }
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
    pub async fn entered(&self) {
        self.entered.acquire().await.unwrap().forget();
    }
    pub fn release(&self) {
        self.released.add_permits(1);
    }
}
#[async_trait]
impl BlobStore for GatedStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        self.inner.put(key, value).await
    }
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get_range(key, range).await
    }
    async fn get_range_exact(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get_range_exact(key, range).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        value: Bytes,
    ) -> Result<CasOutcome, ObjectLogError> {
        self.inner.compare_and_swap(key, expected, value).await
    }
}
#[async_trait]
impl BoundedMetadataStore for GatedStore {
    async fn read_metadata(
        &self,
        key: &str,
        limits: MetadataLimits,
        owners: MetadataOwners,
    ) -> Result<MetadataSnapshot, ObjectLogError> {
        self.reads
            .lock()
            .unwrap()
            .push((key.to_owned(), limits.payload_bytes));
        if self.read_armed.swap(false, Ordering::AcqRel) {
            *self.read_owner.lock().unwrap() = Some(Arc::downgrade(&owners.physical));
            self.entered.add_permits(1);
            self.released.acquire().await.unwrap().forget();
            let result = self.inner.read_metadata(key, limits, owners).await;
            self.read_completed.store(result.is_ok(), Ordering::Release);
            result
        } else {
            self.inner.read_metadata(key, limits, owners).await
        }
    }
    async fn swap_metadata(
        &self,
        key: &str,
        observed: MetadataSnapshot,
        value: Bytes,
        limits: MetadataLimits,
        owners: MetadataOwners,
    ) -> Result<MetadataSwap, ObjectLogError> {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.entered.add_permits(1);
            self.released.acquire().await.unwrap().forget();
        }
        self.inner
            .swap_metadata(key, observed, value, limits, owners)
            .await
    }
}
impl NativeStore for GatedStore {
    fn page_source_scratch(
        &self,
        key_len: usize,
        limits: MetadataLimits,
    ) -> Result<usize, ObjectLogError> {
        NativeStore::page_source_scratch(self.inner.as_ref(), key_len, limits)
    }
}
