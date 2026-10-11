//! A `BlobStore`-persisted [`Sequencer`]: the offset→location index survives a
//! restart, so a standalone object-log is crash-durable end to end.
//!
//! Each `commit` durably writes one manifest object (≤1 PUT per group-commit,
//! co-amortized with the data object) recording the batches it assigned. On
//! [`open`](ManifestSequencer::open) the index is rebuilt by replaying the
//! manifest objects in order.

use crate::{
    BlobStore, CasOutcome, CommitBatch, CommitOutcome, IndexEntry, ObjectLogError, PartitionKey,
    Sequencer,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::runtime::{Builder, Runtime};

#[derive(Serialize, Deserialize)]
struct ManifestRecord {
    entries: Vec<(PartitionKey, IndexEntry)>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionIndexDoc {
    epoch: u64,
    next_offset: i64,
    log_start: i64,
    entries: Vec<IndexEntry>,
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct IndexCatalog {
    partitions: Vec<String>,
}

#[derive(Default)]
struct Part {
    next_offset: i64,
    log_start: i64,
    epoch: u64,
    entries: Vec<IndexEntry>,
    /// Exact bytes last read or written for this partition's index object.
    observed: Option<Bytes>,
}

struct Inner {
    parts: HashMap<PartitionKey, Part>,
    counter: u64,
    catalog: Option<Bytes>,
}

/// A [`Sequencer`] (`Meta = ()`) that persists its index to a [`BlobStore`].
pub struct ManifestSequencer {
    blob: Arc<dyn BlobStore>,
    prefix: String,
    // `Option` so `Drop` can `shutdown_background()` — dropping a `Runtime` inside
    // an async context (e.g. an `Arc` last-ref drop in a `#[tokio::test]`) panics.
    rt: Option<Runtime>,
    // Serialize mutations across publication without blocking committed reads.
    commit_order: Mutex<()>,
    inner: Mutex<Inner>,
}

impl Drop for ManifestSequencer {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl ManifestSequencer {
    /// Open (or recover) a persisted sequencer. Manifest objects live under
    /// `manifest_prefix` (keep it disjoint from the engine's data-object prefix).
    /// Existing manifests are replayed to rebuild the index.
    pub async fn open(
        blob: Arc<dyn BlobStore>,
        manifest_prefix: impl Into<String>,
    ) -> Result<Self, ObjectLogError> {
        let prefix = manifest_prefix.into();
        let catalog_key = format!("{prefix}catalog");
        let (parts, catalog, counter) = if let Some(catalog_bytes) = blob.get(&catalog_key).await? {
            let catalog: IndexCatalog = serde_json::from_slice(&catalog_bytes)
                .map_err(|e| ObjectLogError::Sequencer(format!("index catalog: {e}")))?;
            let mut parts = HashMap::new();
            for name in &catalog.partitions {
                let key = index_object_key(&prefix, name);
                let Some(doc_bytes) = blob.get(&key).await? else {
                    parts.insert(PartitionKey(name.clone()), Part::default());
                    continue;
                };
                let doc: PartitionIndexDoc = serde_json::from_slice(&doc_bytes).map_err(|e| {
                    ObjectLogError::Sequencer(format!("partition index {name}: {e}"))
                })?;
                parts.insert(PartitionKey(name.clone()), Part::from_doc(doc, doc_bytes));
            }
            let counter = parts.len() as u64;
            (parts, Some(catalog_bytes), counter)
        } else {
            let mut keys = blob.list(&prefix).await?;
            keys.sort();
            let mut parts: HashMap<PartitionKey, Part> = HashMap::new();
            for key in &keys {
                if key == &catalog_key || key.starts_with(&format!("{prefix}index/")) {
                    continue;
                }
                let Some(bytes) = blob.get(key).await? else {
                    continue;
                };
                let Ok(rec) = serde_json::from_slice::<ManifestRecord>(&bytes) else {
                    continue;
                };
                for (pkey, entry) in rec.entries {
                    let p = parts.entry(pkey).or_default();
                    let end = entry.base_offset + entry.record_count as i64;
                    if end > p.next_offset {
                        p.next_offset = end;
                    }
                    p.entries.push(entry);
                }
            }
            (parts, None, keys.len() as u64)
        };
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ObjectLogError::StorageUnavailable(e.to_string()))?;
        Ok(Self {
            blob,
            prefix,
            rt: Some(rt),
            commit_order: Mutex::new(()),
            inner: Mutex::new(Inner {
                parts,
                counter,
                catalog,
            }),
        })
    }

    fn index_key(&self, partition: &PartitionKey) -> String {
        index_object_key(&self.prefix, partition.as_str())
    }

    fn catalog_key(&self) -> String {
        format!("{}catalog", self.prefix)
    }

    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        self.rt
            .as_ref()
            .expect("runtime present until drop")
            .block_on(future)
    }

    fn read_object(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        self.block_on(self.blob.get(key))
    }

    fn cas(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new_value: Bytes,
    ) -> Result<CasOutcome, ObjectLogError> {
        self.block_on(self.blob.compare_and_swap(key, expected, new_value))
    }

    fn ensure_catalog(&self, partitions: &[PartitionKey]) -> Result<(), ObjectLogError> {
        let key = self.catalog_key();
        for _ in 0..8 {
            let current = self.read_object(&key)?;
            let mut names = if let Some(bytes) = &current {
                serde_json::from_slice::<IndexCatalog>(bytes)
                    .map_err(|error| ObjectLogError::Sequencer(format!("index catalog: {error}")))?
                    .partitions
            } else {
                Vec::new()
            };
            let mut changed = false;
            for partition in partitions {
                if !names.iter().any(|name| name == partition.as_str()) {
                    names.push(partition.0.clone());
                    changed = true;
                }
            }
            if !changed {
                self.inner.lock().expect("poisoned").catalog = current;
                return Ok(());
            }
            names.sort();
            names.dedup();
            let new_bytes = Bytes::from(
                serde_json::to_vec(&IndexCatalog { partitions: names })
                    .map_err(|error| ObjectLogError::Sequencer(error.to_string()))?,
            );
            match self.cas(&key, current, new_bytes.clone())? {
                CasOutcome::Stored => {
                    self.inner.lock().expect("poisoned").catalog = Some(new_bytes);
                    return Ok(());
                }
                CasOutcome::Conflict { .. } => continue,
            }
        }
        Err(ObjectLogError::Sequencer(
            "catalog compare-and-swap did not land".into(),
        ))
    }

    fn publish_partition(
        &self,
        partition: &PartitionKey,
        batches: &[CommitBatch<'_, ()>],
        indexes: &[usize],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let epoch = batches[indexes[0]].epoch;
        if indexes.iter().any(|index| batches[*index].epoch != epoch) {
            return Ok(reject_all(
                indexes.len(),
                "one partition commit mixed fence epochs",
            ));
        }
        let (observed, mut doc) = {
            let inner = self.inner.lock().expect("poisoned");
            match inner
                .parts
                .get(partition)
                .and_then(|part| part.observed.clone())
            {
                Some(bytes) => {
                    let doc = serde_json::from_slice::<PartitionIndexDoc>(&bytes)
                        .map_err(|error| ObjectLogError::Sequencer(error.to_string()))?;
                    (Some(bytes), doc)
                }
                None => {
                    let part = inner.parts.get(partition);
                    (
                        None,
                        PartitionIndexDoc {
                            epoch,
                            next_offset: part.map_or(0, |part| part.next_offset),
                            log_start: part.map_or(0, |part| part.log_start),
                            entries: part.map(|part| part.entries.clone()).unwrap_or_default(),
                        },
                    )
                }
            }
        };
        if observed.is_some() && doc.epoch != epoch {
            return Ok(reject_all(
                indexes.len(),
                format!(
                    "partition {} epoch is {}, not {epoch}",
                    partition.as_str(),
                    doc.epoch
                ),
            ));
        }
        let mut outcomes = Vec::with_capacity(indexes.len());
        let mut planned = Vec::with_capacity(indexes.len());
        for index in indexes {
            let batch = &batches[*index];
            let entry = IndexEntry {
                location: batch.location.clone(),
                base_offset: doc.next_offset,
                record_count: batch.record_count,
            };
            outcomes.push(CommitOutcome::Assigned {
                base_offset: entry.base_offset,
                record_count: entry.record_count,
            });
            doc.next_offset += i64::from(batch.record_count);
            planned.push(entry.clone());
            doc.entries.push(entry);
        }
        let new_bytes = Bytes::from(
            serde_json::to_vec(&doc)
                .map_err(|error| ObjectLogError::Sequencer(error.to_string()))?,
        );
        match self.cas(&self.index_key(partition), observed, new_bytes.clone())? {
            CasOutcome::Stored => {
                self.install_doc(partition, &doc, new_bytes);
                Ok(outcomes)
            }
            CasOutcome::Conflict { current } => {
                if let Some(bytes) = current
                    && let Ok(found) = serde_json::from_slice::<PartitionIndexDoc>(&bytes)
                {
                    let already = planned
                        .iter()
                        .all(|entry| found.entries.iter().any(|have| have == entry));
                    self.install_doc(partition, &found, bytes);
                    if already {
                        return Ok(outcomes);
                    }
                }
                Ok(reject_all(
                    indexes.len(),
                    format!("partition {} tail moved", partition.as_str()),
                ))
            }
        }
    }

    /// Install `partition`'s durable index, if any, and return it.
    fn reload_partition(
        &self,
        partition: &PartitionKey,
    ) -> Result<Option<(PartitionIndexDoc, Bytes)>, ObjectLogError> {
        let Some(bytes) = self.read_object(&self.index_key(partition))? else {
            return Ok(None);
        };
        let doc: PartitionIndexDoc = serde_json::from_slice(&bytes)
            .map_err(|error| ObjectLogError::Sequencer(error.to_string()))?;
        self.install_doc(partition, &doc, bytes.clone());
        Ok(Some((doc, bytes)))
    }

    fn install_doc(&self, partition: &PartitionKey, doc: &PartitionIndexDoc, observed: Bytes) {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.parts.insert(
            partition.clone(),
            Part {
                next_offset: doc.next_offset,
                log_start: doc.log_start,
                epoch: doc.epoch,
                entries: doc.entries.clone(),
                observed: Some(observed),
            },
        );
    }

    /// Object ids referenced by any live index entry (for orphan reaping).
    /// Does not include manifest object keys themselves.
    pub fn live_object_ids(&self) -> HashSet<String> {
        let st = self.inner.lock().expect("poisoned");
        let mut live = HashSet::new();
        for p in st.parts.values() {
            for e in &p.entries {
                live.insert(e.location.object_id.clone());
            }
        }
        live
    }

    /// Point-in-time view of the rebuilt index (diagnostics / CLI).
    pub fn snapshot(&self) -> IndexSnapshot {
        let st = self.inner.lock().expect("poisoned");
        let mut partitions: Vec<PartitionSnapshot> = st
            .parts
            .iter()
            .map(|(k, p)| PartitionSnapshot {
                partition: k.0.clone(),
                log_start: p.log_start,
                high_watermark: p.next_offset,
                entry_count: p.entries.len(),
                entries: p.entries.clone(),
            })
            .collect();
        partitions.sort_by(|a, b| a.partition.cmp(&b.partition));
        IndexSnapshot {
            manifest_prefix: self.prefix.clone(),
            manifest_count: st.counter,
            partitions,
        }
    }
}

/// Full index view from [`ManifestSequencer::snapshot`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSnapshot {
    /// Prefix used for manifest objects.
    pub manifest_prefix: String,
    /// Number of manifest objects replayed (commit counter).
    pub manifest_count: u64,
    /// Per-partition index state.
    pub partitions: Vec<PartitionSnapshot>,
}

/// One partition's index bounds and entries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    /// Partition key string.
    pub partition: String,
    /// First readable offset.
    pub log_start: i64,
    /// Next offset to assign (high watermark).
    pub high_watermark: i64,
    /// Number of index entries.
    pub entry_count: usize,
    /// Ordered index entries (may be large; CLI can omit via flag).
    pub entries: Vec<IndexEntry>,
}

impl Sequencer for ManifestSequencer {
    type Meta = ();

    fn supports_multi_object_commit(&self) -> bool {
        true
    }

    fn commit(
        &self,
        batches: &[CommitBatch<'_, ()>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let _commit = self.commit_order.lock().expect("poisoned");
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let mut order: Vec<PartitionKey> = Vec::new();
        let mut groups: HashMap<PartitionKey, Vec<usize>> = HashMap::new();
        for (index, batch) in batches.iter().enumerate() {
            if !groups.contains_key(&batch.partition) {
                order.push(batch.partition.clone());
            }
            groups
                .entry(batch.partition.clone())
                .or_default()
                .push(index);
        }
        self.ensure_catalog(&order)?;
        let mut outcomes = vec![None; batches.len()];
        let mut published = false;
        for partition in order {
            let indexes = &groups[&partition];
            match self.publish_partition(&partition, batches, indexes) {
                Ok(partition_outcomes) => {
                    for (index, outcome) in indexes.iter().zip(partition_outcomes) {
                        if matches!(
                            outcome,
                            CommitOutcome::Assigned { .. } | CommitOutcome::Duplicate { .. }
                        ) {
                            published = true;
                        }
                        outcomes[*index] = Some(outcome);
                    }
                }
                Err(error) => {
                    for index in indexes {
                        outcomes[*index] = Some(CommitOutcome::Rejected {
                            reason: error.to_string(),
                        });
                    }
                }
            }
        }
        if published {
            let mut inner = self.inner.lock().expect("poisoned");
            inner.counter = inner.counter.saturating_add(1);
        }
        Ok(outcomes
            .into_iter()
            .map(|outcome| outcome.expect("every batch has an outcome"))
            .collect())
    }

    fn fence_epoch(
        &self,
        partition: &PartitionKey,
        expected: u64,
        new_epoch: u64,
    ) -> Result<(), ObjectLogError> {
        let _commit = self.commit_order.lock().expect("poisoned");
        if expected == new_epoch {
            return Ok(());
        }
        // Fence the durable index, not this handle's view of it: another writer
        // may have created or advanced the index since this handle opened, and a
        // fence that only compared cached bytes would silently store nothing.
        self.ensure_catalog(std::slice::from_ref(partition))?;
        for _ in 0..8 {
            let (mut doc, observed) = match self.reload_partition(partition)? {
                Some((current, bytes)) => {
                    if current.epoch == new_epoch {
                        return Ok(());
                    }
                    if current.epoch != expected {
                        return Err(ObjectLogError::Sequencer(format!(
                            "partition {} epoch is {}, not {expected}",
                            partition.as_str(),
                            current.epoch
                        )));
                    }
                    (current, Some(bytes))
                }
                None => {
                    // No index yet: create it at the new epoch, so a writer that
                    // never saw one cannot publish under an older epoch.
                    let inner = self.inner.lock().expect("poisoned");
                    let part = inner.parts.get(partition);
                    let doc = PartitionIndexDoc {
                        epoch: new_epoch,
                        next_offset: part.map_or(0, |part| part.next_offset),
                        log_start: part.map_or(0, |part| part.log_start),
                        entries: part.map(|part| part.entries.clone()).unwrap_or_default(),
                    };
                    (doc, None)
                }
            };
            doc.epoch = new_epoch;
            let new_bytes = Bytes::from(
                serde_json::to_vec(&doc)
                    .map_err(|error| ObjectLogError::Sequencer(error.to_string()))?,
            );
            match self.cas(&self.index_key(partition), observed, new_bytes.clone())? {
                CasOutcome::Stored => {
                    self.install_doc(partition, &doc, new_bytes);
                    return Ok(());
                }
                // A concurrent commit or fence moved the index; re-read and retry.
                CasOutcome::Conflict { .. } => continue,
            }
        }
        Err(ObjectLogError::Sequencer(format!(
            "partition {} epoch fence did not land",
            partition.as_str()
        )))
    }

    fn partition_epoch(&self, partition: &PartitionKey) -> Option<u64> {
        let inner = self.inner.lock().expect("poisoned");
        inner
            .parts
            .get(partition)
            .filter(|part| part.observed.is_some())
            .map(|part| part.epoch)
    }

    /// A batch rejected because this partition's index was fenced past its epoch
    /// can never commit: report [`ObjectLogError::Fenced`]. Other rejections stay
    /// [`ObjectLogError::Sequencer`]. The index read during the rejected commit
    /// is the one consulted.
    fn rejection_error(
        &self,
        partition: &PartitionKey,
        epoch: u64,
        reason: String,
    ) -> ObjectLogError {
        #[allow(deprecated)]
        match self.partition_epoch(partition) {
            Some(current) if current > epoch => ObjectLogError::Fenced {
                partition: partition.as_str().to_owned(),
                epoch,
                current,
            },
            _ => ObjectLogError::Sequencer(reason),
        }
    }

    fn refresh_partition(&self, partition: &PartitionKey) -> Result<(), ObjectLogError> {
        let _commit = self.commit_order.lock().expect("poisoned");
        self.reload_partition(partition)?;
        Ok(())
    }

    fn lookup(
        &self,
        partition: &PartitionKey,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>, ObjectLogError> {
        let st = self.inner.lock().expect("poisoned");
        let Some(p) = st.parts.get(partition) else {
            return Ok(Vec::new());
        };
        Ok(p.entries
            .iter()
            .filter(|e| e.base_offset + e.record_count as i64 > fetch_offset)
            .cloned()
            .collect())
    }

    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .inner
            .lock()
            .expect("poisoned")
            .parts
            .get(partition)
            .map_or(0, |p| p.next_offset))
    }

    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .inner
            .lock()
            .expect("poisoned")
            .parts
            .get(partition)
            .map_or(0, |p| p.log_start))
    }

    fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError> {
        let _commit = self.commit_order.lock().expect("poisoned");
        let snapshot = {
            let st = self.inner.lock().expect("poisoned");
            let Some(part) = st.parts.get(partition) else {
                return Ok(Vec::new());
            };
            (
                part.observed.clone(),
                part.entries.clone(),
                part.next_offset,
                part.epoch,
                part.log_start,
            )
        };
        let (observed, entries, next_offset, epoch, mut log_start) = snapshot;
        let mut dropped = Vec::new();
        let mut kept = Vec::new();
        for entry in entries {
            if entry.base_offset + i64::from(entry.record_count) <= offset {
                dropped.push(entry.location.object_id.clone());
            } else {
                kept.push(entry);
            }
        }
        if offset > log_start {
            log_start = offset.min(next_offset);
        }
        let doc = PartitionIndexDoc {
            epoch,
            next_offset,
            log_start,
            entries: kept,
        };
        if observed.is_some() {
            let new_bytes = Bytes::from(
                serde_json::to_vec(&doc)
                    .map_err(|error| ObjectLogError::Sequencer(error.to_string()))?,
            );
            match self.cas(&self.index_key(partition), observed, new_bytes.clone())? {
                CasOutcome::Stored => self.install_doc(partition, &doc, new_bytes),
                CasOutcome::Conflict { .. } => {
                    return Err(ObjectLogError::Sequencer(format!(
                        "partition {} truncate lost the compare-and-swap",
                        partition.as_str()
                    )));
                }
            }
        } else {
            let mut st = self.inner.lock().expect("poisoned");
            if let Some(part) = st.parts.get_mut(partition) {
                part.entries = doc.entries.clone();
                part.log_start = doc.log_start;
            }
        }
        let live = {
            let st = self.inner.lock().expect("poisoned");
            let mut live = HashSet::new();
            for part in st.parts.values() {
                for entry in &part.entries {
                    live.insert(entry.location.object_id.clone());
                }
            }
            live
        };
        let mut dead = Vec::new();
        let mut seen = HashSet::new();
        for object_id in dropped {
            if !live.contains(&object_id) && seen.insert(object_id.clone()) {
                dead.push(object_id);
            }
        }
        Ok(dead)
    }
}

impl Part {
    fn from_doc(doc: PartitionIndexDoc, observed: Bytes) -> Self {
        Self {
            next_offset: doc.next_offset,
            log_start: doc.log_start,
            epoch: doc.epoch,
            entries: doc.entries,
            observed: Some(observed),
        }
    }
}

fn index_object_key(prefix: &str, partition: &str) -> String {
    let mut encoded = String::with_capacity(partition.len() * 2);
    for byte in partition.as_bytes() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    format!("{prefix}index/{encoded}")
}

fn reject_all(count: usize, reason: impl Into<String>) -> Vec<CommitOutcome> {
    let reason = reason.into();
    vec![CommitOutcome::Rejected { reason }; count]
}

#[cfg(test)]
mod index_tests {
    use super::*;
    use crate::{
        BatchLocation, BlobStore, CommitBatch, CommitOutcome, Durability, FlushConfig,
        InMemorySequencer, LogEngine, MemoryBlobStore, ObjectLogError, PartitionKey,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn location(object: &str) -> BatchLocation {
        BatchLocation {
            object_id: object.to_string(),
            byte_start: 0,
            byte_len: 1,
        }
    }

    fn run_commit(
        seq: Arc<ManifestSequencer>,
        partitions: Vec<(&str, &str, u64)>,
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let owned: Vec<(String, String, u64)> = partitions
            .into_iter()
            .map(|(partition, object, epoch)| (partition.to_string(), object.to_string(), epoch))
            .collect();
        std::thread::spawn(move || {
            let meta = ();
            let batches: Vec<CommitBatch<'_, ()>> = owned
                .iter()
                .map(|(partition, object, epoch)| CommitBatch {
                    partition: PartitionKey(partition.clone()),
                    record_count: 1,
                    location: location(object),
                    meta: &meta,
                    epoch: *epoch,
                })
                .collect();
            seq.commit(&batches)
        })
        .join()
        .expect("commit thread")
    }

    /// Run a blocking sequencer call off the async test thread.
    fn on_thread<T: Send + 'static>(
        seq: &Arc<ManifestSequencer>,
        call: impl FnOnce(&ManifestSequencer) -> T + Send + 'static,
    ) -> T {
        let seq = Arc::clone(seq);
        std::thread::spawn(move || call(&seq))
            .join()
            .expect("sequencer thread")
    }

    struct ListProbe {
        inner: MemoryBlobStore,
        lists: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BlobStore for ListProbe {
        async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
            self.inner.put(key, value).await
        }
        async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
            self.inner.get(key).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: std::ops::Range<u64>,
        ) -> Result<Option<Bytes>, ObjectLogError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
            self.inner.delete(key).await
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            expected: Option<Bytes>,
            new_value: Bytes,
        ) -> Result<crate::CasOutcome, ObjectLogError> {
            self.inner.compare_and_swap(key, expected, new_value).await
        }
    }

    #[tokio::test]
    async fn two_writers_cannot_commit_the_same_partition_tail() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let first = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let second = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let won = run_commit(first, vec![("p", "obj-a", 1)]).unwrap();
        assert!(matches!(
            won[0],
            CommitOutcome::Assigned {
                base_offset: 0,
                record_count: 1
            }
        ));
        let lost = run_commit(second, vec![("p", "obj-b", 1)]).unwrap();
        assert!(
            matches!(lost[0], CommitOutcome::Rejected { .. }),
            "stale tail must fail closed, got {lost:?}"
        );
        let reopened = ManifestSequencer::open(blob, "manifest/").await.unwrap();
        let partition = PartitionKey("p".into());
        assert_eq!(reopened.high_watermark(&partition).unwrap(), 1);
        let entries = reopened.lookup(&partition, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].location.object_id, "obj-a");
    }

    #[tokio::test]
    async fn mixed_seal_keeps_the_partition_that_won_and_rejects_the_moved_tail() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let first = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let second = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        run_commit(first, vec![("p", "obj-a", 1)]).unwrap();
        let mixed = run_commit(second, vec![("p", "obj-b", 1), ("q", "obj-b", 1)]).unwrap();
        assert!(matches!(mixed[0], CommitOutcome::Rejected { .. }));
        assert!(matches!(
            mixed[1],
            CommitOutcome::Assigned { base_offset: 0, .. }
        ));
        let reopened = ManifestSequencer::open(blob, "manifest/").await.unwrap();
        assert_eq!(
            reopened.lookup(&PartitionKey("p".into()), 0).unwrap().len(),
            1
        );
        assert_eq!(
            reopened.high_watermark(&PartitionKey("p".into())).unwrap(),
            1,
            "the rejected partition must not skip an offset"
        );
        assert_eq!(
            reopened.lookup(&PartitionKey("q".into()), 0).unwrap()[0]
                .location
                .object_id,
            "obj-b"
        );
    }

    #[tokio::test]
    async fn open_does_not_list_the_manifest_prefix_when_an_index_exists() {
        let memory = MemoryBlobStore::new();
        let blob: Arc<dyn BlobStore> = Arc::new(memory.clone());
        let seq = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        run_commit(seq, vec![("p", "obj-a", 4)]).unwrap();
        let lists = Arc::new(AtomicUsize::new(0));
        let probe = Arc::new(ListProbe {
            inner: memory,
            lists: Arc::clone(&lists),
        });
        let reopened = ManifestSequencer::open(probe, "manifest/").await.unwrap();
        assert_eq!(
            lists.load(Ordering::SeqCst),
            0,
            "a current index catalog must be read by key, not by listing manifests"
        );
        assert_eq!(
            reopened.high_watermark(&PartitionKey("p".into())).unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn distinct_writers_do_not_share_a_data_object_key() {
        let blob = Arc::new(MemoryBlobStore::new());
        let config = FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        };
        let left = LogEngine::new_with_writer(
            Arc::clone(&blob) as Arc<dyn BlobStore>,
            Arc::new(InMemorySequencer::new()),
            config,
            "data/",
            "writer-a",
        );
        let right = LogEngine::new_with_writer(
            Arc::clone(&blob) as Arc<dyn BlobStore>,
            Arc::new(InMemorySequencer::new()),
            config,
            "data/",
            "writer-b",
        );
        left.produce_at_epoch(
            PartitionKey("p".into()),
            Bytes::from_static(b"left"),
            1,
            (),
            Durability::Sequenced,
            3,
        )
        .await
        .unwrap();
        right
            .produce_at_epoch(
                PartitionKey("p".into()),
                Bytes::from_static(b"right"),
                1,
                (),
                Durability::Sequenced,
                3,
            )
            .await
            .unwrap();
        let keys = blob.list("data/").await.unwrap();
        assert_eq!(
            keys.len(),
            2,
            "writers must not overwrite one object: {keys:?}"
        );
        assert!(keys.iter().any(|key| key.contains("writer-a")));
        assert!(keys.iter().any(|key| key.contains("writer-b")));
        assert!(
            keys.iter()
                .all(|key| key.contains("/00000000000000000003/"))
        );
    }

    #[tokio::test]
    async fn fence_from_a_handle_that_never_saw_the_index_rejects_the_stale_writer() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let owner = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        // The standby opens before the owner's first commit, so it holds no index.
        let standby = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        run_commit(Arc::clone(&owner), vec![("p", "obj-a", 1)]).unwrap();
        let partition = PartitionKey("p".into());
        let fence_partition = partition.clone();
        on_thread(&standby, move |seq| seq.fence_epoch(&fence_partition, 1, 2)).unwrap();

        let stale = run_commit(Arc::clone(&owner), vec![("p", "obj-stale", 1)]).unwrap();
        assert!(
            matches!(stale[0], CommitOutcome::Rejected { .. }),
            "a commit at the fenced epoch must fail closed, got {stale:?}"
        );
        let fresh = run_commit(standby, vec![("p", "obj-b", 2)]).unwrap();
        assert!(matches!(
            fresh[0],
            CommitOutcome::Assigned { base_offset: 1, .. }
        ));
        let reopened = ManifestSequencer::open(blob, "manifest/").await.unwrap();
        let objects: Vec<_> = reopened
            .lookup(&partition, 0)
            .unwrap()
            .into_iter()
            .map(|entry| entry.location.object_id)
            .collect();
        assert_eq!(objects, ["obj-a", "obj-b"]);
    }

    #[tokio::test]
    async fn fence_before_any_commit_creates_the_index_at_the_new_epoch() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let stale = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let fencer = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        on_thread(&fencer, |seq| {
            seq.fence_epoch(&PartitionKey("p".into()), 0, 2)
        })
        .unwrap();
        let lost = run_commit(stale, vec![("p", "obj-stale", 1)]).unwrap();
        assert!(
            matches!(lost[0], CommitOutcome::Rejected { .. }),
            "a writer that never saw the fenced index must not commit, got {lost:?}"
        );
        let won = run_commit(fencer, vec![("p", "obj-b", 2)]).unwrap();
        assert!(matches!(
            won[0],
            CommitOutcome::Assigned { base_offset: 0, .. }
        ));
        let reopened = ManifestSequencer::open(blob, "manifest/").await.unwrap();
        assert_eq!(
            reopened.snapshot().partitions[0].entries[0]
                .location
                .object_id,
            "obj-b"
        );
    }

    #[tokio::test]
    async fn refresh_partition_reveals_another_writers_commits() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let writer = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let reader = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        run_commit(writer, vec![("p", "obj-a", 1), ("p", "obj-b", 1)]).unwrap();
        let partition = PartitionKey("p".into());
        assert_eq!(reader.high_watermark(&partition).unwrap(), 0);
        let refreshed = partition.clone();
        on_thread(&reader, move |seq| seq.refresh_partition(&refreshed)).unwrap();
        assert_eq!(reader.high_watermark(&partition).unwrap(), 2);
        assert_eq!(reader.lookup(&partition, 0).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_fenced_writer_gets_a_typed_fenced_error() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let config = FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        };
        let owner_sequencer = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let standby = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let owner = LogEngine::new_with_writer(
            Arc::clone(&blob),
            owner_sequencer,
            config,
            "data/",
            "writer-a",
        );
        let partition = PartitionKey("p".into());
        owner
            .produce_at_epoch(
                partition.clone(),
                Bytes::from_static(b"before"),
                1,
                (),
                Durability::Sequenced,
                1,
            )
            .await
            .unwrap();
        let fenced = partition.clone();
        on_thread(&standby, move |seq| seq.fence_epoch(&fenced, 1, 2)).unwrap();

        let stale = owner
            .produce_at_epoch(
                partition.clone(),
                Bytes::from_static(b"stale"),
                1,
                (),
                Durability::Sequenced,
                1,
            )
            .await;
        match stale {
            Err(ObjectLogError::Fenced {
                partition: name,
                epoch: 1,
                current: 2,
            }) => assert_eq!(name, "p"),
            other => panic!("a write behind the fence must be Fenced, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn default_rejection_error_is_a_sequencer_error() {
        let seq = crate::InMemorySequencer::new();
        let error = seq.rejection_error(&PartitionKey("p".into()), 1, "tail moved".into());
        assert!(matches!(error, ObjectLogError::Sequencer(reason) if reason == "tail moved"));
    }
}
