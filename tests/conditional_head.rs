use async_trait::async_trait;
use bytes::Bytes;
use object_log::{
    AppendOwner, BatchLocation, BlobStore, CasOutcome, CommitBatch, CommitOutcome,
    ConditionalCommitOutcome, Durability, FlushConfig, InMemorySequencer, IndexEntry, LogEngine,
    ManifestSequencer, MemoryBlobStore, ObjectLogError, PartitionKey, Sequencer,
};
use std::{
    ops::Range,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};
fn batch(name: &str, epoch: u64) -> CommitBatch<'static, ()> {
    CommitBatch {
        partition: PartitionKey("queue".into()),
        location: BatchLocation {
            object_id: name.into(),
            byte_start: 0,
            byte_len: 1,
        },
        record_count: 1,
        meta: &(),
        epoch,
    }
}
struct ControlledCas {
    inner: MemoryBlobStore,
    mode: AtomicU8,
    calls: AtomicUsize,
    barrier: Barrier,
    entered: tokio::sync::Notify,
    release: tokio::sync::watch::Sender<bool>,
}
impl ControlledCas {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryBlobStore::new(),
            mode: AtomicU8::new(0),
            calls: AtomicUsize::new(0),
            barrier: Barrier::new(2),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        })
    }
}
#[async_trait]
impl BlobStore for ControlledCas {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        self.inner.put(key, value).await
    }
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        let value = self.inner.get(key).await?;
        if key == "meta/catalog" && self.mode.load(Ordering::SeqCst) == 13 && value.is_none() {
            // Another creator registers then loses its index after this
            // handle's absence snapshot, before its registration CAS.
            self.inner
                .put(key, Bytes::from_static(br#"{"partitions":["queue"]}"#))
                .await?;
        }
        Ok(value)
    }
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get_range(key, range).await
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
        new: Bytes,
    ) -> Result<CasOutcome, ObjectLogError> {
        let doc = serde_json::from_slice::<serde_json::Value>(&new).ok();
        let is_append = doc
            .as_ref()
            .and_then(|doc| doc.get("next_offset"))
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|head| head > 0);
        if self.mode.load(Ordering::SeqCst) == 9
            && doc.as_ref().is_some_and(|doc| doc.get("epoch").is_some())
        {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < 2 {
                self.barrier.wait();
            }
        }
        if self.mode.load(Ordering::SeqCst) == 14
            && doc.as_ref().is_some_and(|doc| doc.get("epoch").is_some())
        {
            return Err(ObjectLogError::StorageUnavailable(
                "index publication failed".into(),
            ));
        }
        if matches!(self.mode.load(Ordering::SeqCst), 16..=18) && key == "meta/catalog" {
            self.inner
                .compare_and_swap(key, expected, new.clone())
                .await?;
            self.inner.delete(key).await?;
            return Ok(CasOutcome::Conflict {
                current: match self.mode.load(Ordering::SeqCst) {
                    16 => Some(new),
                    17 => None,
                    _ => Some(Bytes::from_static(b"{broken")),
                },
            });
        }
        if self.mode.load(Ordering::SeqCst) == 15 && key == "meta/catalog" {
            self.inner.compare_and_swap(key, expected, new).await?;
            return Err(ObjectLogError::StorageUnavailable(
                "catalog reply lost".into(),
            ));
        }
        if matches!(self.mode.load(Ordering::SeqCst), 10..=12)
            && doc.as_ref().is_some_and(|doc| doc.get("epoch").is_some())
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner
                .compare_and_swap(key, expected, new.clone())
                .await?;
            self.inner.delete(key).await?;
            return Ok(CasOutcome::Conflict {
                current: if self.mode.load(Ordering::SeqCst) == 10 {
                    Some(new)
                } else if self.mode.load(Ordering::SeqCst) == 12 {
                    Some(Bytes::from_static(b"{broken"))
                } else {
                    None
                },
            });
        }
        if is_append {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                1 if call < 2 => {
                    self.barrier.wait();
                }
                2 => {
                    self.inner
                        .compare_and_swap(key, expected, new.clone())
                        .await?;
                    return Ok(CasOutcome::Conflict { current: Some(new) });
                }
                3 => {
                    self.inner.compare_and_swap(key, expected, new).await?;
                    return Err(ObjectLogError::StorageUnavailable(
                        "lost publication reply".into(),
                    ));
                }
                4 => {
                    return Ok(CasOutcome::Conflict {
                        current: self.inner.get(key).await?,
                    });
                }
                5 => {
                    return Ok(CasOutcome::Conflict {
                        current: Some(Bytes::from_static(b"{broken")),
                    });
                }
                7 => {
                    let mut doc = doc.unwrap();
                    let next = doc["next_offset"].clone();
                    doc["entries"] = serde_json::json!([]);
                    doc["log_start"] = next;
                    let changed = Bytes::from(serde_json::to_vec(&doc).unwrap());
                    self.inner
                        .compare_and_swap(key, expected, changed.clone())
                        .await?;
                    return Ok(CasOutcome::Conflict {
                        current: Some(changed),
                    });
                }
                8 => {
                    let mut release = self.release.subscribe();
                    self.entered.notify_one();
                    release.wait_for(|ready| *ready).await.unwrap();
                }
                6 => {
                    let mut doc = doc.unwrap();
                    doc["entries"].as_array_mut().unwrap().remove(0);
                    doc["log_start"] = serde_json::json!(1);
                    let changed = Bytes::from(serde_json::to_vec(&doc).unwrap());
                    self.inner
                        .compare_and_swap(key, expected, changed.clone())
                        .await?;
                    return Ok(CasOutcome::Conflict {
                        current: Some(changed),
                    });
                }
                _ => {}
            }
        }
        self.inner.compare_and_swap(key, expected, new).await
    }
}
fn open(blob: Arc<dyn BlobStore>) -> Arc<ManifestSequencer> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    Arc::new(
        runtime
            .block_on(ManifestSequencer::open(blob, "meta/"))
            .unwrap(),
    )
}
#[test]
fn same_epoch_independent_writers_have_one_head_zero_winner() {
    let blob = ControlledCas::new();
    let first = open(blob.clone());
    let second = open(blob.clone());
    let key = PartitionKey("queue".into());
    first.fence_epoch(&key, 0, 1).unwrap();
    second.fence_epoch(&key, 0, 1).unwrap();
    blob.mode.store(1, Ordering::SeqCst);
    let a = std::thread::spawn(move || {
        first
            .commit_conditionally(&[batch("a", 1)], &[Some(0)])
            .unwrap()
    });
    let b = std::thread::spawn(move || {
        second
            .commit_conditionally(&[batch("b", 1)], &[Some(0)])
            .unwrap()
    });
    let outcomes = [a.join().unwrap().remove(0), b.join().unwrap().remove(0)];
    assert_eq!(
        outcomes
            .iter()
            .filter(|row| matches!(
                row,
                ConditionalCommitOutcome::Committed(CommitOutcome::Assigned { base_offset: 0, .. })
            ))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|row| matches!(
                row,
                ConditionalCommitOutcome::HeadRejected {
                    expected: 0,
                    current: 1
                }
            ))
            .count(),
        1
    );
    let reopened = open(blob);
    assert_eq!(reopened.high_watermark(&key).unwrap(), 1);
    assert_eq!(reopened.lookup(&key, 0).unwrap().len(), 1);
}
#[test]
fn stale_handle_refresh_and_ordered_siblings_use_original_heads() {
    let blob = ControlledCas::new();
    let first = open(blob.clone());
    let stale = open(blob.clone());
    let key = PartitionKey("queue".into());
    first.fence_epoch(&key, 0, 1).unwrap();
    first
        .commit_conditionally(&[batch("a", 1)], &[Some(0)])
        .unwrap();
    let rows = stale
        .commit_conditionally(
            &[batch("b", 1), batch("c", 1), batch("d", 1)],
            &[Some(1), Some(1), Some(2)],
        )
        .unwrap();
    assert!(matches!(
        rows[0],
        ConditionalCommitOutcome::Committed(CommitOutcome::Assigned { base_offset: 1, .. })
    ));
    assert_eq!(
        rows[1],
        ConditionalCommitOutcome::HeadRejected {
            expected: 1,
            current: 2
        }
    );
    assert!(matches!(
        rows[2],
        ConditionalCommitOutcome::Committed(CommitOutcome::Assigned { base_offset: 2, .. })
    ));
    let writes = blob.calls.load(Ordering::SeqCst);
    let rejected = stale
        .commit_conditionally(&[batch("e", 1)], &[Some(0)])
        .unwrap();
    assert_eq!(
        rejected[0],
        ConditionalCommitOutcome::HeadRejected {
            expected: 0,
            current: 3
        }
    );
    assert_eq!(blob.calls.load(Ordering::SeqCst), writes);
    assert_eq!(stale.high_watermark(&key).unwrap(), 3);
    assert!(stale.commit_conditionally(&[batch("x", 1)], &[]).is_err());
    assert!(
        stale
            .commit_conditionally(&[batch("x", 1)], &[Some(-1)])
            .is_err()
    );
    assert_eq!(blob.calls.load(Ordering::SeqCst), writes);
}
#[test]
fn exact_conflict_reconciliation_and_unknown_publication_remain_distinct() {
    for mode in [2, 3, 4, 5, 6, 7] {
        let blob = ControlledCas::new();
        let seq = open(blob.clone());
        let key = PartitionKey("queue".into());
        seq.fence_epoch(&key, 0, 1).unwrap();
        blob.mode.store(mode, Ordering::SeqCst);
        let rows = if mode == 6 {
            seq.commit_conditionally(&[batch("a", 1), batch("b", 1)], &[Some(0), Some(1)])
                .unwrap()
        } else {
            seq.commit_conditionally(&[batch("a", 1)], &[Some(0)])
                .unwrap()
        };
        if mode == 2 {
            assert!(matches!(
                rows[0],
                ConditionalCommitOutcome::Committed(CommitOutcome::Assigned { base_offset: 0, .. })
            ));
        } else {
            assert!(
                rows.iter()
                    .all(|row| matches!(row, ConditionalCommitOutcome::Unknown(_)))
            );
        }
        if mode == 4 {
            assert_eq!(blob.calls.load(Ordering::SeqCst), 8);
        }
        if mode == 3 {
            blob.mode.store(0, Ordering::SeqCst);
            let duplicate = seq
                .commit_conditionally(&[batch("a", 1)], &[Some(0)])
                .unwrap();
            assert_eq!(
                duplicate[0],
                ConditionalCommitOutcome::Committed(CommitOutcome::Duplicate { base_offset: 0 })
            );
        }
    }
}
struct LegacyCounting {
    inner: InMemorySequencer,
    commits: AtomicUsize,
}
impl Sequencer for LegacyCounting {
    type Meta = ();
    fn commit(
        &self,
        batches: &[CommitBatch<'_, ()>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.inner.commit(batches)
    }
    fn lookup(&self, key: &PartitionKey, offset: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(key, offset)
    }
    fn high_watermark(&self, key: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(key)
    }
    fn log_start_offset(&self, key: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(key)
    }
    fn truncate_before(
        &self,
        key: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(key, offset)
    }
}
#[tokio::test]
async fn unsupported_condition_never_calls_legacy_or_uploads() {
    let seq = Arc::new(LegacyCounting {
        inner: InMemorySequencer::new(),
        commits: AtomicUsize::new(0),
    });
    assert!(
        seq.commit_conditionally(&[batch("a", 0)], &[Some(0)])
            .is_err()
    );
    assert_eq!(seq.commits.load(Ordering::SeqCst), 0);
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(blob.clone(), seq.clone(), FlushConfig::default(), "data/");
    let owner: AppendOwner = Arc::new(());
    assert!(
        engine
            .produce_conditional_at_epoch(
                PartitionKey("queue".into()),
                Bytes::from_static(b"body"),
                1,
                (),
                0,
                0,
                owner
            )
            .await
            .is_err()
    );
    assert!(blob.list("data/").await.unwrap().is_empty());
    assert_eq!(seq.commits.load(Ordering::SeqCst), 0);
    // Legacy behavior remains available unchanged.
    engine
        .produce(
            PartitionKey("queue".into()),
            Bytes::from_static(b"legacy"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(seq.commits.load(Ordering::SeqCst), 1);
}

#[test]
fn engine_waits_for_head_decision_and_returns_typed_rejection() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let blob = ControlledCas::new();
    let seq = open(blob.clone());
    let key = PartitionKey("queue".into());
    seq.fence_epoch(&key, 0, 1).unwrap();
    blob.mode.store(8, Ordering::SeqCst);
    let engine = Arc::new(LogEngine::new(
        blob.clone(),
        seq.clone(),
        FlushConfig {
            linger: std::time::Duration::ZERO,
            ..Default::default()
        },
        "data/",
    ));
    runtime.block_on(async {
        let writer = engine.clone();
        let first = tokio::spawn(async move {
            writer
                .produce_conditional_at_epoch(
                    PartitionKey("queue".into()),
                    Bytes::from_static(b"genesis"),
                    1,
                    (),
                    1,
                    0,
                    Arc::new(()),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), blob.entered.notified())
            .await
            .unwrap();
        assert!(!blob.list("data/").await.unwrap().is_empty());
        assert!(!first.is_finished());
        blob.release.send_replace(true);
        let accepted = first.await.unwrap().unwrap();
        assert_eq!(accepted.base_offset, Some(0));
        assert!(accepted.durable && accepted.sequenced);
        let second = engine
            .produce_conditional_at_epoch(
                key.clone(),
                Bytes::from_static(b"loser"),
                1,
                (),
                1,
                0,
                Arc::new(()),
            )
            .await;
        assert_eq!(
            second,
            Err(ObjectLogError::HeadRejected {
                expected: 0,
                current: 1
            })
        );
    });
    assert_eq!(seq.high_watermark(&key).unwrap(), 1);
    assert_eq!(seq.lookup(&key, 0).unwrap().len(), 1);
}

#[test]
fn vanished_known_index_is_unknown_and_never_recreated() {
    let blob = ControlledCas::new();
    let seq = open(blob.clone());
    let key = PartitionKey("queue".into());
    seq.fence_epoch(&key, 0, 1).unwrap();
    seq.commit_conditionally(&[batch("a", 1)], &[Some(0)])
        .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let index = runtime
        .block_on(blob.list("meta/"))
        .unwrap()
        .into_iter()
        .find(|key| key != "meta/catalog")
        .unwrap();
    runtime.block_on(blob.delete(&index)).unwrap();
    let rows = seq
        .commit_conditionally(&[batch("b", 1)], &[Some(0)])
        .unwrap();
    assert!(matches!(rows[0], ConditionalCommitOutcome::Unknown(_)));
    assert!(runtime.block_on(blob.get(&index)).unwrap().is_none());
    // Genuinely never-observed creation remains supported.
    let fresh = open(ControlledCas::new());
    let rows = fresh
        .commit_conditionally(&[batch("fresh", 0)], &[Some(0)])
        .unwrap();
    assert!(matches!(
        rows[0],
        ConditionalCommitOutcome::Committed(CommitOutcome::Assigned { base_offset: 0, .. })
    ));
}
#[test]
fn ambiguous_candidate_retained_away_cannot_become_definite_rejection() {
    let blob = ControlledCas::new();
    let seq = open(blob.clone());
    let key = PartitionKey("queue".into());
    seq.fence_epoch(&key, 0, 1).unwrap();
    blob.mode.store(3, Ordering::SeqCst);
    let first = seq
        .commit_conditionally(&[batch("a", 1)], &[Some(0)])
        .unwrap();
    assert!(matches!(first[0], ConditionalCommitOutcome::Unknown(_)));
    blob.mode.store(0, Ordering::SeqCst);
    // Refresh the ambiguous commit, then retain its whole indexed range.
    let observer = open(blob.clone());
    observer.truncate_before(&key, 1).unwrap();
    let retried = seq
        .commit_conditionally(&[batch("a", 1)], &[Some(0)])
        .unwrap();
    assert!(matches!(retried[0], ConditionalCommitOutcome::Unknown(_)));
}

#[test]
fn authoritative_state_is_fresh_and_missing_or_corrupt_history_fails_closed() {
    let blob = ControlledCas::new();
    let first = open(blob.clone());
    let stale = open(blob.clone());
    let key = PartitionKey("queue".into());
    let zero = object_log::AuthoritativePartitionState {
        epoch: 0,
        log_start: 0,
        next_offset: 0,
    };
    assert_eq!(stale.authoritative_partition_state(&key).unwrap(), zero);
    first.fence_epoch(&key, 0, 1).unwrap();
    assert_eq!(stale.authoritative_partition_state(&key).unwrap().epoch, 1);
    first
        .commit_conditionally(&[batch("first", 1)], &[Some(0)])
        .unwrap();
    assert_eq!(
        stale
            .authoritative_partition_state(&key)
            .unwrap()
            .next_offset,
        1
    );
    assert_eq!(stale.high_watermark(&key).unwrap(), 1);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let index = runtime
        .block_on(blob.list("meta/"))
        .unwrap()
        .into_iter()
        .find(|key| key != "meta/catalog")
        .unwrap();
    let valid = runtime.block_on(blob.get(&index)).unwrap().unwrap();
    let mut malformed: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    malformed["next_offset"] = serde_json::json!(99);
    runtime
        .block_on(blob.put(&index, Bytes::from(serde_json::to_vec(&malformed).unwrap())))
        .unwrap();
    assert!(stale.authoritative_partition_state(&key).is_err());
    assert_eq!(stale.high_watermark(&key).unwrap(), 1);
    runtime.block_on(blob.delete(&index)).unwrap();
    assert!(stale.authoritative_partition_state(&key).is_err());
    assert_eq!(stale.high_watermark(&key).unwrap(), 1);
    assert!(runtime.block_on(blob.get(&index)).unwrap().is_none());

    let empty_blob = ControlledCas::new();
    let empty = open(empty_blob.clone());
    empty.fence_epoch(&key, 0, 7).unwrap();
    let index = runtime
        .block_on(empty_blob.list("meta/"))
        .unwrap()
        .into_iter()
        .find(|key| key != "meta/catalog")
        .unwrap();
    runtime.block_on(empty_blob.delete(&index)).unwrap();
    assert!(empty.authoritative_partition_state(&key).is_err());
    assert!(runtime.block_on(empty_blob.get(&index)).unwrap().is_none());
    assert!(matches!(
        InMemorySequencer::new().authoritative_partition_state(&key),
        Err(ObjectLogError::InvalidConfig(_))
    ));
}

#[test]
fn native_epoch_acquisitions_are_distinct_and_return_matching_replay_bounds() {
    let blob = ControlledCas::new();
    let first = open(blob.clone());
    let second = open(blob.clone());
    let key = PartitionKey("queue".into());
    first.acquire_authoritative_epoch(&key).unwrap();
    blob.mode.store(9, Ordering::SeqCst);
    let a = std::thread::spawn(move || {
        first
            .acquire_authoritative_epoch(&PartitionKey("queue".into()))
            .unwrap()
    });
    let b = std::thread::spawn(move || {
        second
            .acquire_authoritative_epoch(&PartitionKey("queue".into()))
            .unwrap()
    });
    let mut epochs = [a.join().unwrap(), b.join().unwrap()];
    epochs.sort_by_key(|state| state.epoch);
    assert_eq!((epochs[0].epoch, epochs[1].epoch), (2, 3));
    assert!(
        epochs
            .iter()
            .all(|state| state.next_offset == 0 && state.log_start == 0)
    );
    blob.mode.store(0, Ordering::SeqCst);
    let seq = open(blob.clone());
    seq.commit_conditionally(&[batch("first", 3)], &[Some(0)])
        .unwrap();
    let acquired = seq.acquire_authoritative_epoch(&key).unwrap();
    assert_eq!(
        acquired,
        object_log::AuthoritativePartitionState {
            epoch: 4,
            log_start: 0,
            next_offset: 1
        }
    );
    assert_eq!(seq.lookup(&key, 0).unwrap()[0].location.object_id, "first");
    assert!(matches!(
        seq.commit_conditionally(&[batch("stale", 3)], &[Some(1)])
            .unwrap()[0],
        ConditionalCommitOutcome::Committed(CommitOutcome::Rejected { .. })
    ));
    assert!(matches!(
        InMemorySequencer::new().acquire_authoritative_epoch(&key),
        Err(ObjectLogError::InvalidConfig(_))
    ));
}

#[test]
fn native_epoch_lost_reply_exhaustion_overflow_and_disappearance_fail_closed() {
    let blob = ControlledCas::new();
    let seq = open(blob.clone());
    let key = PartitionKey("queue".into());
    seq.acquire_authoritative_epoch(&key).unwrap();
    seq.commit_conditionally(&[batch("first", 1)], &[Some(0)])
        .unwrap();
    blob.mode.store(3, Ordering::SeqCst);
    assert!(seq.acquire_authoritative_epoch(&key).is_err());
    blob.mode.store(0, Ordering::SeqCst);
    assert_eq!(seq.authoritative_partition_state(&key).unwrap().epoch, 2);
    assert_eq!(seq.acquire_authoritative_epoch(&key).unwrap().epoch, 3);
    blob.calls.store(0, Ordering::SeqCst);
    blob.mode.store(4, Ordering::SeqCst);
    assert!(matches!(
        seq.acquire_authoritative_epoch(&key),
        Err(ObjectLogError::StorageUnavailable(_))
    ));
    assert_eq!(blob.calls.load(Ordering::SeqCst), 8);
    blob.mode.store(0, Ordering::SeqCst);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let index = runtime
        .block_on(blob.list("meta/"))
        .unwrap()
        .into_iter()
        .find(|key| key != "meta/catalog")
        .unwrap();
    let mut doc: serde_json::Value =
        serde_json::from_slice(&runtime.block_on(blob.get(&index)).unwrap().unwrap()).unwrap();
    doc["epoch"] = serde_json::json!(u64::MAX);
    let exhausted = Bytes::from(serde_json::to_vec(&doc).unwrap());
    runtime
        .block_on(blob.put(&index, exhausted.clone()))
        .unwrap();
    assert!(seq.acquire_authoritative_epoch(&key).is_err());
    assert_eq!(
        runtime.block_on(blob.get(&index)).unwrap().unwrap(),
        exhausted
    );
    runtime.block_on(blob.delete(&index)).unwrap();
    assert!(seq.acquire_authoritative_epoch(&key).is_err());
    assert!(runtime.block_on(blob.get(&index)).unwrap().is_none());
}

#[test]
fn epoch_conflict_then_disappearance_never_recreates_unseen_index() {
    for mode in [10, 11, 12] {
        let blob = ControlledCas::new();
        let seq = open(blob.clone());
        let key = PartitionKey("queue".into());
        blob.mode.store(mode, Ordering::SeqCst);
        assert!(seq.acquire_authoritative_epoch(&key).is_err());
        assert_eq!(blob.calls.load(Ordering::SeqCst), 1);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(
            runtime.block_on(blob.list("meta/")).unwrap(),
            vec!["meta/catalog".to_string()]
        );
        assert!(seq.authoritative_partition_state(&key).is_err());
        assert!(matches!(
            seq.commit_conditionally(&[batch("later", 1)], &[Some(0)])
                .unwrap()[0],
            ConditionalCommitOutcome::Unknown(_)
        ));
        blob.mode.store(0, Ordering::SeqCst);
        assert!(seq.acquire_authoritative_epoch(&key).is_err());
        assert_eq!(
            runtime.block_on(blob.list("meta/")).unwrap(),
            vec!["meta/catalog".to_string()]
        );
    }
}

#[test]
fn catalog_known_missing_authority_fails_for_cold_and_long_lived_handles() {
    let blob = ControlledCas::new();
    let stale = open(blob.clone());
    let writer = open(blob.clone());
    let key = PartitionKey("queue".into());
    writer.acquire_authoritative_epoch(&key).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let index = runtime
        .block_on(blob.list("meta/"))
        .unwrap()
        .into_iter()
        .find(|key| key != "meta/catalog")
        .unwrap();
    runtime.block_on(blob.delete(&index)).unwrap();
    let cold = open(blob.clone());
    for reader in [&stale, &cold] {
        assert!(reader.authoritative_partition_state(&key).is_err());
        assert!(reader.acquire_authoritative_epoch(&key).is_err());
        assert!(matches!(
            reader
                .commit_conditionally(&[batch("later", 1)], &[Some(0)])
                .unwrap()[0],
            ConditionalCommitOutcome::Unknown(_)
        ));
        assert!(runtime.block_on(blob.get(&index)).unwrap().is_none());
    }
    runtime.block_on(blob.delete("meta/catalog")).unwrap();
    cold.acquire_authoritative_epoch(&PartitionKey("unrelated".into()))
        .unwrap();
    for reader in [&stale, &cold] {
        assert!(reader.authoritative_partition_state(&key).is_err());
        assert!(reader.acquire_authoritative_epoch(&key).is_err());
        assert!(runtime.block_on(blob.get(&index)).unwrap().is_none());
    }
}

#[test]
fn raced_catalog_registration_never_recreates_missing_authority() {
    for conditional in [false, true] {
        let blob = ControlledCas::new();
        let seq = open(blob.clone());
        let key = PartitionKey("queue".into());
        blob.mode.store(13, Ordering::SeqCst);
        if conditional {
            assert!(matches!(
                seq.commit_conditionally(&[batch("new", 1)], &[Some(0)])
                    .unwrap()[0],
                ConditionalCommitOutcome::Unknown(_)
            ));
        } else {
            assert!(seq.acquire_authoritative_epoch(&key).is_err());
        }
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(
            runtime.block_on(blob.list("meta/")).unwrap(),
            vec!["meta/catalog".to_string()]
        );
    }
}

#[test]
fn rejected_first_condition_does_not_register_or_poison_new_partition() {
    let blob = ControlledCas::new();
    let seq = open(blob.clone());
    assert!(matches!(
        seq.commit_conditionally(&[batch("rejected", 1)], &[Some(1)])
            .unwrap()[0],
        ConditionalCommitOutcome::HeadRejected { current: 0, .. }
    ));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    assert!(runtime.block_on(blob.list("meta/")).unwrap().is_empty());
    assert!(matches!(
        seq.commit_conditionally(&[batch("accepted", 1)], &[Some(0)])
            .unwrap()[0],
        ConditionalCommitOutcome::Committed(CommitOutcome::Assigned { base_offset: 0, .. })
    ));
    let cold = open(blob);
    assert_eq!(
        cold.authoritative_partition_state(&PartitionKey("queue".into()))
            .unwrap()
            .next_offset,
        1
    );
}

#[test]
fn attempted_native_catalog_publication_retains_evidence_after_deletion() {
    for mode in [14, 15] {
        let blob = ControlledCas::new();
        let seq = open(blob.clone());
        let key = PartitionKey("queue".into());
        blob.mode.store(mode, Ordering::SeqCst);
        assert!(matches!(
            seq.commit_conditionally(&[batch("first", 1)], &[Some(0)])
                .unwrap()[0],
            ConditionalCommitOutcome::Unknown(_)
        ));
        blob.mode.store(0, Ordering::SeqCst);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(blob.delete("meta/catalog")).unwrap();
        assert!(seq.authoritative_partition_state(&key).is_err());
        assert!(seq.acquire_authoritative_epoch(&key).is_err());
        assert!(matches!(
            seq.commit_conditionally(&[batch("retry", 1)], &[Some(0)])
                .unwrap()[0],
            ConditionalCommitOutcome::Unknown(_)
        ));
        assert!(runtime.block_on(blob.list("meta/")).unwrap().is_empty());
    }
}

#[test]
fn catalog_conflict_then_disappearance_never_recreates_authority() {
    for mode in [16, 17, 18] {
        for conditional in [false, true] {
            let blob = ControlledCas::new();
            let seq = open(blob.clone());
            let key = PartitionKey("queue".into());
            blob.mode.store(mode, Ordering::SeqCst);
            if conditional {
                assert!(matches!(
                    seq.commit_conditionally(&[batch("first", 1)], &[Some(0)])
                        .unwrap()[0],
                    ConditionalCommitOutcome::Unknown(_)
                ));
            } else {
                assert!(seq.acquire_authoritative_epoch(&key).is_err());
            }
            let runtime = tokio::runtime::Runtime::new().unwrap();
            assert!(runtime.block_on(blob.list("meta/")).unwrap().is_empty());
        }
    }
}

#[test]
fn observed_malformed_index_cannot_become_unseen_after_deletion() {
    let blob = ControlledCas::new();
    let stale = open(blob.clone());
    let writer = open(blob.clone());
    let key = PartitionKey("queue".into());
    writer.acquire_authoritative_epoch(&key).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let index = runtime
        .block_on(blob.list("meta/"))
        .unwrap()
        .into_iter()
        .find(|key| key != "meta/catalog")
        .unwrap();
    runtime
        .block_on(blob.put(&index, Bytes::from_static(b"{broken")))
        .unwrap();
    assert!(stale.authoritative_partition_state(&key).is_err());
    runtime.block_on(blob.delete(&index)).unwrap();
    runtime.block_on(blob.delete("meta/catalog")).unwrap();
    assert!(stale.authoritative_partition_state(&key).is_err());
    assert!(stale.acquire_authoritative_epoch(&key).is_err());
    assert!(matches!(
        stale
            .commit_conditionally(&[batch("retry", 1)], &[Some(0)])
            .unwrap()[0],
        ConditionalCommitOutcome::Unknown(_)
    ));
    assert!(runtime.block_on(blob.list("meta/")).unwrap().is_empty());
}
