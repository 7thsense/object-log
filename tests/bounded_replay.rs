use bytes::Bytes;
use object_log::{
    BlobStore, BoundedSequencer, Durability, FlushConfig, InMemorySequencer, LocalBlobStore,
    LogEngine, ManifestSequencer, ObjectLogError, PartitionKey, Sequencer,
};
use std::{sync::Arc, time::Duration};
#[tokio::test]
async fn metadata_plan_admits_oversized_first_batch_and_replays_exactly_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(root.path()));
    let partition = PartitionKey("native".into());
    let large = Bytes::from(vec![7; 5 << 20]);
    {
        let seq = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "meta/")
                .await
                .unwrap(),
        );
        let engine = LogEngine::new(
            Arc::clone(&blob),
            seq,
            FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            },
            "data/",
        );
        engine
            .produce(
                partition.clone(),
                large.clone(),
                3,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        for value in [8, 9] {
            engine
                .produce(
                    partition.clone(),
                    Bytes::from(vec![value; 17]),
                    1,
                    (),
                    Durability::Sequenced,
                )
                .await
                .unwrap();
        }
    }
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "meta/")
            .await
            .unwrap(),
    );
    let engine = LogEngine::new(
        Arc::clone(&blob),
        Arc::clone(&seq),
        FlushConfig::default(),
        "data/",
    );
    let first = engine.plan_fetch(&partition, 0, 8, 4096, 4 << 20).unwrap();
    assert_eq!(first.entries().len(), 1);
    assert_eq!(first.payload_bytes(), large.len() as u64);
    assert!(matches!(
        engine.read_planned_batch(&first, 0, (5 << 20) - 1).await,
        Err(ObjectLogError::BudgetExceeded(_))
    ));
    assert_eq!(
        engine
            .read_planned_batch(&first, 0, 5 << 20)
            .await
            .unwrap()
            .payload,
        large
    );
    let mid = engine.plan_fetch(&partition, 2, 1, 4096, 1).unwrap();
    assert_eq!(mid.entries()[0].base_offset, 0);
    assert_eq!(mid.entries()[0].record_count, 3);
    let mut offset = 0;
    let mut actual = vec![];
    loop {
        let plan = engine.plan_fetch(&partition, offset, 1, 4096, 1).unwrap();
        if plan.entries().is_empty() {
            break;
        }
        let batch = engine.read_planned_batch(&plan, 0, 6 << 20).await.unwrap();
        offset = batch.base_offset + i64::from(batch.record_count);
        actual.push(batch.payload);
    }
    assert_eq!(
        actual,
        vec![large, Bytes::from(vec![8; 17]), Bytes::from(vec![9; 17])]
    );
    assert_eq!(offset, 5);
    let retention = Arc::clone(&seq);
    let retained_partition = partition.clone();
    tokio::task::spawn_blocking(move || retention.truncate_before(&retained_partition, 2))
        .await
        .unwrap()
        .unwrap();
    assert!(engine.plan_fetch(&partition, 0, 1, 4096, 1).is_err());
    assert!(seq.lookup_page(&partition, 0, 1, 4096).is_err());
    assert_eq!(
        engine
            .plan_fetch(&partition, 2, 1, 4096, 1)
            .unwrap()
            .entries()[0]
            .base_offset,
        0
    );
    let other = LogEngine::new(
        Arc::new(object_log::MemoryBlobStore::new()),
        Arc::clone(&seq),
        FlushConfig::default(),
        "other/",
    );
    assert!(other.read_planned_batch(&first, 0, 6 << 20).await.is_err());
}
#[test]
fn memory_page_concatenation_equals_legacy_and_invalid_commits_are_atomic() {
    let seq = InMemorySequencer::new();
    let p = PartitionKey("p".into());
    let batches: Vec<_> = (0..200)
        .map(|index| object_log::CommitBatch {
            partition: p.clone(),
            record_count: 3,
            location: object_log::BatchLocation {
                object_id: format!("object-{index}"),
                byte_start: 0,
                byte_len: 1,
            },
            meta: &(),
            epoch: 0,
        })
        .collect();
    seq.commit(&batches).unwrap();
    let expected = seq.lookup(&p, 0).unwrap();
    let mut actual = vec![];
    let mut offset = 0;
    loop {
        let page = seq.lookup_page(&p, offset, 7, 4096).unwrap();
        if page.is_empty() {
            break;
        }
        offset = page.last().unwrap().base_offset + 3;
        actual.extend(page);
    }
    assert_eq!(actual, expected);
    let mut bad = object_log::CommitBatch {
        partition: p.clone(),
        record_count: 1,
        location: object_log::BatchLocation {
            object_id: "ok".into(),
            byte_start: 0,
            byte_len: 1,
        },
        meta: &(),
        epoch: 0,
    };
    bad.record_count = 0;
    assert!(
        seq.commit(&[batches.into_iter().next().unwrap(), bad])
            .is_err()
    );
    assert_eq!(seq.high_watermark(&p).unwrap(), 600);
}
#[tokio::test]
async fn exact_local_and_memory_ranges_handle_empty_reads_without_full_objects() {
    let root = tempfile::tempdir().unwrap();
    for blob in [
        Arc::new(LocalBlobStore::new(root.path())) as Arc<dyn BlobStore>,
        Arc::new(object_log::MemoryBlobStore::new()),
    ] {
        blob.put("object", Bytes::from_static(b"abcdef"))
            .await
            .unwrap();
        assert_eq!(
            blob.get_range_exact("object", 2..4).await.unwrap().unwrap(),
            Bytes::from_static(b"cd")
        );
        assert_eq!(
            blob.get_range_exact("object", 6..6).await.unwrap().unwrap(),
            Bytes::new()
        );
        assert!(blob.get_range_exact("object", 7..7).await.is_err());
        assert!(
            blob.get_range_exact("object", std::ops::Range { start: 5, end: 4 })
                .await
                .is_err()
        );
        assert!(
            blob.get_range_exact("missing", 0..0)
                .await
                .unwrap()
                .is_none()
        );
    }
}

struct RetentionRace {
    inner: InMemorySequencer,
    once: std::sync::atomic::AtomicBool,
}
impl Sequencer for RetentionRace {
    type Meta = ();
    fn commit(
        &self,
        batches: &[object_log::CommitBatch<'_, ()>],
    ) -> Result<Vec<object_log::CommitOutcome>, ObjectLogError> {
        self.inner.commit(batches)
    }
    fn lookup(
        &self,
        p: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<object_log::IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, offset)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        let before = self.inner.log_start_offset(p)?;
        if !self.once.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.inner
                .truncate_before(p, self.inner.high_watermark(p)?)?;
        }
        Ok(before)
    }
    fn truncate_before(
        &self,
        p: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, offset)
    }
}
impl BoundedSequencer for RetentionRace {
    fn lookup_page(
        &self,
        p: &PartitionKey,
        offset: i64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<object_log::IndexEntry>, ObjectLogError> {
        self.inner.lookup_page(p, offset, count, bytes)
    }
}
#[test]
fn retention_between_initial_check_and_page_is_loss_of_history_not_clean_eof() {
    let seq = Arc::new(RetentionRace {
        inner: InMemorySequencer::new(),
        once: std::sync::atomic::AtomicBool::new(false),
    });
    let p = PartitionKey("race".into());
    seq.commit(&[object_log::CommitBatch {
        partition: p.clone(),
        record_count: 3,
        location: object_log::BatchLocation {
            object_id: "object".into(),
            byte_start: 0,
            byte_len: 1,
        },
        meta: &(),
        epoch: 0,
    }])
    .unwrap();
    let engine = LogEngine::new(
        Arc::new(object_log::MemoryBlobStore::new()),
        Arc::clone(&seq),
        FlushConfig::default(),
        "data/",
    );
    assert!(matches!(
        engine.plan_fetch(&p, 0, 1, 4096, 1),
        Err(ObjectLogError::RangeOutOfBounds(_))
    ));
    assert!(seq.lookup_page(&p, 0, 1, 4096).is_err());
}
struct TransientStrict {
    inner: object_log::MemoryBlobStore,
    calls: std::sync::atomic::AtomicUsize,
    legacy_calls: std::sync::atomic::AtomicUsize,
    failures: usize,
}
#[async_trait::async_trait]
impl BlobStore for TransientStrict {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        self.inner.put(key, value).await
    }
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get(key).await
    }
    async fn get_range(
        &self,
        _key: &str,
        _range: std::ops::Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.legacy_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(ObjectLogError::InvalidConfig(
            "strict reads cannot downgrade".into(),
        ))
    }
    async fn get_range_exact(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call < self.failures {
            return Err(ObjectLogError::StorageUnavailable(
                "transient strict body read".into(),
            ));
        }
        self.inner.get_range_exact(key, range).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
}
#[tokio::test]
async fn strict_reads_preserve_transient_retry_and_exhaustion_without_downgrade() {
    for failures in [1, usize::MAX] {
        let blob = Arc::new(TransientStrict {
            inner: object_log::MemoryBlobStore::new(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            legacy_calls: std::sync::atomic::AtomicUsize::new(0),
            failures,
        });
        let seq = Arc::new(InMemorySequencer::new());
        let p = PartitionKey("retry".into());
        let engine = LogEngine::new(
            Arc::clone(&blob) as Arc<dyn BlobStore>,
            seq,
            FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            },
            "data/",
        );
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"exact"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        let plan = engine.plan_fetch(&p, 0, 1, 4096, 1).unwrap();
        let result = engine.read_planned_batch(&plan, 0, 5).await;
        let expected = if failures == 1 { 2 } else { 6 };
        if failures == 1 {
            assert_eq!(result.unwrap().payload, Bytes::from_static(b"exact"));
        } else {
            assert!(matches!(result, Err(ObjectLogError::StorageUnavailable(_))));
        }
        assert_eq!(
            blob.calls.load(std::sync::atomic::Ordering::SeqCst),
            expected
        );
        assert_eq!(
            blob.legacy_calls.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}

#[tokio::test]
async fn corrupt_persisted_indexes_fail_open_and_refresh_preserves_valid_cache() {
    let blob: Arc<dyn BlobStore> = Arc::new(object_log::MemoryBlobStore::new());
    let p = PartitionKey("corrupt".into());
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "meta/")
            .await
            .unwrap(),
    );
    let engine = LogEngine::new(
        Arc::clone(&blob),
        Arc::clone(&seq),
        FlushConfig {
            linger: Duration::ZERO,
            ..FlushConfig::default()
        },
        "data/",
    );
    engine
        .produce(
            p.clone(),
            Bytes::from_static(b"valid"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let key = blob
        .list("meta/index/")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let original = blob.get(&key).await.unwrap().unwrap();
    let good: serde_json::Value = serde_json::from_slice(&original).unwrap();
    for scenario in 0..4 {
        let mut invalid = good.clone();
        match scenario {
            0 => invalid["entries"][0]["record_count"] = 0.into(),
            1 => invalid["entries"][0]["base_offset"] = (-1).into(),
            2 => {
                invalid["entries"][0]["base_offset"] = i64::MAX.into();
                invalid["next_offset"] = i64::MAX.into();
            }
            _ => {
                let duplicate = invalid["entries"][0].clone();
                invalid["entries"].as_array_mut().unwrap().push(duplicate);
                invalid["next_offset"] = 2.into();
            }
        }
        blob.put(&key, Bytes::from(serde_json::to_vec(&invalid).unwrap()))
            .await
            .unwrap();
        assert!(
            ManifestSequencer::open(Arc::clone(&blob), "meta/")
                .await
                .is_err()
        );
        let refreshing = Arc::clone(&seq);
        let partition = p.clone();
        assert!(
            tokio::task::spawn_blocking(move || refreshing.refresh_partition(&partition))
                .await
                .unwrap()
                .is_err()
        );
        let retained = seq.lookup_page(&p, 0, 1, 4096).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].base_offset, 0);
        assert_eq!(retained[0].record_count, 1);
        assert_eq!(seq.high_watermark(&p).unwrap(), 1);
        blob.put(&key, original.clone()).await.unwrap();
    }
}
