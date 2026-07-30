//! Engine invariant tests: durability, group-commit cost, ordering, atomicity,
//! idempotency, fault-on-commit, and retention.

use async_trait::async_trait;
use bytes::Bytes;
use object_log::{
    BlobStore, BudgetConfig, BudgetMode, CommitBatch, CommitOutcome, Durability, FlushConfig,
    InMemorySequencer, IndexEntry, LogEngine, MemoryBlobStore, ObjectLogError, PartitionKey,
    Sequencer,
};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.to_string())
}

/// Flush only when `n` batches have accumulated — makes multiplexing deterministic.
fn coalesce_after(n: usize) -> FlushConfig {
    FlushConfig {
        max_bytes: usize::MAX,
        max_batches: n,
        linger: Duration::from_secs(3600),
        max_inflight_flushes: 1,
        max_buffered_bytes: usize::MAX,
        budget: object_log::BudgetConfig {
            enabled: false,
            ..object_log::BudgetConfig::default()
        },
    }
}

#[tokio::test]
async fn produce_fetch_round_trip() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let a = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"aa"),
            2,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let b = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"bbb"),
            3,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(a.base_offset, Some(0));
    assert_eq!(a.last_offset, Some(1));
    assert_eq!(b.base_offset, Some(2));
    assert_eq!(b.last_offset, Some(4));

    let all = engine.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].payload, "aa");
    assert_eq!(all[1].base_offset, 2);
    assert_eq!(all[1].payload, "bbb");

    // Mid-offset fetch returns only the covering batch.
    let tail = engine.fetch(&p, 2, 1 << 20).await.unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].payload, "bbb");
}

#[tokio::test]
async fn put_count_independent_of_partition_count() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        coalesce_after(100),
        "log/",
    );
    // One flush spanning 100 partitions -> ONE object.
    for i in 0..99 {
        engine
            .produce(
                pk(&format!("t-{i}")),
                Bytes::from_static(b"x"),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
    }
    engine
        .produce(
            pk("t-99"),
            Bytes::from_static(b"x"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(
        blob.object_count(),
        1,
        "100 partitions multiplexed into one object"
    );
}

#[tokio::test]
async fn sequenced_implies_durable() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    assert_eq!(blob.object_count(), 0);
    let out = engine
        .produce(
            pk("t-0"),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    // The object exists by the time produce returns: PUT happened before the ack.
    assert!(out.durable && out.sequenced);
    assert_eq!(blob.object_count(), 1);
}

#[tokio::test]
async fn concurrent_producers_get_dense_contiguous_offsets() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = Arc::new(LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    ));
    let p = pk("t-0");
    let mut handles = Vec::new();
    for _ in 0..50 {
        let engine = Arc::clone(&engine);
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            engine
                .produce(p, Bytes::from_static(b"r"), 1, (), Durability::Sequenced)
                .await
                .unwrap()
        }));
    }
    let mut bases = Vec::new();
    for h in handles {
        bases.push(h.await.unwrap().base_offset.unwrap());
    }
    bases.sort();
    // Dense, contiguous, no gaps or dupes.
    assert_eq!(bases, (0..50).collect::<Vec<i64>>());

    let all = engine.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 50);
    for (i, b) in all.iter().enumerate() {
        assert_eq!(b.base_offset, i as i64);
    }
}

// ---- A blob store whose put always fails. ----
struct FailingPut;
#[async_trait]
impl BlobStore for FailingPut {
    async fn put(&self, _: &str, _: Bytes) -> Result<(), ObjectLogError> {
        Err(ObjectLogError::StorageUnavailable("disk on fire".into()))
    }
    async fn get(&self, _: &str) -> Result<Option<Bytes>, ObjectLogError> {
        Ok(None)
    }
    async fn get_range(&self, _: &str, _: Range<u64>) -> Result<Option<Bytes>, ObjectLogError> {
        Ok(None)
    }
    async fn list(&self, _: &str) -> Result<Vec<String>, ObjectLogError> {
        Ok(Vec::new())
    }
    async fn delete(&self, _: &str) -> Result<(), ObjectLogError> {
        Ok(())
    }
}

#[tokio::test]
async fn put_failure_yields_no_ack_no_offset() {
    let seq = Arc::new(InMemorySequencer::new());
    let engine = LogEngine::new(
        Arc::new(FailingPut),
        Arc::clone(&seq),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let err = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectLogError::StorageUnavailable(_)));
    // Nothing sequenced.
    assert_eq!(seq.high_watermark(&p).unwrap(), 0);
}

struct FailsPutOnce {
    inner: MemoryBlobStore,
    failures: AtomicUsize,
}

#[async_trait]
impl BlobStore for FailsPutOnce {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        if self.failures.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ObjectLogError::StorageUnavailable("transient put".into()));
        }
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

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn transient_put_failure_is_retried_before_ack() {
    let blob = Arc::new(FailsPutOnce {
        inner: MemoryBlobStore::new(),
        failures: AtomicUsize::new(0),
    });
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let out = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(out.base_offset, Some(0));
    assert_eq!(blob.failures.load(Ordering::SeqCst), 2);
    assert_eq!(engine.fetch(&p, 0, 1024).await.unwrap().len(), 1);
}

struct FailsRangeOnce {
    inner: MemoryBlobStore,
    failures: AtomicUsize,
}

#[async_trait]
impl BlobStore for FailsRangeOnce {
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
        if self.failures.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ObjectLogError::StorageUnavailable(
                "transient get_range".into(),
            ));
        }
        self.inner.get_range(key, range).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn transient_range_failure_is_retried_on_fetch() {
    let blob = Arc::new(FailsRangeOnce {
        inner: MemoryBlobStore::new(),
        failures: AtomicUsize::new(0),
    });
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let out = engine.fetch(&p, 0, 1024).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(blob.failures.load(Ordering::SeqCst), 2);
}

// ---- A sequencer that fails its first commit, then works (fault BETWEEN put and commit). ----
struct FlakyCommit {
    inner: InMemorySequencer,
    failed_once: AtomicUsize,
}
impl Sequencer for FlakyCommit {
    type Meta = ();
    fn commit(
        &self,
        batches: &[CommitBatch<'_, ()>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        if self.failed_once.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ObjectLogError::Sequencer("transient".into()));
        }
        self.inner.commit(batches)
    }
    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test]
async fn commit_failure_orphans_object_and_retry_is_exactly_once() {
    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(FlakyCommit {
        inner: InMemorySequencer::new(),
        failed_once: AtomicUsize::new(0),
    });
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    // First attempt: PUT succeeds (orphan object), commit fails -> Err, no offset.
    let err = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"once"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectLogError::Sequencer(_)));
    assert_eq!(seq.high_watermark(&p).unwrap(), 0, "nothing committed");
    assert_eq!(blob.object_count(), 1, "the PUT object is orphaned");

    // Retry: a fresh object id, commit succeeds -> exactly once.
    let out = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"once"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(out.base_offset, Some(0));
    assert_eq!(seq.high_watermark(&p).unwrap(), 1);
    assert_eq!(blob.object_count(), 2, "orphan + the committed object");
    let all = engine.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 1, "exactly one record visible");
    assert_eq!(all[0].payload, "once");
}

// ---- A sequencer that rejects the whole object if any batch is poison. ----
#[derive(Default)]
struct PoisonIfAny {
    inner: InMemorySequencer,
}
impl Sequencer for PoisonIfAny {
    type Meta = bool; // true = poison
    fn commit(
        &self,
        batches: &[CommitBatch<'_, bool>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        if batches.iter().any(|b| *b.meta) {
            return Err(ObjectLogError::Sequencer("poison batch".into()));
        }
        // Delegate the assignment via () batches.
        let clean: Vec<CommitBatch<'_, ()>> = batches
            .iter()
            .map(|b| CommitBatch {
                partition: b.partition.clone(),
                record_count: b.record_count,
                location: b.location.clone(),
                meta: &(),
            })
            .collect();
        self.inner.commit(&clean)
    }
    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test]
async fn multiplexed_commit_is_all_or_nothing() {
    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(PoisonIfAny::default());
    let engine = Arc::new(LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        coalesce_after(2),
        "log/",
    ));
    // One object with a healthy batch (p-a) and a poison batch (p-b): the flush
    // fires only once both have accumulated (coalesce_after(2)).
    let e2 = Arc::clone(&engine);
    let healthy = tokio::spawn(async move {
        e2.produce(
            pk("p-a"),
            Bytes::from_static(b"ok"),
            1,
            false,
            Durability::Sequenced,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await; // let the healthy batch enqueue first
    let poison = engine
        .produce(
            pk("p-b"),
            Bytes::from_static(b"bad"),
            1,
            true,
            Durability::Sequenced,
        )
        .await;
    assert!(matches!(poison, Err(ObjectLogError::Sequencer(_))));
    assert!(matches!(
        healthy.await.unwrap(),
        Err(ObjectLogError::Sequencer(_))
    ));
    // Neither partition advanced — all-or-nothing.
    assert_eq!(seq.high_watermark(&pk("p-a")).unwrap(), 0);
    assert_eq!(seq.high_watermark(&pk("p-b")).unwrap(), 0);
}

// ---- A sequencer that dedups on a token in Meta. ----
#[derive(Default)]
struct DedupSeq {
    inner: InMemorySequencer,
    seen: Mutex<HashMap<u64, i64>>, // token -> base_offset
}
impl Sequencer for DedupSeq {
    type Meta = u64;
    fn commit(
        &self,
        batches: &[CommitBatch<'_, u64>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let mut seen = self.seen.lock().unwrap();
        let mut out = Vec::with_capacity(batches.len());
        for b in batches {
            if let Some(base) = seen.get(b.meta) {
                out.push(CommitOutcome::Duplicate { base_offset: *base });
                continue;
            }
            let clean = [CommitBatch {
                partition: b.partition.clone(),
                record_count: b.record_count,
                location: b.location.clone(),
                meta: &(),
            }];
            let r = self.inner.commit(&clean)?;
            if let CommitOutcome::Assigned { base_offset, .. } = r[0] {
                seen.insert(*b.meta, base_offset);
            }
            out.push(r.into_iter().next().unwrap());
        }
        Ok(out)
    }
    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test]
async fn idempotent_retry_does_not_duplicate() {
    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(DedupSeq::default());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let first = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            7u64,
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let retry = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            7u64,
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(first.base_offset, Some(0));
    assert_eq!(retry.base_offset, Some(0)); // duplicate -> original offset
    assert_eq!(
        seq.high_watermark(&p).unwrap(),
        1,
        "only one record committed"
    );
}

#[tokio::test]
async fn truncate_before_deletes_dead_objects() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    for _ in 0..3 {
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"r"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
    }
    assert_eq!(blob.object_count(), 3);
    // Drop everything below offset 2 -> the first two single-record objects die.
    engine.truncate_before(&p, 2).await.unwrap();
    assert_eq!(blob.object_count(), 1, "two covered objects reaped");
    assert_eq!(engine.fetch(&p, 2, 1 << 20).await.unwrap().len(), 1);
}

#[tokio::test]
async fn pipeline_snapshot_exposes_budget_defaults() {
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let snap = engine.pipeline_snapshot();
    assert!(snap.budget_enabled);
    assert_eq!(snap.budget_mode, BudgetMode::LatencyPriority);
    assert!(snap.budget_per_sec.effective > 0.0);
    assert_eq!(snap.max_linger_ms, 50);
}

#[tokio::test]
async fn fail_closed_rejects_when_budget_starved() {
    let budget = BudgetConfig {
        mode: BudgetMode::FailClosed,
        default_capacity_per_sec: 0.0,
        budget_fraction: 0.0,
        budget_per_sec_cap: Some(0.0),
        ..BudgetConfig::default()
    };
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            budget,
            ..FlushConfig::default()
        },
        "log/",
    );
    // Force empty tokens: predicted cost > 0 and capacity 0.
    {
        // Produce once may still work if tokens bootstrapped; drain tokens by
        // setting predicted high via a produce then starve — simpler: capacity 0
        // still starts with tokens = max(1.0) in BudgetRuntime::new. So burn them.
        for _ in 0..5 {
            let _ = engine
                .produce(
                    pk("burn"),
                    Bytes::from_static(b"x"),
                    1,
                    (),
                    Durability::Sequenced,
                )
                .await;
        }
    }
    // With zero refill rate, further fail_closed admissions should eventually fail.
    let mut saw_budget = false;
    for _ in 0..20 {
        match engine
            .produce(
                pk("starve"),
                Bytes::from_static(b"y"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
        {
            Err(ObjectLogError::BudgetExceeded(_)) => {
                saw_budget = true;
                break;
            }
            Ok(_) => continue,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert!(
        saw_budget,
        "expected BudgetExceeded under fail_closed starvation"
    );
}

#[tokio::test]
async fn headroom_allows_fast_single_produce() {
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let start = std::time::Instant::now();
    engine
        .produce(
            pk("fast"),
            Bytes::from_static(b"hi"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    // Should not wait the full 50ms linger when headroom early-flush applies.
    assert!(
        start.elapsed() < Duration::from_millis(40),
        "idle produce should early-flush, elapsed={:?}",
        start.elapsed()
    );
}
