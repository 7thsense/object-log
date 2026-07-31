//! Throughput: arbitrary client chunks + durable ack/`flush` should approach
//! single-stream durable sequential write (dd-class), not per-chunk fsync rates.
//!
//! Baselines (same temp dir / FS):
//! - **B0**: one file, write all, `fdatasync` + dir fsync (dd-like)
//! - **B1**: one object via LocalBlobStore protocol (temp + sync_data + rename + dir)
//! - **B2**: engine — many small produces + `flush()` / Durable pipeline
//!
//! Volume: `OBJECT_LOG_PERF_BYTES` (default 32 MiB).

use bytes::Bytes;
use object_log::{
    BlobStore, BudgetConfig, Durability, FlushConfig, InMemorySequencer, LocalBlobStore, LogEngine,
    PartitionKey,
};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn total_bytes() -> usize {
    std::env::var("OBJECT_LOG_PERF_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024 * 1024)
}

fn mib_s(bytes: usize, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64().max(1e-9);
    (bytes as f64 / (1024.0 * 1024.0)) / secs
}

/// B0: streaming write + one fdatasync + dir fsync.
fn baseline_b0_dd_like(dir: &Path, total: usize) -> Duration {
    let path = dir.join("b0.bin");
    let chunk = 8 * 1024 * 1024;
    let buf = vec![0u8; chunk];
    let t0 = Instant::now();
    {
        let mut f = File::create(&path).unwrap();
        let mut left = total;
        while left > 0 {
            let n = left.min(chunk);
            f.write_all(&buf[..n]).unwrap();
            left -= n;
        }
        f.flush().unwrap();
        f.sync_data().unwrap(); // fdatasync
    }
    File::open(dir).unwrap().sync_all().unwrap();
    t0.elapsed()
}

/// B1: one LocalBlobStore put of `total` bytes.
async fn baseline_b1_one_put(dir: &Path, total: usize) -> Duration {
    let store = LocalBlobStore::new(dir);
    let payload = Bytes::from(vec![0xABu8; total]);
    let t0 = Instant::now();
    store.put("b1/one", payload).await.unwrap();
    t0.elapsed()
}

fn next_chunk(seed: &mut u64, min: usize, max: usize) -> usize {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let span = max - min + 1;
    min + (*seed as usize % span)
}

fn high_ceiling_config() -> FlushConfig {
    FlushConfig {
        max_bytes: FlushConfig::default().max_bytes, // 1 GiB ceiling
        max_batches: usize::MAX,
        linger: Duration::from_millis(50),
        max_inflight_flushes: 2,
        max_buffered_bytes: FlushConfig::default().max_buffered_bytes,
        budget: BudgetConfig {
            enabled: true,
            default_capacity_per_sec: 10_000.0,
            budget_fraction: 1.0,
            budget_per_sec_cap: None,
            // Early-flush only when queue is tiny; bulk must wait linger/flush.
            early_flush_max_queued_bytes: 4 * 1024 * 1024,
            early_flush_fill_ratio: 0.5,
            ..BudgetConfig::default()
        },
    }
}

#[tokio::test]
async fn local_bulk_flush_near_single_object_and_dd_class() {
    let total = total_bytes();
    let dir = tempfile::tempdir().unwrap();
    let b0_dir = dir.path().join("b0");
    let b1_dir = dir.path().join("b1");
    let b2_dir = dir.path().join("b2");
    std::fs::create_dir_all(&b0_dir).unwrap();
    std::fs::create_dir_all(&b1_dir).unwrap();
    std::fs::create_dir_all(&b2_dir).unwrap();

    let b0_elapsed = baseline_b0_dd_like(&b0_dir, total);
    let b0_rate = mib_s(total, b0_elapsed);

    let b1_elapsed = baseline_b1_one_put(&b1_dir, total).await;
    let b1_rate = mib_s(total, b1_elapsed);

    let blob = Arc::new(LocalBlobStore::new(&b2_dir));
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        high_ceiling_config(),
        "data/",
    );

    let partition = PartitionKey("thr-0".into());
    let mut seed = 0xC0FFEE_u64;
    let mut produced = 0usize;
    let mut client_chunks = 0u64;
    let t0 = Instant::now();
    while produced < total {
        let chunk = next_chunk(&mut seed, 1024, 256 * 1024).min(total - produced);
        engine
            .produce(
                partition.clone(),
                Bytes::from(vec![0xCDu8; chunk]),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
        produced += chunk;
        client_chunks += 1;
    }
    engine.flush().await.expect("flush");
    let b2_elapsed = t0.elapsed();
    let b2_rate = mib_s(total, b2_elapsed);

    let objects = blob.list("data/").await.unwrap().len() as u64;
    let snap = engine.pipeline_snapshot();

    eprintln!(
        "perf B0/B1/B2: total_mib={:.1} B0_dd={:.1} B1_one_put={:.1} B2_engine={:.1} MiB/s | objects={} chunks={} flushes={} media_ops={}",
        total as f64 / (1024.0 * 1024.0),
        b0_rate,
        b1_rate,
        b2_rate,
        objects,
        client_chunks,
        snap.flushes_total,
        snap.media_ops_total,
    );

    assert!(
        objects <= 2,
        "bulk+flush under 1GiB ceiling must not early-flush fanout: objects={objects}"
    );
    assert!(
        objects * 50 < client_chunks,
        "client chunks should dominate objects"
    );
    // Engine within ~30% of raw one-object Local put (protocol parity).
    assert!(
        b2_rate >= b1_rate * 0.70,
        "B2 should be near B1: b2={b2_rate:.1} b1={b1_rate:.1}"
    );
    // And a solid fraction of dd-like single fdatasync (protocol + rename tax).
    assert!(
        b2_rate >= b0_rate * 0.50,
        "B2 should be ≥50% of B0 dd-like: b2={b2_rate:.1} b0={b0_rate:.1}"
    );
}

#[tokio::test]
async fn local_pipelined_chunks_final_durable_ack_near_bulk() {
    let total = (total_bytes() / 2).max(16 * 1024 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let b0_dir = dir.path().join("b0");
    let b2_dir = dir.path().join("b2");
    std::fs::create_dir_all(&b0_dir).unwrap();
    std::fs::create_dir_all(&b2_dir).unwrap();

    let b0_rate = mib_s(total, baseline_b0_dd_like(&b0_dir, total));

    let blob = Arc::new(LocalBlobStore::new(&b2_dir));
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        high_ceiling_config(),
        "ack/",
    );

    let partition = PartitionKey("ack-0".into());
    let mut seed = 7u64;
    let mut produced = 0usize;
    let mut client_chunks = 0u64;
    let t0 = Instant::now();
    while produced < total {
        let remaining = total - produced;
        let chunk = next_chunk(&mut seed, 1024, 256 * 1024).min(remaining);
        let last = chunk == remaining;
        engine
            .produce(
                partition.clone(),
                Bytes::from(vec![0x11u8; chunk]),
                1,
                (),
                if last {
                    Durability::Durable
                } else {
                    Durability::Buffered
                },
            )
            .await
            .unwrap();
        produced += chunk;
        client_chunks += 1;
    }
    engine.flush().await.unwrap();
    let b2_rate = mib_s(total, t0.elapsed());
    let objects = blob.list("ack/").await.unwrap().len() as u64;

    eprintln!(
        "perf ack: total_mib={:.1} B0={:.1} B2={:.1} MiB/s objects={} chunks={}",
        total as f64 / (1024.0 * 1024.0),
        b0_rate,
        b2_rate,
        objects,
        client_chunks,
    );

    assert!(objects <= 2, "expected ≤2 seals, got {objects}");
    assert!(b2_rate >= b0_rate * 0.45, "ack path too slow vs B0");
}

#[tokio::test]
async fn flush_drains_buffered_produces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LogEngine::new(
        Arc::new(LocalBlobStore::new(dir.path())) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            max_bytes: usize::MAX,
            max_batches: usize::MAX,
            linger: Duration::from_secs(3600),
            max_inflight_flushes: 1,
            max_buffered_bytes: usize::MAX,
            budget: BudgetConfig {
                enabled: false,
                ..BudgetConfig::default()
            },
        },
        "f/",
    );
    for i in 0..10 {
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from(format!("row-{i}")),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    let rows = engine
        .fetch(&PartitionKey("p".into()), 0, 1 << 20)
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);
}

#[tokio::test]
async fn put_chunks_matches_put_without_premerge() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(dir.path());
    let chunks = vec![
        Bytes::from_static(b"hello "),
        Bytes::from_static(b"world"),
        Bytes::from_static(b"!"),
    ];
    store.put_chunks("k/chunks", chunks.clone()).await.unwrap();
    store
        .put("k/single", Bytes::from_static(b"hello world!"))
        .await
        .unwrap();
    assert_eq!(
        store.get("k/chunks").await.unwrap().unwrap(),
        store.get("k/single").await.unwrap().unwrap()
    );
}
