//! Throughput demonstration: arbitrary client chunk sizes + durable ack/flush
//! should approach bulk durable sequential write rates (dd-class), not
//! per-chunk fsync rates.
//!
//! Baseline: write the same total bytes as large sequential files with
//! file+dir fsync (same durability unit as [`LocalBlobStore`]).
//! object-log path: many small/variable produces, then wait via
//! [`Durability::Durable`] or [`LogEngine::flush`].
//!
//! Override volume with `OBJECT_LOG_PERF_BYTES` (default 64 MiB).

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

/// Same durability protocol as LocalBlobStore: write, fsync file, fsync parent dir.
fn bulk_durable_write(dir: &Path, total: usize, chunk: usize) -> (Duration, u64) {
    let mut written = 0usize;
    let mut objects = 0u64;
    let t0 = Instant::now();
    let mut idx = 0u64;
    let payload = vec![0xABu8; chunk];
    while written < total {
        let n = (total - written).min(chunk);
        let path = dir.join(format!("bulk-{idx:08}.bin"));
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&payload[..n]).unwrap();
            f.sync_all().unwrap();
        }
        // parent dir fsync
        File::open(dir).unwrap().sync_all().unwrap();
        written += n;
        objects += 1;
        idx += 1;
    }
    (t0.elapsed(), objects)
}

/// Deterministic pseudo-random chunk sizes in [min, max] inclusive.
fn next_chunk(seed: &mut u64, min: usize, max: usize) -> usize {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let span = max - min + 1;
    min + (*seed as usize % span)
}

#[tokio::test]
async fn local_arbitrary_chunks_with_flush_near_bulk_durable_throughput() {
    let total = total_bytes();
    let dir = tempfile::tempdir().unwrap();
    let bulk_dir = dir.path().join("bulk");
    let olog_dir = dir.path().join("olog");
    std::fs::create_dir_all(&bulk_dir).unwrap();
    std::fs::create_dir_all(&olog_dir).unwrap();

    // Baseline: large sequential durable objects (~1 MiB), same fsync shape as LocalBlobStore.
    let bulk_chunk = 1024 * 1024;
    let (bulk_elapsed, bulk_objects) = bulk_durable_write(&bulk_dir, total, bulk_chunk);
    let bulk_rate = mib_s(total, bulk_elapsed);

    // object-log: clients write arbitrary small/medium chunks, fire-and-forget, then flush.
    let blob = Arc::new(LocalBlobStore::new(&olog_dir));
    let segment = 8 * 1024 * 1024; // co-buffer up to 8 MiB objects
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            max_bytes: segment,
            max_batches: usize::MAX,
            linger: Duration::from_millis(50),
            max_inflight_flushes: 4,
            max_buffered_bytes: 64 * 1024 * 1024,
            budget: BudgetConfig {
                enabled: true,
                // Generous budget so Local is not rate-starved; linger/size do the packing.
                default_capacity_per_sec: 10_000.0,
                budget_fraction: 1.0,
                budget_per_sec_cap: None,
                early_flush_fill_ratio: 0.95,
                ..BudgetConfig::default()
            },
        },
        "data/",
    );

    let partition = PartitionKey("thr-0".into());
    let mut seed = 0xC0FFEE_u64;
    let mut produced = 0usize;
    let mut client_chunks = 0u64;
    let t0 = Instant::now();
    while produced < total {
        let chunk = next_chunk(&mut seed, 1024, 256 * 1024).min(total - produced);
        let payload = Bytes::from(vec![0xCDu8; chunk]);
        engine
            .produce(
                partition.clone(),
                payload,
                1,
                (),
                Durability::Buffered, // arbitrary client chunks, no per-chunk wait
            )
            .await
            .unwrap();
        produced += chunk;
        client_chunks += 1;
    }
    engine.flush().await.expect("flush drains barrier");
    let olog_elapsed = t0.elapsed();
    let olog_rate = mib_s(total, olog_elapsed);

    let objects = blob.list("data/").await.unwrap().len() as u64;
    let snap = engine.pipeline_snapshot();

    eprintln!(
        "perf_throughput: total_mib={:.1} bulk={:.1} MiB/s ({} objects) olog_flush={:.1} MiB/s ({} objects, {} client_chunks) flushes={} media_ops={}",
        total as f64 / (1024.0 * 1024.0),
        bulk_rate,
        bulk_objects,
        olog_rate,
        objects,
        client_chunks,
        snap.flushes_total,
        snap.media_ops_total,
    );

    // Packing: far fewer durable objects than client chunks.
    assert!(
        objects * 10 < client_chunks,
        "expected strong co-buffering: objects={objects} client_chunks={client_chunks}"
    );
    assert!(
        objects <= (total / segment) as u64 + 4,
        "object count should track max_bytes packing, got {objects}"
    );

    // Throughput: within the same order as bulk durable sequential writes.
    // CI/VMs vary; require at least 25% of the bulk durable baseline measured
    // in-process (same disk, same fsync protocol).
    let ratio = olog_rate / bulk_rate.max(0.1);
    assert!(
        ratio >= 0.25,
        "object-log+flush should approach bulk durable rate: olog={olog_rate:.1} bulk={bulk_rate:.1} ratio={ratio:.2}"
    );

    // Absolute floor so a totally broken path fails even if bulk is also slow.
    assert!(
        olog_rate > 5.0,
        "object-log durable throughput too low: {olog_rate:.1} MiB/s"
    );
}

/// Pipelined client: many arbitrary chunks with [`Durability::Buffered`], then a
/// final [`Durability::Durable`] produce that acks only after co-buffered seal.
/// Measures end-to-end rate including the durable ack.
#[tokio::test]
async fn local_pipelined_chunks_final_durable_ack_near_bulk() {
    let total = (total_bytes() / 2).max(32 * 1024 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let bulk_dir = dir.path().join("bulk");
    let olog_dir = dir.path().join("olog");
    std::fs::create_dir_all(&bulk_dir).unwrap();
    std::fs::create_dir_all(&olog_dir).unwrap();

    let (bulk_elapsed, _) = bulk_durable_write(&bulk_dir, total, 1024 * 1024);
    let bulk_rate = mib_s(total, bulk_elapsed);

    let blob = Arc::new(LocalBlobStore::new(&olog_dir));
    let segment = 8 * 1024 * 1024;
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            max_bytes: segment,
            max_batches: usize::MAX,
            linger: Duration::from_millis(50),
            max_inflight_flushes: 4,
            max_buffered_bytes: 64 * 1024 * 1024,
            budget: BudgetConfig {
                enabled: true,
                default_capacity_per_sec: 10_000.0,
                budget_fraction: 1.0,
                early_flush_fill_ratio: 0.95,
                ..BudgetConfig::default()
            },
        },
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
                    Durability::Durable // ack after co-buffered object is durable
                } else {
                    Durability::Buffered
                },
            )
            .await
            .unwrap();
        produced += chunk;
        client_chunks += 1;
    }
    // Ensure any trailing buffer after the last Durable is empty (last produce
    // may have sealed only its own object if size triggered mid-stream).
    engine.flush().await.unwrap();
    let olog_elapsed = t0.elapsed();
    let olog_rate = mib_s(total, olog_elapsed);
    let objects = blob.list("ack/").await.unwrap().len() as u64;

    eprintln!(
        "perf_throughput ack: total_mib={:.1} bulk={:.1} MiB/s olog_pipelined_ack={:.1} MiB/s objects={} chunks={}",
        total as f64 / (1024.0 * 1024.0),
        bulk_rate,
        olog_rate,
        objects,
        client_chunks,
    );

    assert!(
        objects * 10 < client_chunks,
        "ack path must still co-buffer"
    );
    let ratio = olog_rate / bulk_rate.max(0.1);
    assert!(
        ratio >= 0.25,
        "pipelined Durable-ack path should stay near bulk durable rate: ratio={ratio:.2} olog={olog_rate:.1} bulk={bulk_rate:.1}"
    );
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
            linger: Duration::from_secs(3600), // would never fire without flush
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
