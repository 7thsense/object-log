//! Honest Local throughput harness (Rust-only, `--release`).
//!
//! | Label | What is timed |
//! |-------|----------------|
//! | **dd**  | Optional shell `dd conv=fdatasync` (host reference; not same process) |
//! | **B0**  | Rust: stream zeros → `sync_data` (no dir fsync) |
//! | **B0d** | Rust: stream zeros → `sync_data` + parent dir `fsync` |
//! | **B1**  | Rust: one `LocalBlobStore::put` of zeros (alloc **before** timer) |
//! | **B2**  | Rust: prebuilt zero `Bytes` chunks → `produce(Buffered)` + `flush()` |
//!
//! Same payload (zeros), same total, same FS root. No Python.
//!
//! ```text
//! OBJECT_LOG_PERF_BYTES=$((256*1024*1024)) \
//!   cargo test --release --test perf_throughput -- --nocapture
//! ```

use bytes::Bytes;
use object_log::{
    BlobStore, BudgetConfig, Durability, FlushConfig, InMemorySequencer, LocalBlobStore, LogEngine,
    PartitionKey,
};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn total_bytes() -> usize {
    std::env::var("OBJECT_LOG_PERF_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024 * 1024)
}

/// Returns (work_dir, guard). Keep `guard` alive for the test duration when using a tempdir.
fn perf_root() -> (PathBuf, Option<tempfile::TempDir>) {
    if let Ok(p) = std::env::var("OBJECT_LOG_PERF_DIR") {
        let p = PathBuf::from(p);
        std::fs::create_dir_all(&p).ok();
        return (p, None);
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_path_buf();
    (path, Some(tmp))
}

fn mib_s(bytes: usize, elapsed: Duration) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(1e-9)
}

fn zeros(n: usize) -> Vec<u8> {
    vec![0u8; n]
}

/// Host reference: `dd if=/dev/zero of=... bs=8M conv=fdatasync`.
fn baseline_dd(dir: &Path, total: usize) -> Option<(Duration, f64)> {
    let path = dir.join("dd_ref.bin");
    let bs = 8 * 1024 * 1024;
    if !total.is_multiple_of(bs) {
        return None;
    }
    let count = total / bs;
    let t0 = Instant::now();
    let status = Command::new("dd")
        .args([
            "if=/dev/zero",
            &format!("of={}", path.display()),
            &format!("bs={bs}"),
            &format!("count={count}"),
            "conv=fdatasync",
            "status=none",
        ])
        .status()
        .ok()?;
    if !status.success() {
        return None;
    }
    let elapsed = t0.elapsed();
    let _ = std::fs::remove_file(&path);
    Some((elapsed, mib_s(total, elapsed)))
}

/// B0: stream write + sync_data only.
fn baseline_b0(dir: &Path, total: usize) -> Duration {
    let path = dir.join("b0.bin");
    let chunk = 8 * 1024 * 1024;
    let buf = zeros(chunk);
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
        f.sync_data().unwrap();
    }
    t0.elapsed()
}

/// B0d: B0 + parent dir fsync.
fn baseline_b0_dir(dir: &Path, total: usize) -> Duration {
    let path = dir.join("b0d.bin");
    let chunk = 8 * 1024 * 1024;
    let buf = zeros(chunk);
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
        f.sync_data().unwrap();
    }
    File::open(dir).unwrap().sync_all().unwrap();
    t0.elapsed()
}

/// B1: one Local put; payload allocated **before** the timer.
async fn baseline_b1(dir: &Path, payload: Bytes) -> Duration {
    let store = LocalBlobStore::new(dir);
    let t0 = Instant::now();
    store.put("b1/one", payload).await.unwrap();
    t0.elapsed()
}

fn next_chunk_len(seed: &mut u64, min: usize, max: usize) -> usize {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    min + (*seed as usize % (max - min + 1))
}

/// Prebuild zero chunks (not timed).
fn prebuild_chunks(total: usize) -> Vec<Bytes> {
    let mut seed = 0xC0FFEE_u64;
    let mut out = Vec::new();
    let mut produced = 0usize;
    while produced < total {
        let n = next_chunk_len(&mut seed, 1024, 256 * 1024).min(total - produced);
        out.push(Bytes::from(zeros(n)));
        produced += n;
    }
    out
}

fn engine_config() -> FlushConfig {
    FlushConfig {
        max_bytes: FlushConfig::default().max_bytes,
        max_batches: usize::MAX,
        linger: Duration::from_millis(50),
        max_inflight_flushes: 2,
        max_buffered_bytes: FlushConfig::default().max_buffered_bytes,
        budget: BudgetConfig {
            enabled: true,
            default_capacity_per_sec: 10_000.0,
            budget_fraction: 1.0,
            early_flush_max_queued_bytes: 4 * 1024 * 1024,
            early_flush_idle: Duration::from_millis(10),
            early_flush_fill_ratio: 0.5,
            ..BudgetConfig::default()
        },
    }
}

/// B2: prebuilt chunks → produce Buffered → flush. Only produce+flush timed.
async fn baseline_b2(dir: &Path, chunks: &[Bytes]) -> (Duration, u64, u64) {
    let blob = Arc::new(LocalBlobStore::new(dir));
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        engine_config(),
        "b2/",
    );
    let partition = PartitionKey("p0".into());
    let t0 = Instant::now();
    for c in chunks {
        engine
            .produce(partition.clone(), c.clone(), 1, (), Durability::Buffered)
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    let elapsed = t0.elapsed();
    let objects = blob.list("b2/").await.unwrap().len() as u64;
    let flushes = engine.pipeline_snapshot().flushes_total;
    (elapsed, objects, flushes)
}

fn print_row(name: &str, total: usize, elapsed: Duration, extra: &str) {
    eprintln!(
        "  {name:<12} {:>8.1} MiB/s   {:>7.3}s   {extra}",
        mib_s(total, elapsed),
        elapsed.as_secs_f64(),
    );
}

#[tokio::test]
async fn honest_local_throughput_table() {
    let total = total_bytes();
    assert!(total >= 1024 * 1024, "need at least 1 MiB");
    let (root, _guard) = perf_root();
    let run = root.join(format!("run-{}", std::process::id()));
    std::fs::create_dir_all(&run).unwrap();

    eprintln!("=== object-log local throughput (Rust --release, zeros) ===");
    eprintln!(
        "total={} MiB  dir={}  profile=release",
        total / (1024 * 1024),
        run.display()
    );

    // dd reference (best effort)
    if let Some((elapsed, rate)) = baseline_dd(&run, total) {
        eprintln!(
            "  {:<12} {:>8.1} MiB/s   {:>7.3}s   (shell dd conv=fdatasync)",
            "dd",
            rate,
            elapsed.as_secs_f64()
        );
    } else {
        eprintln!("  dd           (skipped: size not multiple of 8MiB or dd failed)");
    }

    let b0 = baseline_b0(&run, total);
    print_row("B0", total, b0, "(Rust stream + sync_data)");

    let b0d = baseline_b0_dir(&run, total);
    print_row("B0d", total, b0d, "(B0 + dir fsync)");

    let payload = Bytes::from(zeros(total));
    let b1 = baseline_b1(&run.join("b1"), payload).await;
    print_row(
        "B1",
        total,
        b1,
        "(LocalBlobStore::put once; alloc outside timer)",
    );

    let chunks = prebuild_chunks(total);
    let n_chunks = chunks.len();
    let (b2, objects, flushes) = baseline_b2(&run.join("b2"), &chunks).await;
    print_row(
        "B2",
        total,
        b2,
        &format!("(engine {n_chunks} chunks + flush; objects={objects} flushes={flushes})"),
    );

    let r_b1_b0 = mib_s(total, b1) / mib_s(total, b0).max(0.1);
    let r_b2_b1 = mib_s(total, b2) / mib_s(total, b1).max(0.1);
    let r_b2_b0 = mib_s(total, b2) / mib_s(total, b0).max(0.1);
    eprintln!("  ratios       B1/B0={r_b1_b0:.2}  B2/B1={r_b2_b1:.2}  B2/B0={r_b2_b0:.2}");

    // Bulk under 1GiB ceiling: idle early-flush must not fan out seals.
    assert!(
        objects <= 2,
        "B2 should pack bulk into ≤2 objects, got {objects}"
    );
    // Engine near one Local put when seals≈1.
    assert!(
        r_b2_b1 >= 0.75,
        "B2 should be near B1 when packing works: B2/B1={r_b2_b1:.2}"
    );

    let _ = std::fs::remove_dir_all(&run);
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
    for _i in 0..10 {
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from(zeros(16)),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    assert_eq!(
        engine
            .fetch(&PartitionKey("p".into()), 0, 1 << 20)
            .await
            .unwrap()
            .len(),
        10
    );
}

#[tokio::test]
async fn put_chunks_matches_put_without_premerge() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(dir.path());
    let chunks = vec![
        Bytes::from(zeros(3)),
        Bytes::from(zeros(5)),
        Bytes::from(zeros(2)),
    ];
    store.put_chunks("k/c", chunks).await.unwrap();
    store.put("k/s", Bytes::from(zeros(10))).await.unwrap();
    assert_eq!(
        store.get("k/c").await.unwrap().unwrap().len(),
        store.get("k/s").await.unwrap().unwrap().len()
    );
}

#[tokio::test]
async fn idle_single_produce_still_snappy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LogEngine::new(
        Arc::new(LocalBlobStore::new(dir.path())) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "idle/",
    );
    // Quiet gaps between produces → early-flush allowed.
    let mut samples = Vec::new();
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let t0 = Instant::now();
        engine
            .produce(
                PartitionKey("i".into()),
                Bytes::from(zeros(64)),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        samples.push(t0.elapsed());
    }
    samples.sort();
    let p50 = samples[samples.len() / 2];
    assert!(
        p50 < Duration::from_millis(100),
        "idle produce p50 {p50:?} should stay snappy with early-flush after idle gap"
    );
}
