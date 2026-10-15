//! Honest Local throughput harness (Rust-only, `--release`).
//!
//! | Label | What is timed |
//! |-------|----------------|
//! | **dd**   | Shell `dd conv=fdatasync` (host reference) |
//! | **B0**   | Rust stream zeros → `sync_data` |
//! | **B0d**  | B0 + parent dir `fsync` |
//! | **B1**   | Warm `LocalBlobStore::put` flat key (one per matched round) |
//! | **B2**   | Prebuilt zero chunks → produce + flush (split: enqueue vs flush) |
//!
//! ```text
//! OBJECT_LOG_PERF_BYTES=$((256*1024*1024)) \
//!   cargo test --release --test perf_throughput honest -- --nocapture --test-threads=1
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
        .unwrap_or(64 * 1024 * 1024)
}

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
    (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
}

fn zeros(n: usize) -> Vec<u8> {
    vec![0u8; n]
}

fn print_row(name: &str, total: usize, elapsed: Duration, extra: &str) {
    eprintln!(
        "  {name:<14} {:>8.1} MiB/s   {:>7.3}s   {extra}",
        mib_s(total, elapsed),
        elapsed.as_secs_f64(),
    );
}

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

async fn baseline_b1(dir: &Path, payload: &Bytes) -> Duration {
    let store = LocalBlobStore::new(dir);
    store
        .put("warm", Bytes::from_static(b"warm"))
        .await
        .unwrap();
    store.delete("warm").await.unwrap();
    let start = Instant::now();
    store.put("obj", payload.clone()).await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(store.get("obj").await.unwrap().unwrap(), *payload);
    elapsed
}

const ORDERS: [[usize; 4]; 8] = [
    [0, 1, 3, 2],
    [1, 2, 0, 3],
    [2, 3, 1, 0],
    [3, 0, 2, 1],
    [0, 1, 3, 2],
    [1, 2, 0, 3],
    [2, 3, 1, 0],
    [3, 0, 2, 1],
];

fn median(mut values: Vec<f64>) -> f64 {
    assert!(!values.is_empty() && values.iter().all(|v| v.is_finite() && *v > 0.0));
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn paired_ratios(samples: &[[Duration; 4]]) -> (f64, f64) {
    let ratio = |arm: usize| {
        median(
            samples
                .iter()
                .map(|s| {
                    assert!(s.iter().all(|d| !d.is_zero()));
                    s[arm].as_secs_f64() / s[3].as_secs_f64()
                })
                .collect(),
        )
    };
    (ratio(1), ratio(0))
}

fn qualifies(samples: &[[Duration; 4]]) -> bool {
    let (dir, data) = paired_ratios(samples);
    dir >= 0.70 || data >= 0.60
}

#[test]
fn honest_paired_gate_controls() {
    for position in 0..4 {
        for label in 0..4 {
            assert_eq!(ORDERS.iter().filter(|o| o[position] == label).count(), 2);
        }
    }
    let ms = Duration::from_millis;
    let boundary = [[ms(60), ms(70), ms(90), ms(100)]; 8];
    assert!(qualifies(&boundary));
    assert!(qualifies(&[[ms(59), ms(70), ms(90), ms(100)]; 8]));
    assert!(qualifies(&[[ms(60), ms(69), ms(90), ms(100)]; 8]));
    let below = [[ms(59), ms(69), ms(90), ms(100)]; 8];
    assert!(!qualifies(&below));
    let slowed = boundary.map(|mut s| {
        s[3] *= 2;
        s
    });
    assert!(!qualifies(&slowed));
    // A short final flush cannot hide a slow enqueue interval.
    let enqueue = ms(190);
    let flush = ms(10);
    assert!(!qualifies(&[[ms(60), ms(70), ms(90), enqueue + flush]; 8]));
    let asymmetric = [
        [ms(1), ms(1), ms(1), ms(1)],
        [ms(100), ms(100), ms(1), ms(2)],
        [ms(101), ms(101), ms(1), ms(1000)],
    ];
    assert_eq!(paired_ratios(&asymmetric), (1.0, 1.0));
    assert_ne!(
        median(vec![1.0, 100.0, 101.0]) / median(vec![1.0, 2.0, 1000.0]),
        1.0
    );
}

fn next_chunk_len(seed: &mut u64, min: usize, max: usize) -> usize {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    min + (*seed as usize % (max - min + 1))
}

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
        // Single-flight Local put: flush thread block_on + block_in_place (no extra hops).
        max_inflight_flushes: 1,
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

struct B2Timing {
    enqueue: Duration,
    flush: Duration,
    total: Duration,
    objects: u64,
    flushes: u64,
    chunks: usize,
}

async fn baseline_b2_split(dir: &Path, chunks: &[Bytes]) -> B2Timing {
    baseline_b2_timed(dir, chunks, Duration::ZERO).await
}

async fn baseline_b2_timed(dir: &Path, chunks: &[Bytes], enqueue_delay: Duration) -> B2Timing {
    let blob = Arc::new(LocalBlobStore::new(dir));
    // Warm store root.
    blob.put("b2/warm", Bytes::from(zeros(4096))).await.unwrap();
    let _ = blob.delete("b2/warm").await;

    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        engine_config(),
        "b2/",
    );
    let partition = PartitionKey("p0".into());

    let t_enq0 = Instant::now();
    if !enqueue_delay.is_zero() {
        tokio::time::sleep(enqueue_delay).await;
    }
    for c in chunks {
        engine
            .produce(partition.clone(), c.clone(), 1, (), Durability::Buffered)
            .await
            .unwrap();
    }
    let enqueue = t_enq0.elapsed();

    let t_fl0 = Instant::now();
    engine.flush().await.unwrap();
    let flush = t_fl0.elapsed();
    let total = t_enq0.elapsed();

    let stored = engine.fetch(&partition, 0, usize::MAX).await.unwrap();
    assert_eq!(stored.len(), chunks.len());
    for (batch, expected) in stored.iter().zip(chunks) {
        assert_eq!(batch.payload, *expected);
    }
    let objects = blob.list("b2/").await.unwrap().len() as u64;
    assert!((1..=2).contains(&objects), "packed objects={objects}");
    let flushes = engine.pipeline_snapshot().flushes_total;
    drop(engine); // Drop joins the owned committer before the next sample.
    B2Timing {
        enqueue,
        flush,
        total,
        objects,
        flushes,
        chunks: chunks.len(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn honest_actual_enqueue_delay_is_scored() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = prebuild_chunks(1024 * 1024);
    let delay = Duration::from_secs(1);
    let timing = baseline_b2_timed(dir.path(), &chunks, delay).await;
    assert!(timing.enqueue >= delay);
    assert!(
        timing.total >= timing.enqueue + timing.flush,
        "total omitted enqueue: enqueue={:?} flush={:?} total={:?}",
        timing.enqueue,
        timing.flush,
        timing.total
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn honest_local_throughput_table() {
    let total = total_bytes();
    assert!(total >= 1024 * 1024, "need at least 1 MiB");
    let (root, _guard) = perf_root();
    let run = root.join(format!("run-{}", std::process::id()));
    std::fs::create_dir_all(&run).unwrap();

    eprintln!("=== object-log local throughput (Rust --release, zeros, fair timers) ===");
    eprintln!("total={} MiB  dir={}", total / (1024 * 1024), run.display());

    if let Some((elapsed, rate)) = baseline_dd(&run, total) {
        eprintln!(
            "  {:<14} {:>8.1} MiB/s   {:>7.3}s   (shell dd conv=fdatasync)",
            "dd",
            rate,
            elapsed.as_secs_f64()
        );
    } else {
        eprintln!("  dd             (skipped)");
    }

    let payload = Bytes::from(zeros(total));
    let chunks = prebuild_chunks(total);
    let mut samples = Vec::new();
    for (round, order) in ORDERS.iter().enumerate() {
        let mut times = [Duration::ZERO; 4];
        eprintln!("round={round} order={order:?}");
        for &label in order {
            let dir = run.join(format!("round-{round}-sample-{label}"));
            std::fs::create_dir_all(&dir).unwrap();
            // Warm the actual publication parent for every arm, outside timing.
            let parent = if label == 3 {
                dir.join("b2")
            } else {
                dir.clone()
            };
            std::fs::create_dir_all(&parent).unwrap();
            let warm = parent.join("warm-parent");
            File::create(&warm).unwrap().sync_data().unwrap();
            std::fs::remove_file(warm).unwrap();
            File::open(&parent).unwrap().sync_all().unwrap();
            times[label] = match label {
                0 => baseline_b0(&dir, total),
                1 => baseline_b0_dir(&dir, total),
                2 => baseline_b1(&dir, &payload).await,
                3 => {
                    let b2 = baseline_b2_split(&dir, &chunks).await;
                    eprintln!(
                        "round={round} enqueue={:?} flush={:?} objects={} flushes={} chunks={} bytes={total}",
                        b2.enqueue, b2.flush, b2.objects, b2.flushes, b2.chunks
                    );
                    b2.total
                }
                _ => unreachable!(),
            };
            print_row(
                ["B0", "B0d", "B1", "B2.total"][label],
                total,
                times[label],
                &format!("round={round} ns={}", times[label].as_nanos()),
            );
            std::fs::remove_dir_all(&dir).unwrap();
        }
        samples.push(times);
    }
    let (dir_ratio, data_ratio) = paired_ratios(&samples);
    eprintln!("paired median B2.total/B0d={dir_ratio:.6} B2.total/B0={data_ratio:.6}");
    if cfg!(not(debug_assertions))
        || std::env::var("OBJECT_LOG_PERF_ASSERT").ok().as_deref() == Some("1")
    {
        assert!(
            qualifies(&samples),
            "B2 end-to-end paired ratios below unchanged 0.70/0.60 floors: {dir_ratio}, {data_ratio}"
        );
    }

    let _ = std::fs::remove_dir_all(&run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
    for _ in 0..10 {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_chunks_matches_put_without_premerge() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(dir.path());
    store
        .put_chunks(
            "k/c",
            vec![
                Bytes::from(zeros(3)),
                Bytes::from(zeros(5)),
                Bytes::from(zeros(2)),
            ],
        )
        .await
        .unwrap();
    store.put("k/s", Bytes::from(zeros(10))).await.unwrap();
    assert_eq!(
        store.get("k/c").await.unwrap().unwrap().len(),
        store.get("k/s").await.unwrap().unwrap().len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_single_produce_still_snappy() {
    // Memory: measure early-flush policy, not Local fsync latency.
    let engine = LogEngine::new(
        Arc::new(object_log::MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "idle/",
    );
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
        p50 < Duration::from_millis(40),
        "idle early-flush should beat full linger, p50={p50:?}"
    );
}
