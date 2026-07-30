//! Single-partition LogEngine -> S3/Garage write-saturation benchmark.
//!
//! This bypasses Kafka/Fjord and drives the durable object-log engine directly
//! with one partition key. It measures whether the storage engine can fill large
//! segments and saturate Garage before Kafka protocol overhead is involved.
//!
//! Required env:
//!   FJORD_GARAGE_ENDPOINT
//!   FJORD_GARAGE_BUCKET
//!   FJORD_GARAGE_KEY_ID
//!   FJORD_GARAGE_SECRET
//!
//! Tuning env:
//!   OLOG_SAT_TOTAL_BYTES          default 8589934592       (8 GiB)
//!   OLOG_SAT_BATCH_BYTES          default 8388608          (8 MiB)
//!   OLOG_SAT_SEGMENT_BYTES        default 134217728        (128 MiB)
//!   OLOG_SAT_PART_BYTES           default 8388608          (8 MiB)
//!   OLOG_SAT_PRODUCERS            default 32
//!   OLOG_SAT_FLUSH_INFLIGHT       default 16
//!   OLOG_SAT_MEMORY_BYTES         default 2147483648       (2 GiB)
//!   OLOG_SAT_LINGER_MS            default 5000
//!   OLOG_SAT_PREFIX               default object-log-engine-saturate/<pid>
//!   OLOG_SAT_DELETE               default 1
//!   OLOG_SAT_EVIDENCE_FILE        optional JSON summary path
//!
//! Run:
//!   cargo run --release --features s3 --example logengine_saturate

#[cfg(not(feature = "s3"))]
fn main() {
    eprintln!("logengine_saturate requires `--features s3`");
    std::process::exit(2);
}

#[cfg(feature = "s3")]
use async_trait::async_trait;
#[cfg(feature = "s3")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "s3")]
use object_log::{
    BlobStore, Durability, FlushConfig, InMemorySequencer, LogEngine, ObjectLogError, PartitionKey,
    S3BlobStore,
};
#[cfg(feature = "s3")]
use std::fs;
#[cfg(feature = "s3")]
use std::ops::Range;
#[cfg(feature = "s3")]
use std::path::PathBuf;
#[cfg(feature = "s3")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "s3")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "s3")]
use std::time::{Duration, Instant};

#[cfg(feature = "s3")]
#[derive(Clone, Debug)]
struct Config {
    endpoint: String,
    region: String,
    bucket: String,
    key_id: String,
    secret: String,
    total_bytes: u64,
    batch_bytes: usize,
    segment_bytes: usize,
    part_bytes: usize,
    producers: usize,
    flush_inflight: usize,
    memory_bytes: usize,
    linger_ms: u64,
    prefix: String,
    delete_after: bool,
    evidence_file: Option<PathBuf>,
}

#[cfg(feature = "s3")]
#[derive(Default)]
struct Counts {
    put_objects: AtomicU64,
    put_bytes: AtomicU64,
    sizes: Mutex<Vec<usize>>,
    keys: Mutex<Vec<String>>,
}

#[cfg(feature = "s3")]
struct CountingBlobStore {
    inner: Arc<dyn BlobStore>,
    counts: Arc<Counts>,
}

#[cfg(feature = "s3")]
impl CountingBlobStore {
    fn new(inner: Arc<dyn BlobStore>) -> Self {
        Self {
            inner,
            counts: Arc::new(Counts::default()),
        }
    }

    fn counts(&self) -> Arc<Counts> {
        Arc::clone(&self.counts)
    }

    fn record(&self, key: &str, bytes: usize) {
        self.counts.put_objects.fetch_add(1, Ordering::Relaxed);
        self.counts
            .put_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.counts.sizes.lock().expect("sizes").push(bytes);
        self.counts.keys.lock().expect("keys").push(key.to_string());
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl BlobStore for CountingBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        let bytes = value.len();
        self.inner.put(key, value).await?;
        self.record(key, bytes);
        Ok(())
    }

    async fn put_chunks(&self, key: &str, chunks: Vec<Bytes>) -> Result<(), ObjectLogError> {
        let bytes = chunks.iter().map(Bytes::len).sum();
        self.inner.put_chunks(key, chunks).await?;
        self.record(key, bytes);
        Ok(())
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

#[cfg(feature = "s3")]
fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[cfg(feature = "s3")]
fn env_required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

#[cfg(feature = "s3")]
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(feature = "s3")]
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(feature = "s3")]
fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name).as_deref() {
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES") => true,
        Ok("0") | Ok("false") | Ok("FALSE") | Ok("no") | Ok("NO") => false,
        _ => default,
    }
}

#[cfg(feature = "s3")]
fn config() -> Config {
    let batch_bytes = env_usize("OLOG_SAT_BATCH_BYTES", 8 * 1024 * 1024);
    let segment_bytes = env_usize("OLOG_SAT_SEGMENT_BYTES", 128 * 1024 * 1024);
    let part_bytes = env_usize("OLOG_SAT_PART_BYTES", 8 * 1024 * 1024);
    let producers = env_usize("OLOG_SAT_PRODUCERS", 32).max(1);
    let flush_inflight = env_usize("OLOG_SAT_FLUSH_INFLIGHT", 16).max(1);
    let memory_bytes = env_usize("OLOG_SAT_MEMORY_BYTES", 2 * 1024 * 1024 * 1024);
    assert!(batch_bytes > 0, "OLOG_SAT_BATCH_BYTES must be > 0");
    assert!(
        segment_bytes >= batch_bytes,
        "segment must fit at least one batch"
    );
    assert!(
        part_bytes >= 5 * 1024 * 1024,
        "S3 part size must be >= 5 MiB"
    );
    assert!(
        memory_bytes >= segment_bytes,
        "memory budget must fit at least one segment"
    );

    Config {
        endpoint: env_required("FJORD_GARAGE_ENDPOINT"),
        region: env_string("FJORD_GARAGE_REGION", "garage"),
        bucket: env_required("FJORD_GARAGE_BUCKET"),
        key_id: env_required("FJORD_GARAGE_KEY_ID"),
        secret: env_required("FJORD_GARAGE_SECRET"),
        total_bytes: env_u64("OLOG_SAT_TOTAL_BYTES", 8 * 1024 * 1024 * 1024),
        batch_bytes,
        segment_bytes,
        part_bytes,
        producers,
        flush_inflight,
        memory_bytes,
        linger_ms: env_u64("OLOG_SAT_LINGER_MS", 5000),
        prefix: env_string(
            "OLOG_SAT_PREFIX",
            &format!("object-log-engine-saturate/{}", std::process::id()),
        ),
        delete_after: env_bool("OLOG_SAT_DELETE", true),
        evidence_file: std::env::var("OLOG_SAT_EVIDENCE_FILE")
            .ok()
            .map(PathBuf::from),
    }
}

#[cfg(feature = "s3")]
fn make_batch(size: usize) -> Bytes {
    let mut out = BytesMut::with_capacity(size);
    let mut x = 0x517c_c1b7_2722_0a95u64;
    while out.len() < size {
        x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(size);
    out.freeze()
}

#[cfg(feature = "s3")]
fn rss_kib() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(pages * 4)
}

#[cfg(feature = "s3")]
fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(feature = "s3")]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config();
    let batches = cfg.total_bytes.div_ceil(cfg.batch_bytes as u64);
    let effective_total = batches * cfg.batch_bytes as u64;
    let base_blob: Arc<dyn BlobStore> = Arc::new(
        S3BlobStore::new(
            &cfg.endpoint,
            &cfg.region,
            &cfg.bucket,
            &cfg.key_id,
            &cfg.secret,
        )
        .with_multipart(cfg.part_bytes, cfg.part_bytes),
    );
    let counting = CountingBlobStore::new(base_blob);
    let counts = counting.counts();
    let blob: Arc<dyn BlobStore> = Arc::new(counting);
    let engine = Arc::new(LogEngine::new(
        Arc::clone(&blob),
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            max_bytes: cfg.segment_bytes,
            max_batches: usize::MAX,
            linger: Duration::from_millis(cfg.linger_ms),
            max_inflight_flushes: cfg.flush_inflight,
            max_buffered_bytes: cfg.memory_bytes,
            budget: object_log::BudgetConfig::default(),
        },
        format!("{}/data/", cfg.prefix),
    ));
    let partition = PartitionKey("single-0".to_string());
    let batch = Arc::new(make_batch(cfg.batch_bytes));
    let next = Arc::new(AtomicU64::new(0));
    let produced = Arc::new(AtomicU64::new(0));

    eprintln!(
        "logengine_saturate start: total={} batch={} segment={} part={} producers={} flush_inflight={} memory={} prefix={}",
        effective_total,
        cfg.batch_bytes,
        cfg.segment_bytes,
        cfg.part_bytes,
        cfg.producers,
        cfg.flush_inflight,
        cfg.memory_bytes,
        cfg.prefix
    );
    let started = Instant::now();
    let mut handles = Vec::with_capacity(cfg.producers);
    for _ in 0..cfg.producers {
        let engine = Arc::clone(&engine);
        let partition = partition.clone();
        let batch = Arc::clone(&batch);
        let next = Arc::clone(&next);
        let produced = Arc::clone(&produced);
        let record_count = (cfg.batch_bytes / 1024).max(1) as i32;
        handles.push(tokio::spawn(async move {
            loop {
                let n = next.fetch_add(1, Ordering::Relaxed);
                if n >= batches {
                    break;
                }
                engine
                    .produce(
                        partition.clone(),
                        Bytes::clone(&batch),
                        record_count,
                        (),
                        Durability::Sequenced,
                    )
                    .await?;
                produced.fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
            Ok::<(), ObjectLogError>(())
        }));
    }
    for handle in handles {
        handle.await??;
    }
    drop(engine);

    let elapsed = started.elapsed().as_secs_f64();
    let produced_bytes = produced.load(Ordering::Relaxed);
    let put_bytes = counts.put_bytes.load(Ordering::Relaxed);
    let put_objects = counts.put_objects.load(Ordering::Relaxed);
    let sizes = counts.sizes.lock().expect("sizes").clone();
    let avg_object_bytes = if sizes.is_empty() {
        0.0
    } else {
        sizes.iter().sum::<usize>() as f64 / sizes.len() as f64
    };
    let min_object_bytes = sizes.iter().copied().min().unwrap_or(0);
    let max_object_bytes = sizes.iter().copied().max().unwrap_or(0);
    let fill_ratio = avg_object_bytes / cfg.segment_bytes as f64;
    let bytes_per_sec = produced_bytes as f64 / elapsed.max(0.001);
    let mib_per_sec = bytes_per_sec / 1024.0 / 1024.0;
    let gbit_per_sec = bytes_per_sec * 8.0 / 1_000_000_000.0;
    let percent_target = gbit_per_sec * 100.0 / 2.5;
    let rss_kib = rss_kib().unwrap_or(0);
    eprintln!(
        "logengine_saturate done: {:.1} MiB/s {:.3} Gbit/s ({:.1}% target), objects={} avg_object={:.1}MiB fill={:.2} rss_kib={}",
        mib_per_sec,
        gbit_per_sec,
        percent_target,
        put_objects,
        avg_object_bytes / 1024.0 / 1024.0,
        fill_ratio,
        rss_kib
    );

    let summary = format!(
        concat!(
            "{{\n",
            "  \"endpoint\":\"{}\",\n",
            "  \"bucket\":\"{}\",\n",
            "  \"prefix\":\"{}\",\n",
            "  \"partition\":\"single-0\",\n",
            "  \"total_bytes\":{},\n",
            "  \"produced_bytes\":{},\n",
            "  \"put_bytes\":{},\n",
            "  \"batch_bytes\":{},\n",
            "  \"segment_bytes\":{},\n",
            "  \"part_bytes\":{},\n",
            "  \"batches\":{},\n",
            "  \"producers\":{},\n",
            "  \"flush_inflight\":{},\n",
            "  \"memory_budget_bytes\":{},\n",
            "  \"put_objects\":{},\n",
            "  \"avg_object_bytes\":{:.1},\n",
            "  \"min_object_bytes\":{},\n",
            "  \"max_object_bytes\":{},\n",
            "  \"fill_ratio\":{:.4},\n",
            "  \"rss_kib\":{},\n",
            "  \"elapsed_secs\":{:.3},\n",
            "  \"bytes_per_sec\":{:.0},\n",
            "  \"mib_per_sec\":{:.1},\n",
            "  \"gbit_per_sec\":{:.3},\n",
            "  \"percent_of_2_5_gbit\":{:.1},\n",
            "  \"delete_after\":{}\n",
            "}}\n"
        ),
        json_escape(&cfg.endpoint),
        json_escape(&cfg.bucket),
        json_escape(&cfg.prefix),
        effective_total,
        produced_bytes,
        put_bytes,
        cfg.batch_bytes,
        cfg.segment_bytes,
        cfg.part_bytes,
        batches,
        cfg.producers,
        cfg.flush_inflight,
        cfg.memory_bytes,
        put_objects,
        avg_object_bytes,
        min_object_bytes,
        max_object_bytes,
        fill_ratio,
        rss_kib,
        elapsed,
        bytes_per_sec,
        mib_per_sec,
        gbit_per_sec,
        percent_target,
        cfg.delete_after
    );
    print!("{summary}");
    if let Some(path) = &cfg.evidence_file {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &summary)?;
    }

    if cfg.delete_after {
        let keys = counts.keys.lock().expect("keys").clone();
        let cleanup_started = Instant::now();
        for key in keys {
            blob.delete(&key).await?;
        }
        eprintln!(
            "logengine_saturate cleanup complete in {:.3}s",
            cleanup_started.elapsed().as_secs_f64()
        );
    }

    Ok(())
}
