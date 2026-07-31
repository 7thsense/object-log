//! Diagnostics CLI for object-log stores (local filesystem, optional S3).
//!
//! ```text
//! cargo run --features cli --bin object-log -- --help
//! cargo run --features cli --bin object-log -- list --root /path/to/store --prefix log/
//! cargo run --features cli,s3 --bin object-log -- inspect --s3-endpoint … --s3-bucket …
//! ```

use clap::{Parser, Subcommand};
use object_log::{
    BlobStore, FlushConfig, LocalBlobStore, LogEngine, ManifestSequencer, PartitionKey,
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "object-log",
    about = "Inspect object-log data and ManifestSequencer indexes",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List object keys under a prefix.
    List {
        #[command(flatten)]
        store: StoreArgs,
        /// Key prefix to list.
        #[arg(long, default_value = "")]
        prefix: String,
    },
    /// Print the rebuilt ManifestSequencer index (partitions, bounds, entries).
    Inspect {
        #[command(flatten)]
        store: StoreArgs,
        /// Manifest object prefix (must match the writer).
        #[arg(long, default_value = "_manifest/")]
        manifest_prefix: String,
        /// Emit JSON instead of a human table.
        #[arg(long)]
        json: bool,
        /// Omit per-entry rows (summary only).
        #[arg(long)]
        summary: bool,
    },
    /// Find (and optionally delete) data objects not referenced by the index.
    Orphans {
        #[command(flatten)]
        store: StoreArgs,
        /// Data object prefix used by LogEngine.
        #[arg(long, default_value = "log/")]
        data_prefix: String,
        /// Manifest object prefix used by ManifestSequencer.
        #[arg(long, default_value = "_manifest/")]
        manifest_prefix: String,
        /// Actually delete orphans (default is dry-run).
        #[arg(long)]
        delete: bool,
    },
    /// Fetch batches for a partition via LogEngine + ManifestSequencer.
    Fetch {
        #[command(flatten)]
        store: StoreArgs,
        #[arg(long, default_value = "log/")]
        data_prefix: String,
        #[arg(long, default_value = "_manifest/")]
        manifest_prefix: String,
        /// Partition key string.
        #[arg(long)]
        partition: String,
        /// Inclusive start offset.
        #[arg(long, default_value_t = 0)]
        offset: i64,
        /// Byte budget for one fetch call.
        #[arg(long, default_value_t = 1 << 20)]
        max_bytes: usize,
        /// Print payload as UTF-8 (lossy); default is length + hex preview.
        #[arg(long)]
        text: bool,
    },
}

#[derive(clap::Args, Debug)]
struct StoreArgs {
    /// Local directory root for LocalBlobStore.
    #[arg(long, conflicts_with_all = ["s3_endpoint", "s3_bucket"])]
    root: Option<PathBuf>,

    /// S3-compatible endpoint URL (requires a build with `--features cli,s3`).
    #[arg(long)]
    s3_endpoint: Option<String>,
    #[arg(long)]
    s3_bucket: Option<String>,
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, env = "OBJECT_LOG_S3_KEY_ID")]
    s3_key_id: Option<String>,
    #[arg(long, env = "OBJECT_LOG_S3_SECRET")]
    s3_secret: Option<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async move {
        match cli.command {
            Command::List { store, prefix } => cmd_list(store, prefix).await,
            Command::Inspect {
                store,
                manifest_prefix,
                json,
                summary,
            } => cmd_inspect(store, manifest_prefix, json, summary).await,
            Command::Orphans {
                store,
                data_prefix,
                manifest_prefix,
                delete,
            } => cmd_orphans(store, data_prefix, manifest_prefix, delete).await,
            Command::Fetch {
                store,
                data_prefix,
                manifest_prefix,
                partition,
                offset,
                max_bytes,
                text,
            } => {
                cmd_fetch(
                    store,
                    data_prefix,
                    manifest_prefix,
                    partition,
                    offset,
                    max_bytes,
                    text,
                )
                .await
            }
        }
    })
}

async fn open_store(args: &StoreArgs) -> Result<Arc<dyn BlobStore>, String> {
    if let Some(root) = &args.root {
        std::fs::create_dir_all(root).map_err(|e| format!("create root: {e}"))?;
        return Ok(Arc::new(LocalBlobStore::new(root)));
    }

    #[cfg(feature = "s3")]
    {
        let endpoint = args
            .s3_endpoint
            .as_deref()
            .ok_or("provide --root or --s3-endpoint")?;
        let bucket = args
            .s3_bucket
            .as_deref()
            .ok_or("--s3-bucket is required with --s3-endpoint")?;
        let key_id = args
            .s3_key_id
            .as_deref()
            .ok_or("--s3-key-id or OBJECT_LOG_S3_KEY_ID is required")?;
        let secret = args
            .s3_secret
            .as_deref()
            .ok_or("--s3-secret or OBJECT_LOG_S3_SECRET is required")?;
        Ok(Arc::new(object_log::S3BlobStore::new(
            endpoint,
            &args.s3_region,
            bucket,
            key_id,
            secret,
        )))
    }

    #[cfg(not(feature = "s3"))]
    {
        let _ = (
            &args.s3_endpoint,
            &args.s3_bucket,
            &args.s3_key_id,
            &args.s3_secret,
            &args.s3_region,
        );
        Err("provide --root DIR (or rebuild with --features cli,s3 for S3 stores)".into())
    }
}

async fn cmd_list(store: StoreArgs, prefix: String) -> Result<(), String> {
    let blob = open_store(&store).await?;
    let mut keys = blob.list(&prefix).await.map_err(|e| e.to_string())?;
    keys.sort();
    for k in &keys {
        println!("{k}");
    }
    eprintln!("# {} key(s)", keys.len());
    Ok(())
}

async fn cmd_inspect(
    store: StoreArgs,
    manifest_prefix: String,
    json: bool,
    summary: bool,
) -> Result<(), String> {
    let blob = open_store(&store).await?;
    let seq = ManifestSequencer::open(blob, manifest_prefix)
        .await
        .map_err(|e| e.to_string())?;
    let mut snap = seq.snapshot();
    if summary {
        for p in &mut snap.partitions {
            p.entries.clear();
        }
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&snap).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    println!(
        "manifest_prefix={}  manifests={}  partitions={}",
        snap.manifest_prefix,
        snap.manifest_count,
        snap.partitions.len()
    );
    for p in &snap.partitions {
        println!(
            "  partition={:?}  log_start={}  hwm={}  entries={}",
            p.partition, p.log_start, p.high_watermark, p.entry_count
        );
        if !summary {
            for e in &p.entries {
                println!(
                    "    base={} count={} object={} range={}+{}",
                    e.base_offset,
                    e.record_count,
                    e.location.object_id,
                    e.location.byte_start,
                    e.location.byte_len
                );
            }
        }
    }
    Ok(())
}

async fn cmd_orphans(
    store: StoreArgs,
    data_prefix: String,
    manifest_prefix: String,
    delete: bool,
) -> Result<(), String> {
    let blob = open_store(&store).await?;
    let seq = ManifestSequencer::open(Arc::clone(&blob), manifest_prefix)
        .await
        .map_err(|e| e.to_string())?;
    let live = seq.live_object_ids();
    let mut keys = blob.list(&data_prefix).await.map_err(|e| e.to_string())?;
    keys.sort();
    let mut orphans = Vec::new();
    for k in keys {
        if !live.contains(&k) {
            orphans.push(k);
        }
    }
    if orphans.is_empty() {
        eprintln!("no orphans under {data_prefix:?} (live={})", live.len());
        return Ok(());
    }
    for k in &orphans {
        println!("{k}");
    }
    eprintln!(
        "# {} orphan(s); live_index_objects={}",
        orphans.len(),
        live.len()
    );
    if delete {
        for k in &orphans {
            blob.delete(k).await.map_err(|e| e.to_string())?;
        }
        eprintln!("# deleted {}", orphans.len());
    } else {
        eprintln!("# dry-run only; pass --delete to remove");
    }
    Ok(())
}

async fn cmd_fetch(
    store: StoreArgs,
    data_prefix: String,
    manifest_prefix: String,
    partition: String,
    offset: i64,
    max_bytes: usize,
    text: bool,
) -> Result<(), String> {
    let blob = open_store(&store).await?;
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), manifest_prefix)
            .await
            .map_err(|e| e.to_string())?,
    );
    let engine = LogEngine::new(
        blob,
        seq,
        FlushConfig {
            budget: object_log::BudgetConfig {
                enabled: false,
                ..Default::default()
            },
            ..FlushConfig::default()
        },
        data_prefix,
    );
    let p = PartitionKey(partition);
    let batches = engine
        .fetch(&p, offset, max_bytes)
        .await
        .map_err(|e| e.to_string())?;
    if batches.is_empty() {
        eprintln!("# no batches at/after offset {offset}");
        return Ok(());
    }
    for b in &batches {
        if text {
            println!(
                "offset={} count={} payload={}",
                b.base_offset,
                b.record_count,
                String::from_utf8_lossy(&b.payload)
            );
        } else {
            let preview: String = b
                .payload
                .iter()
                .take(16)
                .map(|x| format!("{x:02x}"))
                .collect::<Vec<_>>()
                .join("");
            let more = if b.payload.len() > 16 { "…" } else { "" };
            println!(
                "offset={} count={} bytes={} hex={preview}{more}",
                b.base_offset,
                b.record_count,
                b.payload.len()
            );
        }
    }
    eprintln!("# {} batch(es)", batches.len());
    Ok(())
}
