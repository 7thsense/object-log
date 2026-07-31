//! End-to-end smoke for the `object-log` diagnostics binary (feature `cli`).
#![cfg(feature = "cli")]

use bytes::Bytes;
use object_log::{
    BlobStore, Durability, FlushConfig, LocalBlobStore, LogEngine, ManifestSequencer, PartitionKey,
};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

fn bin() -> PathBuf {
    // cargo sets CARGO_BIN_EXE_<name> for integration tests when the bin is built.
    PathBuf::from(env!("CARGO_BIN_EXE_object-log"))
}

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.into())
}

#[tokio::test]
async fn cli_list_inspect_fetch_against_local_store() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(root));
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "_manifest/")
            .await
            .unwrap(),
    );
    let engine = LogEngine::new(
        Arc::clone(&blob),
        Arc::clone(&seq),
        FlushConfig {
            max_batches: 1,
            linger: Duration::from_secs(3600),
            budget: object_log::BudgetConfig {
                enabled: false,
                ..Default::default()
            },
            ..FlushConfig::default()
        },
        "log/",
    );
    engine
        .produce(
            pk("events-0"),
            Bytes::from_static(b"hello"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    drop(engine);
    drop(seq);

    let list = Command::new(bin())
        .args(["list", "--root"])
        .arg(root)
        .args(["--prefix", "log/"])
        .output()
        .expect("run list");
    assert!(list.status.success(), "list stderr={}", String::from_utf8_lossy(&list.stderr));
    let list_out = String::from_utf8_lossy(&list.stdout);
    assert!(list_out.contains("log/"), "list stdout={list_out}");

    let inspect = Command::new(bin())
        .args(["inspect", "--root"])
        .arg(root)
        .args(["--manifest-prefix", "_manifest/", "--json", "--summary"])
        .output()
        .expect("run inspect");
    assert!(
        inspect.status.success(),
        "inspect stderr={}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    let body = String::from_utf8_lossy(&inspect.stdout);
    assert!(body.contains("events-0"), "inspect={body}");
    assert!(body.contains("\"high_watermark\": 1"), "inspect={body}");

    let fetch = Command::new(bin())
        .args(["fetch", "--root"])
        .arg(root)
        .args([
            "--data-prefix",
            "log/",
            "--manifest-prefix",
            "_manifest/",
            "--partition",
            "events-0",
            "--text",
        ])
        .output()
        .expect("run fetch");
    assert!(
        fetch.status.success(),
        "fetch stderr={}",
        String::from_utf8_lossy(&fetch.stderr)
    );
    let fetch_out = String::from_utf8_lossy(&fetch.stdout);
    assert!(fetch_out.contains("hello"), "fetch={fetch_out}");

    let orphans = Command::new(bin())
        .args(["orphans", "--root"])
        .arg(root)
        .args([
            "--data-prefix",
            "log/",
            "--manifest-prefix",
            "_manifest/",
        ])
        .output()
        .expect("run orphans");
    assert!(
        orphans.status.success(),
        "orphans stderr={}",
        String::from_utf8_lossy(&orphans.stderr)
    );
    let oerr = String::from_utf8_lossy(&orphans.stderr);
    assert!(
        oerr.contains("no orphans") || oerr.contains("orphan"),
        "orphans stderr={oerr}"
    );
}
