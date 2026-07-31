#![cfg(feature = "s3")]
//! Live S3-compatible adapter tests. Runs only when credentials + endpoint are
//! configured; otherwise skips and passes so default CI stays hermetic.
//!
//! Preferred env (object-log names):
//!
//! ```text
//! OBJECT_LOG_S3_ENDPOINT=http://127.0.0.1:19000 \
//! OBJECT_LOG_S3_BUCKET=object-log-test \
//! OBJECT_LOG_S3_KEY_ID=minioadmin \
//! OBJECT_LOG_S3_SECRET=minioadmin \
//! OBJECT_LOG_S3_REGION=us-east-1 \
//!   cargo test --features s3 --test s3 -- --nocapture
//! ```
//!
//! Legacy aliases: `FJORD_GARAGE_ENDPOINT`, `FJORD_GARAGE_BUCKET`,
//! `FJORD_GARAGE_KEY_ID`, `FJORD_GARAGE_SECRET`, `FJORD_GARAGE_REGION`.

use bytes::Bytes;
use object_log::{BlobStore, S3BlobStore};

struct S3Env {
    endpoint: String,
    bucket: String,
    key_id: String,
    secret: String,
    region: String,
}

fn env_first(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

fn load_env() -> Option<S3Env> {
    let endpoint = env_first(&["OBJECT_LOG_S3_ENDPOINT", "FJORD_GARAGE_ENDPOINT"])?;
    let bucket = env_first(&["OBJECT_LOG_S3_BUCKET", "FJORD_GARAGE_BUCKET"])?;
    let key_id = env_first(&["OBJECT_LOG_S3_KEY_ID", "FJORD_GARAGE_KEY_ID"])?;
    let secret = env_first(&["OBJECT_LOG_S3_SECRET", "FJORD_GARAGE_SECRET"])?;
    let region = env_first(&["OBJECT_LOG_S3_REGION", "FJORD_GARAGE_REGION"])
        .unwrap_or_else(|| "us-east-1".into());
    Some(S3Env {
        endpoint,
        bucket,
        key_id,
        secret,
        region,
    })
}

fn store(env: &S3Env) -> S3BlobStore {
    S3BlobStore::new(
        &env.endpoint,
        &env.region,
        &env.bucket,
        &env.key_id,
        &env.secret,
    )
}

fn skip() {
    eprintln!(
        "OBJECT_LOG_S3_* (or FJORD_GARAGE_*) not fully set — skipping live S3 tests"
    );
}

#[tokio::test]
async fn s3_blob_store_round_trip() {
    let Some(env) = load_env() else {
        skip();
        return;
    };
    let store = store(&env);

    let key = format!("object-log-test/{}-roundtrip", std::process::id());
    store
        .put(&key, Bytes::from_static(b"hello world"))
        .await
        .unwrap();
    assert_eq!(
        store.get(&key).await.unwrap().unwrap(),
        Bytes::from_static(b"hello world")
    );
    assert_eq!(
        store.get_range(&key, 6..11).await.unwrap().unwrap(),
        Bytes::from_static(b"world")
    );
    assert!(
        store
            .list("object-log-test/")
            .await
            .unwrap()
            .iter()
            .any(|k| k == &key)
    );
    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.unwrap().is_none());
}

/// Large put forces multipart when threshold is lowered below payload size.
#[tokio::test]
async fn s3_multipart_put_get_range_round_trip() {
    let Some(env) = load_env() else {
        skip();
        return;
    };
    // 6 MiB payload; threshold 1 MiB, part size 5 MiB (S3 minimum).
    let store = store(&env).with_multipart(1024 * 1024, 5 * 1024 * 1024);
    let key = format!("object-log-test/{}-multipart", std::process::id());

    let mut payload = vec![0u8; 6 * 1024 * 1024];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let payload = Bytes::from(payload);

    store.put(&key, payload.clone()).await.unwrap();
    let got = store.get(&key).await.unwrap().unwrap();
    assert_eq!(got.len(), payload.len());
    assert_eq!(got, payload);

    let mid = store
        .get_range(&key, 1000..1000 + 64)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mid, payload.slice(1000..1000 + 64));

    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.unwrap().is_none());
}
