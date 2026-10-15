use super::*;
use crate::native_v2::semantic_map::TestGatedStore;
use std::ops::Bound;
#[tokio::test]
async fn accepted_scan_cancellation_replays_token_and_all_credit_releases_after_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let provider = TestGatedStore::new(Arc::new(LocalBlobStore::new(directory.path())));
    let log = NativeLog::new(
        provider.clone(),
        "semantic-owner".into(),
        NativeLimits {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 4 << 20,
            version_bytes: 4096,
            binding_bytes: 4096,
            group_entries: 2,
            group_bytes: 1 << 20,
        },
    )
    .unwrap();
    let before = log.owner.resources.bytes.available();
    let work = NativeSemanticWork {
        pages: 1000,
        hydrated_bytes: 10000,
    };
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let empty = log.capture(&namespace).await.unwrap();
    let attempt = log
        .prepare_with_edits(
            &writer,
            &empty,
            &[NativeAppend {
                identity: "seed",
                payload: b"seed",
                records: 1,
            }],
            &[
                NativeSemanticEdit {
                    key: b"a",
                    value: Some(b"first"),
                },
                NativeSemanticEdit {
                    key: b"b",
                    value: Some(b"second"),
                },
            ],
            NativeEditLimits {
                edits: 2,
                input_bytes: 100,
                work,
            },
        )
        .await
        .unwrap();
    log.submit(&attempt).await.unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    let value = log
        .semantic_lookup(&cut, b"a", work)
        .await
        .unwrap()
        .unwrap();
    let scan_limits = NativeScanLimits {
        rows: 1,
        output_bytes: 100,
        work,
    };
    let page = log
        .semantic_scan(
            &cut,
            NativeSemanticRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            scan_limits,
        )
        .await
        .unwrap();
    let token = page.continuation().unwrap().clone();
    provider.arm_read();
    let owned = log.clone();
    let input = token.clone();
    let caller = tokio::spawn(async move { owned.semantic_scan_next(&input, scan_limits).await });
    tokio::time::timeout(std::time::Duration::from_secs(30), provider.entered())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
    let original_owner = provider.armed_read_owner();
    assert!(original_owner.upgrade().is_some());
    assert!(!provider.armed_read_completed());
    provider.release();
    let replay = log.semantic_scan_next(&token, scan_limits).await.unwrap();
    let again = log.semantic_scan_next(&token, scan_limits).await.unwrap();
    assert_eq!(replay.rows()[0].key(), b"b");
    assert_eq!(replay.rows()[0].value(), b"second");
    assert_eq!(again.rows()[0].key(), b"b");
    log.shutdown().await.unwrap();
    assert!(provider.armed_read_completed());
    assert!(original_owner.upgrade().is_none());
    assert_eq!(value.bytes(), b"first");
    assert_eq!(page.rows()[0].key(), b"a");
    drop((namespace, writer, empty, cut, attempt, page, replay, again));
    let retained = log.owner.resources.bytes.available();
    assert!(retained < before);
    drop(value);
    assert!(log.owner.resources.bytes.available() > retained);
    drop(token);
    assert_eq!(log.owner.resources.bytes.available(), before);
}
