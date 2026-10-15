use super::*;
use crate::native_v2::semantic_map::TestGatedStore;
use std::ops::Bound;
#[tokio::test]
async fn accepted_scan_cancellation_replays_token_and_all_credit_releases_after_shutdown() {
    scan_cancellation_contract(false).await;
}
#[tokio::test]
async fn accepted_scan_survives_caller_runtime_destruction_with_external_credit() {
    scan_cancellation_contract(true).await;
}
async fn scan_cancellation_contract(destroy_runtime: bool) {
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
    let external_credit = crate::native_v2::page_store::ByteBudget::new(8192).unwrap();
    let external = external_credit.charge(4096).unwrap();
    let external_weak = Arc::downgrade(&external);
    let owned = log.with_operation_owner(external);
    assert!(log.external_owner.is_none());
    assert!(Arc::ptr_eq(&log.owner, &owned.owner));
    let input = token.clone();
    provider.arm_read();
    if destroy_runtime {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let (gone_tx, gone_rx) = tokio::sync::oneshot::channel();
        let caller = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                tokio::spawn(async move { owned.semantic_scan_next(&input, scan_limits).await });
                stop_rx.await.unwrap();
            });
            drop(runtime);
            gone_tx.send(()).unwrap();
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), provider.entered())
            .await
            .unwrap();
        stop_tx.send(()).unwrap();
        gone_rx.await.unwrap();
        caller.join().unwrap();
    } else {
        let caller =
            tokio::spawn(async move { owned.semantic_scan_next(&input, scan_limits).await });
        tokio::time::timeout(std::time::Duration::from_secs(30), provider.entered())
            .await
            .unwrap();
        caller.abort();
        assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
    }
    assert_eq!(external_credit.available(), 8192 - 4096);
    assert!(external_weak.upgrade().is_some());
    let original_owner = provider.armed_read_owner();
    assert!(original_owner.upgrade().is_some());
    assert!(!provider.armed_read_completed());
    provider.release();
    let replay = log.semantic_scan_next(&token, scan_limits).await.unwrap();
    let again = log.semantic_scan_next(&token, scan_limits).await.unwrap();
    assert!(external_weak.upgrade().is_none());
    assert_eq!(external_credit.available(), 8192);
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

#[tokio::test]
async fn scoped_owner_covers_publication_and_resolution_without_funding_retained_carriers() {
    let directory = tempfile::tempdir().unwrap();
    let provider = TestGatedStore::new(Arc::new(LocalBlobStore::new(directory.path())));
    let log = NativeLog::new(
        provider.clone(),
        "publication-owner".into(),
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
    let external_credit = crate::native_v2::page_store::ByteBudget::new(8192).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    let attempt = {
        let owned = log.with_operation_owner(external_credit.charge(4096).unwrap());
        owned
            .prepare(
                &writer,
                &[NativeAppend {
                    identity: "seed",
                    payload: b"seed",
                    records: 1,
                }],
            )
            .await
            .unwrap()
    };
    // The original attempt and cut remain live, but do not retain this token.
    assert_eq!(external_credit.available(), 8192);
    let token = external_credit.charge(4096).unwrap();
    let weak = Arc::downgrade(&token);
    let owned = log.with_operation_owner(token);
    let input = attempt.clone();
    provider.arm();
    let caller = tokio::spawn(async move { owned.submit(&input).await });
    tokio::time::timeout(std::time::Duration::from_secs(30), provider.entered())
        .await
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(weak.upgrade().is_some());
    assert_eq!(external_credit.available(), 4096);
    provider.release();
    assert!(matches!(
        log.resolve(&attempt, 1).await.unwrap(),
        NativeResolution::DurableContent
    ));
    assert!(weak.upgrade().is_none());
    assert_eq!(external_credit.available(), 8192);

    let token = external_credit.charge(4096).unwrap();
    let weak = Arc::downgrade(&token);
    let owned = log.with_operation_owner(token);
    let input = attempt.clone();
    provider.arm_read();
    let caller = tokio::spawn(async move { owned.resolve(&input, 1).await });
    tokio::time::timeout(std::time::Duration::from_secs(30), provider.entered())
        .await
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(weak.upgrade().is_some());
    assert_eq!(external_credit.available(), 4096);
    provider.release();
    let _current = log.capture(&namespace).await.unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(external_credit.available(), 8192);
    assert_eq!(cut.head().bounds(), (0, 0));
    log.shutdown().await.unwrap();
}
