use super::*;
use crate::{BlobStore, LocalBlobStore};

fn head() -> NativeHead {
    NativeHead {
        namespace: [11; 32],
        incarnation: [17; 32],
        epoch: 7,
        log_start: 0,
        next_offset: 9,
        index_root: [23; 32],
        semantic_anchor: [29; 32],
    }
}
#[tokio::test]
async fn bounded_local_head_acquires_exact_roots_and_rejects_missing_legacy_and_scope() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalBlobStore::new(directory.path()));
    let initial = head();
    store
        .put("native/head", Bytes::copy_from_slice(&initial.encode()))
        .await
        .unwrap();
    let reader = NativeHeadReader::new_known(
        store.clone(),
        "native/head".into(),
        initial.namespace,
        initial.incarnation,
    )
    .unwrap();
    assert_eq!(reader.read().await.unwrap(), initial);
    assert_eq!(initial.bounds(), (0, 9));
    assert_eq!(initial.index_root(), &[23; 32]);
    assert_eq!(initial.semantic_anchor(), &[29; 32]);
    let next = reader.acquire().await.unwrap();
    assert_eq!(
        next,
        NativeHead {
            epoch: 8,
            ..initial
        }
    );
    let reopened = Arc::new(LocalBlobStore::new(directory.path()));
    assert_eq!(
        reopened
            .read_metadata("native/head", HEAD_BYTES, Arc::new(()))
            .await
            .unwrap()
            .bytes()
            .unwrap(),
        &next.encode()
    );
    for (key, namespace, incarnation) in [
        ("native/missing", initial.namespace, initial.incarnation),
        ("native/head", [12; 32], initial.incarnation),
        ("native/head", initial.namespace, [18; 32]),
    ] {
        let bad =
            NativeHeadReader::new_known(store.clone(), key.into(), namespace, incarnation).unwrap();
        assert!(bad.acquire().await.is_err());
        bad.close_and_drain().await;
    }
    store
        .put(
            "legacy",
            Bytes::from_static(b"{\"epoch\":7,\"entries\":[]}"),
        )
        .await
        .unwrap();
    let legacy = NativeHeadReader::new_known(
        store.clone(),
        "legacy".into(),
        initial.namespace,
        initial.incarnation,
    )
    .unwrap();
    assert!(legacy.acquire().await.is_err());
    assert_eq!(
        store.get("legacy").await.unwrap().unwrap().as_ref(),
        b"{\"epoch\":7,\"entries\":[]}"
    );
    legacy.close_and_drain().await;
    let exhausted = NativeHead {
        epoch: u64::MAX,
        ..initial
    };
    store
        .put("native/head", Bytes::copy_from_slice(&exhausted.encode()))
        .await
        .unwrap();
    store.take_media_op_stats();
    assert_eq!(reader.read().await.unwrap(), exhausted);
    assert!(reader.acquire().await.is_err());
    assert_eq!(store.take_media_op_stats().unwrap().media_ops, 0);
    reader.close_and_drain().await;
    assert!(reader.read().await.is_err());
}
#[tokio::test]
async fn independent_local_fencers_compare_observed_versions_and_preserve_roots() {
    let directory = tempfile::tempdir().unwrap();
    let initial = head();
    let first = Arc::new(LocalBlobStore::new(directory.path()));
    first
        .put("head", Bytes::copy_from_slice(&initial.encode()))
        .await
        .unwrap();
    let second = Arc::new(LocalBlobStore::new(directory.path()));
    let a =
        NativeHeadReader::new_known(first, "head".into(), initial.namespace, initial.incarnation)
            .unwrap();
    let b = NativeHeadReader::new_known(
        second,
        "head".into(),
        initial.namespace,
        initial.incarnation,
    )
    .unwrap();
    let (one, two) = tokio::join!(a.acquire(), b.acquire());
    let one = one.unwrap();
    let two = two.unwrap();
    assert_eq!([one.epoch.min(two.epoch), one.epoch.max(two.epoch)], [8, 9]);
    assert_eq!(one.index_root, initial.index_root);
    assert_eq!(two.index_root, initial.index_root);
    assert_eq!(one.semantic_anchor, initial.semantic_anchor);
    assert_eq!(two.semantic_anchor, initial.semantic_anchor);
    a.close_and_drain().await;
    b.close_and_drain().await;
}
#[tokio::test]
async fn local_metadata_exact_budget_conflict_and_token_scope_have_zero_rejected_effects() {
    let directory = tempfile::tempdir().unwrap();
    let first = LocalBlobStore::new(directory.path());
    let second = LocalBlobStore::new(directory.path());
    first
        .put("head", Bytes::from_static(b"abcd"))
        .await
        .unwrap();
    let observed = first.read_metadata("head", 4, Arc::new(())).await.unwrap();
    assert_eq!(observed.bytes().unwrap(), b"abcd");
    assert!(first.read_metadata("head", 3, Arc::new(())).await.is_err());
    assert!(
        first
            .swap_metadata(
                "other",
                observed.clone(),
                Bytes::from_static(b"next"),
                4,
                Arc::new(())
            )
            .await
            .is_err()
    );
    assert!(
        second
            .swap_metadata(
                "head",
                observed.clone(),
                Bytes::from_static(b"next"),
                4,
                Arc::new(())
            )
            .await
            .is_err()
    );
    assert!(
        first
            .swap_metadata(
                "head",
                observed.clone(),
                Bytes::from_static(b"next!"),
                4,
                Arc::new(())
            )
            .await
            .is_err()
    );
    first
        .put("head", Bytes::from_static(b"abcd"))
        .await
        .unwrap();
    // Equal replacement bytes still have a new local file version.
    assert!(matches!(
        first
            .swap_metadata(
                "head",
                observed,
                Bytes::from_static(b"next"),
                4,
                Arc::new(())
            )
            .await
            .unwrap(),
        MetadataSwap::Conflict(_)
    ));
    let absent = first
        .read_metadata("absent", 4, Arc::new(()))
        .await
        .unwrap();
    assert!(matches!(
        first
            .swap_metadata(
                "absent",
                absent,
                Bytes::from_static(b"seed"),
                4,
                Arc::new(())
            )
            .await
            .unwrap(),
        MetadataSwap::Stored
    ));
    let old = first.read_metadata("head", 4, Arc::new(())).await.unwrap();
    first
        .put("head", Bytes::from_static(b"oversized"))
        .await
        .unwrap();
    assert!(
        first
            .swap_metadata("head", old, Bytes::from_static(b"next"), 4, Arc::new(()))
            .await
            .is_err()
    );
    assert_eq!(
        first.get("head").await.unwrap().unwrap().as_ref(),
        b"oversized"
    );
}
#[test]
fn head_codec_has_independent_fixed_layout_and_rejects_unchecked_variations() {
    let initial = head();
    let encoded = initial.encode();
    assert_eq!(encoded.len(), 192);
    assert_eq!(&encoded[..8], b"ONH2\0\0\0\x01");
    assert_eq!(&encoded[72..80], &[0, 0, 0, 0, 0, 0, 0, 7]);
    assert_eq!(&encoded[88..96], &[0, 0, 0, 0, 0, 0, 0, 9]);
    assert_eq!(
        &encoded[160..],
        &[
            21, 3, 216, 6, 231, 182, 47, 234, 170, 48, 151, 58, 245, 210, 137, 193, 136, 93, 72,
            49, 30, 174, 177, 99, 166, 39, 199, 72, 171, 216, 5, 5
        ]
    );
    assert_eq!(
        NativeHead::decode(&encoded, &[11; 32], &[17; 32]).unwrap(),
        initial
    );
    for position in [0, 8, 40, 72, 80, 88, 96, 128, 160, 191] {
        let mut wrong = encoded;
        wrong[position] ^= 1;
        assert!(NativeHead::decode(&wrong, &[11; 32], &[17; 32]).is_err());
    }
    assert!(NativeHead::decode(&encoded[..191], &[11; 32], &[17; 32]).is_err());
    let mut extra = encoded.to_vec();
    extra.push(0);
    assert!(NativeHead::decode(&extra, &[11; 32], &[17; 32]).is_err());
    for wrong in [
        NativeHead {
            next_offset: i64::MAX as u64 + 1,
            ..initial
        },
        NativeHead {
            log_start: 10,
            ..initial
        },
        NativeHead {
            index_root: [0; 32],
            ..initial
        },
        NativeHead {
            semantic_anchor: [0; 32],
            ..initial
        },
    ] {
        assert!(NativeHead::decode(&wrong.encode(), &[11; 32], &[17; 32]).is_err());
    }
}

#[cfg(feature = "s3")]
mod s3;

#[test]
fn local_physical_write_owns_admission_after_caller_runtime_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalBlobStore::new(directory.path()));
    let initial = head();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(store.put("head", Bytes::copy_from_slice(&initial.encode())))
        .unwrap();
    let (started, release) = store.pause_metadata_swap(false);
    let reader = NativeHeadReader::new_known(
        store.clone(),
        "head".into(),
        initial.namespace,
        initial.incarnation,
    )
    .unwrap();
    let owned = reader.clone();
    let caller = runtime.spawn(async move { owned.acquire().await });
    runtime
        .block_on(async {
            tokio::task::spawn_blocking(move || {
                started
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap()
            })
            .await
        })
        .unwrap();
    assert_eq!(reader.owner.lifecycle.lock().unwrap().owned, 1);
    assert_eq!(reader.owner.gate.available_permits(), 0);
    // Drops the async wrappers while the actual flock/fsync closure is paused.
    caller.abort();
    runtime.shutdown_timeout(std::time::Duration::ZERO);
    assert_eq!(reader.owner.lifecycle.lock().unwrap().owned, 1);
    assert_eq!(reader.owner.gate.available_permits(), 0);
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    observer.block_on(async {
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                reader.close_and_drain()
            )
            .await
            .is_err()
        );
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), reader.close_and_drain())
            .await
            .unwrap();
        assert_eq!(reader.owner.lifecycle.lock().unwrap().owned, 0);
        assert_eq!(reader.owner.gate.available_permits(), 1);
        let raw = store.get("head").await.unwrap().unwrap();
        assert_eq!(
            NativeHead::decode(&raw, &initial.namespace, &initial.incarnation)
                .unwrap()
                .epoch(),
            8
        );
    });
}

#[tokio::test]
async fn local_physical_panic_drains_unlocks_and_preserves_previous_head() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalBlobStore::new(directory.path()));
    let initial = head();
    store
        .put("head", Bytes::copy_from_slice(&initial.encode()))
        .await
        .unwrap();
    let (started, release) = store.pause_metadata_swap(true);
    let reader = NativeHeadReader::new_known(
        store.clone(),
        "head".into(),
        initial.namespace,
        initial.incarnation,
    )
    .unwrap();
    let owned = reader.clone();
    let caller = tokio::spawn(async move { owned.acquire().await });
    tokio::task::spawn_blocking(move || {
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    assert!(caller.await.unwrap().is_err());
    assert_eq!(reader.owner.lifecycle.lock().unwrap().owned, 0);
    assert_eq!(reader.owner.gate.available_permits(), 1);
    assert_eq!(reader.read().await.unwrap(), initial);
    // The actual flock and failed worker must permit a subsequent acquisition.
    assert_eq!(reader.acquire().await.unwrap().epoch(), 8);
    reader.close_and_drain().await;
}

#[test]
fn metadata_snapshot_aliases_release_payload_before_physical_owner() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Payload(Arc<AtomicBool>);
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            b"owned"
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct Lifetime(Arc<AtomicBool>);
    impl Drop for Lifetime {
        fn drop(&mut self) {
            assert!(
                self.0.load(Ordering::SeqCst),
                "physical owner released before payload"
            );
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let lifetime = Arc::new(Lifetime(dropped.clone()));
    let weak = Arc::downgrade(&lifetime);
    let value = MetadataSnapshot {
        owner: Arc::new(()),
        key: "head".into(),
        bytes: Some(Bytes::from_owner(Payload(dropped.clone()))),
        version: crate::blob::authority::MetadataVersion::Local(None),
        _physical: lifetime,
    };
    let payload: &[u8] = value.bytes().unwrap();
    assert_eq!(payload, b"owned");
    let alias = value.clone();
    drop(value);
    assert!(weak.upgrade().is_some());
    assert!(!dropped.load(Ordering::SeqCst));
    drop(alias);
    assert!(weak.upgrade().is_none());
    assert!(dropped.load(Ordering::SeqCst));
}
