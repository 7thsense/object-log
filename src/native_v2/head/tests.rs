use super::*;
use crate::{BlobStore, LocalBlobStore};

#[tokio::test]
async fn local_metadata_version_limit_is_explicit_and_checked_before_io() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let exact = MetadataLimits {
        payload_bytes: 4,
        version_bytes: 56,
    };
    let short = MetadataLimits {
        version_bytes: 55,
        ..exact
    };
    assert!(
        store
            .read_metadata("missing", short, metadata_owners(Arc::new(())))
            .await
            .is_err()
    );
    assert!(store.list("").await.unwrap().is_empty());
    let absent = store
        .read_metadata("head", exact, metadata_owners(Arc::new(())))
        .await
        .unwrap();
    assert!(
        store
            .swap_metadata(
                "head",
                absent.clone(),
                Bytes::from_static(b"data"),
                short,
                metadata_owners(Arc::new(()))
            )
            .await
            .is_err()
    );
    assert!(store.get("head").await.unwrap().is_none());
    assert!(matches!(
        store
            .swap_metadata(
                "head",
                absent,
                Bytes::from_static(b"data"),
                exact,
                metadata_owners(Arc::new(()))
            )
            .await
            .unwrap(),
        MetadataSwap::Stored
    ));
    assert_eq!(
        store
            .read_metadata("head", exact, metadata_owners(Arc::new(())))
            .await
            .unwrap()
            .bytes(),
        Some(&b"data"[..])
    );
}

fn head() -> NativeHead {
    NativeHead {
        namespace: [11; 32],
        incarnation: [17; 32],
        epoch: 7,
        log_start: 0,
        next_offset: 9,
        index_root: Some(Descriptor {
            hash: [23; 32],
            encoded_len: 240,
            kind: Kind::Leaf,
            height: 0,
            start: 0,
            end: 9,
            entries: 1,
        }),
        semantic_anchor: SemanticCoverage::Covered {
            through: 9,
            root: [29; 32],
        },
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
    assert_eq!(initial.index_root().unwrap().hash, [23; 32]);
    assert_eq!(
        initial.semantic_anchor(),
        SemanticCoverage::Covered {
            through: 9,
            root: [29; 32]
        }
    );
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
            .read_metadata(
                "native/head",
                metadata_limits(HEAD_BYTES),
                metadata_owners(Arc::new(()))
            )
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
    let observed = first
        .read_metadata("head", metadata_limits(4), metadata_owners(Arc::new(())))
        .await
        .unwrap();
    assert_eq!(observed.bytes().unwrap(), b"abcd");
    assert!(
        first
            .read_metadata("head", metadata_limits(3), metadata_owners(Arc::new(())))
            .await
            .is_err()
    );
    assert!(
        first
            .swap_metadata(
                "other",
                observed.clone(),
                Bytes::from_static(b"next"),
                metadata_limits(4),
                metadata_owners(Arc::new(()))
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
                metadata_limits(4),
                metadata_owners(Arc::new(()))
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
                metadata_limits(4),
                metadata_owners(Arc::new(()))
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
                metadata_limits(4),
                metadata_owners(Arc::new(()))
            )
            .await
            .unwrap(),
        MetadataSwap::Conflict(_)
    ));
    let absent = first
        .read_metadata("absent", metadata_limits(4), metadata_owners(Arc::new(())))
        .await
        .unwrap();
    assert!(matches!(
        first
            .swap_metadata(
                "absent",
                absent,
                Bytes::from_static(b"seed"),
                metadata_limits(4),
                metadata_owners(Arc::new(()))
            )
            .await
            .unwrap(),
        MetadataSwap::Stored
    ));
    let old = first
        .read_metadata("head", metadata_limits(4), metadata_owners(Arc::new(())))
        .await
        .unwrap();
    first
        .put("head", Bytes::from_static(b"oversized"))
        .await
        .unwrap();
    assert!(
        first
            .swap_metadata(
                "head",
                old,
                Bytes::from_static(b"next"),
                metadata_limits(4),
                metadata_owners(Arc::new(()))
            )
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
    assert_eq!(encoded.len(), 240);
    assert_eq!(&encoded[..8], b"ONH2\0\0\0\x02");
    assert_eq!(&encoded[72..80], &[0, 0, 0, 0, 0, 0, 0, 7]);
    assert_eq!(&encoded[88..96], &[0, 0, 0, 0, 0, 0, 0, 9]);
    assert_eq!(
        &encoded[208..],
        &[
            145, 201, 57, 125, 147, 107, 51, 171, 197, 124, 129, 9, 148, 39, 177, 135, 224, 208,
            244, 1, 194, 111, 219, 189, 245, 2, 205, 198, 37, 14, 45, 225
        ]
    );
    assert_eq!(
        NativeHead::decode(&encoded, &[11; 32], &[17; 32]).unwrap(),
        initial
    );
    for position in [0, 8, 40, 72, 80, 88, 96, 128, 160, 208, 239] {
        let mut wrong = encoded;
        wrong[position] ^= 1;
        assert!(NativeHead::decode(&wrong, &[11; 32], &[17; 32]).is_err());
    }
    assert!(NativeHead::decode(&encoded[..239], &[11; 32], &[17; 32]).is_err());
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
            index_root: None,
            ..initial
        },
        NativeHead {
            semantic_anchor: SemanticCoverage::Covered {
                through: 10,
                root: [29; 32],
            },
            ..initial
        },
    ] {
        assert!(NativeHead::decode(&wrong.encode(), &[11; 32], &[17; 32]).is_err());
    }
}

#[test]
fn head_coverage_and_descriptor_reject_recommitted_invalid_fields() {
    let initial = head();
    for (position, value) in [(160, 2), (161, 1), (175, 10), (135, 1), (151, 8)] {
        let mut raw = initial.encode();
        raw[position] = value;
        let checksum = Sha256::digest(&raw[..208]);
        raw[208..].copy_from_slice(&checksum);
        assert!(NativeHead::decode(&raw, &[11; 32], &[17; 32]).is_err());
    }
    let empty = NativeHead {
        next_offset: 0,
        index_root: None,
        semantic_anchor: SemanticCoverage::Uninitialized,
        ..initial
    };
    assert_eq!(
        NativeHead::decode(&empty.encode(), &[11; 32], &[17; 32]).unwrap(),
        empty
    );
    let uninitialized = NativeHead {
        semantic_anchor: SemanticCoverage::Uninitialized,
        ..initial
    };
    assert_eq!(
        NativeHead::decode(&uninitialized.encode(), &[11; 32], &[17; 32]).unwrap(),
        uninitialized
    );
    let mut mixed = uninitialized.encode();
    mixed[176] = 1;
    let checksum = Sha256::digest(&mixed[..208]);
    mixed[208..].copy_from_slice(&checksum);
    assert!(NativeHead::decode(&mixed, &[11; 32], &[17; 32]).is_err());
    assert!(NativeHead::decode(&[0; 192], &[11; 32], &[17; 32]).is_err());
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
        inner: Arc::new(crate::blob::authority::MetadataObservation {
            owner: Arc::new(()),
            key: "head".into(),
            bytes: Some(Bytes::from_owner(Payload(dropped.clone()))),
            version: crate::blob::authority::MetadataVersion::Local(None),
            _retained: lifetime,
        }),
    };
    let payload: &[u8] = value.bytes().unwrap();
    assert_eq!(payload, b"owned");
    let alias = value.clone();
    assert!(Arc::ptr_eq(&value.inner, &alias.inner));
    assert_eq!(value.inner.key.as_ptr(), alias.inner.key.as_ptr());
    drop(value);
    assert!(weak.upgrade().is_some());
    assert!(!dropped.load(Ordering::SeqCst));
    drop(alias);
    assert!(weak.upgrade().is_none());
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn captured_head_checks_real_root_and_keeps_original_cut_after_head_changes() {
    use crate::native_v2::{
        codec::{LeafEntry, PAGE_BYTES, Scope},
        index::Index,
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    };
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 1 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local.clone(),
        "pages".into(),
        Scope {
            namespace: [11; 32],
            incarnation: [17; 32],
        },
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: PAGE_BYTES,
            version_bytes: 56,
        },
    )
    .unwrap();
    assert!(CapturedHead::capture(&pages, "head").await.is_err());
    let unscoped = pages.clone();
    let foreign = PageStore::new(
        local.clone(),
        "pages".into(),
        Scope {
            namespace: [11; 32],
            incarnation: [17; 32],
        },
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: PAGE_BYTES,
            version_bytes: 56,
        },
    )
    .unwrap();
    let worker = pages.clone();
    let provider = local.clone();
    let captured = resources
        .run(move |physical| async move {
            let foreign = foreign.in_operation(physical.clone())?;
            let pages = worker.in_operation(physical)?;
            let index = Index::new(pages.clone());
            let id = index.store_object_id("data/object").await?;
            let root = index
                .append(
                    None,
                    LeafEntry {
                        object_id: id,
                        logical_id: id,
                        base: 0,
                        record_count: 1,
                        byte_start: 0,
                        byte_len: 4,
                        payload_hash: [3; 32],
                    },
                )
                .await?;
            let initial = NativeHead {
                index_root: Some(root),
                next_offset: 1,
                semantic_anchor: SemanticCoverage::Uninitialized,
                ..head()
            };
            provider
                .put("head", Bytes::copy_from_slice(&initial.encode()))
                .await?;
            let captured = CapturedHead::capture(&pages, "head").await?;
            assert!(captured.lookup(unscoped.clone(), 0).await.is_err());
            assert!(captured.lookup(unscoped, 99).await.is_err());
            assert!(captured.lookup(foreign, 0).await.is_err());
            assert!(captured.acquire(&pages, "other/head").await.is_err());
            assert!(provider.get("other/head").await?.is_none());
            let recaptured = captured.acquire_and_recapture(&pages, "head").await?;
            let next = recaptured.snapshot().head;
            assert_eq!(next.epoch, initial.epoch + 1);
            assert_eq!(next.index_root, initial.index_root);
            assert!(captured.acquire(&pages, "head").await.is_err());
            // The recaptured token is the new version; the original token
            // cannot be promoted by merely changing its epoch field.
            assert_eq!(
                recaptured
                    .snapshot()
                    .lookup(pages.clone(), 0)
                    .await?
                    .unwrap()
                    .entry()
                    .base,
                0
            );
            provider
                .put("head", Bytes::copy_from_slice(&next.encode()))
                .await?;
            assert_eq!(captured.head.epoch, initial.epoch);
            assert_eq!(
                captured
                    .lookup(pages.clone(), 0)
                    .await?
                    .unwrap()
                    .entry()
                    .base,
                0
            );
            let invalid = NativeHead {
                index_root: Some(Descriptor {
                    hash: [99; 32],
                    ..root
                }),
                ..next
            };
            provider
                .put("head", Bytes::copy_from_slice(&invalid.encode()))
                .await?;
            assert!(CapturedHead::capture(&pages, "head").await.is_err());
            let missing_hash = "63".repeat(32);
            let missing_key = provider.list("pages").await?.into_iter().next().unwrap();
            let prefix = missing_key.rsplit_once('/').unwrap().0;
            provider
                .put(
                    &format!("{prefix}/{missing_hash}"),
                    Bytes::from_static(b"corrupt"),
                )
                .await?;
            assert!(CapturedHead::capture(&pages, "head").await.is_err());
            assert_eq!(captured.lookup(pages, 0).await?.unwrap().entry().base, 0);
            Ok(captured)
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(captured.head.next_offset, 1);
    assert!(resources.bytes.available() < 1 << 20);
    drop(captured);
    assert_eq!(resources.bytes.available(), 1 << 20);
}

#[tokio::test]
async fn prepared_append_publishes_actual_bytes_and_rejects_original_head_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    exercise_prepared_append(local, "pages".into(), "head".into(), 56).await;
}
async fn exercise_prepared_append(
    local: Arc<dyn crate::native_v2::page_store::NativeStore>,
    prefix: String,
    head_key: String,
    version_bytes: usize,
) {
    use crate::native_v2::{
        codec::{PAGE_BYTES, Scope},
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    };
    let empty = NativeHead {
        next_offset: 0,
        index_root: None,
        semantic_anchor: SemanticCoverage::Uninitialized,
        ..head()
    };
    local
        .put(&head_key, Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 1 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local.clone(),
        prefix.clone(),
        Scope {
            namespace: [11; 32],
            incarnation: [17; 32],
        },
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: PAGE_BYTES,
            version_bytes,
        },
    )
    .unwrap();
    let (retained, physical, writer_cap) = resources
        .run(move |physical| async move {
            let weak = Arc::downgrade(&physical);
            let pages = pages.in_operation(physical)?;
            let writer = CapturedHead::capture(&pages, &head_key)
                .await?
                .acquire_and_recapture(&pages, &head_key)
                .await?;
            let before_prepare = local.get(&head_key).await?.unwrap();
            let invalid = writer.clone();
            assert!(
                invalid
                    .prepare_append(pages.clone(), "oversized", &vec![0; PAGE_BYTES + 1], 1)
                    .await
                    .is_err()
            );
            assert!(
                local.list(&format!("{prefix}/")).await?.is_empty(),
                "oversize must fail before overflow/data I/O"
            );
            let invalid = writer.clone();
            assert!(
                invalid
                    .prepare_append(pages.clone(), "zero-count", b"payload", 0)
                    .await
                    .is_err()
            );
            assert!(local.list(&format!("{prefix}/")).await?.is_empty());
            let first = writer.clone();
            let second = writer.clone();
            let mut first = first
                .prepare_append(pages.clone(), "head", b"actual bytes", 2)
                .await?;
            let mut second = second
                .prepare_append(pages.clone(), "head.olog-tmp", b"other bytes", 1)
                .await?;
            let mut pending_before_rollback = writer
                .clone()
                .prepare_append(pages.clone(), "pending-before-rollback", b"pending", 1)
                .await?;
            assert_eq!(
                local.get(&head_key).await?.unwrap().as_ref(),
                before_prepare.as_ref()
            );
            let published = first.publish(pages.clone(), &head_key).await?;
            assert_eq!(published.next_offset, 2);
            assert_eq!(
                first.resolve_range(pages.clone(), &head_key).await?,
                Resolution::DurableContent
            );
            assert_eq!(
                first.resolve_exact(pages.clone(), &head_key).await?,
                Some(published)
            );
            assert!(first.publish(pages.clone(), &head_key).await.is_err());
            assert!(second.publish(pages.clone(), &head_key).await.is_err());
            assert!(
                second
                    .resolve_exact(pages.clone(), &head_key)
                    .await?
                    .is_none()
            );
            assert_eq!(
                second.resolve_range(pages.clone(), &head_key).await?,
                Resolution::Rejected
            );
            // Restore byte-identical old authority: S3 may restore its old ETag.
            // Shared knowledge must still block stale preparation/publication.
            local.put(&head_key, before_prepare.clone()).await?;
            assert!(writer.refresh(pages.clone(), &head_key).await.is_err());
            let objects_before = local.list(&format!("{prefix}/")).await?;
            assert!(
                writer
                    .clone()
                    .prepare_append(pages.clone(), "blocked-stale", b"blocked", 1)
                    .await
                    .is_err()
            );
            assert!(
                pending_before_rollback
                    .publish(pages.clone(), &head_key)
                    .await
                    .is_err()
            );
            assert_eq!(
                local.get(&head_key).await?.unwrap().as_ref(),
                before_prepare.as_ref()
            );
            assert_eq!(local.list(&format!("{prefix}/")).await?, objects_before);
            local
                .put(&head_key, Bytes::copy_from_slice(&published.encode()))
                .await?;
            let captured = CapturedHead::capture(&pages, &head_key).await?;
            assert!(local.get(&format!("{head_key}.olog-tmp")).await?.is_none());
            assert!(
                CapturedHead::capture(&pages, &format!("{prefix}/data/head"))
                    .await
                    .is_err()
            );
            let entry = captured.lookup(pages.clone(), 1).await?.unwrap();
            assert_eq!(entry.entry().record_count, 2);
            assert_eq!(
                entry.entry().payload_hash,
                <[u8; 32]>::from(Sha256::digest(b"actual bytes"))
            );
            assert_eq!(
                super::super::data::DataFrame::decode(
                    local
                        .get(
                            crate::native_v2::index::Index::new(pages.clone())
                                .object_id(entry.entry().object_id)
                                .await?
                                .as_str()
                        )
                        .await?
                        .unwrap()
                        .as_ref()
                )?
                .payload,
                b"actual bytes"
            );
            assert_eq!(
                captured
                    .read_payload(pages.clone(), 1)
                    .await?
                    .unwrap()
                    .bytes(),
                b"actual bytes"
            );
            let retained = captured.read_payload(pages.clone(), 1).await?.unwrap();
            assert!(captured.read_payload(pages.clone(), 2).await?.is_none());
            let physical = crate::native_v2::index::Index::new(pages.clone())
                .object_id(entry.entry().object_id)
                .await?;
            let original = local.get(physical.as_str()).await?.unwrap();
            let mut wrong_identity = original.to_vec();
            wrong_identity[24] ^= 1;
            local
                .put(physical.as_str(), Bytes::from(wrong_identity))
                .await?;
            assert!(captured.read_payload(pages.clone(), 1).await.is_err());
            local.put(physical.as_str(), original.clone()).await?;
            let mut wrong_mapping = *entry.entry();
            let alternate = pages.data_key("mapping-only alternate location")?;
            pages.upload_data(alternate.as_str(), &original).await?;
            wrong_mapping.object_id = crate::native_v2::index::Index::new(pages.clone())
                .store_object_id(alternate.as_str())
                .await?;
            let mut wrong_range = *entry.entry();
            wrong_range.byte_start += 1;
            wrong_range.byte_len -= 1;
            let mut wrong_start = *entry.entry();
            wrong_start.byte_start += 1;
            let mut wrong_length = *entry.entry();
            wrong_length.byte_len += 1;
            let mut wrong_digest = *entry.entry();
            wrong_digest.payload_hash[0] ^= 1;
            for changed in [
                wrong_mapping,
                wrong_range,
                wrong_start,
                wrong_length,
                wrong_digest,
            ] {
                let root = crate::native_v2::index::Index::new(pages.clone())
                    .append(None, changed)
                    .await?;
                let invalid_head = NativeHead {
                    index_root: Some(root),
                    ..captured.head
                };
                local
                    .put(&head_key, Bytes::copy_from_slice(&invalid_head.encode()))
                    .await?;
                let altered = CapturedHead::capture(&pages, &head_key).await?;
                assert!(altered.read_payload(pages.clone(), 1).await.is_err());
            }
            local
                .put(&head_key, Bytes::copy_from_slice(&captured.head.encode()))
                .await?;
            local.take_media_op_stats();
            let later = writer.refresh(pages.clone(), &head_key).await?;
            assert_eq!(
                local.take_media_op_stats().unwrap().bytes,
                0,
                "refresh cannot publish another fence"
            );
            assert_eq!(later.snapshot().head.epoch, writer.snapshot().head.epoch);
            let mut later = later
                .prepare_append(pages.clone(), "later-work", b"later", 1)
                .await?;
            let later_head = later.publish(pages.clone(), &head_key).await?;
            assert_eq!(
                later.resolve_range(pages.clone(), &head_key).await?,
                Resolution::DurableContent
            );
            let mut changed_prefix = *entry.entry();
            changed_prefix.payload_hash[0] ^= 1;
            let rewritten = pages
                .write(pages.leaf(&[changed_prefix, later.certificates.entries[0]])?)
                .await?;
            let rewritten_head = NativeHead {
                index_root: Some(rewritten),
                ..later_head
            };
            local
                .put(&head_key, Bytes::copy_from_slice(&rewritten_head.encode()))
                .await?;
            assert_eq!(
                later.resolve_range(pages.clone(), &head_key).await?,
                Resolution::Unknown
            );
            local
                .put(&head_key, Bytes::copy_from_slice(&later_head.encode()))
                .await?;
            assert!(
                first
                    .resolve_exact(pages.clone(), &head_key)
                    .await?
                    .is_none()
            );
            assert_eq!(
                first.resolve_range(pages.clone(), &head_key).await?,
                Resolution::DurableContent
            );

            let mut corrupt = local.get(physical.as_str()).await?.unwrap().to_vec();
            *corrupt.last_mut().unwrap() ^= 1;
            local.put(physical.as_str(), Bytes::from(corrupt)).await?;
            assert!(captured.read_payload(pages.clone(), 1).await.is_err());
            assert!(captured.lookup(pages.clone(), 2).await?.is_none());
            let latest = writer.refresh(pages.clone(), &head_key).await?;
            let before_fence = local.get(&head_key).await?.unwrap();
            let mut pending_fence = latest
                .prepare_append(pages.clone(), "pending-fence", b"fenced", 1)
                .await?;
            CapturedHead::capture(&pages, &head_key)
                .await?
                .acquire_and_recapture(&pages, &head_key)
                .await?;
            // A conflict observation itself must permanently revoke this writer.
            assert!(
                pending_fence
                    .publish(pages.clone(), &head_key)
                    .await
                    .is_err()
            );
            assert_eq!(
                pending_fence
                    .resolve_range(pages.clone(), &head_key)
                    .await?,
                Resolution::Unknown
            );
            local.put(&head_key, before_fence).await?;
            assert!(writer.refresh(pages.clone(), &head_key).await.is_err());
            assert!(
                writer
                    .clone()
                    .prepare_append(pages.clone(), "revoked", b"blocked", 1)
                    .await
                    .is_err()
            );
            Ok((retained, weak, writer))
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert!(physical.upgrade().is_none());
    assert_eq!(retained.bytes(), b"actual bytes");
    assert!(resources.bytes.available() < 1 << 20);
    drop(retained);
    assert!(resources.bytes.available() < 1 << 20);
    let alias = writer_cap.clone();
    drop(writer_cap);
    assert!(resources.bytes.available() < 1 << 20);
    drop(alias);
    assert_eq!(resources.bytes.available(), 1 << 20);
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires configured S3 provider; mandatory native-v2 qualification lane"]
async fn real_s3_runs_identical_captured_append_contract_in_both_signing_modes() {
    let required = |name: &str| {
        std::env::var(name).expect("native-v2 captured append requires complete S3 configuration")
    };
    let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
    let region = required("OBJECT_LOG_S3_REGION");
    let bucket = required("OBJECT_LOG_S3_BUCKET");
    let key = required("OBJECT_LOG_S3_KEY_ID");
    let secret = required("OBJECT_LOG_S3_SECRET");
    for unsigned in [false, true] {
        let store = Arc::new(
            crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                .with_payload_signing_disabled(unsigned),
        );
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!("native-v2-captured-{nonce}-{unsigned}");
        let head_key = format!("{prefix}-head");
        exercise_prepared_append(store.clone(), prefix.clone(), head_key.clone(), 4096).await;
        for object in store.list(&format!("{prefix}/")).await.unwrap() {
            store.delete(&object).await.unwrap();
        }
        store.delete(&head_key).await.unwrap();
    }
}

#[tokio::test]
async fn durable_publication_lost_reply_resolves_after_later_append_without_retry() {
    use crate::native_v2::{
        codec::{PAGE_BYTES, Scope},
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    };
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let empty = NativeHead {
        index_root: None,
        next_offset: 0,
        semantic_anchor: SemanticCoverage::Uninitialized,
        ..head()
    };
    local
        .put("head", Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 1 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local.clone(),
        "pages".into(),
        Scope {
            namespace: [11; 32],
            incarnation: [17; 32],
        },
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: PAGE_BYTES,
            version_bytes: 56,
        },
    )
    .unwrap();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let captured = CapturedHead::capture(&pages, "head").await?;
            let writer = captured.acquire_and_recapture(&pages, "head").await?;
            let mut prepared = writer.clone()
                .prepare_append(pages.clone(), "first", b"first payload", 1)
                .await?;
            local.lose_next_metadata_reply("head");
            assert!(prepared.publish(pages.clone(), "head").await.is_err());
            assert!(matches!(prepared.publish(pages.clone(), "head").await,
            Err(ObjectLogError::Sequencer(reason)) if reason == "append publication already attempted"));
            let current = CapturedHead::capture(&pages, "head").await?;
            assert_eq!(current.head.next_offset, 1);
            let mut later = writer.refresh(pages.clone(), "head").await?
                .prepare_append(pages.clone(), "second", b"second payload", 1)
                .await?;
            later.publish(pages.clone(), "head").await?;
            assert!(prepared.resolve_exact(pages.clone(), "head").await?.is_none());
            assert_eq!(
                prepared.resolve_range(pages.clone(), "head").await?,
                Resolution::DurableContent
            );
            assert_eq!(
                later.resolve_range(pages, "head").await?,
                Resolution::DurableContent
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires configured S3 provider; mandatory native-v2 qualification lane"]
async fn real_s3_lost_response_processing_resolves_after_later_append_without_resubmission() {
    use crate::native_v2::{
        codec::{PAGE_BYTES, Scope},
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    };
    let required = |name: &str| {
        std::env::var(name).expect("S3 response-loss qualification requires complete configuration")
    };
    let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
    let bucket = required("OBJECT_LOG_S3_BUCKET");
    let region = required("OBJECT_LOG_S3_REGION");
    let key = required("OBJECT_LOG_S3_KEY_ID");
    let secret = required("OBJECT_LOG_S3_SECRET");
    for (unsigned, grouped) in [(false, false), (true, false), (false, true), (true, true)] {
        let local = Arc::new(
            crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                .with_payload_signing_disabled(unsigned),
        );
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!("native-v2-response-loss-{nonce}-{unsigned}-{grouped}");
        let head_key = format!("{prefix}-head");
        let empty = NativeHead {
            index_root: None,
            next_offset: 0,
            semantic_anchor: SemanticCoverage::Uninitialized,
            ..head()
        };
        local
            .put(&head_key, Bytes::copy_from_slice(&empty.encode()))
            .await
            .unwrap();
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            local.clone(),
            prefix.clone(),
            Scope {
                namespace: [11; 32],
                incarnation: [17; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 4096,
            },
        )
        .unwrap();
        let provider = local.clone();
        let authority = head_key.clone();
        resources.run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let captured = CapturedHead::capture(&pages, &authority).await?;
            let writer = captured.acquire_and_recapture(&pages, &authority).await?;
            let mut prepared = if grouped {
                writer.clone().prepare_group(pages.clone(), &[
                    AppendInput { object_id: "first", payload: b"first payload", records: 2 },
                    AppendInput { object_id: "group-second", payload: b"group second payload", records: 3 },
                ], AppendGroupLimits { entries: 2, framed_bytes: 1024 }).await?
            } else {
                writer.clone().prepare_append(pages.clone(), "first", b"first payload", 1).await?
            };
            let calls = provider.lose_next_metadata_reply(&authority);
            assert!(matches!(prepared.publish(pages.clone(), &authority).await, Err(ObjectLogError::StorageUnavailable(_))));
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert!(matches!(prepared.publish(pages.clone(), &authority).await,
                Err(ObjectLogError::Sequencer(reason)) if reason == "append publication already attempted"));
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            let current = CapturedHead::capture(&pages, &authority).await?;
            assert_eq!(current.head.next_offset, if grouped { 5 } else { 1 });
            let mut later = writer.refresh(pages.clone(), &authority).await?.prepare_append(pages.clone(), "second", b"second payload", 1).await?;
            later.publish(pages.clone(), &authority).await?;
            if grouped {
                assert_eq!(prepared.resolve_group(pages.clone(), &authority, ProofBudget { entries: 1 }).await?, Resolution::Unknown);
                assert_eq!(prepared.resolve_group(pages.clone(), &authority, ProofBudget { entries: 2 }).await?, Resolution::DurableContent);
                let recaptured = CapturedHead::capture(&pages, &authority).await?;
                for (offset, payload) in [(0, &b"first payload"[..]), (2, &b"group second payload"[..])] {
                    assert_eq!(recaptured.read_payload(pages.clone(), offset).await?.unwrap().bytes(), payload);
                }
            } else {
                assert_eq!(prepared.resolve_range(pages.clone(), &authority).await?, Resolution::DurableContent);
            }
            assert_eq!(later.resolve_range(pages, &authority).await?, Resolution::DurableContent);
            Ok(())
        }).await.unwrap();
        resources.close_and_drain().await.unwrap();
        for object in local.list(&format!("{prefix}/")).await.unwrap() {
            local.delete(&object).await.unwrap();
        }
        local.delete(&head_key).await.unwrap();
    }
}

#[tokio::test]
async fn partial_resolution_preserves_independent_prefix_and_unreadable_fence_revokes() {
    use crate::native_v2::{
        codec::{PAGE_BYTES, Scope},
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    };
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let empty = NativeHead {
        index_root: None,
        next_offset: 0,
        semantic_anchor: SemanticCoverage::Uninitialized,
        ..head()
    };
    local
        .put("head", Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 2,
        bytes: 1 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local.clone(),
        "pages".into(),
        Scope {
            namespace: [11; 32],
            incarnation: [17; 32],
        },
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: PAGE_BYTES,
            version_bytes: 56,
        },
    )
    .unwrap();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let writer = CapturedHead::capture(&pages, "head")
                .await?
                .acquire_and_recapture(&pages, "head")
                .await?;
            let mut target = writer
                .clone()
                .prepare_append(pages.clone(), "target", b"target", 2)
                .await?;
            let partial = writer
                .clone()
                .prepare_append(pages.clone(), "independent", b"other", 1)
                .await?;
            local.lose_next_metadata_reply("head");
            assert!(matches!(
                target.publish(pages.clone(), "head").await,
                Err(ObjectLogError::StorageUnavailable(_))
            ));
            // This valid independently observed prefix is not installed by this
            // lineage's successful publication or a definite CAS conflict.
            local
                .put("head", Bytes::copy_from_slice(&partial.next.encode()))
                .await?;
            assert_eq!(
                target.resolve_range(pages.clone(), "head").await?,
                Resolution::Unknown
            );
            assert_eq!(writer.lineage.gate.lock().await.floor, partial.next);
            local
                .put("head", Bytes::copy_from_slice(&target.next.encode()))
                .await?;
            assert_eq!(
                target.resolve_range(pages.clone(), "head").await?,
                Resolution::Unknown
            );
            assert_eq!(writer.lineage.gate.lock().await.floor, partial.next);
            let higher = NativeHead {
                epoch: partial.next.epoch + 1,
                index_root: Some(Descriptor {
                    hash: [99; 32],
                    ..partial.next.index_root.unwrap()
                }),
                ..partial.next
            };
            local
                .put("head", Bytes::copy_from_slice(&higher.encode()))
                .await?;
            assert!(target.resolve_exact(pages.clone(), "head").await.is_err());
            local
                .put("head", Bytes::copy_from_slice(&partial.next.encode()))
                .await?;
            assert!(writer.refresh(pages.clone(), "head").await.is_err());
            assert!(
                writer
                    .prepare_append(pages.clone(), "revoked", b"blocked", 1)
                    .await
                    .is_err()
            );
            let fresh = CapturedHead::capture(&pages, "head")
                .await?
                .acquire_and_recapture(&pages, "head")
                .await?;
            let mut new_target = fresh
                .clone()
                .prepare_append(pages.clone(), "new-target", b"target", 2)
                .await?;
            let complete_peer = fresh
                .clone()
                .prepare_append(pages.clone(), "new-peer", b"peer", 2)
                .await?;
            local.lose_next_metadata_reply("head");
            assert!(matches!(
                new_target.publish(pages.clone(), "head").await,
                Err(ObjectLogError::StorageUnavailable(_))
            ));
            local
                .put("head", Bytes::copy_from_slice(&complete_peer.next.encode()))
                .await?;
            assert_eq!(
                new_target.resolve_range(pages.clone(), "head").await?,
                Resolution::Rejected
            );
            assert_eq!(fresh.lineage.gate.lock().await.floor, complete_peer.next);
            local
                .put("head", Bytes::copy_from_slice(&new_target.next.encode()))
                .await?;
            assert_eq!(
                new_target.resolve_range(pages, "head").await?,
                Resolution::Unknown
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 1 << 20);
}

#[tokio::test]
async fn serialized_lineage_observations_do_not_lose_stronger_head() {
    use crate::native_v2::{
        codec::{PAGE_BYTES, Scope},
        index::Index,
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    };
    use std::future::Future;
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let empty = NativeHead {
        index_root: None,
        next_offset: 0,
        semantic_anchor: SemanticCoverage::Uninitialized,
        ..head()
    };
    local
        .put("head", Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 2,
        bytes: 1 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local.clone(),
        "pages".into(),
        Scope {
            namespace: [11; 32],
            incarnation: [17; 32],
        },
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: PAGE_BYTES,
            version_bytes: 56,
        },
    )
    .unwrap();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let writer = CapturedHead::capture(&pages, "head")
                .await?
                .acquire_and_recapture(&pages, "head")
                .await?;
            let mut first = writer
                .clone()
                .prepare_append(pages.clone(), "first", b"first", 1)
                .await?;
            first.publish(pages.clone(), "head").await?;
            let writer = writer.refresh(pages.clone(), "head").await?;
            let second = writer
                .clone()
                .prepare_append(pages.clone(), "second", b"second", 1)
                .await?;
            let mut third = second.certificates.entries[0];
            third.base = 2;
            let third_root = Index::new(pages.clone())
                .append(second.next.index_root, third)
                .await?;
            let strong = NativeHead {
                index_root: Some(third_root),
                next_offset: 3,
                ..second.next
            };
            local
                .put("head", Bytes::copy_from_slice(&strong.encode()))
                .await?;
            let mut gate = writer.lineage.gate.lock().await;
            let strong_observation = pages.read_head("head", HEAD_BYTES).await?;
            // The delayed observation already owns H3 bytes. A concurrent refresh
            // seeing H2 must wait for H3's proof and installation under this gate.
            local
                .put("head", Bytes::copy_from_slice(&second.next.encode()))
                .await?;
            let cloned = writer.clone();
            let competing = cloned.refresh(pages.clone(), "head");
            tokio::pin!(competing);
            std::future::poll_fn(|cx| {
                assert!(competing.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let installed = WriterLineage::validate(&mut gate, pages.clone(), strong_observation)
                .await?
                .unwrap();
            assert_eq!(installed.head, strong);
            drop(gate);
            assert!(competing.await.is_err());
            assert_eq!(writer.lineage.gate.lock().await.floor, strong);
            assert!(
                writer
                    .clone()
                    .prepare_append(pages, "stale", b"blocked", 1)
                    .await
                    .is_err()
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 1 << 20);
}

trait GroupTestStore: crate::native_v2::page_store::NativeStore {
    fn lose_group_reply(&self, key: &str);
}
impl GroupTestStore for LocalBlobStore {
    fn lose_group_reply(&self, key: &str) {
        self.lose_next_metadata_reply(key);
    }
}
#[cfg(feature = "s3")]
impl GroupTestStore for crate::S3BlobStore {
    fn lose_group_reply(&self, key: &str) {
        self.lose_next_metadata_reply(key);
    }
}
async fn group_contract(factory: Arc<dyn Fn() -> Arc<dyn GroupTestStore> + Send + Sync>) {
    use crate::native_v2::{
        codec::{PAGE_BYTES, Scope},
        operation::{ExecutionConfig, Resources},
        page_store::PageStore,
    };
    for competitor_counts in [vec![], vec![5], vec![1, 1, 1, 1, 1], vec![2, 3]] {
        let local = factory();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!("native-group-{nonce}");
        let authority = format!("{prefix}-head");
        let empty = NativeHead {
            index_root: None,
            next_offset: 0,
            semantic_anchor: SemanticCoverage::Uninitialized,
            ..head()
        };
        local
            .put(&authority, Bytes::copy_from_slice(&empty.encode()))
            .await
            .unwrap();
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 2,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            local.clone(),
            prefix.clone(),
            Scope {
                namespace: [11; 32],
                incarnation: [17; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 4096,
            },
        )
        .unwrap();
        let published_target = competitor_counts.is_empty();
        let operation_prefix = prefix.clone();
        let operation_authority = authority.clone();
        let retained_group = resources
            .run(move |physical| async move {
                let prefix = operation_prefix;
                let authority = operation_authority;
                let pages = pages.in_operation(physical)?;
                let writer = CapturedHead::capture(&pages, &authority)
                    .await?
                    .acquire_and_recapture(&pages, &authority)
                    .await?;
                let before = local.list(&prefix).await?;
                let head_before = local.get(&authority).await?;
                for (last_id, last_records) in [("invalid", 0), ("first", 3)] {
                    assert!(
                        writer
                            .clone()
                            .prepare_group(
                                pages.clone(),
                                &[
                                    AppendInput {
                                        object_id: "first",
                                        payload: b"one",
                                        records: 2
                                    },
                                    AppendInput {
                                        object_id: last_id,
                                        payload: b"two",
                                        records: last_records
                                    },
                                ],
                                AppendGroupLimits {
                                    entries: 2,
                                    framed_bytes: 1024
                                }
                            )
                            .await
                            .is_err()
                    );
                    assert_eq!(local.list(&prefix).await?, before);
                    assert_eq!(local.get(&authority).await?, head_before);
                }
                assert!(
                    writer
                        .clone()
                        .prepare_group(
                            pages.clone(),
                            &[],
                            AppendGroupLimits {
                                entries: 2,
                                framed_bytes: 1024
                            }
                        )
                        .await
                        .is_err()
                );
                assert_eq!(local.list(&prefix).await?, before);
                assert_eq!(local.get(&authority).await?, head_before);
                let oversized = vec![7; PAGE_BYTES];
                assert!(writer.clone().prepare_group(pages.clone(), &[
                    AppendInput { object_id: "valid-first", payload: b"small", records: 1 },
                    AppendInput { object_id: "oversized-last", payload: &oversized, records: 1 },
                ], AppendGroupLimits { entries: 2, framed_bytes: 2 * PAGE_BYTES }).await.is_err());
                assert_eq!(local.list(&prefix).await?, before);
                assert_eq!(local.get(&authority).await?, head_before);
                drop(oversized);
                // A corrupt second immutable object fails only after the first
                // frame and COW pages exist; none of that work authorizes a head.
                let bad_key = pages.data_key("late-corrupt")?;
                local.put(bad_key.as_str(), Bytes::from_static(b"corrupt existing frame")).await?;
                assert!(writer.clone().prepare_group(pages.clone(), &[
                    AppendInput { object_id: "early-orphan", payload: b"early", records: 1 },
                    AppendInput { object_id: "late-corrupt", payload: b"late", records: 1 },
                ], AppendGroupLimits { entries: 2, framed_bytes: 1024 }).await.is_err());
                assert_eq!(local.get(&authority).await?, head_before);
                let early_key = pages.data_key("early-orphan")?;
                assert!(local.get(early_key.as_str()).await?.is_some());
                assert_eq!(local.get(bad_key.as_str()).await?, Some(Bytes::from_static(b"corrupt existing frame")));
                drop(early_key);
                drop(bad_key);
                let mut target = writer
                    .clone()
                    .prepare_group(
                        pages.clone(),
                        &[
                            AppendInput {
                                object_id: "first",
                                payload: b"one",
                                records: 2,
                            },
                            AppendInput {
                                object_id: "second",
                                payload: b"two",
                                records: 3,
                            },
                        ],
                        AppendGroupLimits {
                            entries: 2,
                            framed_bytes: 1024,
                        },
                    )
                    .await?;
                if competitor_counts.is_empty() {
                    local.lose_group_reply(&authority);
                    assert!(target.publish(pages.clone(), &authority).await.is_err());
                    assert!(matches!(target.publish(pages.clone(), &authority).await,
                        Err(ObjectLogError::Sequencer(reason)) if reason == "append publication already attempted"));
                    let mut later = writer.refresh(pages.clone(), &authority).await?
                        .prepare_append(pages.clone(), "later", b"later", 1).await?;
                    later.publish(pages.clone(), &authority).await?;
                    assert_eq!(
                        target
                            .resolve_group(pages.clone(), &authority, ProofBudget { entries: 1 })
                            .await?,
                        Resolution::Unknown
                    );
                    assert_eq!(
                        target
                            .resolve_group(pages.clone(), &authority, ProofBudget { entries: 2 })
                            .await?,
                        Resolution::DurableContent
                    );
                    let recaptured = CapturedHead::capture(&pages, &authority).await?;
                    assert_eq!(recaptured.head.next_offset, 6);
                    for (offset, payload) in [(0, &b"one"[..]), (2, &b"two"[..])] {
                        assert_eq!(
                            recaptured
                                .read_payload(pages.clone(), offset)
                                .await?
                                .unwrap()
                                .bytes(),
                            payload
                        );
                    }
                } else {
                    let wrong_last = competitor_counts == vec![2, 3];
                    let ids: Vec<_> = (0..competitor_counts.len())
                        .map(|n| if wrong_last && n == 0 { "first".to_owned() } else { format!("peer-{n}") })
                        .collect();
                    let inputs: Vec<_> = ids
                        .iter()
                        .zip(&competitor_counts)
                        .map(|(id, count)| AppendInput {
                            object_id: id,
                            payload: if wrong_last && id == "first" { b"one" } else { b"peer" },
                            records: *count,
                        })
                        .collect();
                    let mut peer = writer
                        .prepare_group(
                            pages.clone(),
                            &inputs,
                            AppendGroupLimits {
                                entries: inputs.len(),
                                framed_bytes: 4096,
                            },
                        )
                        .await?;
                    if competitor_counts.len() > 1 {
                        // Force a branch so the first mismatching entry is readable
                        // while a later child is unavailable. Root validation alone
                        // must never authorize early rejection of the target group.
                        let first = pages.write(pages.leaf(&peer.certificates.entries[..1])?).await?;
                        let rest = pages.write(pages.leaf(&peer.certificates.entries[1..])?).await?;
                        peer.next.index_root = Some(pages.write(pages.branch(1, &[first, rest])?).await?);
                        let suffix = rest.hash.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
                        let missing_key = local.list(&prefix).await?.into_iter()
                            .find(|key| key.ends_with(&suffix)).expect("written later leaf");
                        let saved = local.get(&missing_key).await?.unwrap();
                        local.delete(&missing_key).await?;
                        peer.publish(pages.clone(), &authority).await?;
                        assert!(target.publish(pages.clone(), &authority).await.is_err());
                        assert!(target.resolve_group(pages.clone(), &authority,
                            ProofBudget { entries: competitor_counts.len() }).await.is_err());
                        local.put(&missing_key, Bytes::from_static(b"corrupt later leaf")).await?;
                        assert!(target.resolve_group(pages.clone(), &authority,
                            ProofBudget { entries: competitor_counts.len() }).await.is_err());
                        local.put(&missing_key, saved).await?;
                    } else {
                        peer.publish(pages.clone(), &authority).await?;
                    }

                    assert!(target.publish(pages.clone(), &authority).await.is_err());
                    let n = competitor_counts.len();
                    if n > 1 {
                        assert_eq!(
                            target
                                .resolve_group(
                                    pages.clone(),
                                    &authority,
                                    ProofBudget { entries: n - 1 }
                                )
                                .await?,
                            Resolution::Unknown
                        );
                    }
                    assert_eq!(
                        target
                            .resolve_group(pages.clone(), &authority, ProofBudget { entries: n })
                            .await?,
                        Resolution::Rejected
                    );
                }
                Ok(target)
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert!(resources.bytes.available() < (1 << 20));
        assert_eq!(retained_group.certificates.entries.len(), 2);
        drop(retained_group);
        assert_eq!(resources.bytes.available(), 1 << 20);
        drop(resources);
        if published_target {
            let fresh = factory();
            let reopened_resources = Resources::new(ExecutionConfig {
                workers: 2,
                blocking_workers: 2,
                operations: 1,
                bytes: 1 << 20,
            })
            .unwrap();
            let reopened_pages = PageStore::new(
                fresh,
                prefix.clone(),
                Scope {
                    namespace: [11; 32],
                    incarnation: [17; 32],
                },
                reopened_resources.bytes.clone(),
                MetadataLimits {
                    payload_bytes: PAGE_BYTES,
                    version_bytes: 4096,
                },
            )
            .unwrap();
            let reopened_authority = authority.clone();
            reopened_resources
                .run(move |physical| async move {
                    let pages = reopened_pages.in_operation(physical)?;
                    let reopened = CapturedHead::capture(&pages, &reopened_authority).await?;
                    assert_eq!(reopened.head.next_offset, 6);
                    for (offset, count, payload) in [(0, 2, &b"one"[..]), (2, 3, &b"two"[..])] {
                        let owned = reopened.lookup(pages.clone(), offset).await?.unwrap();
                        assert_eq!(owned.entry().base, offset);
                        assert_eq!(owned.entry().record_count, count);
                        drop(owned);
                        assert_eq!(
                            reopened
                                .read_payload(pages.clone(), offset)
                                .await?
                                .unwrap()
                                .bytes(),
                            payload
                        );
                    }
                    Ok(())
                })
                .await
                .unwrap();
            reopened_resources.close_and_drain().await.unwrap();
        }
        let cleanup = factory();
        for object in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&object).await.unwrap();
        }
        cleanup.delete(&authority).await.unwrap();
    }
}

#[tokio::test]
async fn groups_preflight_all_items_and_resolve_complete_actual_boundaries() {
    let directory = Arc::new(tempfile::tempdir().unwrap());
    group_contract(Arc::new(move || {
        Arc::new(LocalBlobStore::new(directory.path()))
    }))
    .await;
}
#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires configured S3 provider; mandatory native-v2 qualification lane"]
async fn real_s3_runs_identical_group_contract_in_both_signing_modes() {
    let required = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
    let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
    let region = required("OBJECT_LOG_S3_REGION");
    let bucket = required("OBJECT_LOG_S3_BUCKET");
    let key = required("OBJECT_LOG_S3_KEY_ID");
    let secret = required("OBJECT_LOG_S3_SECRET");
    for unsigned in [false, true] {
        let (endpoint, region, bucket, key, secret) = (
            endpoint.clone(),
            region.clone(),
            bucket.clone(),
            key.clone(),
            secret.clone(),
        );
        group_contract(Arc::new(move || {
            Arc::new(
                crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                    .with_payload_signing_disabled(unsigned),
            )
        }))
        .await;
    }
}
