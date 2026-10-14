#![cfg(feature = "native")]
use bytes::Bytes;
use object_log::{
    BlobStore, LocalBlobStore, NativeAppend, NativeLimits, NativeLog, NativePublishOutcome,
    NativeResolution,
};
use sha2::{Digest, Sha256};

fn stored(outcome: NativePublishOutcome) -> object_log::NativeHead {
    match outcome {
        NativePublishOutcome::Stored(head) => head,
        NativePublishOutcome::Conflict => panic!("unexpected publication conflict"),
    }
}

fn limits() -> NativeLimits {
    NativeLimits {
        workers: 2,
        blocking_workers: 2,
        operations: 4,
        bytes: 4 << 20,
        version_bytes: 4096,
        binding_bytes: 4096,
        group_entries: 16,
        group_bytes: 1 << 20,
    }
}
async fn head_key(store: &LocalBlobStore, prefix: &str) -> String {
    let heads: Vec<_> = store
        .list(prefix)
        .await
        .unwrap()
        .into_iter()
        .filter(|key| key.ends_with("/head"))
        .collect();
    assert_eq!(heads.len(), 1);
    heads[0].clone()
}

#[tokio::test]
async fn external_native_api_retains_fixed_cuts_and_shared_writer_progress() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(store.clone(), "public-native".into(), limits()).unwrap();
    let namespace = log.provision_new(b"tenant\0queue").await.unwrap();
    let alias = namespace.clone();
    let writer = log.acquire(&namespace).await.unwrap();
    let attempt = log
        .prepare(
            &writer,
            &[
                NativeAppend {
                    identity: "first",
                    payload: b"first checked record",
                    records: 1,
                },
                NativeAppend {
                    identity: "second",
                    payload: b"second checked record",
                    records: 1,
                },
            ],
        )
        .await
        .unwrap();
    let receipt_alias = attempt.clone();
    assert_eq!(stored(log.submit(&attempt).await.unwrap()).bounds(), (0, 2));
    assert_eq!(
        log.resolve(&receipt_alias, 2).await.unwrap(),
        NativeResolution::DurableContent
    );
    let cut = log.capture(&namespace).await.unwrap();
    let record = log.read(&cut, 0).await.unwrap().unwrap();
    assert_eq!(
        (record.base_offset(), record.record_count(), record.bytes()),
        (0, 1, &b"first checked record"[..])
    );
    let key = head_key(&store, "public-native").await;
    let old_head = store.get(&key).await.unwrap().unwrap();
    // A fixed cut reads authenticated immutable pages, without a fresh head GET.
    store.delete(&key).await.unwrap();
    assert_eq!(
        log.read(&cut, 1).await.unwrap().unwrap().bytes(),
        b"second checked record"
    );
    store.put(&key, old_head.clone()).await.unwrap();
    let refreshed = log.refresh_writer(&writer).await.unwrap();
    assert_eq!(refreshed.head().bounds(), (0, 2));
    let next = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: "third",
                payload: b"third checked record",
                records: 1,
            }],
        )
        .await
        .unwrap();
    assert_eq!(stored(log.submit(&next).await.unwrap()).bounds(), (0, 3));
    assert!(log.read(&cut, 2).await.unwrap().is_none());
    assert_eq!(
        log.read(&cut, 0).await.unwrap().unwrap().bytes(),
        b"first checked record"
    );
    // A namespace alias must remember progress published by its derived writer.
    store.put(&key, old_head).await.unwrap();
    assert!(log.capture(&alias).await.is_err());
    assert!(log.refresh_writer(&writer).await.is_err());
    assert!(
        log.prepare(
            &writer,
            &[NativeAppend {
                identity: "stale",
                payload: b"must not publish",
                records: 1
            }]
        )
        .await
        .is_err()
    );
    log.shutdown().await.unwrap();
    // Byte-only cuts, namespace aliases, payloads and original resolvers survive drain.
    assert_eq!(record.bytes(), b"first checked record");
    assert_eq!(cut.head().bounds(), (0, 2));
    drop((
        namespace,
        alias,
        writer,
        attempt,
        receipt_alias,
        cut,
        refreshed,
        record,
    ));
}

#[tokio::test]
async fn external_namespace_observation_keeps_unreadable_higher_fence_revocation() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(store.clone(), "public-fence".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let attempt = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: "one",
                payload: b"checked",
                records: 1,
            }],
        )
        .await
        .unwrap();
    let published = stored(log.submit(&attempt).await.unwrap());
    let cut = log.capture(&namespace).await.unwrap();
    let key = head_key(&store, "public-fence").await;
    let original = store.get(&key).await.unwrap().unwrap();
    let mut higher = original.to_vec();
    // Independent ONH2 wire manipulation, not a facade root setter.
    higher[72..80].copy_from_slice(&(published.epoch() + 10).to_be_bytes());
    higher[96..128].fill(88);
    let checksum = Sha256::digest(&higher[..208]);
    higher[208..240].copy_from_slice(&checksum);
    store.put(&key, Bytes::from(higher)).await.unwrap();
    assert!(log.capture(&namespace).await.is_err());
    store.put(&key, original).await.unwrap();
    assert!(log.capture(&namespace).await.is_err());
    assert!(log.refresh_writer(&writer).await.is_err());
    assert!(
        log.prepare(
            &writer,
            &[NativeAppend {
                identity: "after-revocation",
                payload: b"blocked",
                records: 1
            }]
        )
        .await
        .is_err()
    );
    assert_eq!(
        log.read(&cut, 0).await.unwrap().unwrap().bytes(),
        b"checked"
    );
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn external_foreign_handles_and_invalid_limits_have_no_storage_effects() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(first.path());
    let peer_store = LocalBlobStore::new(second.path());
    let log = NativeLog::local(store.clone(), "owner".into(), limits()).unwrap();
    let peer = NativeLog::local(peer_store.clone(), "owner".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    assert!(peer.capture(&namespace).await.is_err());
    assert!(peer.acquire(&namespace).await.is_err());
    assert!(peer_store.list("").await.unwrap().is_empty());
    let mut invalid = limits();
    invalid.operations = 0;
    assert!(NativeLog::local(peer_store.clone(), "invalid".into(), invalid).is_err());
    assert!(peer_store.list("").await.unwrap().is_empty());
    log.shutdown().await.unwrap();
    peer.shutdown().await.unwrap();
}

#[tokio::test]
async fn external_large_payload_is_independent_of_namespace_binding_limit() {
    let directory = tempfile::tempdir().unwrap();
    let mut configured = limits();
    configured.binding_bytes = 256;
    let log = NativeLog::local(
        LocalBlobStore::new(directory.path()),
        "large-record".into(),
        configured,
    )
    .unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let identity = "literal-identity/".repeat(400);
    let payload = vec![19; 16 * 1024];
    let publication = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: &identity,
                payload: &payload,
                records: 1,
            }],
        )
        .await
        .unwrap();
    log.submit(&publication).await.unwrap();
    assert_eq!(
        log.resolve(&publication, 1).await.unwrap(),
        NativeResolution::DurableContent
    );
    let cut = log.capture(&namespace).await.unwrap();
    assert_eq!(log.read(&cut, 0).await.unwrap().unwrap().bytes(), payload);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn external_conditional_conflict_retains_original_resolution() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(store.clone(), "typed-conflict".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let attempt = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: "original",
                payload: b"original",
                records: 1,
            }],
        )
        .await
        .unwrap();
    // A separately opened family changes durable authority without refreshing
    // the first family's knowledge, so submit must reach the actual storage CAS.
    let other = NativeLog::local(store, "typed-conflict".into(), limits()).unwrap();
    let other_namespace = other.open(b"queue").await.unwrap().unwrap();
    let other_writer = other.acquire(&other_namespace).await.unwrap();
    assert_eq!(
        log.submit(&attempt).await.unwrap(),
        NativePublishOutcome::Conflict
    );
    assert_eq!(
        log.resolve(&attempt, 1).await.unwrap(),
        NativeResolution::Unknown
    );
    assert!(log.submit(&attempt).await.is_err());
    drop(other_writer);
    other.shutdown().await.unwrap();
    log.shutdown().await.unwrap();
}

async fn append_reopen_contract(log: &NativeLog) {
    let namespace = log.provision_new(b"contract\0queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let empty = log.capture(&namespace).await.unwrap();
    let identity = "full-literal-identity/".repeat(300);
    let payload = vec![71; 16 * 1024];
    let publication = log
        .prepare(
            &writer,
            &[
                NativeAppend {
                    identity: &identity,
                    payload: &payload,
                    records: 2,
                },
                NativeAppend {
                    identity: "second",
                    payload: b"tail",
                    records: 1,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        stored(log.submit(&publication).await.unwrap()).bounds(),
        (0, 3)
    );
    assert_eq!(
        log.resolve(&publication, 2).await.unwrap(),
        NativeResolution::DurableContent
    );
    assert!(log.read(&empty, 0).await.unwrap().is_none());
    let reopened = log.open(b"contract\0queue").await.unwrap().unwrap();
    let cut = log.capture(&reopened).await.unwrap();
    let record = log.read(&cut, 1).await.unwrap().unwrap();
    assert_eq!((record.base_offset(), record.record_count()), (0, 2));
    assert_eq!(record.bytes(), payload);
    assert_eq!(log.read(&cut, 2).await.unwrap().unwrap().bytes(), b"tail");
    assert!(log.read(&cut, 3).await.unwrap().is_none());
}

async fn cold_read_contract(log: &NativeLog) {
    let namespace = log.open(b"contract\0queue").await.unwrap().unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    assert_eq!(cut.head().bounds(), (0, 3));
    let first = log.read(&cut, 1).await.unwrap().unwrap();
    assert_eq!((first.base_offset(), first.record_count()), (0, 2));
    assert_eq!(first.bytes(), vec![71; 16 * 1024]);
    assert_eq!(log.read(&cut, 2).await.unwrap().unwrap().bytes(), b"tail");
    assert!(log.read(&cut, 3).await.unwrap().is_none());
}

#[tokio::test]
async fn external_local_common_append_reopen_contract() {
    let directory = tempfile::tempdir().unwrap();
    let mut configured = limits();
    configured.binding_bytes = 256;
    let log = NativeLog::local(
        LocalBlobStore::new(directory.path()),
        "shared-contract".into(),
        configured,
    )
    .unwrap();
    append_reopen_contract(&log).await;
    log.shutdown().await.unwrap();
    drop(log);
    let reopened = NativeLog::local(
        LocalBlobStore::new(directory.path()),
        "shared-contract".into(),
        configured,
    )
    .unwrap();
    cold_read_contract(&reopened).await;
    reopened.shutdown().await.unwrap();
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires an explicitly configured live S3 provider"]
async fn external_s3_common_append_reopen_contract_both_signing_modes() {
    let required =
        |name| std::env::var(name).expect("live S3 qualification requires complete configuration");
    let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
    let region = required("OBJECT_LOG_S3_REGION");
    let bucket = required("OBJECT_LOG_S3_BUCKET");
    let key = required("OBJECT_LOG_S3_KEY_ID");
    let secret = required("OBJECT_LOG_S3_SECRET");
    for disabled in [false, true] {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let nonce: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let prefix = format!("native-facade-contract/{nonce}/{disabled}");
        let store = object_log::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
            .with_payload_signing_disabled(disabled);
        let mut configured = limits();
        configured.binding_bytes = 256;
        let log = NativeLog::s3(store, prefix.clone(), configured).unwrap();
        append_reopen_contract(&log).await;
        log.shutdown().await.unwrap();
        drop(log);
        let fresh_store = object_log::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
            .with_payload_signing_disabled(disabled);
        let reopened = NativeLog::s3(fresh_store, prefix.clone(), configured).unwrap();
        cold_read_contract(&reopened).await;
        reopened.shutdown().await.unwrap();
        let cleanup = object_log::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret);
        for object in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&object).await.unwrap();
        }
    }
}

#[tokio::test]
async fn external_regressed_cas_authority_is_an_error_not_a_validated_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(store.clone(), "regressed-conflict".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let key = head_key(&store, "regressed-conflict").await;
    let older = store.get(&key).await.unwrap().unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let publication = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: "original",
                payload: b"original",
                records: 1,
            }],
        )
        .await
        .unwrap();
    store.put(&key, older).await.unwrap();
    assert!(log.submit(&publication).await.is_err());
    assert!(log.submit(&publication).await.is_err());
    // The retained resolver cannot certify content from regressed authority.
    assert_eq!(
        log.resolve(&publication, 1).await.unwrap(),
        NativeResolution::Unknown
    );
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn external_cloned_submitters_share_one_terminal_publication() {
    let directory = tempfile::tempdir().unwrap();
    let log = NativeLog::local(
        LocalBlobStore::new(directory.path()),
        "cloned-submit".into(),
        limits(),
    )
    .unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let publication = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: "one",
                payload: b"one",
                records: 1,
            }],
        )
        .await
        .unwrap();
    let alias = publication.clone();
    let (first, second) = tokio::join!(log.submit(&publication), log.submit(&alias));
    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Ok(NativePublishOutcome::Stored(_))))
            .count(),
        1
    );
    assert_eq!(outcomes.iter().filter(|result| result.is_err()).count(), 1);
    let loser = outcomes
        .iter()
        .find_map(|result| result.as_ref().err())
        .unwrap();
    assert!(
        matches!(loser, object_log::ObjectLogError::Sequencer(message) if message == "append publication already attempted")
    );
    assert_eq!(
        log.resolve(&publication, 1).await.unwrap(),
        NativeResolution::DurableContent
    );
    assert_eq!(
        log.resolve(&alias, 1).await.unwrap(),
        NativeResolution::DurableContent
    );
    let cut = log.capture(&namespace).await.unwrap();
    assert_eq!(cut.head().bounds(), (0, 1));
    log.shutdown().await.unwrap();
}
