//! Costs are proven by counting storage calls, never by timing.
use super::*;
use crate::native_v2::semantic_map::TestCountingStore as CountingStore;
use crate::{BlobStore, LocalBlobStore};
use std::path::Path;

pub(super) fn limits() -> NativeLimits {
    NativeLimits {
        workers: 2,
        blocking_workers: 2,
        operations: 4,
        bytes: 16 << 20,
        version_bytes: 4096,
        binding_bytes: 4096,
        group_entries: 16,
        group_bytes: 1 << 20,
    }
}
pub(super) fn counting_log(directory: &Path, prefix: &str) -> (NativeLog, Arc<CountingStore>) {
    let store = CountingStore::new(Arc::new(LocalBlobStore::new(directory)));
    let log = NativeLog::new(store.clone(), prefix.into(), limits()).unwrap();
    (log, store)
}
fn payload(n: u64) -> Vec<u8> {
    format!("payload-{n:08}")
        .repeat(1 + (n % 5) as usize)
        .into()
}
async fn append_one(
    log: &NativeLog,
    writer: &NativeWriter,
    n: u64,
) -> Result<NativeHead, ObjectLogError> {
    let bytes = payload(n);
    let identity = format!("record-{n:08}");
    let publication = log
        .prepare(
            writer,
            &[NativeAppend {
                identity: &identity,
                payload: &bytes,
                records: 1,
            }],
        )
        .await?;
    match log.submit(&publication).await? {
        NativePublishOutcome::Stored(head) => {
            assert_eq!(log.advance_writer(writer, &publication).await?, head);
            Ok(head)
        }
        NativePublishOutcome::Conflict => Err(invalid("conflict")),
    }
}
async fn head_key(directory: &Path, prefix: &str) -> String {
    let heads: Vec<_> = LocalBlobStore::new(directory)
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
async fn advanced_writer_appends_without_refresh_and_reads_back() {
    let directory = tempfile::tempdir().unwrap();
    let (log, store) = counting_log(directory.path(), "advance");
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    store.take();
    for n in 0..60 {
        assert_eq!(
            append_one(&log, &writer, n).await.unwrap().bounds(),
            (0, n + 1)
        );
        // The head is written once, and only after everything it references.
        let calls = store.take();
        let writes = calls.writes;
        assert_eq!(writes.iter().filter(|k| k.ends_with("/head")).count(), 1);
        assert!(writes.last().unwrap().ends_with("/head"));
        if n > 0 {
            // Every read is the probe before one immutable write: the index
            // position is retained, so none of them reads the right spine.
            assert_eq!(calls.reads.len(), writes.len() - 1, "append {n}");
        }
    }
    let cut = log.capture(&namespace).await.unwrap();
    assert_eq!(cut.head().bounds(), (0, 60));
    for n in 0..60 {
        let record = log.read(&cut, n).await.unwrap().unwrap();
        assert_eq!((record.base_offset(), record.bytes()), (n, &payload(n)[..]));
    }
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn advance_refuses_unsubmitted_repeated_and_foreign_publications() {
    let directory = tempfile::tempdir().unwrap();
    let (log, _) = counting_log(directory.path(), "refuse");
    let first = log.provision_new(b"first").await.unwrap();
    let second = log.provision_new(b"second").await.unwrap();
    let writer = log.acquire(&first).await.unwrap();
    let other_writer = log.acquire(&second).await.unwrap();
    let input = [NativeAppend {
        identity: "one",
        payload: b"one",
        records: 1,
    }];
    let publication = log.prepare(&writer, &input).await.unwrap();
    assert!(log.advance_writer(&writer, &publication).await.is_err());
    let NativePublishOutcome::Stored(head) = log.submit(&publication).await.unwrap() else {
        panic!("unexpected conflict");
    };
    // Another writer family, even over the same log, cannot adopt it.
    assert!(
        log.advance_writer(&other_writer, &publication)
            .await
            .is_err()
    );
    assert_eq!(
        log.advance_writer(&writer, &publication).await.unwrap(),
        head
    );
    // Adopted once; a publication no longer extends the captured head.
    assert!(log.advance_writer(&writer, &publication).await.is_err());
    // A second publication prepared before the first was adopted is no successor either.
    let stale = log.prepare(&writer, &input_two()).await.unwrap();
    log.submit(&stale).await.unwrap();
    let skipped = log.prepare(&writer, &input_three()).await;
    // The shared floor moved on submit, so preparing from the old capture fails.
    assert!(skipped.is_err());
    assert!(log.advance_writer(&writer, &stale).await.is_ok());
    // A fresh log handle with another origin refuses both carriers.
    let (foreign, _) = counting_log(directory.path(), "refuse");
    assert!(foreign.advance_writer(&writer, &stale).await.is_err());
    log.shutdown().await.unwrap();
    foreign.shutdown().await.unwrap();
}
fn input_two() -> [NativeAppend<'static>; 1] {
    [NativeAppend {
        identity: "two",
        payload: b"two",
        records: 1,
    }]
}
fn input_three() -> [NativeAppend<'static>; 1] {
    [NativeAppend {
        identity: "three",
        payload: b"three",
        records: 1,
    }]
}

#[tokio::test]
async fn conflicting_publish_and_stale_writer_store_no_head_and_cannot_advance() {
    let directory = tempfile::tempdir().unwrap();
    let (log, _) = counting_log(directory.path(), "stale");
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    for n in 0..3 {
        append_one(&log, &writer, n).await.unwrap();
    }
    // A newer family fences this writer without refreshing it.
    let other = NativeLog::local(
        LocalBlobStore::new(directory.path()),
        "stale".into(),
        limits(),
    )
    .unwrap();
    let other_namespace = other.open(b"queue").await.unwrap().unwrap();
    let _newer = other.acquire(&other_namespace).await.unwrap();
    let key = head_key(directory.path(), "stale").await;
    let fenced = LocalBlobStore::new(directory.path())
        .get(&key)
        .await
        .unwrap()
        .unwrap();
    let publication = log.prepare(&writer, &input_two()).await.unwrap();
    assert_eq!(
        log.submit(&publication).await.unwrap(),
        NativePublishOutcome::Conflict
    );
    assert!(log.advance_writer(&writer, &publication).await.is_err());
    assert_eq!(
        LocalBlobStore::new(directory.path())
            .get(&key)
            .await
            .unwrap()
            .unwrap(),
        fenced
    );
    // The stale writer's next attempt never stores a head either.
    assert!(append_one(&log, &writer, 99).await.is_err());
    assert_eq!(
        LocalBlobStore::new(directory.path())
            .get(&key)
            .await
            .unwrap()
            .unwrap(),
        fenced
    );
    other.shutdown().await.unwrap();
    log.shutdown().await.unwrap();
}
