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
            assert_eq!(calls.reads, Vec::<String>::new(), "append {n}");
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

#[tokio::test]
async fn a_thousand_advanced_appends_make_zero_reads_and_exact_writes() {
    let directory = tempfile::tempdir().unwrap();
    let (log, store) = counting_log(directory.path(), "zero-read");
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    // The first append builds the writer's position; it has no tree to read yet.
    append_one(&log, &writer, 0).await.unwrap();
    store.take();
    let mut per_append = std::collections::BTreeMap::<usize, usize>::new();
    for n in 1..=1000u64 {
        append_one(&log, &writer, n).await.unwrap();
        let calls = store.take();
        assert_eq!(calls.reads, Vec::<String>::new(), "append {n}");
        // Two identity pages, one data frame, the index pages that changed
        // (one leaf; a split adds the new leaf and the rewritten parent), one head.
        let index_pages = if n < LEAF_SLOTS_PER_PAGE { 1 } else { 2 };
        assert_eq!(calls.writes.len(), 3 + index_pages + 1, "append {n}");
        assert!(calls.writes.last().unwrap().ends_with("/head"));
        *per_append.entry(calls.writes.len()).or_default() += 1;
    }
    eprintln!("writes per append (writes -> appends): {per_append:?}");
    // The tree crossed a leaf split, root growth and many more splits.
    let cut = log.capture(&namespace).await.unwrap();
    assert_eq!(cut.head().bounds(), (0, 1001));
    assert_eq!(cut.head().index_root().unwrap().height, 1);
    // A new acquisition has no position: its first append reads the right
    // spine, h + 1 pages, and the rest are read-free again.
    let writer = log.acquire(&namespace).await.unwrap();
    store.take();
    append_one(&log, &writer, 1001).await.unwrap();
    assert_eq!(store.take().reads.len(), 2);
    for n in 1002..1010 {
        append_one(&log, &writer, n).await.unwrap();
        assert_eq!(store.take().reads, Vec::<String>::new());
    }
    log.shutdown().await.unwrap();
}
const LEAF_SLOTS_PER_PAGE: u64 = super::super::codec::LEAF_SLOTS as u64;

#[tokio::test]
async fn advanced_groups_of_many_entries_read_nothing_and_read_back() {
    let directory = tempfile::tempdir().unwrap();
    let (log, store) = counting_log(directory.path(), "groups");
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let mut next = 0u64;
    let mut expected = Vec::new();
    for round in 0..30u64 {
        let width = 1 + (round % 5) as usize;
        let identities: Vec<_> = (0..width).map(|i| format!("group-{round}-{i}")).collect();
        let payloads: Vec<_> = (0..width)
            .map(|i| format!("payload-{round}-{i}").into_bytes())
            .collect();
        let inputs: Vec<_> = (0..width)
            .map(|i| NativeAppend {
                identity: &identities[i],
                payload: &payloads[i],
                records: 1 + (i % 2) as u32,
            })
            .collect();
        store.take();
        let publication = log.prepare(&writer, &inputs).await.unwrap();
        assert!(matches!(
            log.submit(&publication).await.unwrap(),
            NativePublishOutcome::Stored(_)
        ));
        log.advance_writer(&writer, &publication).await.unwrap();
        if round > 0 {
            assert_eq!(store.take().reads, Vec::<String>::new(), "round {round}");
        }
        for input in &inputs {
            expected.push((next, input.records, input.payload.to_vec()));
            next += u64::from(input.records);
        }
    }
    let cut = log.capture(&namespace).await.unwrap();
    assert_eq!(cut.head().bounds(), (0, next));
    for (base, records, payload) in expected {
        for offset in base..base + u64::from(records) {
            let record = log.read(&cut, offset).await.unwrap().unwrap();
            assert_eq!(
                (record.base_offset(), record.record_count(), record.bytes()),
                (base, records, &payload[..])
            );
        }
    }
    log.shutdown().await.unwrap();
}
