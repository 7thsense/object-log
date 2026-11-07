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

#[tokio::test]
async fn one_appends_immutable_objects_upload_together_before_the_head() {
    let directory = tempfile::tempdir().unwrap();
    let inner = Arc::new(LocalBlobStore::new(directory.path()));
    let store = CountingStore::new(inner.clone());
    let log = NativeLog::new(store.clone(), "concurrent".into(), limits()).unwrap();
    let before = log.owner.resources.bytes.available();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    append_one(&log, &writer, 0).await.unwrap();
    store.take();
    store.take_max_concurrent_swaps();
    store.delay_swaps(40);
    let bytes = payload(1);
    let publication = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: "record-one",
                payload: &bytes,
                records: 1,
            }],
        )
        .await
        .unwrap();
    // Prepare returns only when every object is stored, and never writes the head.
    let calls = store.take();
    let overlap = store.take_max_concurrent_swaps();
    assert!((2..=8).contains(&overlap), "{overlap} uploads overlapped");
    assert_eq!(store.swaps_in_flight(), 0);
    assert_eq!(calls.writes.len(), 4);
    assert!(calls.writes.iter().all(|key| !key.ends_with("/head")));
    for key in &calls.writes {
        assert!(inner.get(key).await.unwrap().is_some(), "{key} not durable");
    }
    // Reservations are held through the retained publication, then fully returned.
    assert!(log.owner.resources.bytes.available() < before);
    store.delay_swaps(0);
    log.submit(&publication).await.unwrap();
    assert_eq!(store.take_max_concurrent_swaps(), 1);
    log.advance_writer(&writer, &publication).await.unwrap();
    drop((publication, writer, namespace));
    log.shutdown().await.unwrap();
    assert_eq!(log.owner.resources.bytes.available(), before);
}

type KeyFilter = fn(&str) -> bool;

#[tokio::test]
async fn any_upload_failure_fails_the_prepare_and_leaves_the_writer_usable() {
    // A data frame, then an identity or index page.
    let kinds: [(&str, KeyFilter); 2] = [
        ("data", |key| key.contains("/data/")),
        ("page", |key| {
            !key.contains("/data/") && !key.ends_with("/head")
        }),
    ];
    for (needle, matching) in kinds {
        let directory = tempfile::tempdir().unwrap();
        let (log, store) = counting_log(directory.path(), "failure");
        let before = log.owner.resources.bytes.available();
        let namespace = log.provision_new(b"queue").await.unwrap();
        let writer = log.acquire(&namespace).await.unwrap();
        for n in 0..3 {
            append_one(&log, &writer, n).await.unwrap();
        }
        let key = head_key(directory.path(), "failure").await;
        let head = LocalBlobStore::new(directory.path())
            .get(&key)
            .await
            .unwrap();
        store.fail_next_swap(matching);
        let bytes = payload(3);
        let input = [NativeAppend {
            identity: "record-three",
            payload: &bytes,
            records: 1,
        }];
        assert!(log.prepare(&writer, &input).await.is_err(), "{needle}");
        assert_eq!(
            LocalBlobStore::new(directory.path())
                .get(&key)
                .await
                .unwrap(),
            head
        );
        // The writer lost its retained position, not its place: the next
        // append rebuilds it by reading and still succeeds.
        store.take();
        append_one(&log, &writer, 3).await.unwrap();
        assert!(!store.take().reads.is_empty());
        append_one(&log, &writer, 4).await.unwrap();
        assert!(store.take().reads.is_empty());
        let cut = log.capture(&namespace).await.unwrap();
        assert_eq!(cut.head().bounds(), (0, 5));
        drop((cut, writer, namespace));
        log.shutdown().await.unwrap();
        assert_eq!(log.owner.resources.bytes.available(), before);
    }
}

#[tokio::test]
async fn plan_range_walks_once_per_leaf_and_matches_read_for_every_record() {
    let directory = tempfile::tempdir().unwrap();
    let (log, store) = counting_log(directory.path(), "range");
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let empty = log.capture(&namespace).await.unwrap();
    assert!(log.plan_range(&empty, 0, 4).await.unwrap().is_empty());
    // Entries of one to three records, so offsets never align with entries.
    let mut expected = Vec::new();
    let mut next = 0u64;
    for n in 0..130u64 {
        let records = 1 + (n % 3) as u32;
        let bytes = payload(n);
        let identity = format!("range-{n}");
        let publication = log
            .prepare(
                &writer,
                &[NativeAppend {
                    identity: &identity,
                    payload: &bytes,
                    records,
                }],
            )
            .await
            .unwrap();
        log.submit(&publication).await.unwrap();
        log.advance_writer(&writer, &publication).await.unwrap();
        expected.push((next, records, bytes));
        next += u64::from(records);
    }
    let cut = log.capture(&namespace).await.unwrap();
    let height = usize::from(cut.head().index_root().unwrap().height);
    assert_eq!(height, 1);
    let mut covered = Vec::new();
    let mut offset = 0;
    let mut leaves = 0;
    while offset < next {
        store.take();
        let plans = log.plan_range(&cut, offset, usize::MAX).await.unwrap();
        // One root-to-leaf walk, however many entries the leaf holds.
        assert_eq!(store.take().reads.len(), height + 1, "offset {offset}");
        leaves += 1;
        assert!(plans.len() > 1 || offset + 1 >= next);
        let mut tasks = Vec::new();
        for plan in plans {
            let log = log.clone();
            tasks.push(tokio::spawn(async move {
                let payload = plan.payload_bytes();
                log.read_planned(plan, payload).await
            }));
        }
        for task in tasks {
            let record = task.await.unwrap().unwrap();
            offset = record.base_offset() + u64::from(record.record_count());
            covered.push((
                record.base_offset(),
                record.record_count(),
                record.bytes().to_vec(),
            ));
        }
    }
    assert_eq!(covered, expected);
    // 130 entries fill seven leaves: seven walks, not one per record.
    assert_eq!(leaves, 7);
    for (base, records, bytes) in &expected {
        for at in *base..*base + u64::from(*records) {
            let record = log.read(&cut, at).await.unwrap().unwrap();
            assert_eq!((record.base_offset(), record.bytes()), (*base, &bytes[..]));
        }
    }
    // Starting inside an entry yields that entry first, as plan_read does.
    let (base, records, _) = &expected[4];
    assert!(*records > 1);
    let plans = log.plan_range(&cut, base + 1, 3).await.unwrap();
    assert_eq!(plans.len(), 3);
    assert_eq!(plans[0].base_offset(), *base);
    let single = log.plan_read(&cut, base + 1).await.unwrap().unwrap();
    assert_eq!(single.base_offset(), plans[0].base_offset());
    assert_eq!(single.payload_bytes(), plans[0].payload_bytes());
    // The cut bounds the run: nothing at or past its end.
    assert!(log.plan_range(&cut, next, 4).await.unwrap().is_empty());
    assert!(
        log.plan_range(&cut, next + 1000, 4)
            .await
            .unwrap()
            .is_empty()
    );
    let tail = log.plan_range(&cut, next - 1, 100).await.unwrap();
    assert_eq!(tail.len(), 1);
    assert!(matches!(
        log.plan_range(&cut, 0, 0).await,
        Err(ObjectLogError::InvalidConfig(_))
    ));
    // A cut taken before later appends does not see them.
    let bytes = payload(999);
    let publication = log
        .prepare(
            &writer,
            &[NativeAppend {
                identity: "after-the-cut",
                payload: &bytes,
                records: 1,
            }],
        )
        .await
        .unwrap();
    log.submit(&publication).await.unwrap();
    log.advance_writer(&writer, &publication).await.unwrap();
    assert!(log.plan_range(&cut, next, 4).await.unwrap().is_empty());
    let (other, _) = counting_log(directory.path(), "range");
    assert!(other.plan_range(&cut, 0, 4).await.is_err());
    other.shutdown().await.unwrap();
    drop((plans, single, tail));
    log.shutdown().await.unwrap();
}
