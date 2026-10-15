use super::super::*;
use crate::{
    BlobStore,
    native_v2::{
        codec::{Descriptor, Kind, LeafEntry},
        index::Index,
        semantic_map::TestGatedStore,
    },
};

fn limits() -> NativeLimits {
    NativeLimits {
        workers: 2,
        blocking_workers: 2,
        operations: 2,
        bytes: 32 << 20,
        version_bytes: 4096,
        binding_bytes: 4096,
        group_entries: 1,
        group_bytes: 1 << 20,
    }
}

fn within(reads: &[(String, usize)], requirements: NativeReadRequirements) {
    assert!(
        reads.len() as u64 <= requirements.hydration_reads(),
        "{reads:?}"
    );
    assert!(
        reads.iter().map(|(_, bytes)| *bytes as u64).sum::<u64>() <= requirements.hydration_bytes()
    );
}

async fn plan(
    log: &NativeLog,
    provider: &TestGatedStore,
    coupled: bool,
    identity: &str,
) -> NativeReadPlan {
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let input = [NativeAppend {
        identity,
        payload: b"x",
        records: 1,
    }];
    let original = if coupled {
        let empty = log.capture(&namespace).await.unwrap();
        log.prepare_with_edits(
            &writer,
            &empty,
            &input,
            &[],
            NativeEditLimits {
                edits: 0,
                input_bytes: 0,
                work: NativeSemanticWork {
                    pages: 100,
                    hydrated_bytes: 10000,
                },
            },
        )
        .await
        .unwrap()
    } else {
        log.prepare(&writer, &input).await.unwrap()
    };
    assert!(matches!(
        log.submit(&original).await.unwrap(),
        NativePublishOutcome::Stored(_)
    ));
    let cut = log.capture(&namespace).await.unwrap();
    provider.take_reads();
    let plan = log.plan_read(&cut, 0).await.unwrap().unwrap();
    let index_reads = provider.take_reads();
    assert!(!index_reads.is_empty());
    assert!(index_reads.len() <= 17);
    assert!(index_reads.iter().all(|(_, bytes)| *bytes <= 4096));
    plan
}

#[tokio::test]
async fn sealed_read_requirements_cover_actual_legacy_and_coupled_hydration() {
    for coupled in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        let provider = TestGatedStore::new(local.clone());
        // Many short path components also exercise physical identity funding
        // without relying on a filesystem accepting oversized single components.
        let log = NativeLog::new(
            provider.clone(),
            format!("{}queue", "long-prefix/".repeat(100)),
            limits(),
        )
        .unwrap();
        let identity = "€".repeat(4000);
        let plan = plan(&log, &provider, coupled, &identity).await;
        let requirements = plan.requirements().unwrap();
        assert_eq!(requirements.logical_identity_bytes(), identity.len() as u64);
        assert!(requirements.physical_identity_bytes() > 1000);
        assert!(requirements.identity_pages() > 2);
        assert_eq!(requirements.validation_pages(), u64::from(coupled));
        let expected_frame = 24 + identity.len() as u64 + 1 + if coupled { 224 } else { 0 };
        assert_eq!(requirements.frame_bytes(), expected_frame);
        assert!(provider.take_reads().is_empty(), "getters perform no I/O");
        let record = log.read_planned(plan, 1).await.unwrap();
        let reads = provider.take_reads();
        within(&reads, requirements);
        assert_eq!(reads.len() as u64, requirements.hydration_reads());
        let frame_read = reads
            .iter()
            .find(|(_, cap)| *cap as u64 == expected_frame)
            .unwrap();
        assert_eq!(frame_read.1 as u64, requirements.frame_bytes());
        assert_eq!(
            frame_read.0.len() as u64,
            requirements.physical_identity_bytes()
        );
        assert_eq!(record.bytes(), b"x");
        log.shutdown().await.unwrap();
    }
}

/// Construct authenticated leaf/overflow objects using the real page codec and
/// lookup. This deliberately exercises valid noncanonical chain packing which
/// the public writer does not currently emit; it grants no public constructor.
async fn one_byte_chain(
    log: &NativeLog,
    local: &LocalBlobStore,
    mut plan: NativeReadPlan,
    identity: &str,
    mismatch: Option<&'static str>,
) -> (NativeReadPlan, String, String, String) {
    let owner = log.owner.clone();
    let identity = identity.to_owned();
    let (plan, tail_hash, root_hash, frame_key) = log
        .run(move |physical| async move {
            let pages = owner
                .pages
                .in_operation(physical)?
                .for_binding(&plan.binding)?;
            let mut next = None;
            let mut tail = None;
            for (offset, byte) in identity.bytes().enumerate().rev() {
                let descriptor = pages
                    .write(pages.overflow(offset as u64, &[byte], next)?)
                    .await?;
                tail.get_or_insert(descriptor.hash);
                next = Some(descriptor);
            }
            let mut entry = *plan.entry.entry();
            entry.logical_id = next.unwrap();
            let root_hash = entry.logical_id.hash;
            match mismatch {
                Some("length-mismatch") => entry.logical_id.end += 1,
                Some("count-mismatch") => entry.logical_id.entries -= 1,
                _ => (),
            }
            let root = pages.write(pages.leaf(&[entry])?).await?;
            plan.entry = Index::new(pages.clone())
                .lookup(Some(root), 0)
                .await?
                .unwrap();
            let frame_key = pages.data_key(&identity)?.as_str().to_owned();
            Ok((plan, tail.unwrap(), root_hash, frame_key))
        })
        .await
        .unwrap();
    let keys = local.list("").await.unwrap();
    let key_for = |hash: [u8; 32]| {
        let suffix: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
        let found: Vec<_> = keys.iter().filter(|key| key.ends_with(&suffix)).collect();
        assert_eq!(found.len(), 1);
        found[0].clone()
    };
    (plan, key_for(tail_hash), key_for(root_hash), frame_key)
}

#[tokio::test]
async fn sealed_read_requirements_bound_valid_one_byte_identity_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let provider = TestGatedStore::new(local.clone());
    let log = NativeLog::new(provider.clone(), "one-byte".into(), limits()).unwrap();
    let identity = "€€€";
    let original = plan(&log, &provider, false, identity).await;
    let (plan, _, _, _) = one_byte_chain(&log, &local, original, identity, None).await;
    provider.take_reads();
    let requirements = plan.requirements().unwrap();
    assert_eq!(requirements.identity_pages(), identity.len() as u64 + 1);
    assert!(provider.take_reads().is_empty());
    assert_eq!(log.read_planned(plan, 1).await.unwrap().bytes(), b"x");
    let reads = provider.take_reads();
    within(&reads, requirements);
    assert_eq!(reads.len() as u64, requirements.hydration_reads());
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn sealed_read_requirements_cover_failure_prefixes() {
    for fault in [
        "missing-tail",
        "corrupt-tail",
        "length-mismatch",
        "count-mismatch",
        "oversized-frame",
        "payload-limit",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        let provider = TestGatedStore::new(local.clone());
        let log = NativeLog::new(provider.clone(), format!("fault-{fault}"), limits()).unwrap();
        let identity = "€€€";
        let original = plan(&log, &provider, false, identity).await;
        let (plan, tail, root, frame) =
            one_byte_chain(&log, &local, original, identity, Some(fault)).await;
        match fault {
            "missing-tail" => local.delete(&tail).await.unwrap(),
            "corrupt-tail" => local
                .put(&tail, bytes::Bytes::from_static(b"corrupt"))
                .await
                .unwrap(),
            "oversized-frame" => {
                let mut raw = local.get(&frame).await.unwrap().unwrap().to_vec();
                raw.push(0);
                local.put(&frame, raw.into()).await.unwrap();
            }
            _ => (),
        }
        provider.take_reads();
        let requirements = plan.requirements().unwrap();
        assert!(provider.take_reads().is_empty());
        assert!(
            log.read_planned(plan, usize::from(fault != "payload-limit"))
                .await
                .is_err(),
            "{fault}"
        );
        let reads = provider.take_reads();
        within(&reads, requirements);
        match fault {
            "payload-limit" => assert!(reads.is_empty()),
            "length-mismatch" | "count-mismatch" => {
                assert_eq!(reads.len(), 1, "{fault}");
                assert_eq!(reads[0].0, root);
            }
            "missing-tail" | "corrupt-tail" => {
                assert_eq!(reads.len(), identity.len(), "{fault}");
                assert_eq!(reads.last().unwrap().0, tail);
            }
            "oversized-frame" => {
                assert_eq!(reads.len(), identity.len() + 2);
                assert_eq!(reads.last().unwrap().0, frame);
                assert_eq!(reads.last().unwrap().1 as u64, requirements.frame_bytes());
            }
            _ => unreachable!(),
        }
        log.shutdown().await.unwrap();
    }
}

fn descriptor(length: u64, entries: u64) -> Descriptor {
    Descriptor {
        hash: [1; 32],
        encoded_len: 177,
        kind: Kind::Overflow,
        height: 0,
        start: 0,
        end: length,
        entries,
    }
}

#[test]
fn sealed_read_requirements_reject_checked_overflow_and_invalid_descriptors() {
    let mut entry = LeafEntry {
        object_id: descriptor(i64::MAX as u64, i64::MAX as u64),
        logical_id: descriptor(i64::MAX as u64, i64::MAX as u64),
        base: 0,
        record_count: 1,
        byte_start: 10,
        byte_len: 1,
        payload_hash: [2; 32],
        frame_kind: crate::native_v2::codec::FrameKind::Coupled,
    };
    assert!(matches!(
        NativeReadRequirements::from_entry(&entry),
        Err(ObjectLogError::BudgetExceeded(_))
    ));
    // Representable call count, but metadata-byte multiplication overflows.
    entry.frame_kind = crate::native_v2::codec::FrameKind::Legacy;
    let pages = u64::MAX / 4096 + 1;
    entry.logical_id = descriptor(pages - 1, pages - 1);
    entry.object_id = descriptor(1, 1);
    assert!(matches!(
        NativeReadRequirements::from_entry(&entry),
        Err(ObjectLogError::BudgetExceeded(_))
    ));
    // The page product fits; adding the independently authenticated frame does not.
    let pages = u64::MAX / 4096;
    entry.logical_id = descriptor(pages - 1, pages - 1);
    entry.byte_start = 4096;
    assert!(matches!(
        NativeReadRequirements::from_entry(&entry),
        Err(ObjectLogError::BudgetExceeded(_))
    ));
    entry.object_id = descriptor(1, 2);
    assert!(NativeReadRequirements::from_entry(&entry).is_err());
}

#[tokio::test]
async fn sealed_read_requirements_survive_drain_without_granting_foreign_authority() {
    let directory = tempfile::tempdir().unwrap();
    let provider = TestGatedStore::new(Arc::new(LocalBlobStore::new(directory.path())));
    let log = NativeLog::new(provider.clone(), "drained-plan".into(), limits()).unwrap();
    let plan = plan(&log, &provider, false, "retained").await;
    let requirements = plan.requirements().unwrap();
    log.shutdown().await.unwrap();
    assert_eq!(plan.requirements().unwrap(), requirements);
    assert!(provider.take_reads().is_empty());
    let foreign_directory = tempfile::tempdir().unwrap();
    let foreign_provider =
        TestGatedStore::new(Arc::new(LocalBlobStore::new(foreign_directory.path())));
    let foreign =
        NativeLog::new(foreign_provider.clone(), "foreign-plan".into(), limits()).unwrap();
    assert!(matches!(foreign.read_planned(plan, 1).await,
        Err(ObjectLogError::InvalidBatch(reason)) if reason.contains("native handle belongs to another store")));
    assert!(foreign_provider.take_reads().is_empty());
    assert!(provider.take_reads().is_empty());
    foreign.shutdown().await.unwrap();
}
