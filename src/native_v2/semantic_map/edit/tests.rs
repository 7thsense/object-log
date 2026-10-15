use super::super::{
    codec::NodePage,
    range::Cursor,
    test_support::{fixture, resources},
};
use super::*;
use crate::{BlobStore, LocalBlobStore};
use std::collections::BTreeMap;

#[tokio::test]
async fn cow_exact_peak_credit_and_one_byte_short_are_decisive_before_write() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local.clone(), &resources);
    let ledger = resources.bytes.clone();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let peak = (LEAF_SLOTS + 1) * std::mem::size_of::<Entry>()
                + std::mem::size_of::<Node>()
                + (usize::from(MAX_HEIGHT) + 1) * std::mem::size_of::<FlushFrame>()
                + pages.allocation();
            let filler = ledger.charge(ledger.available() - peak)?;
            let root = apply(
                &pages,
                None,
                &[Edit {
                    key: b"a",
                    value: Some(b"v"),
                }],
                1,
                2,
                &mut WorkBudget::new(2, 10),
            )
            .await?
            .unwrap();
            assert_eq!(ledger.available(), peak);
            drop(filler);
            let before = local.list("map").await?.len();
            let filler = ledger.charge(ledger.available() - (peak - 1))?;
            assert!(matches!(
                apply(
                    &pages,
                    None,
                    &[Edit {
                        key: b"b",
                        value: Some(b"v")
                    }],
                    1,
                    2,
                    &mut WorkBudget::new(2, 10)
                )
                .await,
                Err(ObjectLogError::BudgetExceeded(_))
            ));
            assert_eq!(ledger.available(), peak - 1);
            assert_eq!(local.list("map").await?.len(), before);
            drop(filler);
            assert_eq!(
                read::lookup(&pages, Some(root), b"a", &mut WorkBudget::new(100, 100))
                    .await?
                    .unwrap()
                    .as_slice(),
                b"v"
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn partial_flush_failure_returns_no_root_and_preserves_original_with_released_credit() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local.clone(), &resources);
    let ledger = resources.bytes.clone();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(100_000, 1 << 24);
            let oracle: BTreeMap<Vec<u8>, Vec<u8>> = (0u32..1000)
                .map(|n| (n.to_be_bytes().to_vec(), n.to_be_bytes().to_vec()))
                .collect();
            let inserts: Vec<_> = oracle
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            let original = apply(&pages, None, &inserts, 2000, 1 << 20, &mut work)
                .await?
                .unwrap();
            let nodes = tree_nodes(&pages, original, &mut work).await?;
            let before = ledger.available();
            let objects = local.list("map").await?.len();
            let replacements: Vec<_> = oracle
                .keys()
                .map(|key| Edit {
                    key,
                    value: Some(b"changed"),
                })
                .collect();
            let result = apply(
                &pages,
                Some(original),
                &replacements,
                2000,
                1 << 20,
                &mut WorkBudget::new(nodes as u64 + 1, 1 << 24),
            )
            .await;
            assert!(matches!(result, Err(ObjectLogError::BudgetExceeded(_))));
            assert_eq!(ledger.available(), before);
            assert_eq!(
                local.list("map").await?.len(),
                objects + 1,
                "one final leaf written before deliberate flush failure"
            );
            compare_oracle(&pages, Some(original), &oracle, &mut work).await?;
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn caller_cancellation_during_flush_holds_physical_and_dirty_credit_until_drain() {
    use super::super::test_support::GatedStore;
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, Ordering},
        task::Poll,
        time::Duration,
    };
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let gate = GatedStore::new(local);
    let resources = resources();
    let pages = fixture(gate.clone(), &resources);
    let worker = pages.clone();
    let original = resources
        .run(move |physical| async move {
            let pages = worker.in_operation(physical)?;
            let values: BTreeMap<Vec<u8>, Vec<u8>> = (0u32..100)
                .map(|n| (n.to_be_bytes().to_vec(), vec![1]))
                .collect();
            let edits: Vec<_> = values
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            apply(
                &pages,
                None,
                &edits,
                100,
                1000,
                &mut WorkBudget::new(1000, 1 << 20),
            )
            .await
        })
        .await
        .unwrap()
        .unwrap();
    gate.arm();
    let done = Arc::new(AtomicBool::new(false));
    let completed = done.clone();
    let worker = pages.clone();
    let owner = resources.clone();
    let (send, receive) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(async move {
        owner
            .run(move |physical| async move {
                send.send(Arc::downgrade(&physical)).unwrap();
                let pages = worker.in_operation(physical)?;
                let keys: Vec<_> = (0u32..100).map(|n| n.to_be_bytes()).collect();
                let edits: Vec<_> = keys
                    .iter()
                    .map(|key| Edit {
                        key,
                        value: Some(b"changed"),
                    })
                    .collect();
                let mut work = WorkBudget::new(1000, 1 << 20);
                let new = apply(&pages, Some(original), &edits, 100, 2000, &mut work).await?;
                assert_ne!(new, Some(original));
                assert_eq!(
                    read::lookup(&pages, Some(original), &0u32.to_be_bytes(), &mut work)
                        .await?
                        .unwrap()
                        .as_slice(),
                    [1]
                );
                completed.store(true, Ordering::Release);
                Ok(new)
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), gate.entered())
        .await
        .unwrap();
    let physical = receive.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(!done.load(Ordering::Acquire));
    assert!(physical.upgrade().is_some());
    assert!(resources.bytes.available() < 4 << 20);
    let mut close = Box::pin(resources.close_and_drain());
    std::future::poll_fn(|cx| {
        assert!(close.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    gate.release();
    tokio::time::timeout(Duration::from_secs(10), close)
        .await
        .unwrap()
        .unwrap();
    assert!(done.load(Ordering::Acquire));
    assert!(physical.upgrade().is_none());
    assert_eq!(resources.bytes.available(), 4 << 20);
}

async fn compare_oracle(
    pages: &PageStore,
    root: Option<Descriptor>,
    oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
    work: &mut WorkBudget,
) -> Result<(), ObjectLogError> {
    let mut cursor = Cursor::start(pages, root, None, None, work).await?;
    let mut actual = Vec::new();
    while let Some(item) = cursor.next(pages, work).await? {
        actual.push((item.key.as_slice().to_vec(), item.value.as_slice().to_vec()));
    }
    assert_eq!(
        actual,
        oracle
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    );
    for (key, value) in oracle.iter().step_by(37) {
        assert_eq!(
            read::lookup(pages, root, key, work)
                .await?
                .unwrap()
                .as_slice(),
            value
        );
    }
    Ok(())
}

async fn tree_nodes(
    pages: &PageStore,
    root: Descriptor,
    work: &mut WorkBudget,
) -> Result<usize, ObjectLogError> {
    let mut stack = vec![(root, true)];
    let mut count = 0;
    while let Some((descriptor, is_root)) = stack.pop() {
        work.page()?;
        let page = NodePage::read(pages, descriptor).await?;
        count += 1;
        let minimum = if descriptor.height == 0 {
            LEAF_MIN
        } else if is_root {
            2
        } else {
            BRANCH_MIN
        };
        assert!(
            page.slots()
                >= if is_root && descriptor.height == 0 {
                    1
                } else {
                    minimum
                }
        );
        if descriptor.height > 0 {
            for slot in 0..page.slots() {
                stack.push((page.child(slot)?.descriptor, false));
            }
        }
    }
    Ok(count)
}

// This workload's keys/values are inline: every new object must be a node in
// the final graph, including replacement/deletion batches and historical reuse.
async fn assert_final_graph_writes(
    pages: &PageStore,
    local: &LocalBlobStore,
    before: Vec<String>,
    root: Option<Descriptor>,
    work: &mut WorkBudget,
) -> Result<(), ObjectLogError> {
    use std::collections::BTreeSet;
    let suffixes = |keys: Vec<String>| {
        keys.into_iter()
            .map(|key| key.rsplit('/').next().unwrap().to_owned())
            .collect::<BTreeSet<_>>()
    };
    let before = suffixes(before);
    let after = suffixes(local.list("map").await?);
    let mut reachable = BTreeSet::new();
    let mut stack: Vec<_> = root.into_iter().collect();
    while let Some(descriptor) = stack.pop() {
        reachable.insert(
            descriptor
                .hash
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        );
        work.page()?;
        let page = NodePage::read(pages, descriptor).await?;
        if descriptor.height > 0 {
            for slot in 0..page.slots() {
                stack.push(page.child(slot)?.descriptor);
            }
        }
    }
    assert_eq!(
        after.difference(&before).collect::<Vec<_>>(),
        reachable.difference(&before).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn batched_cow_splits_replaces_deletes_collapses_and_preserves_old_roots() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local.clone(), &resources);
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(1_000_000, 1 << 28);
            let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = (0u32..2500)
                .map(|n| {
                    (
                        n.to_be_bytes().to_vec(),
                        n.wrapping_mul(7).to_be_bytes().to_vec(),
                    )
                })
                .collect();
            let inserts: Vec<_> = oracle
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            let original = apply(&pages, None, &inserts, 10_000, 1 << 20, &mut work)
                .await?
                .unwrap();
            assert!(original.height >= 2);
            assert_eq!(original.entries, 2500);
            let nodes = tree_nodes(&pages, original, &mut work).await?;
            assert_eq!(
                local.list("map").await?.len(),
                nodes,
                "only final dirty nodes may be written"
            );
            compare_oracle(&pages, Some(original), &oracle, &mut work).await?;
            let old_oracle = oracle.clone();
            let old_cursor = Cursor::start(&pages, Some(original), None, None, &mut work).await?;
            let before = local.list("map").await?.len();
            let noop = apply(&pages, Some(original), &inserts, 10_000, 1 << 20, &mut work).await?;
            assert_eq!(noop, Some(original));
            assert_eq!(local.list("map").await?.len(), before);
            drop(inserts);
            let replacements: BTreeMap<_, _> = oracle
                .keys()
                .step_by(3)
                .cloned()
                .map(|key| (key, vec![0, 255, 0]))
                .collect();
            let edits: Vec<_> = replacements
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            let before = local.list("map").await?;
            let mut root =
                apply(&pages, Some(original), &edits, 10_000, 1 << 20, &mut work).await?;
            assert_final_graph_writes(&pages, &local, before, root, &mut work).await?;
            oracle.extend(replacements);
            compare_oracle(&pages, root, &oracle, &mut work).await?;
            tree_nodes(&pages, root.unwrap(), &mut work).await?;
            let remove: Vec<_> = oracle
                .keys()
                .filter(|key| u32::from_be_bytes(key.as_slice().try_into().unwrap()) % 4 != 0)
                .cloned()
                .collect();
            let deletes: Vec<_> = remove.iter().map(|key| Edit { key, value: None }).collect();
            let before = local.list("map").await?;
            root = apply(&pages, root, &deletes, 10_000, 1 << 20, &mut work).await?;
            assert_final_graph_writes(&pages, &local, before, root, &mut work).await?;
            for key in remove {
                oracle.remove(&key);
            }
            compare_oracle(&pages, root, &oracle, &mut work).await?;
            tree_nodes(&pages, root.unwrap(), &mut work).await?;
            let remove: Vec<_> = oracle.keys().cloned().collect();
            let deletes: Vec<_> = remove.iter().map(|key| Edit { key, value: None }).collect();
            let before = local.list("map").await?;
            root = apply(&pages, root, &deletes, 10_000, 1 << 20, &mut work).await?;
            assert_final_graph_writes(&pages, &local, before, root, &mut work).await?;
            assert_eq!(root, None);
            oracle.clear();
            compare_oracle(&pages, root, &oracle, &mut work).await?;
            compare_oracle(&pages, Some(original), &old_oracle, &mut work).await?;
            let mut old_cursor = old_cursor;
            assert_eq!(
                old_cursor
                    .next(&pages, &mut work)
                    .await?
                    .unwrap()
                    .value
                    .as_slice(),
                old_oracle.first_key_value().unwrap().1
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn cow_full_byte_keys_large_values_preserve_replaced_and_untouched_data() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(1_000_000, 1 << 28);
            let mut oracle = BTreeMap::from([
                (vec![], vec![]),
                (vec![0], vec![0]),
                (vec![0, 0], vec![255]),
                (vec![255], vec![0, 255]),
            ]);
            for n in 0u32..80 {
                let mut key = vec![0; 3993];
                key.extend_from_slice(&n.to_be_bytes());
                oracle.insert(key, vec![n as u8; 7920]);
            }
            let inserts: Vec<_> = oracle
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            let original = apply(&pages, None, &inserts, 1000, 1 << 22, &mut work)
                .await?
                .unwrap();
            compare_oracle(&pages, Some(original), &oracle, &mut work).await?;
            drop(inserts);
            let edits: BTreeMap<_, _> = oracle
                .keys()
                .step_by(2)
                .cloned()
                .map(|key| (key, vec![1, 0, 255]))
                .collect();
            let replacements: Vec<_> = edits
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            let root = apply(
                &pages,
                Some(original),
                &replacements,
                1000,
                1 << 22,
                &mut work,
            )
            .await?;
            oracle.extend(edits);
            compare_oracle(&pages, root, &oracle, &mut work).await?;
            tree_nodes(&pages, root.unwrap(), &mut work).await?;
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn cow_preflight_is_complete_before_immutable_writes_and_failures_release_credit() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local.clone(), &resources);
    let ledger = resources.bytes.clone();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let before = ledger.available();
            let value = vec![7; 8000];
            let sorted = [
                Edit {
                    key: b"a",
                    value: Some(&value),
                },
                Edit {
                    key: b"b",
                    value: Some(&value),
                },
            ];
            assert!(
                apply(
                    &pages,
                    None,
                    &sorted,
                    1,
                    u64::MAX,
                    &mut WorkBudget::new(100, 100_000)
                )
                .await
                .is_err()
            );
            assert!(
                apply(
                    &pages,
                    None,
                    &sorted,
                    2,
                    100,
                    &mut WorkBudget::new(100, 100_000)
                )
                .await
                .is_err()
            );
            let unsorted = [
                Edit {
                    key: b"b",
                    value: Some(&value),
                },
                Edit {
                    key: b"a",
                    value: Some(&value),
                },
            ];
            assert!(
                apply(
                    &pages,
                    None,
                    &unsorted,
                    2,
                    u64::MAX,
                    &mut WorkBudget::new(100, 100_000)
                )
                .await
                .is_err()
            );
            let duplicate = [
                Edit {
                    key: b"a",
                    value: Some(&value),
                },
                Edit {
                    key: b"a",
                    value: Some(&value),
                },
            ];
            assert!(
                apply(
                    &pages,
                    None,
                    &duplicate,
                    2,
                    u64::MAX,
                    &mut WorkBudget::new(100, 100_000)
                )
                .await
                .is_err()
            );
            assert!(local.list("map").await?.is_empty());
            assert_eq!(ledger.available(), before);
            assert!(
                apply(
                    &pages,
                    None,
                    &sorted,
                    2,
                    u64::MAX,
                    &mut WorkBudget::new(0, 100_000)
                )
                .await
                .is_err()
            );
            assert!(local.list("map").await?.is_empty());
            assert_eq!(ledger.available(), before);
            let inserts: BTreeMap<_, _> = (0u32..100)
                .map(|n| (n.to_be_bytes().to_vec(), vec![1]))
                .collect();
            let edits: Vec<_> = inserts
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            assert!(
                apply(
                    &pages,
                    None,
                    &edits,
                    100,
                    1000,
                    &mut WorkBudget::new(1, 100_000)
                )
                .await
                .is_err()
            );
            assert_eq!(ledger.available(), before);
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn repeated_sparse_middle_edits_match_independent_oracle_and_occupancy() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(2_000_000, 1 << 30);
            let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = (0u32..3000)
                .step_by(2)
                .map(|n| (n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()))
                .collect();
            let inserts: Vec<_> = oracle
                .iter()
                .map(|(key, value)| Edit {
                    key,
                    value: Some(value),
                })
                .collect();
            let mut root = apply(&pages, None, &inserts, 10_000, 1 << 20, &mut work).await?;
            drop(inserts);
            let mut random = 0x6f71_a34d_0b82_591du64;
            let mut retained = Vec::new();
            for round in 0..80 {
                let mut changes = BTreeMap::new();
                for _ in 0..32 {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    let key = ((random % 6000) as u32).to_be_bytes().to_vec();
                    let value = if random.is_multiple_of(5) {
                        None
                    } else {
                        Some(random.to_be_bytes().to_vec())
                    };
                    changes.insert(key, value);
                }
                let edits: Vec<_> = changes
                    .iter()
                    .map(|(key, value)| Edit {
                        key,
                        value: value.as_deref(),
                    })
                    .collect();
                root = apply(&pages, root, &edits, 10_000, 1 << 20, &mut work).await?;
                for (key, value) in changes {
                    if let Some(value) = value {
                        oracle.insert(key, value);
                    } else {
                        oracle.remove(&key);
                    }
                }
                compare_oracle(&pages, root, &oracle, &mut work).await?;
                tree_nodes(&pages, root.unwrap(), &mut work).await?;
                if round % 20 == 0 {
                    retained.push((root, oracle.clone()));
                }
            }
            for (root, oracle) in retained {
                compare_oracle(&pages, root, &oracle, &mut work).await?;
            }
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn rebalance_uses_original_source_bounds_after_minimum_key_deletion() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    resources.run(move|physical|async move{
        let pages=pages.in_operation(physical)?;let mut work=WorkBudget::new(10_000,1<<20);let value=ByteRef::inline(b"v")?;
        let mut good:BTreeMap<Vec<u8>,Vec<u8>>=(0u32..=50).map(|n|(n.to_be_bytes().to_vec(),vec![1])).collect();
        let inserts:Vec<_>=good.iter().map(|(key,value)|Edit{key,value:Some(value)}).collect();
        let good_root=apply(&pages,None,&inserts,100,1000,&mut work).await?;drop(inserts);
        let a=25u32.to_be_bytes();let b=26u32.to_be_bytes();let deletes=[Edit{key:&a,value:None},Edit{key:&b,value:None}];
        let after=apply(&pages,good_root,&deletes,2,8,&mut work).await?.unwrap();assert_eq!(after.height,0);assert_eq!(after.entries,49);
        good.remove(a.as_slice());good.remove(b.as_slice());compare_oracle(&pages,Some(after),&good,&mut work).await?;
        // Recommit a malformed source tree: left improperly includes the old
        // right separator. Only old [0,25) bounds expose that corruption.
        let first=ByteRef::inline(&0u32.to_be_bytes())?;let separator=ByteRef::inline(&25u32.to_be_bytes())?;
        let left:Vec<_>=(0u32..=25).map(|n|Entry{key:ByteRef::inline(&n.to_be_bytes()).unwrap(),value}).collect();
        let right:Vec<_>=(25u32..=50).map(|n|Entry{key:ByteRef::inline(&n.to_be_bytes()).unwrap(),value}).collect();
        let(encoded,left)=codec::leaf(&pages,&left)?;pages.write_scoped_bytes(encoded).await?;let(encoded,right)=codec::leaf(&pages,&right)?;pages.write_scoped_bytes(encoded).await?;
        let(encoded,root)=codec::branch(&pages,&[Child{first,descriptor:left},Child{first:separator,descriptor:right}])?;pages.write_scoped_bytes(encoded).await?;
        let a=25u32.to_be_bytes();let b=26u32.to_be_bytes();let edits=[Edit{key:&a,value:None},Edit{key:&b,value:None}];
        assert!(matches!(apply(&pages,Some(root),&edits,2,8,&mut work).await,Err(ObjectLogError::Sequencer(reason))if reason.contains("escapes inherited upper interval")));
        Ok(())
    }).await.unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}
