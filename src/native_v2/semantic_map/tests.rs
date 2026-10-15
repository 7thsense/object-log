use super::read;
use super::test_support::{fixture, resources, scope};
use super::{bytes, codec::*, *};
use crate::BlobStore;
use crate::{LocalBlobStore, MetadataLimits, ObjectLogError, native_v2::page_store::PageStore};
use std::sync::Arc;

#[test]
fn byte_reference_canonical_inline_and_descriptor_boundaries() {
    for value in [vec![], vec![0], vec![0, 255, 0], vec![7; 31], vec![8; 32]] {
        let reference = ByteRef::inline(&value).unwrap();
        let mut raw = [0; REF_BYTES];
        reference.encode(&mut raw);
        assert_eq!(ByteRef::decode(&raw).unwrap(), reference);
        assert_eq!(reference.inline_bytes(), Some(value.as_slice()));
        if value.len() < 32 {
            raw[39] = 1;
            assert!(ByteRef::decode(&raw).is_err());
        }
    }
    assert!(ByteRef::inline(&[0; 33]).is_err());
    assert!(ByteRef::overflow(32, [1; 32]).is_err());
    assert!(ByteRef::overflow(33, [0; 32]).is_err());
    let maximum = Descriptor {
        hash: [1; 32],
        encoded_len: (HEADER + LEAF_SLOTS * REF_BYTES * 2) as u16,
        height: 0,
        entries: LEAF_SLOTS as u64,
    };
    assert_eq!(maximum.validate().unwrap(), LEAF_SLOTS);
    assert!(
        Descriptor {
            entries: maximum.entries + 1,
            ..maximum
        }
        .validate()
        .is_err()
    );
    assert!(
        Descriptor {
            height: MAX_HEIGHT + 1,
            ..maximum
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
async fn canonical_byte_chains_cover_inline_chunks_tails_and_work_exhaustion() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    assert!(
        bytes::store(&pages, b"", &mut WorkBudget::new(0, 0))
            .await
            .is_err()
    );
    let retained = resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(1000, 1 << 20);
            for len in [0, 1, 31, 32, 33, 3959, 3960, 3961, 3992, 3993, 7920] {
                let value: Vec<u8> = (0..len).map(|n| (n % 256) as u8).collect();
                let reference = bytes::store(&pages, &value, &mut work).await?;
                assert_eq!(reference.len(), len as u64);
                assert_eq!(
                    bytes::load(&pages, reference, &mut work).await?.as_slice(),
                    value
                );
                if len > 32 {
                    assert!(matches!(
                        bytes::load(&pages, reference, &mut WorkBudget::new(0, len as u64)).await,
                        Err(ObjectLogError::BudgetExceeded(_))
                    ));
                }
                if len > 0 {
                    assert!(matches!(
                        bytes::load(
                            &pages,
                            reference,
                            &mut WorkBudget::new(1000, len as u64 - 1)
                        )
                        .await,
                        Err(ObjectLogError::BudgetExceeded(_))
                    ));
                }
            }
            let reference = bytes::store(&pages, b"retained after drain", &mut work).await?;
            bytes::load(&pages, reference, &mut work).await
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(retained.as_slice(), b"retained after drain");
    assert!(resources.bytes.available() < 4 << 20);
    drop(retained);
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn map_leaf_and_branch_roundtrip_shape_and_old_page_retention() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    let retained = resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let key = ByteRef::inline(b"\0a").unwrap();
            let value = ByteRef::inline(b"\xff\0").unwrap();
            let (encoded, descriptor) = leaf(&pages, &[Entry { key, value }])?;
            assert_eq!(pages.write_scoped_bytes(encoded).await?, descriptor.hash);
            let old = NodePage::read(&pages, descriptor).await?;
            assert_eq!(old.entry(0)?, Entry { key, value });
            assert!(old.entry(1).is_err());
            assert!(old.child(0).is_err());
            let second_key = ByteRef::inline(b"\0a\0").unwrap();
            let (encoded, second) = leaf(
                &pages,
                &[Entry {
                    key: second_key,
                    value,
                }],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            let (encoded, parent) = branch(
                &pages,
                &[
                    Child {
                        first: key,
                        descriptor,
                    },
                    Child {
                        first: second_key,
                        descriptor: second,
                    },
                ],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            let node = NodePage::read(&pages, parent).await?;
            assert_eq!(node.slots(), 2);
            assert_eq!(node.child(1)?.descriptor, second);
            assert_eq!(node.descriptor().entries, 2);
            assert!(
                NodePage::read(
                    &pages,
                    Descriptor {
                        entries: 3,
                        ..parent
                    }
                )
                .await
                .is_err()
            );
            assert!(leaf(&pages, &[]).is_err());
            assert!(branch(&pages, &[]).is_err());
            Ok(old)
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(
        retained.entry(0).unwrap().key.inline_bytes(),
        Some(&b"\0a"[..])
    );
    drop(retained);
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn ordered_point_reads_validate_full_keys_child_intervals_and_absence() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(1000, 1 << 20);
            let value = ByteRef::inline(b"value")?;
            let first = ByteRef::inline(b"\0")?;
            let last = ByteRef::inline(b"\xff")?;
            let (encoded, left) = leaf(&pages, &[Entry { key: first, value }])?;
            pages.write_scoped_bytes(encoded).await?;
            let (encoded, right) = leaf(&pages, &[Entry { key: last, value }])?;
            pages.write_scoped_bytes(encoded).await?;
            let (encoded, root) = branch(
                &pages,
                &[
                    Child {
                        first,
                        descriptor: left,
                    },
                    Child {
                        first: last,
                        descriptor: right,
                    },
                ],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            for needle in [b"\0".as_slice(), b"\xff".as_slice()] {
                assert_eq!(
                    read::lookup(&pages, Some(root), needle, &mut work)
                        .await?
                        .unwrap()
                        .as_slice(),
                    b"value"
                );
            }
            for needle in [b"".as_slice(), b"\0\0".as_slice(), b"\xff\0".as_slice()] {
                assert!(
                    read::lookup(&pages, Some(root), needle, &mut work)
                        .await?
                        .is_none()
                );
            }
            assert!(
                read::lookup(&pages, None, b"anything", &mut WorkBudget::new(0, 0))
                    .await?
                    .is_none()
            );
            assert!(matches!(
                read::lookup(&pages, Some(root), b"\0", &mut WorkBudget::new(0, 100)).await,
                Err(ObjectLogError::BudgetExceeded(_))
            ));
            let wrong = ByteRef::inline(b"\xfe")?;
            let false_first = ByteRef::inline(b"\x01")?;
            let (encoded, bad_first) = branch(
                &pages,
                &[
                    Child {
                        first: false_first,
                        descriptor: left,
                    },
                    Child {
                        first: last,
                        descriptor: right,
                    },
                ],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            for needle in [b"".as_slice(), b"\0".as_slice()] {
                assert!(
                    read::lookup(&pages, Some(bad_first), needle, &mut work)
                        .await
                        .is_err()
                );
            }
            let (encoded, bad) = branch(
                &pages,
                &[
                    Child {
                        first,
                        descriptor: left,
                    },
                    Child {
                        first: wrong,
                        descriptor: right,
                    },
                ],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            assert!(
                read::lookup(&pages, Some(bad), b"\xff", &mut work)
                    .await
                    .is_err()
            );
            let (encoded, unsorted) = leaf(
                &pages,
                &[Entry { key: last, value }, Entry { key: first, value }],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            assert!(
                read::lookup(&pages, Some(unsorted), b"", &mut work)
                    .await
                    .is_err()
            );
            let (encoded, duplicate) = leaf(
                &pages,
                &[Entry { key: first, value }, Entry { key: first, value }],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            assert!(
                read::lookup(&pages, Some(duplicate), b"\0", &mut work)
                    .await
                    .is_err()
            );
            let (encoded, escaping) = leaf(
                &pages,
                &[Entry { key: first, value }, Entry { key: last, value }],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            let (encoded, bad) = branch(
                &pages,
                &[
                    Child {
                        first,
                        descriptor: escaping,
                    },
                    Child {
                        first: last,
                        descriptor: right,
                    },
                ],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            assert!(
                read::lookup(&pages, Some(bad), b"\0", &mut work)
                    .await
                    .is_err()
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn recommitted_malformed_pages_fail_structural_validation() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let empty = ByteRef::inline(b"")?;
            let (encoded, original) = leaf(
                &pages,
                &[Entry {
                    key: empty,
                    value: empty,
                }],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            let source = pages
                .read_scoped_bytes(&original.hash, original.encoded_len as usize)
                .await?;
            for offset in [72, 73, 74, 76, 84, 88, HEADER + REF_BYTES - 1] {
                let encoded = pages.encode_scoped_bytes(source.bytes().len(), |raw, _| {
                    raw.copy_from_slice(source.bytes());
                    raw[offset] ^= 1;
                    Ok(())
                })?;
                let bad = Descriptor {
                    hash: encoded.hash(),
                    ..original
                };
                pages.write_scoped_bytes(encoded).await?;
                assert!(
                    NodePage::read(&pages, bad).await.is_err(),
                    "recommitted malformed byte {offset}"
                );
            }
            let child = Child {
                first: empty,
                descriptor: original,
            };
            let (encoded, parent) = branch(&pages, &[child, child])?;
            pages.write_scoped_bytes(encoded).await?;
            let source = pages
                .read_scoped_bytes(&parent.hash, parent.encoded_len as usize)
                .await?;
            let encoded = pages.encode_scoped_bytes(source.bytes().len(), |raw, _| {
                raw.copy_from_slice(source.bytes());
                raw[76..84].copy_from_slice(&3u64.to_be_bytes());
                Ok(())
            })?;
            let bad = Descriptor {
                hash: encoded.hash(),
                entries: 3,
                ..parent
            };
            pages.write_scoped_bytes(encoded).await?;
            assert!(NodePage::read(&pages, bad).await.is_err());
            let encoded = pages.encode_scoped_bytes(source.bytes().len(), |raw, _| {
                raw.copy_from_slice(source.bytes());
                raw[HEADER + REF_BYTES + 34] = 1;
                Ok(())
            })?;
            let bad = Descriptor {
                hash: encoded.hash(),
                ..parent
            };
            pages.write_scoped_bytes(encoded).await?;
            assert!(NodePage::read(&pages, bad).await.is_err());
            let bogus = Descriptor {
                hash: [7; 32],
                encoded_len: (HEADER + 2 * (REF_BYTES + DESC_BYTES)) as u16,
                height: 1,
                entries: u64::MAX,
            };
            assert!(
                branch(
                    &pages,
                    &[
                        Child {
                            first: empty,
                            descriptor: bogus
                        },
                        Child {
                            first: empty,
                            descriptor: bogus
                        }
                    ]
                )
                .is_err()
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn overflow_tail_missing_corrupt_and_noncanonical_never_complete() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local.clone(), &resources);
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(1000, 1 << 20);
            let reference = bytes::store(&pages, &vec![9; 3993], &mut work).await?;
            let page = pages
                .read_scoped_bytes(&reference.hash().unwrap(), reference.encoded_page_len()?)
                .await?;
            let (_, tail) = blob_view(page.bytes(), scope(), reference)?;
            let hash = tail.hash().unwrap();
            let suffix: String = hash.iter().map(|b| format!("{b:02x}")).collect();
            let tail_key = local
                .list("map")
                .await?
                .into_iter()
                .find(|key| key.ends_with(&suffix))
                .unwrap();
            let original = local.get(&tail_key).await?.unwrap();
            let mut corrupted = original.to_vec();
            let end = corrupted.len() - 1;
            corrupted[end] ^= 1;
            local.put(&tail_key, corrupted.into()).await?;
            assert!(bytes::load(&pages, reference, &mut work).await.is_err());
            local.put(&tail_key, original).await?;
            local.delete(&tail_key).await?;
            assert!(matches!(
                bytes::load(&pages, reference, &mut work).await,
                Err(ObjectLogError::MissingObject(_))
            ));
            let short = bytes::store(&pages, &[4; 33], &mut work).await?;
            let original = pages
                .read_scoped_bytes(&short.hash().unwrap(), short.encoded_page_len()?)
                .await?;
            for offset in [72, 74, 76, 84, 88, HEADER + 7, HEADER + REF_BYTES - 1] {
                let encoded = pages.encode_scoped_bytes(original.bytes().len(), |raw, _| {
                    raw.copy_from_slice(original.bytes());
                    raw[offset] ^= 1;
                    Ok(())
                })?;
                let bad = ByteRef::overflow(short.len(), encoded.hash())?;
                pages.write_scoped_bytes(encoded).await?;
                assert!(bytes::load(&pages, bad, &mut work).await.is_err());
            }
            let encoded = pages.encode_scoped_bytes(original.bytes().len() + 1, |raw, _| {
                let len = original.bytes().len();
                raw[..len].copy_from_slice(original.bytes());
                raw[len] = 1;
                Ok(())
            })?;
            let trailing = ByteRef::overflow(short.len(), encoded.hash())?;
            pages.write_scoped_bytes(encoded).await?;
            assert!(bytes::load(&pages, trailing, &mut work).await.is_err());
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn scoped_carriers_bind_store_release_failed_encoders_and_retain_only_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local.clone(), &resources);
    let ledger = resources.bytes.clone();
    let outside = pages.clone();
    let encoded = resources
        .run(move |physical| async move {
            let foreign = PageStore::new(
                local.clone(),
                "foreign".into(),
                scope(),
                ledger.clone(),
                MetadataLimits {
                    payload_bytes: PAGE_BYTES,
                    version_bytes: 56,
                },
            )?
            .in_operation(physical.clone())?;
            let pages = pages.in_operation(physical)?;
            let before = ledger.available();
            assert!(
                pages
                    .encode_scoped_bytes(96, |_, _| Err(ObjectLogError::InvalidBatch(
                        "encoder rejected".into()
                    )))
                    .is_err()
            );
            assert_eq!(ledger.available(), before);
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || pages.encode_scoped_bytes(96, |_, _| panic!("encoder unwind"))
                ))
                .is_err()
            );
            assert_eq!(ledger.available(), before);
            let empty = ByteRef::inline(b"")?;
            let (encoded, _) = leaf(
                &pages,
                &[Entry {
                    key: empty,
                    value: empty,
                }],
            )?;
            assert!(foreign.write_scoped_bytes(encoded).await.is_err());
            assert!(local.list("foreign").await?.is_empty());
            assert_eq!(ledger.available(), before);
            let (encoded, _) = leaf(
                &pages,
                &[Entry {
                    key: empty,
                    value: empty,
                }],
            )?;
            Ok(encoded)
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert!(resources.bytes.available() < 4 << 20);
    assert!(outside.write_scoped_bytes(encoded).await.is_err());
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn ordered_ranges_match_independent_bounds_and_keep_original_root() {
    use std::collections::BTreeMap;
    use std::ops::Bound;
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local, &resources);
    let retained = resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(10000, 1 << 20);
            let keys: Vec<Vec<u8>> = vec![
                vec![],
                vec![0],
                vec![0, 0],
                b"a".to_vec(),
                b"a\0".to_vec(),
                vec![255],
                vec![255, 0],
            ];
            let oracle: BTreeMap<_, _> = keys
                .iter()
                .cloned()
                .map(|key| (key, b"original".to_vec()))
                .collect();
            let mut children = Vec::new();
            for chunk in keys.chunks(3) {
                let entries: Vec<_> = chunk
                    .iter()
                    .map(|key| Entry {
                        key: ByteRef::inline(key).unwrap(),
                        value: ByteRef::inline(b"original").unwrap(),
                    })
                    .collect();
                let (encoded, descriptor) = leaf(&pages, &entries)?;
                pages.write_scoped_bytes(encoded).await?;
                children.push(Child {
                    first: entries[0].key,
                    descriptor,
                });
            }
            let (encoded, root) = branch(&pages, &children)?;
            pages.write_scoped_bytes(encoded).await?;
            for (lower, upper) in [
                (None, None),
                (
                    Some((b"\0".as_slice(), true)),
                    Some((b"\xff".as_slice(), true)),
                ),
                (
                    Some((b"\0".as_slice(), false)),
                    Some((b"\xff".as_slice(), false)),
                ),
                (Some((b"a".as_slice(), true)), Some((b"a".as_slice(), true))),
                (
                    Some((b"a".as_slice(), false)),
                    Some((b"a".as_slice(), true)),
                ),
                (Some((b"\xff\0\0".as_slice(), true)), None),
            ] {
                let mut cursor =
                    super::range::Cursor::start(&pages, Some(root), lower, upper, &mut work)
                        .await?;
                assert_eq!(cursor.root(), Some(root));
                let bound = |value: Option<(&[u8], bool)>| match value {
                    None => Bound::Unbounded,
                    Some((value, true)) => Bound::Included(value.to_vec()),
                    Some((value, false)) => Bound::Excluded(value.to_vec()),
                };
                let expected: Vec<_> = if lower
                    .zip(upper)
                    .is_some_and(|(lower, upper)| lower.0 == upper.0 && (!lower.1 || !upper.1))
                {
                    vec![]
                } else {
                    oracle
                        .range((bound(lower), bound(upper)))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                };
                let mut actual = Vec::new();
                while let Some(item) = cursor.next(&pages, &mut work).await? {
                    actual.push((item.key.as_slice().to_vec(), item.value.as_slice().to_vec()));
                }
                assert_eq!(actual, expected);
                assert!(cursor.next(&pages, &mut work).await?.is_none());
            }
            let cursor =
                super::range::Cursor::start(&pages, Some(root), None, None, &mut work).await?;
            let (encoded, new) = leaf(
                &pages,
                &[Entry {
                    key: ByteRef::inline(b"new")?,
                    value: ByteRef::inline(b"new")?,
                }],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            assert_ne!(new, root);
            assert_eq!(
                read::lookup(&pages, Some(root), b"a", &mut work)
                    .await?
                    .unwrap()
                    .as_slice(),
                b"original"
            );
            Ok(cursor)
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert!(resources.bytes.available() < 4 << 20);
    drop(retained);
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn range_missing_successor_and_work_failure_poison_instead_of_eof() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let resources = resources();
    let pages = fixture(local.clone(), &resources);
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let mut work = WorkBudget::new(10000, 1 << 20);
            let a = ByteRef::inline(b"a")?;
            let b = ByteRef::inline(b"b")?;
            let value = ByteRef::inline(b"v")?;
            let (encoded, left) = leaf(&pages, &[Entry { key: a, value }])?;
            pages.write_scoped_bytes(encoded).await?;
            let (encoded, right) = leaf(&pages, &[Entry { key: b, value }])?;
            pages.write_scoped_bytes(encoded).await?;
            let (encoded, root) = branch(
                &pages,
                &[
                    Child {
                        first: a,
                        descriptor: left,
                    },
                    Child {
                        first: b,
                        descriptor: right,
                    },
                ],
            )?;
            pages.write_scoped_bytes(encoded).await?;
            let mut cursor =
                super::range::Cursor::start(&pages, Some(root), None, None, &mut work).await?;
            assert_eq!(
                cursor
                    .next(&pages, &mut work)
                    .await?
                    .unwrap()
                    .key
                    .as_slice(),
                b"a"
            );
            let suffix: String = right.hash.iter().map(|b| format!("{b:02x}")).collect();
            let target = local
                .list("map")
                .await?
                .into_iter()
                .find(|key| key.ends_with(&suffix))
                .unwrap();
            let original = local.get(&target).await?.unwrap();
            let mut bounded = super::range::Cursor::start(
                &pages,
                Some(root),
                Some((b"a", true)),
                Some((b"a", true)),
                &mut work,
            )
            .await?;
            local.delete(&target).await?;
            assert_eq!(
                bounded
                    .next(&pages, &mut work)
                    .await?
                    .unwrap()
                    .key
                    .as_slice(),
                b"a"
            );
            assert!(
                bounded
                    .next(&pages, &mut WorkBudget::new(0, 0))
                    .await?
                    .is_none()
            );
            assert!(matches!(
                cursor.next(&pages, &mut work).await,
                Err(ObjectLogError::MissingObject(_))
            ));
            local.put(&target, original).await?;
            assert!(cursor.next(&pages, &mut work).await.is_err());
            let mut cursor =
                super::range::Cursor::start(&pages, Some(root), None, None, &mut work).await?;
            assert!(matches!(
                cursor.next(&pages, &mut WorkBudget::new(1000, 0)).await,
                Err(ObjectLogError::BudgetExceeded(_))
            ));
            assert!(cursor.next(&pages, &mut work).await.is_err());
            let mut cursor =
                super::range::Cursor::start(&pages, Some(root), None, None, &mut work).await?;
            cursor.next(&pages, &mut work).await?;
            assert!(matches!(
                cursor.next(&pages, &mut WorkBudget::new(0, 1000)).await,
                Err(ObjectLogError::BudgetExceeded(_))
            ));
            assert!(cursor.next(&pages, &mut work).await.is_err());
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}
