#![cfg(feature = "native")]
use object_log::{
    BlobStore, LocalBlobStore, NativeAppend, NativeEditLimits, NativeLimits, NativeLog,
    NativePublishOutcome, NativeResolution, NativeSemanticEdit, NativeSemanticWork,
};
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
fn work() -> NativeSemanticWork {
    NativeSemanticWork {
        pages: 10000,
        hydrated_bytes: 1 << 20,
    }
}
fn edits() -> NativeEditLimits {
    NativeEditLimits {
        edits: 100,
        input_bytes: 1 << 20,
        work: work(),
    }
}
#[tokio::test]
async fn public_semantic_publication_binds_cut_and_retains_old_values() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(store.clone(), "semantic-public".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let empty = log.capture(&namespace).await.unwrap();
    assert!(
        log.semantic_lookup(&empty, b"missing", work())
            .await
            .is_err()
    );
    let input = [NativeAppend {
        identity: "genesis",
        payload: b"canonical genesis and work",
        records: 3,
    }];
    let changes = [
        NativeSemanticEdit {
            key: b"",
            value: Some(b""),
        },
        NativeSemanticEdit {
            key: b"\0",
            value: Some(b"nul"),
        },
        NativeSemanticEdit {
            key: b"\0a",
            value: Some(b"prefix"),
        },
        NativeSemanticEdit {
            key: b"state",
            value: Some(b"one"),
        },
    ];
    let attempt = log
        .prepare_with_edits(&writer, &empty, &input, &changes, edits())
        .await
        .unwrap();
    assert!(matches!(
        log.submit(&attempt).await.unwrap(),
        NativePublishOutcome::Stored(_)
    ));
    assert_eq!(
        log.resolve(&attempt, 1).await.unwrap(),
        NativeResolution::DurableContent
    );
    let first = log.capture(&namespace).await.unwrap();
    assert_eq!(
        log.semantic_lookup(&first, b"", work())
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        b""
    );
    assert_eq!(
        log.semantic_lookup(&first, b"\0a", work())
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        b"prefix"
    );
    assert!(
        log.semantic_lookup(&first, b"absent", work())
            .await
            .unwrap()
            .is_none()
    );
    let retained = log
        .semantic_lookup(&first, b"state", work())
        .await
        .unwrap()
        .unwrap();
    log.refresh_writer(&writer).await.unwrap();
    let next = [NativeAppend {
        identity: "next",
        payload: b"canonical commit",
        records: 1,
    }];
    let changes = [NativeSemanticEdit {
        key: b"state",
        value: Some(b"two"),
    }];
    let second = log
        .prepare_with_edits(&writer, &first, &next, &changes, edits())
        .await
        .unwrap();
    log.submit(&second).await.unwrap();
    let alias = writer.clone();
    log.refresh_writer(&alias).await.unwrap();
    let objects = store.list("semantic-public").await.unwrap();
    assert!(
        log.prepare_with_edits(&writer, &first, &next, &changes, edits())
            .await
            .is_err()
    );
    assert_eq!(store.list("semantic-public").await.unwrap(), objects);
    let latest = log.capture(&namespace).await.unwrap();
    assert_eq!(
        log.semantic_lookup(&latest, b"state", work())
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        b"two"
    );
    assert_eq!(
        log.semantic_lookup(&first, b"state", work())
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        b"one"
    );
    assert!(
        log.semantic_lookup(
            &first,
            b"state",
            NativeSemanticWork {
                pages: 0,
                hydrated_bytes: 10000
            }
        )
        .await
        .is_err()
    );
    log.shutdown().await.unwrap();
    assert_eq!(retained.bytes(), b"one");
}
#[tokio::test]
async fn public_semantic_preflight_rejects_bad_edits_and_v2_overhead_without_writes() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(
        store.clone(),
        "semantic-preflight".into(),
        NativeLimits {
            group_bytes: 200,
            ..limits()
        },
    )
    .unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    let objects = store.list("semantic-preflight").await.unwrap();
    assert!(
        log.prepare_with_edits(
            &writer,
            &cut,
            &[NativeAppend {
                identity: "x",
                payload: b"x",
                records: 1
            }],
            &[],
            edits()
        )
        .await
        .is_err()
    );
    assert_eq!(store.list("semantic-preflight").await.unwrap(), objects);
    log.shutdown().await.unwrap();
    let log = NativeLog::local(store.clone(), "semantic-bad-edits".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    let objects = store.list("semantic-bad-edits").await.unwrap();
    let inputs = [NativeAppend {
        identity: "x",
        payload: b"x",
        records: 1,
    }];
    assert!(matches!(log.prepare_with_edits(&writer,&cut,&[
        NativeAppend {identity:"valid",payload:b"valid",records:1},
        NativeAppend {identity:"invalid-last",payload:b"invalid",records:i32::MAX as u32+1},
    ],&[],edits()).await,Err(object_log::ObjectLogError::InvalidBatch(reason)) if reason=="native append identity or count"));
    assert_eq!(store.list("semantic-bad-edits").await.unwrap(), objects);

    for changes in [
        vec![
            NativeSemanticEdit {
                key: b"a",
                value: None,
            },
            NativeSemanticEdit {
                key: b"a",
                value: Some(b"v"),
            },
        ],
        vec![
            NativeSemanticEdit {
                key: b"b",
                value: None,
            },
            NativeSemanticEdit {
                key: b"a",
                value: None,
            },
        ],
    ] {
        assert!(
            log.prepare_with_edits(&writer, &cut, &inputs, &changes, edits())
                .await
                .is_err()
        );
        assert_eq!(store.list("semantic-bad-edits").await.unwrap(), objects);
    }
    assert!(
        log.prepare_with_edits(
            &writer,
            &cut,
            &inputs,
            &[NativeSemanticEdit {
                key: b"a",
                value: Some(b"value")
            }],
            NativeEditLimits {
                input_bytes: 5,
                ..edits()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(store.list("semantic-bad-edits").await.unwrap(), objects);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_semantic_scan_continuations_are_exact_immutable_and_fail_closed() {
    use object_log::{NativeScanLimits, NativeScanStatus, NativeSemanticRange};
    use std::ops::Bound;
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(store.clone(), "semantic-scan".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let empty = log.capture(&namespace).await.unwrap();
    let range = NativeSemanticRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let scan_limits = NativeScanLimits {
        rows: 200,
        output_bytes: 10000,
        work: work(),
    };
    assert!(log.semantic_scan(&empty, range, scan_limits).await.is_err());
    let keys: Vec<_> = (0..110u8).map(|key| vec![key]).collect();
    let changes: Vec<_> = keys
        .iter()
        .map(|key| NativeSemanticEdit {
            key,
            value: Some(b"v"),
        })
        .collect();
    let input = [NativeAppend {
        identity: "seed",
        payload: b"canonical batch",
        records: 110,
    }];
    let attempt = log
        .prepare_with_edits(
            &writer,
            &empty,
            &input,
            &changes,
            NativeEditLimits {
                edits: 110,
                ..edits()
            },
        )
        .await
        .unwrap();
    log.submit(&attempt).await.unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    assert!(
        log.semantic_scan(
            &cut,
            range,
            NativeScanLimits {
                rows: 0,
                ..scan_limits
            }
        )
        .await
        .is_err()
    );
    assert!(
        log.semantic_scan(
            &cut,
            range,
            NativeScanLimits {
                work: NativeSemanticWork {
                    pages: 1,
                    hydrated_bytes: 10000
                },
                ..scan_limits
            }
        )
        .await
        .is_err()
    );
    assert!(
        log.semantic_scan(
            &cut,
            range,
            NativeScanLimits {
                output_bytes: 1,
                ..scan_limits
            }
        )
        .await
        .is_err()
    );
    let exact = log
        .semantic_scan(
            &cut,
            range,
            NativeScanLimits {
                rows: 110,
                ..scan_limits
            },
        )
        .await
        .unwrap();
    assert_eq!(exact.status(), NativeScanStatus::LimitReached);
    assert_eq!(exact.rows().len(), 110);
    let end = log
        .semantic_scan_next(exact.continuation().unwrap(), scan_limits)
        .await
        .unwrap();
    assert_eq!(end.status(), NativeScanStatus::Complete);
    assert!(end.rows().is_empty());
    let prefix = log
        .semantic_scan(
            &cut,
            range,
            NativeScanLimits {
                output_bytes: 3,
                ..scan_limits
            },
        )
        .await
        .unwrap();
    assert_eq!(prefix.status(), NativeScanStatus::LimitReached);
    assert_eq!(prefix.rows().len(), 1);
    assert_eq!(prefix.rows()[0].key(), &[0]);
    let token = prefix.continuation().unwrap().clone();
    let a = log.semantic_scan_next(&token, scan_limits).await.unwrap();
    let b = log.semantic_scan_next(&token, scan_limits).await.unwrap();
    assert_eq!(a.status(), NativeScanStatus::Complete);
    assert_eq!(a.rows().len(), 109);
    assert_eq!(
        a.rows().iter().map(|row| row.key()).collect::<Vec<_>>(),
        b.rows().iter().map(|row| row.key()).collect::<Vec<_>>()
    );
    let foreign = NativeLog::local(store.clone(), "semantic-scan".into(), limits()).unwrap();
    assert!(
        foreign
            .semantic_scan_next(&token, scan_limits)
            .await
            .is_err()
    );
    foreign.shutdown().await.unwrap();
    // Remove an actual successor leaf. Traversal may read a valid prefix but
    // must fail the whole page instead of converting missing source into EOF.
    let mut victim = None;
    for key in store.list("semantic-scan").await.unwrap() {
        let raw = store.get(&key).await.unwrap().unwrap();
        if raw.get(..8) == Some(b"ONM2\0\0\0\x01")
            && raw.get(72) == Some(&1)
            && raw[96..]
                .as_chunks::<80>()
                .0
                .iter()
                .any(|slot| slot[..8] == 1u64.to_be_bytes() && slot[8] == 109)
        {
            victim = Some((key, raw));
            break;
        }
    }
    let (key, raw) = victim.expect("actual final map leaf");
    store.delete(&key).await.unwrap();
    assert!(log.semantic_scan_next(&token, scan_limits).await.is_err());
    store.put(&key, raw).await.unwrap();
    let replay = log.semantic_scan_next(&token, scan_limits).await.unwrap();
    assert_eq!(replay.rows().len(), 109);
    assert_eq!(replay.rows()[0].key(), &[1]);
    let bounded = log
        .semantic_scan(
            &cut,
            NativeSemanticRange {
                lower: Bound::Included(&[10]),
                upper: Bound::Excluded(&[13]),
            },
            scan_limits,
        )
        .await
        .unwrap();
    assert_eq!(bounded.status(), NativeScanStatus::Complete);
    assert_eq!(
        bounded
            .rows()
            .iter()
            .map(|row| row.key()[0])
            .collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
    log.shutdown().await.unwrap();
    assert_eq!(prefix.rows()[0].value(), b"v");
    drop(token);
}

#[tokio::test]
async fn public_scan_output_limit_precedes_overflow_value_source_io() {
    use object_log::{NativeScanLimits, NativeScanStatus, NativeSemanticRange, ObjectLogError};
    use std::ops::Bound;
    let directory = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(directory.path());
    let log = NativeLog::local(store.clone(), "semantic-overflow".into(), limits()).unwrap();
    let namespace = log.provision_new(b"queue").await.unwrap();
    let writer = log.acquire(&namespace).await.unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    let value = vec![0xa7; 5000];
    let attempt = log
        .prepare_with_edits(
            &writer,
            &cut,
            &[NativeAppend {
                identity: "seed",
                payload: b"seed",
                records: 1,
            }],
            &[
                NativeSemanticEdit {
                    key: b"a",
                    value: Some(b"v"),
                },
                NativeSemanticEdit {
                    key: b"b",
                    value: Some(&value),
                },
            ],
            edits(),
        )
        .await
        .unwrap();
    log.submit(&attempt).await.unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    let mut deleted = 0;
    for key in store.list("semantic-overflow").await.unwrap() {
        let raw = store.get(&key).await.unwrap().unwrap();
        if raw.get(..8) == Some(b"ONM2\0\0\0\x01")
            && raw.get(72) == Some(&3)
            && raw.len() > 136
            && raw[136..].iter().all(|b| *b == 0xa7)
        {
            store.delete(&key).await.unwrap();
            deleted += 1;
        }
    }
    assert_eq!(deleted, 2, "both actual overflow pages must be removed");
    let range = NativeSemanticRange {
        lower: Bound::Included(b"b"),
        upper: Bound::Unbounded,
    };
    let scan = NativeScanLimits {
        rows: 10,
        output_bytes: 5000,
        work: work(),
    };
    assert!(matches!(
        log.semantic_scan(&cut, range, scan).await,
        Err(ObjectLogError::ReadLimitExceeded {
            actual: 5001,
            limit: 5000
        })
    ));
    assert!(matches!(
        log.semantic_scan(
            &cut,
            range,
            NativeScanLimits {
                output_bytes: 5001,
                ..scan
            }
        )
        .await,
        Err(ObjectLogError::MissingObject(_))
    ));
    let page = log
        .semantic_scan(
            &cut,
            NativeSemanticRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            scan,
        )
        .await
        .unwrap();
    assert_eq!(page.status(), NativeScanStatus::LimitReached);
    assert_eq!(page.rows().len(), 1);
    assert_eq!(page.rows()[0].key(), b"a");
    assert!(matches!(
        log.semantic_scan_next(page.continuation().unwrap(), scan)
            .await,
        Err(ObjectLogError::ReadLimitExceeded {
            actual: 5001,
            limit: 5000
        })
    ));
    assert!(matches!(
        log.semantic_scan_next(
            page.continuation().unwrap(),
            NativeScanLimits {
                output_bytes: 5001,
                ..scan
            }
        )
        .await,
        Err(ObjectLogError::MissingObject(_))
    ));
    log.shutdown().await.unwrap();
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires configured S3 provider; mandatory public semantic qualification"]
async fn public_semantic_s3_coupled_state_and_scan_both_signing_modes() {
    use object_log::{NativeScanLimits, NativeScanStatus, NativeSemanticRange, S3BlobStore};
    use std::ops::Bound;
    let required = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
    let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
    let region = required("OBJECT_LOG_S3_REGION");
    let bucket = required("OBJECT_LOG_S3_BUCKET");
    let access = required("OBJECT_LOG_S3_KEY_ID");
    let secret = required("OBJECT_LOG_S3_SECRET");
    for unsigned in [false, true] {
        let provider = S3BlobStore::new(&endpoint, &region, &bucket, &access, &secret)
            .with_payload_signing_disabled(unsigned);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let log = NativeLog::s3(
            provider,
            format!("public-semantic-{nonce}-{unsigned}"),
            limits(),
        )
        .unwrap();
        let namespace = log.provision_new(b"queue\0literal").await.unwrap();
        let writer = log.acquire(&namespace).await.unwrap();
        let initial = log.capture(&namespace).await.unwrap();
        let keys: Vec<_> = (0..200)
            .map(|i| format!("work/雪\0{i:04}").into_bytes())
            .collect();
        let values: Vec<_> = (0..200)
            .map(|i| vec![i as u8; if i == 0 { 8192 } else { 1024 }])
            .collect();
        let changes: Vec<_> = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| NativeSemanticEdit {
                key,
                value: Some(value),
            })
            .collect();
        let attempt = log
            .prepare_with_edits(
                &writer,
                &initial,
                &[NativeAppend {
                    identity: "seed",
                    payload: b"canonical complete seed batch",
                    records: 200,
                }],
                &changes,
                NativeEditLimits {
                    edits: 200,
                    ..edits()
                },
            )
            .await
            .unwrap();
        log.submit(&attempt).await.unwrap();
        assert_eq!(
            log.resolve(&attempt, 1).await.unwrap(),
            NativeResolution::DurableContent
        );
        let cut = log.capture(&namespace).await.unwrap();
        assert_eq!(
            log.semantic_lookup(&cut, &keys[0], work())
                .await
                .unwrap()
                .unwrap()
                .bytes(),
            values[0]
        );
        let page = log
            .semantic_scan(
                &cut,
                NativeSemanticRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                NativeScanLimits {
                    rows: 17,
                    output_bytes: 500000,
                    work: work(),
                },
            )
            .await
            .unwrap();
        assert_eq!(page.status(), NativeScanStatus::LimitReached);
        assert_eq!(page.rows().len(), 17);
        log.refresh_writer(&writer).await.unwrap();
        let next = log
            .prepare_with_edits(
                &writer,
                &cut,
                &[NativeAppend {
                    identity: "next",
                    payload: b"canonical state replacement",
                    records: 1,
                }],
                &[
                    NativeSemanticEdit {
                        key: &keys[0],
                        value: Some(b"changed"),
                    },
                    NativeSemanticEdit {
                        key: &keys[50],
                        value: None,
                    },
                ],
                edits(),
            )
            .await
            .unwrap();
        log.submit(&next).await.unwrap();
        let latest = log.capture(&namespace).await.unwrap();
        assert_eq!(
            log.semantic_lookup(&latest, &keys[0], work())
                .await
                .unwrap()
                .unwrap()
                .bytes(),
            b"changed"
        );
        assert!(
            log.semantic_lookup(&latest, &keys[50], work())
                .await
                .unwrap()
                .is_none()
        );
        let remainder = log
            .semantic_scan_next(
                page.continuation().unwrap(),
                NativeScanLimits {
                    rows: 300,
                    output_bytes: 500000,
                    work: work(),
                },
            )
            .await
            .unwrap();
        assert_eq!(remainder.status(), NativeScanStatus::Complete);
        let rows: Vec<_> = page.rows().iter().chain(remainder.rows()).collect();
        assert_eq!(rows.len(), 200);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.key(), keys[i]);
            assert_eq!(row.value(), values[i]);
        }
        assert_eq!(
            log.semantic_lookup(&cut, &keys[0], work())
                .await
                .unwrap()
                .unwrap()
                .bytes(),
            values[0]
        );
        log.shutdown().await.unwrap();
        assert_eq!(page.rows()[0].value(), values[0]);
    }
}
