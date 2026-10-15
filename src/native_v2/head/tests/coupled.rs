use super::*;
use crate::native_v2::{
    codec::Scope,
    operation::{ExecutionConfig, Resources},
    page_store::{NativeStore, PageStore},
    semantic_map::{Edit, WorkBudget},
};

#[tokio::test]
async fn coupled_local_atomic_state_work_orphans_and_lost_reply() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let provider = local.clone();
    let loss: LossHook = Arc::new(move |key| {
        provider.lose_next_metadata_reply(key);
        None
    });
    coupled_contract(local, Some(loss), "coupled-local".into(), 56).await;
}

type LossHook = Arc<dyn Fn(&str) -> Option<Arc<std::sync::atomic::AtomicU64>> + Send + Sync>;
async fn coupled_contract(
    local: Arc<dyn NativeStore>,
    lose_reply: Option<LossHook>,
    prefix: String,
    version_bytes: usize,
) {
    let scope = Scope {
        namespace: [11; 32],
        incarnation: [17; 32],
    };
    let empty = NativeHead::empty(scope);
    let key = format!("{prefix}/head");
    local
        .put(&key, Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 4 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local.clone(),
        format!("{prefix}/pages"),
        scope,
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: 4096,
            version_bytes,
        },
    )
    .unwrap();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let original = CapturedHead::capture(&pages, &key).await?;
            assert!(
                original
                    .semantic_map(&pages, &mut WorkBudget::new(100, 10000))
                    .await
                    .is_err()
            );
            let writer = original.acquire_and_recapture(&pages, &key).await?;
            let inputs = [
                AppendInput {
                    object_id: "same-id",
                    payload: b"winner payload",
                    records: 3,
                },
                AppendInput {
                    object_id: "tail",
                    payload: b"tail",
                    records: 2,
                },
            ];
            let edits = [
                Edit {
                    key: b"next",
                    value: Some(b"work"),
                },
                Edit {
                    key: b"state",
                    value: Some(b"one"),
                },
            ];
            let mut work = WorkBudget::new(10000, 100000);
            let mut winner = writer
                .clone()
                .prepare_semantic_group(
                    pages.clone(),
                    &inputs,
                    AppendGroupLimits {
                        entries: 2,
                        framed_bytes: 4096,
                    },
                    SemanticEdits {
                        edits: &edits,
                        max_edits: 2,
                        max_input_bytes: 100,
                        work: &mut work,
                    },
                )
                .await?;
            let other_inputs = [
                AppendInput {
                    object_id: "same-id",
                    payload: b"orphan payload",
                    records: 3,
                },
                AppendInput {
                    object_id: "tail",
                    payload: b"tail",
                    records: 2,
                },
            ];
            let mut orphan = writer
                .clone()
                .prepare_semantic_group(
                    pages.clone(),
                    &other_inputs,
                    AppendGroupLimits {
                        entries: 2,
                        framed_bytes: 4096,
                    },
                    SemanticEdits {
                        edits: &edits,
                        max_edits: 2,
                        max_input_bytes: 100,
                        work: &mut work,
                    },
                )
                .await?;
            assert_eq!(
                semantic_manifest(&pages, winner.next, &mut work).await?,
                semantic_manifest(&pages, orphan.next, &mut work).await?
            );
            assert_ne!(winner.next.semantic_anchor, orphan.next.semantic_anchor);
            assert_ne!(
                winner.certificates.entries[0].object_id,
                orphan.certificates.entries[0].object_id
            );
            assert_eq!(
                CapturedHead::capture(&pages, &key).await?.head.next_offset,
                0
            );
            let calls = if let Some(inject) = &lose_reply {
                let calls = inject(&key);
                assert!(winner.publish(pages.clone(), &key).await.is_err());
                assert!(winner.publish(pages.clone(), &key).await.is_err());
                if let Some(calls) = &calls {
                    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
                }
                calls
            } else {
                winner.publish(pages.clone(), &key).await?;
                None
            };
            let first = CapturedHead::capture(&pages, &key).await?;
            assert_eq!(first.head.next_offset, 5);
            let first_map = first.semantic_map(&pages, &mut work).await?;
            assert_eq!(
                first_map
                    .lookup(&pages, b"state", &mut work)
                    .await?
                    .unwrap()
                    .as_slice(),
                b"one"
            );
            assert_eq!(
                first_map
                    .lookup(&pages, b"next", &mut work)
                    .await?
                    .unwrap()
                    .as_slice(),
                b"work"
            );
            assert_eq!(
                first.read_payload(pages.clone(), 0).await?.unwrap().bytes(),
                b"winner payload"
            );
            assert_eq!(
                first
                    .read_record_bounded(pages.clone(), 4, 4)
                    .await?
                    .unwrap()
                    .2
                    .bytes(),
                b"tail"
            );
            assert!(
                first
                    .read_record_bounded(pages.clone(), 0, 13)
                    .await
                    .is_err()
            );
            assert!(orphan.publish(pages.clone(), &key).await.is_err());
            let later_writer = writer.refresh(pages.clone(), &key).await?;
            let later_inputs = [AppendInput {
                object_id: "same-id",
                payload: b"later",
                records: 1,
            }];
            let later_edits = [
                Edit {
                    key: b"next",
                    value: None,
                },
                Edit {
                    key: b"state",
                    value: Some(b"two"),
                },
            ];
            let mut later = later_writer
                .prepare_semantic_group(
                    pages.clone(),
                    &later_inputs,
                    AppendGroupLimits {
                        entries: 1,
                        framed_bytes: 4096,
                    },
                    SemanticEdits {
                        edits: &later_edits,
                        max_edits: 2,
                        max_input_bytes: 100,
                        work: &mut work,
                    },
                )
                .await?;
            later.publish(pages.clone(), &key).await?;
            let current = CapturedHead::capture(&pages, &key).await?;
            let current_map = current.semantic_map(&pages, &mut work).await?;
            assert_eq!(
                current_map
                    .lookup(&pages, b"state", &mut work)
                    .await?
                    .unwrap()
                    .as_slice(),
                b"two"
            );
            assert!(
                current_map
                    .lookup(&pages, b"next", &mut work)
                    .await?
                    .is_none()
            );
            assert_eq!(
                first_map
                    .lookup(&pages, b"state", &mut work)
                    .await?
                    .unwrap()
                    .as_slice(),
                b"one"
            );
            assert_eq!(
                winner
                    .resolve_group(pages.clone(), &key, ProofBudget { entries: 1 })
                    .await?,
                Resolution::Unknown
            );
            assert_eq!(
                winner
                    .resolve_group(pages.clone(), &key, ProofBudget { entries: 2 })
                    .await?,
                Resolution::DurableContent
            );
            assert_eq!(
                orphan
                    .resolve_group(pages.clone(), &key, ProofBudget { entries: 2 })
                    .await?,
                Resolution::Rejected
            );
            assert_eq!(
                later
                    .resolve_group(pages.clone(), &key, ProofBudget { entries: 1 })
                    .await?,
                Resolution::DurableContent
            );
            if let Some(calls) = calls {
                assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            }
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn coupled_head_rejects_cross_paired_valid_map_and_append_roots() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let scope = Scope {
        namespace: [11; 32],
        incarnation: [17; 32],
    };
    let empty = NativeHead::empty(scope);
    local
        .put("head", Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 4 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local,
        "pages".into(),
        scope,
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: 4096,
            version_bytes: 56,
        },
    )
    .unwrap();
    resources
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let captured = CapturedHead::capture(&pages, "head").await?;
            let writer = captured.acquire_and_recapture(&pages, "head").await?;
            let inputs = [AppendInput {
                object_id: "one",
                payload: b"log command",
                records: 1,
            }];
            let mut work = WorkBudget::new(1000, 10000);
            let a = writer
                .clone()
                .prepare_semantic_group(
                    pages.clone(),
                    &inputs,
                    AppendGroupLimits {
                        entries: 1,
                        framed_bytes: 4096,
                    },
                    SemanticEdits {
                        edits: &[Edit {
                            key: b"state",
                            value: Some(b"a"),
                        }],
                        max_edits: 1,
                        max_input_bytes: 100,
                        work: &mut work,
                    },
                )
                .await?;
            let b = writer
                .prepare_semantic_group(
                    pages.clone(),
                    &inputs,
                    AppendGroupLimits {
                        entries: 1,
                        framed_bytes: 4096,
                    },
                    SemanticEdits {
                        edits: &[Edit {
                            key: b"state",
                            value: Some(b"b"),
                        }],
                        max_edits: 1,
                        max_input_bytes: 100,
                        work: &mut work,
                    },
                )
                .await?;
            assert_ne!(a.next.semantic_anchor, b.next.semantic_anchor);
            assert!(proves_successor(pages.clone(), a.original.head, a.next).await?);
            assert!(proves_successor(pages.clone(), b.original.head, b.next).await?);
            let fence_only = NativeHead {
                epoch: a.next.epoch + 1,
                ..a.next
            };
            assert!(proves_successor(pages.clone(), a.next, fence_only).await?);
            let wrapped = pages
                .write(pages.branch(1, &[a.next.index_root.unwrap()])?)
                .await?;
            let SemanticCoverage::Covered {
                root: seal,
                through,
            } = a.next.semantic_anchor
            else {
                unreachable!()
            };
            let source = pages.read_scoped_bytes(&seal, 192).await?;
            let encoded = pages.encode_scoped_bytes(192, |raw, _| {
                raw.copy_from_slice(source.bytes());
                wrapped.encode_into(&mut raw[104..168]);
                Ok(())
            })?;
            let rewritten_seal = pages.write_scoped_bytes(encoded).await?;
            let rewritten = NativeHead {
                index_root: Some(wrapped),
                semantic_anchor: SemanticCoverage::Covered {
                    root: rewritten_seal,
                    through,
                },
                ..fence_only
            };
            assert!(
                !proves_successor(pages.clone(), a.next, rewritten).await?,
                "zero-append fencing must preserve both exact roots"
            );
            let mixed = NativeHead {
                semantic_anchor: b.next.semantic_anchor,
                ..a.next
            };
            assert!(
                matches!(proves_successor(pages,a.original.head,mixed).await,
                Err(ObjectLogError::InvalidBatch(reason)) if reason=="coupled publication seal"),
                "independently valid roots must not authorize a cross-paired queue cut"
            );
            Ok(())
        })
        .await
        .unwrap();
    resources.close_and_drain().await.unwrap();
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires configured S3 provider; mandatory coupled publication qualification"]
async fn coupled_s3_atomic_state_work_orphans_and_lost_reply_both_signing_modes() {
    let required = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
    let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
    let region = required("OBJECT_LOG_S3_REGION");
    let bucket = required("OBJECT_LOG_S3_BUCKET");
    let key = required("OBJECT_LOG_S3_KEY_ID");
    let secret = required("OBJECT_LOG_S3_SECRET");
    for unsigned in [false, true] {
        let provider = Arc::new(
            crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                .with_payload_signing_disabled(unsigned),
        );
        let fault = provider.clone();
        let loss: LossHook = Arc::new(move |key| Some(fault.lose_next_metadata_reply(key)));
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        coupled_contract(
            provider,
            Some(loss),
            format!("coupled-{nonce}-{unsigned}"),
            4096,
        )
        .await;
    }
}

#[tokio::test]
async fn coupled_guards_write_nothing_and_bad_seal_keeps_higher_fence() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalBlobStore::new(directory.path()));
    let scope = Scope {
        namespace: [11; 32],
        incarnation: [17; 32],
    };
    let empty = NativeHead::empty(scope);
    local
        .put("legacy-head", Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    local
        .put("covered-head", Bytes::copy_from_slice(&empty.encode()))
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 4 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        local.clone(),
        "pages".into(),
        scope,
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: 4096,
            version_bytes: 56,
        },
    )
    .unwrap();
    resources.run(move|physical|async move {
        let pages=pages.in_operation(physical)?;
        let original=CapturedHead::capture(&pages,"legacy-head").await?;
        let writer=original.acquire_and_recapture(&pages,"legacy-head").await?;
        writer.clone().prepare_append(pages.clone(),"legacy",b"legacy",1).await?.publish(pages.clone(),"legacy-head").await?;
        let inputs=[AppendInput {object_id:"coupled",payload:b"new",records:1}];
        let edits=[Edit {key:b"state",value:Some(b"a")}];
        let mut work=WorkBudget::new(1000,10000);
        let objects=local.list("pages").await?;
        assert!(matches!(writer.refresh(pages.clone(),"legacy-head").await?.prepare_semantic_group(pages.clone(),&inputs,AppendGroupLimits {entries:1,framed_bytes:4096},SemanticEdits {edits:&edits,max_edits:1,max_input_bytes:100,work:&mut work}).await,
            Err(ObjectLogError::Sequencer(reason)) if reason=="semantic bootstrap requires a genuinely empty original head"));
        assert_eq!(local.list("pages").await?,objects);
        let original=CapturedHead::capture(&pages,"covered-head").await?;
        let writer=original.acquire_and_recapture(&pages,"covered-head").await?;
        let mut prepared=writer.clone().prepare_semantic_group(pages.clone(),&inputs,AppendGroupLimits {entries:1,framed_bytes:4096},SemanticEdits {edits:&edits,max_edits:1,max_input_bytes:100,work:&mut work}).await?;
        let committed=prepared.publish(pages.clone(),"covered-head").await?;
        let writer=writer.refresh(pages.clone(),"covered-head").await?;
        let objects=local.list("pages").await?;
        assert!(matches!(writer.clone().prepare_group(pages.clone(),&inputs,AppendGroupLimits {entries:1,framed_bytes:4096}).await,
            Err(ObjectLogError::Sequencer(reason)) if reason=="append requires consumer semantic publication for covered heads"));
        assert_eq!(local.list("pages").await?,objects);
        let captured=CapturedHead::capture(&pages,"covered-head").await?;
        let knowledge=AuthorityKnowledge::new(&captured,&pages)?;
        let later=writer.prepare_semantic_group(pages.clone(),&inputs,AppendGroupLimits {entries:1,framed_bytes:4096},SemanticEdits {edits:&edits,max_edits:1,max_input_bytes:100,work:&mut work}).await?;
        let bad=NativeHead {epoch:committed.epoch+1,semantic_anchor:SemanticCoverage::Covered {through:later.next.next_offset,root:[199;32]},..later.next};
        local.put("covered-head",Bytes::copy_from_slice(&bad.encode())).await?;
        assert!(knowledge.observe(pages.clone(),"covered-head").await.is_err());
        assert_eq!(knowledge.gate.lock().await.highest_epoch,bad.epoch);
        assert_eq!(knowledge.gate.lock().await.floor,committed);
        local.put("covered-head",Bytes::copy_from_slice(&committed.encode())).await?;
        assert!(knowledge.observe(pages.clone(),"covered-head").await?.is_none());
        local.take_metadata_swap_calls();
        assert!(captured.acquire_with_knowledge(&pages,"covered-head",knowledge).await.is_err());
        assert_eq!(local.take_metadata_swap_calls(),0);
        Ok(())
    }).await.unwrap();
    resources.close_and_drain().await.unwrap();
    assert_eq!(resources.bytes.available(), 4 << 20);
}

#[tokio::test]
async fn coupled_cancelled_head_cas_drains_and_retained_attempt_releases_credit() {
    use crate::native_v2::submission::PublicationAttempt;
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(LocalBlobStore::new(directory.path()));
    let scope = Scope {
        namespace: [11; 32],
        incarnation: [17; 32],
    };
    provider
        .put(
            "head",
            Bytes::copy_from_slice(&NativeHead::empty(scope).encode()),
        )
        .await
        .unwrap();
    let resources = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 4 << 20,
    })
    .unwrap();
    let base = PageStore::new(
        provider.clone(),
        "store/pages".into(),
        scope,
        resources.bytes.clone(),
        MetadataLimits {
            payload_bytes: 4096,
            version_bytes: 56,
        },
    )
    .unwrap();
    let root = base.clone();
    let (attempt, expected) = resources
        .run(move |physical| async move {
            let pages = root
                .in_operation(physical)?
                .derive("queue/pages".into(), scope)?;
            let captured = CapturedHead::capture(&pages, "head").await?;
            let writer = captured.acquire_and_recapture(&pages, "head").await?;
            let prepared = writer
                .prepare_semantic_group(
                    pages.clone(),
                    &[AppendInput {
                        object_id: "commit",
                        payload: b"state and next work",
                        records: 2,
                    }],
                    AppendGroupLimits {
                        entries: 1,
                        framed_bytes: 4096,
                    },
                    SemanticEdits {
                        edits: &[
                            Edit {
                                key: b"next",
                                value: Some(b"work"),
                            },
                            Edit {
                                key: b"state",
                                value: Some(b"committed"),
                            },
                        ],
                        max_edits: 2,
                        max_input_bytes: 100,
                        work: &mut WorkBudget::new(1000, 10000),
                    },
                )
                .await?;
            let expected = prepared.next;
            let attempt = PublicationAttempt::new(&pages, prepared, pages.seal_binding()?, "head")?;
            Ok((attempt, expected))
        })
        .await
        .unwrap();
    provider.take_metadata_swap_calls();
    provider.lose_next_metadata_reply("head");
    let (entered, release) = provider.pause_metadata_swap(false);
    let owned = resources.clone();
    let root = base.clone();
    let retained = attempt.clone();
    let caller = tokio::spawn(async move { retained.submit_outcome(&owned, root, None).await });
    tokio::task::spawn_blocking(move || {
        entered
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap()
    })
    .await
    .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let mut closing = Box::pin(resources.close_and_drain());
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(closing.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(resources.bytes.available() < 4 << 20);
    release.send(()).unwrap();
    closing.await.unwrap();
    assert_eq!(provider.take_metadata_swap_calls(), 1);
    let raw = provider.get("head").await.unwrap().unwrap();
    assert_eq!(
        NativeHead::decode(&raw, &scope.namespace, &scope.incarnation).unwrap(),
        expected
    );
    assert!(resources.bytes.available() < 4 << 20);
    drop(attempt);
    assert_eq!(resources.bytes.available(), 4 << 20);
    // Explicit cold reopen validates both roots and reads the committed state.
    let reopened = Resources::new(ExecutionConfig {
        workers: 2,
        blocking_workers: 2,
        operations: 1,
        bytes: 4 << 20,
    })
    .unwrap();
    let pages = PageStore::new(
        provider,
        "queue/pages".into(),
        scope,
        reopened.bytes.clone(),
        MetadataLimits {
            payload_bytes: 4096,
            version_bytes: 56,
        },
    )
    .unwrap();
    reopened
        .run(move |physical| async move {
            let pages = pages.in_operation(physical)?;
            let captured = CapturedHead::capture(&pages, "head").await?;
            let mut work = WorkBudget::new(1000, 10000);
            let map = captured.semantic_map(&pages, &mut work).await?;
            assert_eq!(
                map.lookup(&pages, b"state", &mut work)
                    .await?
                    .unwrap()
                    .as_slice(),
                b"committed"
            );
            assert_eq!(
                map.lookup(&pages, b"next", &mut work)
                    .await?
                    .unwrap()
                    .as_slice(),
                b"work"
            );
            Ok(())
        })
        .await
        .unwrap();
    reopened.close_and_drain().await.unwrap();
    assert_eq!(reopened.bytes.available(), 4 << 20);
}
