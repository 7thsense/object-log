//! Retained original publication authority across owned dispatch and lost replies.
#[cfg(test)]
use super::head::NativeHead;
use super::{
    head::{PreparedAppend, ProofBudget, PublishOutcome, Resolution},
    operation::Resources,
    page_store::{Charge, PageBinding, PageStore},
};
use crate::ObjectLogError;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Only retained bytes/certificates escape; no scoped physical owner is retained.
pub(super) struct PublicationAttempt {
    prepared: Mutex<PreparedAppend>,
    binding: PageBinding,
    head_key: String,
    #[cfg(test)]
    panic_after_publish: std::sync::atomic::AtomicBool,
    _charge: Arc<Charge>,
}
impl PublicationAttempt {
    pub fn new(
        pages: &PageStore,
        prepared: PreparedAppend,
        binding: PageBinding,
        head_key: &str,
    ) -> Result<Arc<Self>, ObjectLogError> {
        Self::validate_input(pages, &prepared, &binding, head_key)?;
        let bytes = head_key
            .len()
            .checked_add(std::mem::size_of::<Self>())
            .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or_else(|| ObjectLogError::BudgetExceeded("retained attempt size".into()))?;
        let charge = pages.charge_output(bytes)?;
        Ok(Arc::new(Self {
            prepared: Mutex::new(prepared),
            binding,
            head_key: head_key.into(),
            #[cfg(test)]
            panic_after_publish: std::sync::atomic::AtomicBool::new(false),
            _charge: charge,
        }))
    }

    fn validate_input(
        pages: &PageStore,
        prepared: &PreparedAppend,
        binding: &PageBinding,
        head_key: &str,
    ) -> Result<(), ObjectLogError> {
        pages.require_operation()?;
        pages.validate_authority_key(head_key)?;
        let selected = pages.for_binding(binding)?;
        prepared.validate_binding(&selected, head_key)
    }

    /// Admission precedes acquisition of the retained attempt. The inner
    /// publication bit is the sole one-shot state and never resets after panic.
    pub async fn submit_outcome(
        self: &Arc<Self>,
        resources: &Arc<Resources>,
        base: Arc<PageStore>,
        external_owner: Option<crate::AppendOwner>,
    ) -> Result<PublishOutcome, ObjectLogError> {
        let attempt = self.clone();
        resources
            .run_with_owner(external_owner, move |physical| async move {
                let mut prepared = attempt.prepared.lock().await;
                let pages = base.in_operation(physical)?.for_binding(&attempt.binding)?;
                let result = prepared.publish_outcome(pages, &attempt.head_key).await;
                #[cfg(test)]
                assert!(
                    !attempt
                        .panic_after_publish
                        .swap(false, std::sync::atomic::Ordering::Relaxed),
                    "injected owned publication task panic"
                );
                drop(prepared);
                result
            })
            .await
    }

    #[cfg(test)]
    pub async fn submit(
        self: &Arc<Self>,
        resources: &Arc<Resources>,
        base: Arc<PageStore>,
    ) -> Result<super::head::NativeHead, ObjectLogError> {
        match self.submit_outcome(resources, base, None).await? {
            PublishOutcome::Stored(head) => Ok(head),
            PublishOutcome::Conflict => Err(ObjectLogError::Sequencer(
                "original append authority changed".into(),
            )),
        }
    }

    /// Resolution always observes storage using the retained original cut and
    /// ordered certificates. DurableContent is content proof, not writer identity.
    pub async fn resolve(
        self: &Arc<Self>,
        resources: &Arc<Resources>,
        base: Arc<PageStore>,
        budget: ProofBudget,
        external_owner: Option<crate::AppendOwner>,
    ) -> Result<Resolution, ObjectLogError> {
        if budget.entries == 0 {
            return Err(ObjectLogError::InvalidBatch(
                "zero group proof budget".into(),
            ));
        }
        let attempt = self.clone();
        resources
            .run_with_owner(external_owner, move |physical| async move {
                let prepared = attempt.prepared.lock().await;
                let pages = base.in_operation(physical)?.for_binding(&attempt.binding)?;
                prepared
                    .resolve_group(pages, &attempt.head_key, budget)
                    .await
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        codec::Scope,
        head::{CapturedHead, HEAD_BYTES},
        operation::ExecutionConfig,
    };
    use super::*;
    use crate::{BlobStore, LocalBlobStore, MetadataLimits, MetadataSwap};

    async fn fixture(
        operations: usize,
    ) -> (
        tempfile::TempDir,
        Arc<LocalBlobStore>,
        Arc<Resources>,
        Arc<PageStore>,
        Arc<PublicationAttempt>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let provider = Arc::new(LocalBlobStore::new(directory.path()));
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations,
            bytes: 1 << 20,
        })
        .unwrap();
        let scope = Scope {
            namespace: [21; 32],
            incarnation: [31; 32],
        };
        let base = PageStore::new(
            provider.clone(),
            "base/pages".into(),
            scope,
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 4096,
                version_bytes: 4096,
            },
        )
        .unwrap();
        let root = base.clone();
        let foreign_root = PageStore::new(
            provider.clone(),
            "foreign/base".into(),
            scope,
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 4096,
                version_bytes: 4096,
            },
        )
        .unwrap();
        let attempt = resources
            .run(move |physical| async move {
                let pages = root
                    .in_operation(physical.clone())?
                    .derive("namespace/pages".into(), scope)?;
                let empty = pages.read_head("namespace/head", HEAD_BYTES).await?;
                assert!(matches!(
                    pages
                        .swap_head("namespace/head", empty, &NativeHead::empty(scope).encode())
                        .await?,
                    MetadataSwap::Stored
                ));
                let captured = CapturedHead::capture(&pages, "namespace/head").await?;
                let writer = captured
                    .acquire_and_recapture(&pages, "namespace/head")
                    .await?;
                let binding = pages.seal_binding()?;
                let other = pages.derive("other/pages".into(), scope)?;
                let wrong_binding = writer
                    .clone()
                    .prepare_append(pages.clone(), "constructor-binding", b"not published", 1)
                    .await?;
                assert!(
                    PublicationAttempt::new(
                        &pages,
                        wrong_binding,
                        other.seal_binding()?,
                        "namespace/head"
                    )
                    .is_err()
                );
                let wrong_key = writer
                    .clone()
                    .prepare_append(pages.clone(), "constructor-key", b"not published", 1)
                    .await?;
                assert!(
                    PublicationAttempt::new(
                        &pages,
                        wrong_key,
                        binding.clone(),
                        "namespace/wrong-head"
                    )
                    .is_err()
                );
                let foreign = foreign_root.in_operation(physical)?;
                let wrong_owner = writer
                    .clone()
                    .prepare_append(pages.clone(), "constructor-owner", b"not published", 1)
                    .await?;
                assert!(
                    PublicationAttempt::new(
                        &foreign,
                        wrong_owner,
                        binding.clone(),
                        "namespace/head"
                    )
                    .is_err()
                );
                let prepared = writer
                    .prepare_append(pages.clone(), "original", b"payload", 1)
                    .await?;
                PublicationAttempt::new(&pages, prepared, binding, "namespace/head")
            })
            .await
            .unwrap();
        provider.take_media_op_stats();
        provider.take_metadata_swap_calls();
        (directory, provider, resources, base, attempt)
    }

    #[tokio::test]
    async fn concurrent_submitters_publish_once_and_retained_aliases_drain() {
        let (_directory, provider, resources, base, attempt) = fixture(2).await;
        let alias = attempt.clone();
        let (left, right) = tokio::join!(
            attempt.submit(&resources, base.clone()),
            alias.submit(&resources, base.clone())
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let rejected = if let Err(error) = left {
            error
        } else {
            right.unwrap_err()
        };
        assert_eq!(
            rejected,
            ObjectLogError::Sequencer("append publication already attempted".into())
        );
        assert_eq!(provider.take_metadata_swap_calls(), 1);
        assert_eq!(
            provider.take_media_op_stats().unwrap().bytes,
            HEAD_BYTES as u64
        );
        assert_eq!(
            attempt
                .resolve(&resources, base, ProofBudget { entries: 1 }, None)
                .await
                .unwrap(),
            Resolution::DurableContent
        );
        resources.close_and_drain().await.unwrap();
        let retained = resources.bytes.available();
        assert!(retained < 1 << 20);
        drop(attempt);
        assert_eq!(resources.bytes.available(), retained);
        drop(alias);
        assert_eq!(resources.bytes.available(), 1 << 20);
    }

    #[tokio::test]
    async fn cancellation_before_admission_does_not_claim_attempt() {
        let (_directory, provider, resources, base, attempt) = fixture(1).await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (owned, signal, wait) = (resources.clone(), entered.clone(), release.clone());
        let held = tokio::spawn(async move {
            owned
                .run(move |_| async move {
                    signal.notify_one();
                    wait.notified().await;
                    Ok(())
                })
                .await
        });
        entered.notified().await;
        let mut waiting = Box::pin(attempt.submit(&resources, base.clone()));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(waiting);
        assert_eq!(provider.take_media_op_stats().unwrap().bytes, 0);
        release.notify_one();
        held.await.unwrap().unwrap();
        attempt.submit(&resources, base.clone()).await.unwrap();
        assert_eq!(
            attempt
                .resolve(&resources, base, ProofBudget { entries: 1 }, None)
                .await
                .unwrap(),
            Resolution::DurableContent
        );
        resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn lost_reply_after_cancel_keeps_original_resolution_and_one_shot() {
        let (_directory, provider, resources, base, attempt) = fixture(1).await;
        provider.lose_next_metadata_reply("namespace/head");
        let (entered, release) = provider.pause_metadata_swap(false);
        let (owned, root, candidate) = (resources.clone(), base.clone(), attempt.clone());
        let caller = tokio::spawn(async move { candidate.submit(&owned, root).await });
        tokio::task::spawn_blocking(move || {
            entered
                .recv_timeout(std::time::Duration::from_secs(30))
                .unwrap()
        })
        .await
        .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        assert_eq!(
            attempt
                .resolve(&resources, base.clone(), ProofBudget { entries: 1 }, None)
                .await
                .unwrap(),
            Resolution::DurableContent
        );
        assert!(attempt.submit(&resources, base.clone()).await.is_err());
        assert_eq!(
            provider.take_media_op_stats().unwrap().bytes,
            HEAD_BYTES as u64
        );
        let root = base.clone();
        let binding = attempt.binding.clone();
        resources
            .run(move |physical| async move {
                let pages = root.in_operation(physical)?.for_binding(&binding)?;
                let captured = CapturedHead::capture(&pages, "namespace/head").await?;
                let writer = captured
                    .acquire_and_recapture(&pages, "namespace/head")
                    .await?;
                let mut next = writer
                    .prepare_append(pages.clone(), "later", b"later payload", 1)
                    .await?;
                next.publish(pages, "namespace/head").await?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            attempt
                .resolve(&resources, base, ProofBudget { entries: 1 }, None)
                .await
                .unwrap(),
            Resolution::DurableContent
        );
        resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn physical_panic_keeps_attempt_terminal_and_releases_owned_work() {
        let (_directory, provider, resources, base, attempt) = fixture(1).await;
        let (entered, release) = provider.pause_metadata_swap(true);
        let (owned, root, candidate) = (resources.clone(), base.clone(), attempt.clone());
        let caller = tokio::spawn(async move { candidate.submit(&owned, root).await });
        tokio::task::spawn_blocking(move || {
            entered
                .recv_timeout(std::time::Duration::from_secs(30))
                .unwrap()
        })
        .await
        .unwrap();
        release.send(()).unwrap();
        assert!(caller.await.unwrap().is_err());
        assert!(attempt.submit(&resources, base.clone()).await.is_err());
        assert_eq!(
            attempt
                .resolve(&resources, base, ProofBudget { entries: 1 }, None)
                .await
                .unwrap(),
            Resolution::Unknown
        );
        assert_eq!(provider.take_media_op_stats().unwrap().bytes, 0);
        resources.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn foreign_binding_and_zero_proof_fail_before_provider_io() {
        let (_directory, provider, resources, base, attempt) = fixture(1).await;
        let foreign_directory = tempfile::tempdir().unwrap();
        let foreign_provider = Arc::new(LocalBlobStore::new(foreign_directory.path()));
        let foreign = PageStore::new(
            foreign_provider.clone(),
            "other/pages".into(),
            Scope {
                namespace: [21; 32],
                incarnation: [31; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 4096,
                version_bytes: 4096,
            },
        )
        .unwrap();
        assert!(attempt.submit(&resources, foreign).await.is_err());
        assert!(
            attempt
                .resolve(&resources, base.clone(), ProofBudget { entries: 0 }, None)
                .await
                .is_err()
        );
        assert!(foreign_provider.list("").await.unwrap().is_empty());
        assert_eq!(provider.take_media_op_stats().unwrap().bytes, 0);
        attempt.submit(&resources, base).await.unwrap();
        resources.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn owned_task_panic_unlocks_original_and_preserves_terminal_submission() {
        let (_directory, provider, resources, base, attempt) = fixture(1).await;
        attempt
            .panic_after_publish
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(attempt.submit(&resources, base.clone()).await.is_err());
        assert_eq!(
            attempt
                .resolve(&resources, base.clone(), ProofBudget { entries: 1 }, None)
                .await
                .unwrap(),
            Resolution::DurableContent
        );
        assert!(attempt.submit(&resources, base).await.is_err());
        assert_eq!(provider.take_metadata_swap_calls(), 1);
        resources.close_and_drain().await.unwrap();
    }
}
