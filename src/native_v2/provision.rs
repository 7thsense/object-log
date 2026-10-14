//! Private candidate election. Existing bindings never authorize empty replacement.
use super::{
    codec::Scope,
    head::{AuthorityKnowledge, CapturedHead, HEAD_BYTES, NativeHead},
    page_store::{Charge, PageBinding, PageStore},
};
use crate::{MetadataSnapshot, MetadataSwap, ObjectLogError};
use sha2::{Digest, Sha256};
use std::sync::Arc;
const MAGIC: &[u8; 8] = b"ONB2\0\0\0\x01";
const HEADER: usize = 80;
const DIGEST: usize = 32;

pub(super) fn candidate_keys(prefix: &str, scope: Scope) -> (String, String) {
    let stem = format!(
        "{prefix}/candidates/{}/{}",
        hex(&scope.namespace),
        hex(&scope.incarnation)
    );
    (format!("{stem}/head"), format!("{stem}/pages"))
}

pub(super) struct NamespaceContext {
    pages: Arc<PageStore>,
    prefix: Arc<str>,
    binding_limit: usize,
    _charge: Arc<Charge>,
}
pub(super) struct ChargedHeadKey {
    pub key: String,
    pub charge: Arc<Charge>,
}
struct OwnedBinding {
    bytes: Vec<u8>,
    _charge: Arc<Charge>,
}
/// Neither a candidate nor its ready seal grants a writer capability.
pub(super) struct NativeProvisionAttempt {
    context: Arc<NamespaceContext>,
    literal: Vec<u8>,
    binding_key: String,
    head_key: String,
    pages: Arc<PageStore>,
    candidate_attempted: bool,
    binding_attempted: bool,
    binding_observed: Option<MetadataSnapshot>,
    ready: Option<ReadyCandidate>,
    _charge: Arc<Charge>,
}
struct ReadyCandidate {
    captured: CapturedHead,
}
pub(super) struct PublishedNamespace {
    literal: Vec<u8>,
    binding: MetadataSnapshot,
    captured: CapturedHead,
    knowledge: Arc<AuthorityKnowledge>,
    identity: Arc<()>,
    registry_prefix: Arc<str>,
    selected: PageBinding,
    scope: Scope,
    _charge: Arc<Charge>,
}
pub(super) struct RetainedRead {
    pub captured: CapturedHead,
    pub binding: PageBinding,
    pub knowledge: Arc<AuthorityKnowledge>,
    pub charge: Arc<Charge>,
}
// Preserve knowledge and byte ownership when a dispatch borrows only the
// captured head and binding fields from this retained carrier.
impl Drop for RetainedRead {
    fn drop(&mut self) {}
}
impl NamespaceContext {
    pub fn new(
        pages: Arc<PageStore>,
        prefix: String,
        binding_limit: usize,
    ) -> Result<Arc<Self>, ObjectLogError> {
        pages.require_operation()?;
        validate_prefix(&prefix)?;
        pages.validate_authority_key(&prefix)?;
        if binding_limit <= HEADER + DIGEST {
            return Err(invalid("binding limit"));
        }
        pages.validate_data_limit(binding_limit)?;
        let bytes = prefix
            .len()
            .checked_mul(2)
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            })
            .ok_or_else(|| invalid("namespace context allocation overflow"))?;
        let charge = pages.charge_output(bytes)?;
        Ok(Arc::new(Self {
            pages,
            prefix: prefix.into(),
            binding_limit,
            _charge: charge,
        }))
    }
    fn length(&self, literal: &[u8]) -> Result<usize, ObjectLogError> {
        if literal.is_empty() {
            return Err(invalid("empty literal namespace"));
        }
        HEADER
            .checked_add(literal.len())
            .and_then(|n| n.checked_add(DIGEST))
            .filter(|n| *n <= self.binding_limit)
            .ok_or_else(|| invalid("binding frame limit"))
    }
    fn key_charge(&self, literal: &[u8]) -> Result<Arc<Charge>, ObjectLogError> {
        self.length(literal)?;
        let bytes = self
            .prefix
            .len()
            .checked_mul(8)
            .and_then(|n| n.checked_add(literal.len().checked_mul(2)?))
            .and_then(|n| n.checked_add(2048 + std::mem::size_of::<NativeProvisionAttempt>()))
            .ok_or_else(|| invalid("provision allocation overflow"))?;
        self.pages.charge_output(bytes)
    }
    fn binding_key(&self, namespace: [u8; 32]) -> String {
        format!("{}/bindings/{}", self.prefix, hex(&namespace))
    }
    fn candidate_keys(&self, scope: Scope) -> (String, String) {
        candidate_keys(&self.prefix, scope)
    }
    pub async fn open(
        self: &Arc<Self>,
        literal: &[u8],
    ) -> Result<Option<PublishedNamespace>, ObjectLogError> {
        let charge = self.key_charge(literal)?;
        let namespace = Sha256::digest(literal).into();
        let key = self.binding_key(namespace);
        let binding = self.pages.read_head(&key, self.binding_limit).await?;
        if binding.bytes().is_none() {
            return Ok(None);
        }
        self.validate_winner(literal, binding, charge)
            .await
            .map(Some)
    }
    async fn validate_winner(
        self: &Arc<Self>,
        literal: &[u8],
        binding: MetadataSnapshot,
        charge: Arc<Charge>,
    ) -> Result<PublishedNamespace, ObjectLogError> {
        let raw = binding
            .bytes()
            .ok_or_else(|| invalid("attempted binding is missing"))?;
        let scope = decode_binding(raw, literal)?;
        if binding.inner.key != self.binding_key(scope.namespace) {
            return Err(invalid("binding literal key mismatch"));
        }
        let (head_key, page_prefix) = self.candidate_keys(scope);
        let pages = self.pages.derive(page_prefix, scope)?;
        let captured = CapturedHead::capture(&pages, &head_key).await?;
        let knowledge = AuthorityKnowledge::new(&captured, &pages)?;
        Ok(PublishedNamespace {
            literal: literal.to_vec(),
            binding,
            captured,
            knowledge,
            identity: self.pages.binding_identity(),
            registry_prefix: self.prefix.clone(),
            selected: pages.seal_binding()?,
            scope,
            _charge: charge,
        })
    }
    pub async fn capture_known(
        self: &Arc<Self>,
        known: PublishedNamespace,
    ) -> Result<PublishedNamespace, ObjectLogError> {
        known.validate_context(self)?;
        let charge = self.key_charge(&known.literal)?;
        let key = self.binding_key(known.scope.namespace);
        let current = self.pages.read_head(&key, self.binding_limit).await?;
        if current.bytes() != known.binding.bytes() {
            return Err(invalid("known immutable binding changed or vanished"));
        }
        let pages = self.pages.for_binding(&known.selected)?;
        let (head_key, _) = self.candidate_keys(known.scope);
        let captured = known
            .knowledge
            .observe(pages, &head_key)
            .await?
            .ok_or_else(|| invalid("known namespace authority regressed"))?;
        Ok(PublishedNamespace {
            literal: known.literal,
            binding: current,
            captured,
            knowledge: known.knowledge,
            identity: known.identity,
            registry_prefix: known.registry_prefix,
            selected: known.selected,
            scope: known.scope,
            _charge: charge,
        })
    }
    /// Fresh entropy is internal; tests inject only the entropy operation.
    pub async fn attempt(
        self: &Arc<Self>,
        literal: &[u8],
    ) -> Result<NativeProvisionAttempt, ObjectLogError> {
        self.attempt_with_entropy(literal, |out| {
            getrandom::fill(out).map_err(|_| invalid("incarnation entropy unavailable"))
        })
        .await
    }
    async fn attempt_with_entropy(
        self: &Arc<Self>,
        literal: &[u8],
        entropy: impl FnOnce(&mut [u8; 32]) -> Result<(), ObjectLogError>,
    ) -> Result<NativeProvisionAttempt, ObjectLogError> {
        let charge = self.key_charge(literal)?;
        let namespace = Sha256::digest(literal).into();
        let mut incarnation = [0; 32];
        entropy(&mut incarnation)?;
        if incarnation == [0; 32] {
            return Err(invalid("zero incarnation entropy"));
        }
        let binding_key = self.binding_key(namespace);
        // An existing registry entry is capture-only, including malformed entries.
        if self
            .pages
            .read_head(&binding_key, self.binding_limit)
            .await?
            .bytes()
            .is_some()
        {
            return Err(invalid("existing namespace requires capture"));
        }
        let scope = Scope {
            namespace,
            incarnation,
        };
        let (head_key, page_prefix) = self.candidate_keys(scope);
        let pages = self.pages.derive(page_prefix, scope)?;
        Ok(NativeProvisionAttempt {
            context: self.clone(),
            literal: literal.to_vec(),
            binding_key,
            head_key,
            pages,
            candidate_attempted: false,
            binding_attempted: false,
            binding_observed: None,
            ready: None,
            _charge: charge,
        })
    }
}
impl NativeProvisionAttempt {
    pub async fn prepare_candidate(&mut self) -> Result<(), ObjectLogError> {
        if self.candidate_attempted {
            return Err(invalid("candidate already attempted"));
        }
        self.candidate_attempted = true;
        let observed = self.pages.read_head(&self.head_key, HEAD_BYTES).await?;
        if observed.bytes().is_some() {
            return Err(invalid("incarnation collision"));
        }
        let expected = NativeHead::empty(self.pages.scope());
        match self
            .pages
            .swap_head(&self.head_key, observed, &expected.encode())
            .await?
        {
            MetadataSwap::Stored => {}
            MetadataSwap::Conflict(_) => return Err(invalid("incarnation collision")),
        }
        let captured = CapturedHead::capture(&self.pages, &self.head_key).await?;
        if captured.head_bytes() != expected.encode() {
            return Err(invalid("candidate changed before readiness"));
        }
        self.ready = Some(ReadyCandidate { captured });
        Ok(())
    }
    fn binding_frame(&self) -> Result<OwnedBinding, ObjectLogError> {
        let ready = self
            .ready
            .as_ref()
            .ok_or_else(|| invalid("candidate is not ready"))?;
        if ready.captured.head_bytes() != NativeHead::empty(self.pages.scope()).encode() {
            return Err(invalid("ready candidate changed"));
        }
        let length = self.context.length(&self.literal)?;
        let charge = self
            .context
            .pages
            .charge_output(length + std::mem::size_of::<Vec<u8>>())?;
        let scope = self.pages.scope();
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&scope.namespace);
        bytes.extend_from_slice(&scope.incarnation);
        bytes.extend_from_slice(&(self.literal.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&self.literal);
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        Ok(OwnedBinding {
            bytes,
            _charge: charge,
        })
    }
    pub async fn prepare_binding(&mut self) -> Result<(), ObjectLogError> {
        if self.binding_attempted || self.binding_observed.is_some() {
            return Err(invalid("binding observation already captured"));
        }
        if self.ready.is_none() {
            return Err(invalid("candidate is not ready"));
        }
        self.binding_observed = Some(
            self.context
                .pages
                .read_head(&self.binding_key, self.context.binding_limit)
                .await?,
        );
        Ok(())
    }
    pub async fn publish(&mut self) -> Result<PublishedNamespace, ObjectLogError> {
        if self.binding_attempted {
            return Err(invalid("binding already attempted"));
        }
        if self.binding_observed.is_none() {
            self.prepare_binding().await?;
        }
        let frame = self.binding_frame()?;
        self.binding_attempted = true;
        let observed = self
            .binding_observed
            .take()
            .expect("one original binding observation");
        if observed.bytes().is_none() {
            // An ambiguous response is resolved by the following fresh read;
            // never replace the original observation or resubmit this CAS.
            match self
                .context
                .pages
                .swap_head(&self.binding_key, observed, &frame.bytes)
                .await
            {
                Ok(_) | Err(ObjectLogError::StorageUnavailable(_)) => {}
                Err(error) => return Err(error),
            }
        }
        self.resolve().await
    }
    pub async fn resolve(&self) -> Result<PublishedNamespace, ObjectLogError> {
        if !self.binding_attempted {
            return Err(invalid("binding not attempted"));
        }
        let charge = self.context.key_charge(&self.literal)?;
        let winner = self
            .context
            .pages
            .read_head(&self.binding_key, self.context.binding_limit)
            .await?;
        self.context
            .validate_winner(&self.literal, winner, charge)
            .await
    }
}
impl PublishedNamespace {
    pub(super) fn retained_read(&self) -> Result<RetainedRead, ObjectLogError> {
        Ok(RetainedRead {
            captured: self.captured.clone(),
            binding: self.selected.clone(),
            knowledge: self.knowledge.clone(),
            charge: self._charge.clone(),
        })
    }
    pub(super) fn duplicate(&self, context: &NamespaceContext) -> Result<Self, ObjectLogError> {
        self.validate_context(context)?;
        let charge = context.key_charge(&self.literal)?;
        Ok(Self {
            literal: self.literal.clone(),
            binding: self.binding.clone(),
            captured: self.captured.clone(),
            knowledge: self.knowledge.clone(),
            identity: self.identity.clone(),
            registry_prefix: self.registry_prefix.clone(),
            selected: self.selected.clone(),
            scope: self.scope,
            _charge: charge,
        })
    }
    pub(super) fn charged_head_key(
        &self,
        context: &NamespaceContext,
        extra: usize,
    ) -> Result<ChargedHeadKey, ObjectLogError> {
        self.validate_context(context)?;
        let bytes = context
            .prefix
            .len()
            .checked_mul(8)
            .and_then(|n| n.checked_add(2048))
            .and_then(|n| n.checked_add(extra))
            .ok_or_else(|| invalid("head key allocation overflow"))?;
        let charge = context.pages.charge_output(bytes)?;
        let (key, _) = context.candidate_keys(self.scope);
        Ok(ChargedHeadKey { key, charge })
    }
    #[cfg(test)]
    pub fn scope(&self) -> Scope {
        self.scope
    }
    #[cfg(test)]
    pub fn head_bytes(&self) -> [u8; HEAD_BYTES] {
        self.captured.head_bytes()
    }
    pub async fn into_writer(
        self,
        context: &NamespaceContext,
    ) -> Result<(super::head::FencedHead, PageBinding), ObjectLogError> {
        self.validate_context(context)?;
        let key = context.binding_key(self.scope.namespace);
        let current = context.pages.read_head(&key, context.binding_limit).await?;
        if current.bytes() != self.binding.bytes() {
            return Err(invalid("known immutable binding changed or vanished"));
        }
        let pages = context.pages.for_binding(&self.selected)?;
        let (head_key, _) = context.candidate_keys(self.scope);
        let writer = self
            .captured
            .acquire_with_knowledge(&pages, &head_key, self.knowledge.clone())
            .await?;
        Ok((writer, self.selected))
    }
    pub fn validate_context(&self, context: &NamespaceContext) -> Result<(), ObjectLogError> {
        if self.registry_prefix != context.prefix
            || !Arc::ptr_eq(&self.identity, &context.pages.binding_identity())
        {
            return Err(invalid("namespace belongs to another context"));
        }
        decode_binding(
            self.binding
                .bytes()
                .ok_or_else(|| invalid("published binding missing"))?,
            &self.literal,
        )?;
        Ok(())
    }
}
fn decode_binding(raw: &[u8], literal: &[u8]) -> Result<Scope, ObjectLogError> {
    if raw.len() < HEADER + DIGEST || &raw[..8] != MAGIC {
        return Err(invalid("binding framing"));
    }
    let len = usize::try_from(u64::from_be_bytes(raw[72..80].try_into().unwrap()))
        .map_err(|_| invalid("binding literal length"))?;
    if HEADER.checked_add(len).and_then(|n| n.checked_add(DIGEST)) != Some(raw.len())
        || &raw[HEADER..HEADER + len] != literal
        || raw[8..40] != Sha256::digest(literal)[..]
        || Sha256::digest(&raw[..raw.len() - DIGEST])[..] != raw[raw.len() - DIGEST..]
    {
        return Err(invalid("binding identity or digest"));
    }
    let scope = Scope {
        namespace: raw[8..40].try_into().unwrap(),
        incarnation: raw[40..72].try_into().unwrap(),
    };
    if scope.incarnation == [0; 32] {
        return Err(invalid("binding zero incarnation"));
    }
    Ok(scope)
}
pub(super) fn validate_prefix(prefix: &str) -> Result<(), ObjectLogError> {
    if prefix.is_empty()
        || prefix.starts_with('/')
        || prefix.contains('\0')
        || prefix
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(invalid("namespace prefix"));
    }
    Ok(())
}
fn hex(raw: &[u8; 32]) -> String {
    raw.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::Sequencer(format!("native provision: {message}"))
}

#[cfg(test)]
mod tests {
    use super::super::{
        operation::{ExecutionConfig, Resources},
        page_store::NativeStore,
    };
    use super::*;
    use crate::{LocalBlobStore, MetadataLimits};
    trait TestStore: NativeStore {
        fn lose_reply(&self, key: &str);
    }
    impl TestStore for LocalBlobStore {
        fn lose_reply(&self, key: &str) {
            self.lose_next_metadata_reply(key);
        }
    }
    #[cfg(feature = "s3")]
    impl TestStore for crate::S3BlobStore {
        fn lose_reply(&self, key: &str) {
            self.lose_next_metadata_reply(key);
        }
    }
    async fn contract(factory: Arc<dyn Fn() -> Arc<dyn TestStore> + Send + Sync>) {
        let provider = factory();
        let independent = factory();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!("native-provision-{nonce}");
        let namespace_prefix = format!("{prefix}/registry");
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 2,
            bytes: 1 << 20,
        })
        .unwrap();
        let base = PageStore::new(
            provider.clone(),
            format!("{prefix}/wire-pages"),
            Scope {
                namespace: [1; 32],
                incarnation: [1; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 4096,
                version_bytes: 4096,
            },
        )
        .unwrap();
        let peer_base = PageStore::new(
            independent,
            format!("{prefix}/wire-pages"),
            Scope {
                namespace: [1; 32],
                incarnation: [1; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 4096,
                version_bytes: 4096,
            },
        )
        .unwrap();
        let budget = resources.bytes.clone();
        let physical_provider = provider.clone();
        let registry_prefix = namespace_prefix.clone();
        let (retained, retained_binding, retained_writer, writer_binding, weak) = resources
            .run(move |physical| async move {
                let weak = Arc::downgrade(&physical);
                let context = NamespaceContext::new(
                    base.in_operation(physical.clone())?,
                    registry_prefix.clone(),
                    4096,
                )?;
                let test_prefix = registry_prefix.clone();
                let peer = NamespaceContext::new(
                    peer_base.in_operation(physical)?,
                    registry_prefix,
                    4096,
                )?;
                let literal = b"tenant\0queue/\xce\xbb";
                let before = physical_provider.list(&test_prefix).await?;
                let before_constructor = budget.available();
                let probe_prefix = format!("{}-allocation", context.prefix);
                let probe =
                    NamespaceContext::new(context.pages.clone(), probe_prefix.clone(), 4096)?;
                let constructor_bytes = before_constructor - budget.available();
                drop(probe);
                let filler = budget.charge(before_constructor - constructor_bytes)?;
                let exact_context =
                    NamespaceContext::new(context.pages.clone(), probe_prefix.clone(), 4096)?;
                assert_eq!(budget.available(), 0);
                drop(exact_context);
                drop(filler);
                let filler = budget.charge(before_constructor - constructor_bytes + 1)?;
                assert!(NamespaceContext::new(context.pages.clone(), probe_prefix, 4096).is_err());
                drop(filler);
                let available = budget.available();
                let sample = context.key_charge(literal)?;
                let needed = available - budget.available();
                drop(sample);
                let filler = budget.charge(available - needed)?;
                let exact = context.key_charge(literal)?;
                assert_eq!(budget.available(), 0);
                drop(exact);
                drop(filler);
                let filler = budget.charge(available - needed + 1)?;
                assert!(context.key_charge(literal).is_err());
                drop(filler);
                assert_eq!(budget.available(), available);
                let fresh = context.attempt(b"real-os-entropy").await?;
                assert_ne!(fresh.pages.scope().incarnation, [0; 32]);
                drop(fresh);

                assert!(
                    context
                        .attempt_with_entropy(literal, |_| Err(invalid("injected entropy failure")))
                        .await
                        .is_err()
                );
                assert!(
                    context
                        .attempt_with_entropy(literal, |_| Ok(()))
                        .await
                        .is_err()
                );
                assert_eq!(physical_provider.list(&test_prefix).await?, before);
                let mut first = context
                    .attempt_with_entropy(literal, |out| {
                        *out = [2; 32];
                        Ok(())
                    })
                    .await?;
                let mut second = peer
                    .attempt_with_entropy(literal, |out| {
                        *out = [3; 32];
                        Ok(())
                    })
                    .await?;
                first.prepare_candidate().await?;
                second.prepare_candidate().await?;
                assert!(first.prepare_candidate().await.is_err());
                let selected = second.publish().await?;
                assert_eq!(selected.scope().incarnation, [3; 32]);
                let (selected_key, _) = context.candidate_keys(selected.scope());
                let selected_scope = selected.scope();
                assert!(context.pages.for_binding(&selected.selected).is_err());
                let (writer, selected_binding) = selected.into_writer(&peer).await?;
                let selected_pages = peer.pages.for_binding(&selected_binding)?;
                let mut appended = writer
                    .prepare_append(selected_pages.clone(), "selected-work", b"selected", 1)
                    .await?;
                appended.publish(selected_pages, &selected_key).await?;
                let advanced = physical_provider.get(&selected_key).await?.unwrap();
                physical_provider.delete(&selected_key).await?;
                // A delayed loser cannot initialize or replace the selected head.
                assert!(first.publish().await.is_err());
                assert!(physical_provider.get(&selected_key).await?.is_none());
                physical_provider.put(&selected_key, advanced).await?;
                let winner = first.resolve().await?;
                assert_eq!(winner.scope(), selected_scope);
                assert_eq!(
                    winner.head_bytes(),
                    context.open(literal).await?.unwrap().head_bytes()
                );
                winner.validate_context(&context)?;
                let other_prefix = NamespaceContext::new(
                    context.pages.clone(),
                    format!("{}-other", context.prefix),
                    4096,
                )?;
                assert!(winner.validate_context(&other_prefix).is_err());
                let foreign_cap = context.open(literal).await?.unwrap();
                assert!(foreign_cap.into_writer(&other_prefix).await.is_err());
                drop(other_prefix);

                assert!(winner.validate_context(&peer).is_err());
                assert!(first.publish().await.is_err());
                assert!(context.attempt(literal).await.is_err());
                let (selected_key, _) = context.candidate_keys(winner.scope());
                let saved = physical_provider.get(&selected_key).await?.unwrap();
                let known_cap = context.open(literal).await?.unwrap();
                physical_provider.delete(&selected_key).await?;
                assert!(context.capture_known(known_cap).await.is_err());
                assert!(context.open(literal).await.is_err());
                assert!(context.attempt(literal).await.is_err());
                physical_provider
                    .put(&selected_key, bytes::Bytes::from_static(b"corrupt"))
                    .await?;
                assert!(context.open(literal).await.is_err());
                let wrong_scope = NativeHead::empty(Scope {
                    namespace: [7; 32],
                    incarnation: [8; 32],
                });
                physical_provider
                    .put(
                        &selected_key,
                        bytes::Bytes::copy_from_slice(&wrong_scope.encode()),
                    )
                    .await?;
                assert!(context.open(literal).await.is_err());
                physical_provider.put(&selected_key, saved).await?;
                // Retained namespace knowledge accepts only a genuine extension.
                let known_before = context.open(literal).await?.unwrap();
                let stale_writer_cap = context.open(literal).await?.unwrap();
                let original_head = physical_provider.get(&selected_key).await?.unwrap();
                let advancement = context.open(literal).await?.unwrap();
                let (advanced_writer, advanced_binding) = advancement.into_writer(&context).await?;
                let advanced_pages = context.pages.for_binding(&advanced_binding)?;
                let mut extension = advanced_writer
                    .prepare_append(advanced_pages.clone(), "advanced-work", b"advanced", 1)
                    .await?;
                extension
                    .publish(advanced_pages.clone(), &selected_key)
                    .await?;
                let extended_head = physical_provider.get(&selected_key).await?.unwrap();
                let advanced_cap = context.capture_known(known_before).await?;
                assert_eq!(advanced_cap.head_bytes().as_slice(), extended_head.as_ref());
                assert!(stale_writer_cap.into_writer(&context).await.is_err());
                assert_eq!(
                    physical_provider.get(&selected_key).await?,
                    Some(extended_head.clone())
                );
                // A valid older head cannot replace already observed progress.
                physical_provider.put(&selected_key, original_head).await?;
                assert!(context.capture_known(advanced_cap).await.is_err());
                physical_provider
                    .put(&selected_key, extended_head.clone())
                    .await?;
                let nonempty_cap = context.open(literal).await?.unwrap();
                let root = super::super::codec::Descriptor::decode(&extended_head[96..160])?;
                let first_entry = super::super::index::Index::new(advanced_pages.clone())
                    .lookup(Some(root), 0)
                    .await?
                    .unwrap();
                let mut changed = *first_entry.entry();
                changed.payload_hash = [99; 32];
                drop(first_entry);
                let last_entry = super::super::index::Index::new(advanced_pages.clone())
                    .lookup(Some(root), 1)
                    .await?
                    .unwrap();
                let unchanged_last = *last_entry.entry();
                drop(last_entry);
                let rewritten = advanced_pages
                    .write(advanced_pages.leaf(&[changed, unchanged_last])?)
                    .await?;
                let mut rewritten_head = extended_head.to_vec();
                rewritten.encode_into(&mut rewritten_head[96..160]);
                let digest = Sha256::digest(&rewritten_head[..208]);
                rewritten_head[208..240].copy_from_slice(&digest);
                physical_provider
                    .put(&selected_key, bytes::Bytes::from(rewritten_head))
                    .await?;
                assert!(context.capture_known(nonempty_cap).await.is_err());
                physical_provider.put(&selected_key, extended_head).await?;
                drop(advanced_pages);
                drop(advanced_binding);
                let alias_literal = b"different-literal";
                let alias_key = context.binding_key(Sha256::digest(alias_literal).into());
                physical_provider
                    .put(
                        &alias_key,
                        bytes::Bytes::copy_from_slice(winner.binding.bytes().unwrap()),
                    )
                    .await?;
                assert!(context.open(alias_literal).await.is_err());
                assert!(context.attempt(alias_literal).await.is_err());

                // Both independent adapters retain absence before either CAS.
                let race_literal = b"actual-two-provider-race";
                let mut left = context
                    .attempt_with_entropy(race_literal, |out| {
                        *out = [6; 32];
                        Ok(())
                    })
                    .await?;
                let mut right = peer
                    .attempt_with_entropy(race_literal, |out| {
                        *out = [7; 32];
                        Ok(())
                    })
                    .await?;
                left.prepare_candidate().await?;
                right.prepare_candidate().await?;
                left.prepare_binding().await?;
                right.prepare_binding().await?;
                assert!(left.binding_observed.as_ref().unwrap().bytes().is_none());
                assert!(right.binding_observed.as_ref().unwrap().bytes().is_none());
                let (left_result, right_result) = tokio::join!(left.publish(), right.publish());
                let left_winner = left_result?;
                let right_winner = right_result?;
                assert_eq!(left_winner.scope(), right_winner.scope());
                assert!([[6; 32], [7; 32]].contains(&left_winner.scope().incarnation));
                let stored = context.open(race_literal).await?.unwrap();
                assert_eq!(left_winner.scope(), stored.scope());
                let loser_key = if left_winner.scope().incarnation == [6; 32] {
                    &right.head_key
                } else {
                    &left.head_key
                };
                assert_eq!(
                    physical_provider.get(loser_key).await?.unwrap().as_ref(),
                    NativeHead::empty(if left_winner.scope().incarnation == [6; 32] {
                        right.pages.scope()
                    } else {
                        left.pages.scope()
                    })
                    .encode()
                );
                drop(left_winner);
                drop(right_winner);
                drop(stored);
                // An ambiguous candidate never receives readiness or binding rights.
                let mut lost_candidate = context
                    .attempt_with_entropy(b"lost-candidate", |out| {
                        *out = [4; 32];
                        Ok(())
                    })
                    .await?;
                physical_provider.lose_reply(&lost_candidate.head_key);
                assert!(lost_candidate.prepare_candidate().await.is_err());
                assert!(lost_candidate.prepare_candidate().await.is_err());
                assert!(lost_candidate.publish().await.is_err());
                assert!(context.open(b"lost-candidate").await?.is_none());
                // Equal existing bytes are still an incarnation collision.
                let mut collision = context
                    .attempt_with_entropy(b"lost-candidate", |out| {
                        *out = [4; 32];
                        Ok(())
                    })
                    .await?;
                assert!(collision.prepare_candidate().await.is_err());
                assert!(collision.publish().await.is_err());
                // Lost binding response resolves the stored selection, without retry.
                let mut lost_binding = context
                    .attempt_with_entropy(b"lost-binding", |out| {
                        *out = [5; 32];
                        Ok(())
                    })
                    .await?;
                lost_binding.prepare_candidate().await?;
                physical_provider.lose_reply(&lost_binding.binding_key);
                let resolved = lost_binding.publish().await?;
                assert_eq!(resolved.scope().incarnation, [5; 32]);
                assert!(lost_binding.publish().await.is_err());
                drop(resolved);
                let selected_alias = winner.selected.clone();
                let final_cap = context.open(literal).await?.unwrap();
                let (retained_writer, writer_binding) = final_cap.into_writer(&context).await?;
                let selected_store = context.pages.for_binding(&writer_binding)?;
                let second_store_alias = selected_store.clone();
                drop(selected_store);
                let credits_before_last_store = budget.available();
                drop(second_store_alias);
                assert!(budget.available() > credits_before_last_store);
                Ok((
                    winner,
                    selected_alias,
                    retained_writer,
                    writer_binding,
                    weak,
                ))
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert!(weak.upgrade().is_none());
        assert!(resources.bytes.available() < (1 << 20));
        assert_eq!(retained.scope().incarnation, [3; 32]);
        drop(retained);
        let after_namespace = resources.bytes.available();
        assert!(after_namespace < (1 << 20));
        let last_alias = retained_binding.clone();
        drop(retained_binding);
        assert_eq!(resources.bytes.available(), after_namespace);
        let before_final_binding_alias = resources.bytes.available();
        drop(last_alias);
        assert!(resources.bytes.available() > before_final_binding_alias);
        assert!(resources.bytes.available() < (1 << 20));
        drop(retained_writer);
        assert!(resources.bytes.available() < (1 << 20));
        drop(writer_binding);
        assert_eq!(resources.bytes.available(), 1 << 20);
        let fresh = factory();
        let reopened_resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            fresh,
            format!("{prefix}/wire-pages"),
            Scope {
                namespace: [1; 32],
                incarnation: [1; 32],
            },
            reopened_resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 4096,
                version_bytes: 4096,
            },
        )
        .unwrap();
        reopened_resources
            .run(move |physical| async move {
                let context =
                    NamespaceContext::new(pages.in_operation(physical)?, namespace_prefix, 4096)?;
                let winner = context.open(b"tenant\0queue/\xce\xbb").await?.unwrap();
                assert_eq!(winner.scope().incarnation, [3; 32]);
                Ok(())
            })
            .await
            .unwrap();
        reopened_resources.close_and_drain().await.unwrap();
        for key in provider.list(&prefix).await.unwrap() {
            provider.delete(&key).await.unwrap();
        }
    }
    #[tokio::test]
    async fn local_candidate_election_loss_collision_known_missing_and_reopen() {
        let directory = Arc::new(tempfile::tempdir().unwrap());
        contract(Arc::new(move || {
            Arc::new(LocalBlobStore::new(directory.path()))
        }))
        .await;
    }
    #[cfg(feature = "s3")]
    #[tokio::test]
    #[ignore = "requires configured S3 provider; mandatory native-v2 qualification lane"]
    async fn real_s3_runs_identical_provision_contract_in_both_signing_modes() {
        let required =
            |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
        let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
        let region = required("OBJECT_LOG_S3_REGION");
        let bucket = required("OBJECT_LOG_S3_BUCKET");
        let key = required("OBJECT_LOG_S3_KEY_ID");
        let secret = required("OBJECT_LOG_S3_SECRET");
        for unsigned in [false, true] {
            let (endpoint, region, bucket, key, secret) = (
                endpoint.clone(),
                region.clone(),
                bucket.clone(),
                key.clone(),
                secret.clone(),
            );
            contract(Arc::new(move || {
                Arc::new(
                    crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                        .with_payload_signing_disabled(unsigned),
                )
            }))
            .await;
        }
    }
}
