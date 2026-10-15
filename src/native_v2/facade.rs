//! Native append-log facade. Queue policy remains in the consumer's planners.
use super::{
    codec::Scope,
    data::DataFrame,
    head::{
        AppendGroupLimits, AppendInput, FencedHead, NativeHead, OwnedPayload, ProofBudget,
        PublishOutcome, Resolution,
    },
    operation::{ExecutionConfig, Physical, Resources},
    page_store::{Charge, NativeStore, PageBinding, PageStore},
    provision::{NamespaceContext, PublishedNamespace, RetainedRead},
    submission::PublicationAttempt,
};
use crate::{LocalBlobStore, MetadataLimits, ObjectLogError};
use std::sync::Arc;
use tokio::sync::Mutex;
mod semantic;
pub use semantic::{
    NativeEditLimits, NativeScanLimits, NativeScanStatus, NativeSemanticContinuation,
    NativeSemanticEdit, NativeSemanticRange, NativeSemanticRow, NativeSemanticScan,
    NativeSemanticValue, NativeSemanticWork,
};

/// Explicit store-wide execution and owned-byte limits.
#[derive(Clone, Copy, Debug)]
pub struct NativeLimits {
    /// Executor worker threads, from one through 64.
    pub workers: usize,
    /// Blocking worker threads, from one through 64.
    pub blocking_workers: usize,
    /// Concurrent physical operations.
    pub operations: usize,
    /// Shared retained and in-flight owned-byte admission.
    pub bytes: u64,
    /// Maximum opaque metadata version bytes.
    pub version_bytes: usize,
    /// Maximum complete literal namespace binding frame bytes.
    pub binding_bytes: usize,
    /// Maximum entries in one conditional append group.
    pub group_entries: usize,
    /// Maximum aggregate framed bytes in one append group.
    pub group_bytes: usize,
}
impl NativeLimits {
    fn validate(self) -> Result<(), ObjectLogError> {
        let bytes = usize::try_from(self.bytes)
            .map_err(|_| ObjectLogError::InvalidConfig("native store limits".into()))?;
        if self.workers == 0
            || self.workers > 64
            || self.blocking_workers == 0
            || self.blocking_workers > 64
            || self.operations == 0
            || self.operations > tokio::sync::Semaphore::MAX_PERMITS
            || self.bytes == 0
            || isize::try_from(self.bytes).is_err()
            || self.version_bytes == 0
            || self.binding_bytes <= 112
            || self.binding_bytes > bytes
            || self.group_entries == 0
            || self.group_bytes == 0
            || self.group_bytes > bytes
        {
            return Err(ObjectLogError::InvalidConfig("native store limits".into()));
        }
        Ok(())
    }
}
struct Owner {
    resources: Arc<Resources>,
    pages: Arc<PageStore>,
    prefix: Arc<str>,
    limits: NativeLimits,
    identity: Arc<()>,
}
impl Owner {
    fn context(&self, physical: Arc<Physical>) -> Result<Arc<NamespaceContext>, ObjectLogError> {
        let pages = self.pages.in_operation(physical)?;
        let _copy = pages.charge_output(self.prefix.len() + std::mem::size_of::<String>())?;
        NamespaceContext::new(pages, self.prefix.to_string(), self.limits.binding_bytes)
    }
}
/// One bounded executor and byte pool shared by all namespaces and clones.
#[derive(Clone)]
pub struct NativeLog {
    owner: Arc<Owner>,
}
/// Validated namespace identity with shared monotonic authority knowledge.
#[derive(Clone)]
pub struct NativeNamespace {
    state: Arc<NamespaceState>,
    origin: Arc<()>,
}
struct NamespaceState {
    known: Mutex<PublishedNamespace>,
    _charge: Arc<Charge>,
}
/// Immutable authenticated read cut; payload reads do not refresh its head.
pub struct NativeReadCut {
    read: RetainedRead,
    origin: Arc<()>,
}
struct WriterBinding {
    writer: FencedHead,
    binding: PageBinding,
    head_key: String,
    _charge: Arc<Charge>,
}
/// Sealed writer family. Clones share knowledge and fencing authority.
#[derive(Clone)]
pub struct NativeWriter {
    inner: Arc<Mutex<WriterBinding>>,
    origin: Arc<()>,
}
/// Caller-retained original publication proof, including after reply loss.
#[derive(Clone)]
pub struct NativePublication {
    attempt: Arc<PublicationAttempt>,
    origin: Arc<()>,
}
/// Borrowed append input; backend-owned copies are admitted before allocation.
pub struct NativeAppend<'a> {
    /// Complete immutable logical identity.
    pub identity: &'a str,
    /// Opaque checked consumer record bytes.
    pub payload: &'a [u8],
    /// Logical records represented by this entry, from one through i32::MAX.
    pub records: u32,
}
struct OwnedAppend {
    identity: String,
    payload: Vec<u8>,
    records: u32,
}
struct LiteralInput {
    bytes: Vec<u8>,
    _charge: Arc<Charge>,
}
struct Inputs {
    entries: Vec<OwnedAppend>,
    _charge: Arc<Charge>,
}
// Drop implementations force whole-container capture under Rust's precise
// closure capture: input bytes cannot outlive their admission field.
impl Drop for LiteralInput {
    fn drop(&mut self) {}
}
impl Drop for Inputs {
    fn drop(&mut self) {}
}

/// Result of the original conditional publication, after authority validation.
#[derive(Debug, PartialEq, Eq)]
pub enum NativePublishOutcome {
    /// The exact original conditional write succeeded.
    Stored(NativeHead),
    /// Storage rejected the original condition; current authority validated.
    Conflict,
}

/// Complete original content resolution; does not establish writer identity.
#[derive(Debug, PartialEq, Eq)]
pub enum NativeResolution {
    /// All original ordered certificates are durably covered.
    DurableContent,
    /// Complete competing coverage is incompatible with the original content.
    IncompatibleCoverage,
    /// Available authenticated coverage cannot classify the complete range.
    Unknown,
}
/// Sealed authenticated entry plan for exact consumer admission before hydration.
pub struct NativeReadPlan {
    entry: super::index::OwnedEntry,
    binding: PageBinding,
    origin: Arc<()>,
    _charge: Arc<Charge>,
}
impl NativeReadPlan {
    /// Exact authenticated payload length; excludes immutable frame headers.
    pub fn payload_bytes(&self) -> usize {
        self.entry.entry().byte_len as usize
    }
    /// First logical offset represented by the entry.
    pub fn base_offset(&self) -> u64 {
        self.entry.entry().base
    }
    /// Logical records represented by the entry.
    pub fn record_count(&self) -> u32 {
        self.entry.entry().record_count
    }
}
impl Drop for NativeReadPlan {
    fn drop(&mut self) {}
}
/// A charged payload and its authenticated logical range.
pub struct NativeRecord {
    base: u64,
    records: u32,
    payload: OwnedPayload,
    _charge: Arc<Charge>,
}
impl NativeRecord {
    /// First logical offset represented by this immutable entry.
    pub fn base_offset(&self) -> u64 {
        self.base
    }
    /// Number of logical records in this entry.
    pub fn record_count(&self) -> u32 {
        self.records
    }
    /// Borrow payload bytes while their retained owner is alive.
    pub fn bytes(&self) -> &[u8] {
        self.payload.bytes()
    }
    /// Transfer payload without copying; every Bytes alias retains its admission.
    pub fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from_owner(self)
    }
}
impl AsRef<[u8]> for NativeRecord {
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}
impl NativeReadCut {
    fn retained(&self) -> RetainedRead {
        RetainedRead {
            captured: self.read.captured.clone(),
            binding: self.read.binding.clone(),
            knowledge: self.read.knowledge.clone(),
            charge: self.read.charge.clone(),
        }
    }

    /// Opaque complete captured authority, without a mutable root interface.
    pub fn head(&self) -> NativeHead {
        self.read.captured.value()
    }
}
impl NativeLog {
    /// Configure a local native log without contacting storage.
    pub fn local(
        store: LocalBlobStore,
        prefix: String,
        limits: NativeLimits,
    ) -> Result<Self, ObjectLogError> {
        Self::new(Arc::new(store), prefix, limits)
    }
    /// Configure an S3 native log over an already configured adapter.
    #[cfg(feature = "s3")]
    pub fn s3(
        store: crate::S3BlobStore,
        prefix: String,
        limits: NativeLimits,
    ) -> Result<Self, ObjectLogError> {
        Self::new(Arc::new(store), prefix, limits)
    }
    /// Pure S3 native configuration validation before client/cache construction.
    #[cfg(feature = "s3")]
    pub fn preflight_s3(
        prefix: &str,
        bucket: &str,
        limits: NativeLimits,
    ) -> Result<(), ObjectLogError> {
        limits.validate()?;
        super::provision::validate_prefix(prefix)?;
        let metadata = MetadataLimits {
            payload_bytes: limits.binding_bytes.max(limits.group_bytes).max(4096),
            version_bytes: limits.version_bytes,
        };
        let registry_len = prefix
            .len()
            .checked_add("/namespaces".len())
            .ok_or_else(|| invalid("registry length overflow"))?;
        let derived_len = super::provision::candidate_page_prefix_len(registry_len)?;
        let required =
            PageStore::planned_allocation_for_len(derived_len, metadata, |key_len, limits| {
                crate::S3BlobStore::planned_native_source_scratch(
                    key_len,
                    limits.version_bytes,
                    bucket.len(),
                )
            })?;
        if required > usize::try_from(limits.bytes).map_err(|_| invalid("native byte capacity"))? {
            return Err(invalid("native configuration exceeds byte admission"));
        }
        Ok(())
    }
    fn new(
        store: Arc<dyn NativeStore>,
        prefix: String,
        limits: NativeLimits,
    ) -> Result<Self, ObjectLogError> {
        limits.validate()?;
        super::provision::validate_prefix(&prefix)?;
        let metadata = MetadataLimits {
            payload_bytes: limits.binding_bytes.max(limits.group_bytes).max(4096),
            version_bytes: limits.version_bytes,
        };
        let registry_len = prefix
            .len()
            .checked_add("/namespaces".len())
            .ok_or_else(|| invalid("registry length overflow"))?;
        let derived_len = super::provision::candidate_page_prefix_len(registry_len)?;
        let required =
            PageStore::planned_allocation_for_len(derived_len, metadata, |key_len, limits| {
                store.page_source_scratch(key_len, limits)
            })?;
        if required > usize::try_from(limits.bytes).map_err(|_| invalid("native byte capacity"))? {
            return Err(invalid("native configuration exceeds byte admission"));
        }
        let page_prefix = format!("{prefix}/wire-pages");
        let registry_prefix = format!("{prefix}/namespaces");
        let resources = Resources::new(ExecutionConfig {
            workers: limits.workers,
            blocking_workers: limits.blocking_workers,
            operations: limits.operations,
            bytes: limits.bytes,
        })?;
        let pages = PageStore::new(
            store,
            page_prefix,
            Scope {
                namespace: [1; 32],
                incarnation: [1; 32],
            },
            resources.bytes.clone(),
            metadata,
        )?;
        Ok(Self {
            owner: Arc::new(Owner {
                resources,
                pages,
                prefix: registry_prefix.into(),
                limits,
                identity: Arc::new(()),
            }),
        })
    }
    fn check(&self, origin: &Arc<()>) -> Result<(), ObjectLogError> {
        if !Arc::ptr_eq(origin, &self.owner.identity) {
            return Err(invalid("native handle belongs to another store"));
        }
        Ok(())
    }
    fn literal(&self, literal: &[u8]) -> Result<LiteralInput, ObjectLogError> {
        if literal.is_empty()
            || literal
                .len()
                .checked_add(112)
                .is_none_or(|n| n > self.owner.limits.binding_bytes)
        {
            return Err(invalid("literal namespace binding limit"));
        }
        let charge = self.owner.pages.charge_output(
            literal
                .len()
                .checked_add(std::mem::size_of::<Vec<u8>>())
                .ok_or_else(|| invalid("literal size"))?,
        )?;
        Ok(LiteralInput {
            bytes: literal.to_vec(),
            _charge: charge,
        })
    }
    /// Capture an existing namespace. Missing/corrupt selected authority fails closed.
    pub async fn open(&self, literal: &[u8]) -> Result<Option<NativeNamespace>, ObjectLogError> {
        let literal = self.literal(literal)?;
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let context = owner.context(physical)?;
                let Some(known) = context.open(&literal.bytes).await? else {
                    return Ok(None);
                };
                let charge = owner.pages.charge_output(
                    std::mem::size_of::<NamespaceState>() + 2 * std::mem::size_of::<usize>(),
                )?;
                Ok(Some(NativeNamespace {
                    state: Arc::new(NamespaceState {
                        known: Mutex::new(known),
                        _charge: charge,
                    }),
                    origin: owner.identity.clone(),
                }))
            })
            .await
    }
    /// Explicitly publish a new empty namespace candidate; this does not provision a queue.
    pub async fn provision_new(&self, literal: &[u8]) -> Result<NativeNamespace, ObjectLogError> {
        let literal = self.literal(literal)?;
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let context = owner.context(physical)?;
                let charge = owner.pages.charge_output(
                    std::mem::size_of::<NamespaceState>() + 2 * std::mem::size_of::<usize>(),
                )?;
                let mut attempt = context.attempt(&literal.bytes).await?;
                attempt.prepare_candidate().await?;
                let known = attempt.publish().await?;
                Ok(NativeNamespace {
                    state: Arc::new(NamespaceState {
                        known: Mutex::new(known),
                        _charge: charge,
                    }),
                    origin: owner.identity.clone(),
                })
            })
            .await
    }
    /// Validate fresh authority and retain its knowledge before delivering a fixed cut.
    pub async fn capture(
        &self,
        namespace: &NativeNamespace,
    ) -> Result<NativeReadCut, ObjectLogError> {
        self.check(&namespace.origin)?;
        let namespace = namespace.clone();
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let context = owner.context(physical)?;
                let mut known = namespace.state.known.lock().await;
                let current = context.capture_known(known.duplicate(&context)?).await?;
                *known = current;
                Ok(NativeReadCut {
                    read: known.retained_read()?,
                    origin: owner.identity.clone(),
                })
            })
            .await
    }
    /// Fence the exact retained observation; no hidden fresh capture precedes CAS.
    pub async fn acquire(
        &self,
        namespace: &NativeNamespace,
    ) -> Result<NativeWriter, ObjectLogError> {
        self.check(&namespace.origin)?;
        let namespace = namespace.clone();
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let context = owner.context(physical)?;
                let known = namespace.state.known.lock().await;
                let selected = known.duplicate(&context)?;
                let charged_key = selected.charged_head_key(
                    &context,
                    std::mem::size_of::<Mutex<WriterBinding>>() + 2 * std::mem::size_of::<usize>(),
                )?;
                let (writer, binding) = selected.into_writer(&context).await?;
                Ok(NativeWriter {
                    inner: Arc::new(Mutex::new(WriterBinding {
                        writer,
                        binding,
                        head_key: charged_key.key,
                        _charge: charged_key.charge,
                    })),
                    origin: owner.identity.clone(),
                })
            })
            .await
    }
    /// Explicitly refresh a writer in its acquired epoch, without another fence.
    /// The fresh observation is retained before the response is delivered.
    pub async fn refresh_writer(
        &self,
        writer: &NativeWriter,
    ) -> Result<NativeReadCut, ObjectLogError> {
        self.check(&writer.origin)?;
        let writer = writer.clone();
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let pages = owner.pages.in_operation(physical)?;
                let mut writer = writer.inner.lock().await;
                let selected = pages.for_binding(&writer.binding)?;
                let current = writer
                    .writer
                    .clone()
                    .refresh(selected, &writer.head_key)
                    .await?;
                writer.writer = current;
                Ok(NativeReadCut {
                    read: RetainedRead {
                        captured: writer.writer.snapshot().clone(),
                        binding: writer.binding.clone(),
                        knowledge: writer.writer.knowledge(),
                        charge: writer._charge.clone(),
                    },
                    origin: owner.identity.clone(),
                })
            })
            .await
    }

    /// Prepare immutable group objects without publishing the authoritative head.
    pub async fn prepare(
        &self,
        writer: &NativeWriter,
        inputs: &[NativeAppend<'_>],
    ) -> Result<NativePublication, ObjectLogError> {
        self.prepare_inner(writer, inputs, None).await
    }
    async fn prepare_inner(
        &self,
        writer: &NativeWriter,
        inputs: &[NativeAppend<'_>],
        semantic: Option<(&NativeReadCut, &[NativeSemanticEdit<'_>], NativeEditLimits)>,
    ) -> Result<NativePublication, ObjectLogError> {
        self.check(&writer.origin)?;
        if inputs.is_empty() || inputs.len() > self.owner.limits.group_entries {
            return Err(invalid("native group entry limit"));
        }
        let mut framed = 0usize;
        let mut allocation = std::mem::size_of::<Inputs>();
        for (i, input) in inputs.iter().enumerate() {
            if input.records == 0
                || input.records > i32::MAX as u32
                || input.identity.is_empty()
                || input.identity.contains('\0')
                || inputs[..i]
                    .iter()
                    .any(|prior| prior.identity == input.identity)
            {
                return Err(invalid("native append identity or count"));
            }
            let length = if semantic.is_some() {
                DataFrame::coupled_encoded_len(input.identity, input.payload.len())?
            } else {
                DataFrame::encoded_len(input.identity, input.payload.len())?
            };
            framed = framed
                .checked_add(length)
                .ok_or_else(|| invalid("native group bytes"))?;
            allocation = allocation
                .checked_add(input.identity.len())
                .and_then(|n| n.checked_add(input.payload.len()))
                .and_then(|n| {
                    n.checked_add(
                        std::mem::size_of::<OwnedAppend>() + std::mem::size_of::<AppendInput<'_>>(),
                    )
                })
                .ok_or_else(|| invalid("native input allocation"))?;
        }
        if framed > self.owner.limits.group_bytes {
            return Err(invalid("native group framed limit"));
        }
        let semantic = semantic
            .map(|(cut, edits, limits)| {
                self.check(&cut.origin)?;
                Ok::<_, ObjectLogError>((
                    cut.retained(),
                    semantic::SemanticInputs::copy(&self.owner.pages, edits, limits)?,
                ))
            })
            .transpose()?;
        let charge = self.owner.pages.charge_output(allocation)?;
        let inputs = Inputs {
            entries: inputs
                .iter()
                .map(|i| OwnedAppend {
                    identity: i.identity.into(),
                    payload: i.payload.into(),
                    records: i.records,
                })
                .collect(),
            _charge: charge,
        };
        let writer = writer.clone();
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let scoped = owner.pages.in_operation(physical)?;
                let writer = writer.inner.lock().await;
                let pages = scoped.for_binding(&writer.binding)?;
                if let Some((expected, _)) = &semantic
                    && !writer.writer.snapshot().same_cut(&expected.captured)
                {
                    return Err(invalid(
                        "semantic edit cut differs from writer original authority",
                    ));
                }
                let references: Vec<_> = inputs
                    .entries
                    .iter()
                    .map(|i| AppendInput {
                        object_id: &i.identity,
                        payload: &i.payload,
                        records: i.records,
                    })
                    .collect();
                let limits = AppendGroupLimits {
                    entries: owner.limits.group_entries,
                    framed_bytes: owner.limits.group_bytes,
                };
                let prepared = if let Some((_, semantic)) = &semantic {
                    let edits = semantic.references();
                    let mut work = semantic.limits.work.budget();
                    writer
                        .writer
                        .clone()
                        .prepare_semantic_group(
                            pages.clone(),
                            &references,
                            limits,
                            super::head::SemanticEdits {
                                edits: &edits,
                                max_edits: semantic.limits.edits,
                                max_input_bytes: semantic.limits.input_bytes,
                                work: &mut work,
                            },
                        )
                        .await?
                } else {
                    writer
                        .writer
                        .clone()
                        .prepare_group(pages.clone(), &references, limits)
                        .await?
                };
                let attempt = PublicationAttempt::new(
                    &pages,
                    prepared,
                    writer.binding.clone(),
                    &writer.head_key,
                )?;
                Ok(NativePublication {
                    attempt,
                    origin: owner.identity.clone(),
                })
            })
            .await
    }
    /// Submit the retained original transition once; errors require original resolution.
    pub async fn submit(
        &self,
        publication: &NativePublication,
    ) -> Result<NativePublishOutcome, ObjectLogError> {
        self.check(&publication.origin)?;
        Ok(
            match publication
                .attempt
                .submit_outcome(&self.owner.resources, self.owner.pages.clone())
                .await?
            {
                PublishOutcome::Stored(head) => NativePublishOutcome::Stored(head),
                PublishOutcome::Conflict => NativePublishOutcome::Conflict,
            },
        )
    }
    /// Resolve the complete original group using bounded actual competing entries.
    pub async fn resolve(
        &self,
        publication: &NativePublication,
        entries: usize,
    ) -> Result<NativeResolution, ObjectLogError> {
        self.check(&publication.origin)?;
        Ok(
            match publication
                .attempt
                .resolve(
                    &self.owner.resources,
                    self.owner.pages.clone(),
                    ProofBudget { entries },
                )
                .await?
            {
                Resolution::DurableContent => NativeResolution::DurableContent,
                Resolution::Rejected => NativeResolution::IncompatibleCoverage,
                Resolution::Unknown => NativeResolution::Unknown,
            },
        )
    }
    /// Authenticate a fixed-cut entry without hydrating identity or payload bytes.
    pub async fn plan_read(
        &self,
        cut: &NativeReadCut,
        offset: u64,
    ) -> Result<Option<NativeReadPlan>, ObjectLogError> {
        self.check(&cut.origin)?;
        let read = RetainedRead {
            captured: cut.read.captured.clone(),
            binding: cut.read.binding.clone(),
            knowledge: cut.read.knowledge.clone(),
            charge: cut.read.charge.clone(),
        };
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let pages = owner
                    .pages
                    .in_operation(physical)?
                    .for_binding(&read.binding)?;
                let charge = pages.charge_output(std::mem::size_of::<NativeReadPlan>())?;
                let Some(entry) = read.captured.lookup(pages, offset).await? else {
                    return Ok(None);
                };
                Ok(Some(NativeReadPlan {
                    entry,
                    binding: read.binding.clone(),
                    origin: owner.identity.clone(),
                    _charge: charge,
                }))
            })
            .await
    }
    /// Hydrate the exact sealed entry after the consumer admits its actual length.
    pub async fn read_planned(
        &self,
        plan: NativeReadPlan,
        max_payload_bytes: usize,
    ) -> Result<NativeRecord, ObjectLogError> {
        self.check(&plan.origin)?;
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let pages = owner
                    .pages
                    .in_operation(physical)?
                    .for_binding(&plan.binding)?;
                let charge = pages.charge_output(std::mem::size_of::<NativeRecord>())?;
                let (base, records, payload) = super::head::CapturedHead::hydrate_record(
                    pages,
                    &plan.entry,
                    max_payload_bytes,
                )
                .await?;
                Ok(NativeRecord {
                    base,
                    records,
                    payload,
                    _charge: charge,
                })
            })
            .await
    }
    /// Read only the original authenticated cut; validate touched pages and frame bytes.
    pub async fn read(
        &self,
        cut: &NativeReadCut,
        offset: u64,
    ) -> Result<Option<NativeRecord>, ObjectLogError> {
        self.read_bounded(cut, offset, self.owner.limits.group_bytes)
            .await
    }
    /// Reject an oversized authenticated payload before issuing its data read.
    pub async fn read_bounded(
        &self,
        cut: &NativeReadCut,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<Option<NativeRecord>, ObjectLogError> {
        self.check(&cut.origin)?;
        let read = RetainedRead {
            captured: cut.read.captured.clone(),
            binding: cut.read.binding.clone(),
            knowledge: cut.read.knowledge.clone(),
            charge: cut.read.charge.clone(),
        };
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let pages = owner
                    .pages
                    .in_operation(physical)?
                    .for_binding(&read.binding)?;
                let charge = pages.charge_output(std::mem::size_of::<NativeRecord>())?;
                let Some((base, records, payload)) = read
                    .captured
                    .read_record_bounded(pages, offset, max_payload_bytes)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(NativeRecord {
                    base,
                    records,
                    payload,
                    _charge: charge,
                }))
            })
            .await
    }
    /// Close admission and drain actual owned work; retained byte-only handles may survive.
    pub async fn shutdown(&self) -> Result<(), ObjectLogError> {
        self.owner.resources.close_and_drain().await
    }
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::InvalidBatch(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn pure_s3_preflight_matches_constructed_adapter_allocation() {
        assert!(super::super::provision::candidate_page_prefix_len(usize::MAX).is_err());
        let bucket = "preflight-β-bucket";
        let prefix = "preflight/λ";
        let metadata = MetadataLimits {
            payload_bytes: 4096,
            version_bytes: 4096,
        };
        let (_, derived) = super::super::provision::candidate_keys(
            &format!("{prefix}/namespaces"),
            Scope {
                namespace: [1; 32],
                incarnation: [1; 32],
            },
        );
        let required = PageStore::planned_allocation_with(&derived, metadata, |key_len, limits| {
            crate::S3BlobStore::planned_native_source_scratch(
                key_len,
                limits.version_bytes,
                bucket.len(),
            )
        })
        .unwrap();
        let mut limits = NativeLimits {
            workers: 1,
            blocking_workers: 1,
            operations: 1,
            bytes: (required - 1).try_into().unwrap(),
            version_bytes: 4096,
            binding_bytes: 256,
            group_entries: 1,
            group_bytes: 4096,
        };
        assert!(NativeLog::preflight_s3(prefix, bucket, limits).is_err());
        limits.bytes = required.try_into().unwrap();
        NativeLog::preflight_s3(prefix, bucket, limits).unwrap();
        let store = crate::S3BlobStore::new(
            "http://127.0.0.1:1",
            "us-east-1",
            bucket,
            "preflight-key",
            "preflight-secret",
        );
        assert_eq!(
            PageStore::planned_allocation(&store, &derived, metadata).unwrap(),
            required
        );
        let log = NativeLog::s3(store, prefix.into(), limits).unwrap();
        log.shutdown().await.unwrap();
        limits.operations = 0;
        assert!(NativeLog::preflight_s3(prefix, bucket, limits).is_err());
        limits.operations = 1;
        assert!(NativeLog::preflight_s3("invalid/../prefix", bucket, limits).is_err());
    }

    #[tokio::test]
    async fn sealed_read_plan_retains_credit_after_drain() {
        let directory = tempfile::tempdir().unwrap();
        let log = NativeLog::local(
            crate::LocalBlobStore::new(directory.path()),
            "plan-owner".into(),
            NativeLimits {
                workers: 1,
                blocking_workers: 1,
                operations: 2,
                bytes: 4 << 20,
                version_bytes: 4096,
                binding_bytes: 4096,
                group_entries: 1,
                group_bytes: 1 << 20,
            },
        )
        .unwrap();
        let before = log.owner.resources.bytes.available();
        let namespace = log.provision_new(b"queue").await.unwrap();
        let writer = log.acquire(&namespace).await.unwrap();
        let publication = log
            .prepare(
                &writer,
                &[NativeAppend {
                    identity: "one",
                    payload: b"planned",
                    records: 1,
                }],
            )
            .await
            .unwrap();
        log.submit(&publication).await.unwrap();
        let cut = log.capture(&namespace).await.unwrap();
        let plan = log.plan_read(&cut, 0).await.unwrap().unwrap();
        log.shutdown().await.unwrap();
        drop((namespace, writer, publication, cut));
        assert_eq!(plan.payload_bytes(), 7);
        assert!(log.owner.resources.bytes.available() < before);
        drop(plan);
        assert_eq!(log.owner.resources.bytes.available(), before);
    }

    #[tokio::test]
    async fn payload_byte_aliases_retain_credit_after_drain() {
        let directory = tempfile::tempdir().unwrap();
        let log = NativeLog::local(
            crate::LocalBlobStore::new(directory.path()),
            "byte-owner".into(),
            NativeLimits {
                workers: 1,
                blocking_workers: 1,
                operations: 2,
                bytes: 4 << 20,
                version_bytes: 4096,
                binding_bytes: 4096,
                group_entries: 1,
                group_bytes: 1 << 20,
            },
        )
        .unwrap();
        let before = log.owner.resources.bytes.available();
        let namespace = log.provision_new(b"queue").await.unwrap();
        let writer = log.acquire(&namespace).await.unwrap();
        let publication = log
            .prepare(
                &writer,
                &[NativeAppend {
                    identity: "one",
                    payload: b"retained",
                    records: 1,
                }],
            )
            .await
            .unwrap();
        log.submit(&publication).await.unwrap();
        let cut = log.capture(&namespace).await.unwrap();
        let bytes = log.read(&cut, 0).await.unwrap().unwrap().into_bytes();
        let alias = bytes.clone();
        drop(bytes);
        log.shutdown().await.unwrap();
        drop((namespace, writer, publication, cut));
        assert!(log.owner.resources.bytes.available() < before);
        assert_eq!(alias.as_ref(), b"retained");
        drop(alias);
        assert_eq!(log.owner.resources.bytes.available(), before);
    }

    #[tokio::test]
    async fn constructor_rejects_derived_page_allocation_minus_one_before_io() {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::LocalBlobStore::new(directory.path());
        let prefix = "derived-allocation";
        let metadata = MetadataLimits {
            payload_bytes: 4096,
            version_bytes: 4096,
        };
        let (_, derived) = super::super::provision::candidate_keys(
            &format!("{prefix}/namespaces"),
            Scope {
                namespace: [1; 32],
                incarnation: [1; 32],
            },
        );
        let required = PageStore::planned_allocation(&store, &derived, metadata).unwrap();
        let base = PageStore::planned_allocation(&store, &format!("{prefix}/wire-pages"), metadata)
            .unwrap();
        assert!(required > base);
        let mut limits = NativeLimits {
            workers: 1,
            blocking_workers: 1,
            operations: 1,
            bytes: (required - 1).try_into().unwrap(),
            version_bytes: 4096,
            binding_bytes: 256,
            group_entries: 1,
            group_bytes: 4096,
        };
        assert!(NativeLog::local(store.clone(), prefix.into(), limits).is_err());
        assert!(crate::BlobStore::list(&store, "").await.unwrap().is_empty());
        limits.bytes = required.try_into().unwrap();
        let log = NativeLog::local(store.clone(), prefix.into(), limits).unwrap();
        assert!(crate::BlobStore::list(&store, "").await.unwrap().is_empty());
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn admitted_namespace_state_retains_credit_after_caller_abort() {
        let directory = tempfile::tempdir().unwrap();
        let log = NativeLog::local(
            crate::LocalBlobStore::new(directory.path()),
            "cancelled-namespace".into(),
            NativeLimits {
                workers: 2,
                blocking_workers: 2,
                operations: 4,
                bytes: 4 << 20,
                version_bytes: 4096,
                binding_bytes: 4096,
                group_entries: 16,
                group_bytes: 1 << 20,
            },
        )
        .unwrap();
        let before = log.owner.resources.bytes.available();
        let namespace = log.provision_new(b"queue").await.unwrap();
        let weak = Arc::downgrade(&namespace.state);
        let admitted_credit = log.owner.resources.bytes.available();
        assert!(admitted_credit < before);
        let resources = log.owner.resources.clone();
        let (started, admitted) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            resources
                .run(move |_physical| async move {
                    // Deliberately use only the state field: this reproduces precise
                    // closure capture in capture/acquire without timing-dependent IO.
                    let guard = namespace.state.known.lock().await;
                    started.send(()).unwrap();
                    resume.await.unwrap();
                    drop(guard);
                    Ok(())
                })
                .await
        });
        admitted.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(weak.upgrade().is_some());
        assert_eq!(log.owner.resources.bytes.available(), admitted_credit);
        release.send(()).unwrap();
        log.shutdown().await.unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(log.owner.resources.bytes.available(), before);
    }
}
