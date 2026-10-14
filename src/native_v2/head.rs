//! Captured read authority, sealed writer fencing, and append-only publication.
#[cfg(test)]
use crate::{AppendOwner, BoundedMetadataStore, MetadataLimits, MetadataOwners};
use crate::{MetadataSnapshot, MetadataSwap, ObjectLogError};
#[cfg(test)]
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use tokio::sync::{Notify, Semaphore, oneshot};

const MAGIC: &[u8; 8] = b"ONH2\0\0\0\x02";
pub(super) const HEAD_BYTES: usize = 240;
use crate::native_v2::codec::{Descriptor, Kind};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SemanticCoverage {
    Uninitialized,
    Covered { through: u64, root: [u8; 32] },
}
#[cfg(test)]
fn metadata_owners(owner: AppendOwner) -> MetadataOwners {
    MetadataOwners {
        retained: owner.clone(),
        physical: owner,
    }
}
#[cfg(test)]
fn metadata_limits(payload_bytes: usize) -> MetadataLimits {
    MetadataLimits {
        payload_bytes,
        version_bytes: 4096,
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PublishOutcome {
    Stored(NativeHead),
    Conflict,
}

/// An exact versioned native head, obtained from bounded durable authority.
/// Roots are opaque commitments; this observation grants neither writer
/// exclusivity nor semantic membership/nonmembership proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeHead {
    namespace: [u8; 32],
    incarnation: [u8; 32],
    epoch: u64,
    log_start: u64,
    next_offset: u64,
    index_root: Option<Descriptor>,
    semantic_anchor: SemanticCoverage,
}
impl NativeHead {
    pub(super) fn empty(scope: super::codec::Scope) -> Self {
        Self {
            namespace: scope.namespace,
            incarnation: scope.incarnation,
            epoch: 0,
            log_start: 0,
            next_offset: 0,
            index_root: None,
            semantic_anchor: SemanticCoverage::Uninitialized,
        }
    }
    /// Durable writer fence, distinct from any historical record epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Retained logical bounds from the same exact head.
    pub fn bounds(&self) -> (u64, u64) {
        (self.log_start, self.next_offset)
    }
    /// Complete immutable index descriptor captured with this head.
    #[cfg(test)]
    pub(crate) fn index_root(&self) -> Option<Descriptor> {
        self.index_root
    }
    #[cfg(test)]
    fn semantic_anchor(&self) -> SemanticCoverage {
        self.semantic_anchor
    }
    pub(super) fn encode(&self) -> [u8; HEAD_BYTES] {
        let mut out = [0; HEAD_BYTES];
        out[..8].copy_from_slice(MAGIC);
        out[8..40].copy_from_slice(&self.namespace);
        out[40..72].copy_from_slice(&self.incarnation);
        out[72..80].copy_from_slice(&self.epoch.to_be_bytes());
        out[80..88].copy_from_slice(&self.log_start.to_be_bytes());
        out[88..96].copy_from_slice(&self.next_offset.to_be_bytes());
        if let Some(root) = self.index_root {
            root.encode_into(&mut out[96..160]);
        }
        if let SemanticCoverage::Covered { through, root } = self.semantic_anchor {
            out[160] = 1;
            out[168..176].copy_from_slice(&through.to_be_bytes());
            out[176..208].copy_from_slice(&root);
        }
        let checksum = Sha256::digest(&out[..208]);
        out[208..].copy_from_slice(&checksum);
        out
    }
    fn decode(
        raw: &[u8],
        namespace: &[u8; 32],
        incarnation: &[u8; 32],
    ) -> Result<Self, ObjectLogError> {
        if raw.len() != HEAD_BYTES
            || &raw[..8] != MAGIC
            || Sha256::digest(&raw[..208]).as_slice() != &raw[208..]
        {
            return Err(invalid("invalid native head format, length or checksum"));
        }
        let head = Self {
            namespace: raw[8..40].try_into().expect("fixed range"),
            incarnation: raw[40..72].try_into().expect("fixed range"),
            epoch: u64::from_be_bytes(raw[72..80].try_into().expect("fixed range")),
            log_start: u64::from_be_bytes(raw[80..88].try_into().expect("fixed range")),
            next_offset: u64::from_be_bytes(raw[88..96].try_into().expect("fixed range")),
            index_root: if raw[96..160] == [0; 64] {
                None
            } else {
                Some(Descriptor::decode(&raw[96..160])?)
            },
            semantic_anchor: match raw[160] {
                0 if raw[161..208] == [0; 47] => SemanticCoverage::Uninitialized,
                1 if raw[161..168] == [0; 7] && raw[176..208] != [0; 32] => {
                    SemanticCoverage::Covered {
                        through: u64::from_be_bytes(raw[168..176].try_into().expect("fixed range")),
                        root: raw[176..208].try_into().expect("fixed range"),
                    }
                }
                _ => return Err(invalid("native semantic coverage framing")),
            },
        };
        if head.namespace != *namespace
            || head.incarnation != *incarnation
            || head.log_start != 0
            || head.next_offset > i64::MAX as u64
            || match head.index_root {
                None => head.next_offset != 0,
                Some(root) => {
                    root.kind == Kind::Overflow || root.start != 0 || root.end != head.next_offset
                }
            }
            || matches!(head.semantic_anchor, SemanticCoverage::Covered { through, .. } if through > head.next_offset)
        {
            return Err(invalid("native head scope, bounds or roots mismatch"));
        }
        Ok(head)
    }
}
/// Sealed captured authority retains only the durable observation's bytes and
/// immutable binding identity. It cannot retain executor admission.
#[derive(Clone)]
pub(super) struct CapturedHead {
    head: NativeHead,
    observed: MetadataSnapshot,
    identity: Arc<()>,
}
struct LineageState {
    floor: NativeHead,
    highest_epoch: u64,
}
pub(super) struct AuthorityKnowledge {
    gate: Arc<tokio::sync::Mutex<LineageState>>,
    identity: Arc<()>,
    key_owner: MetadataSnapshot,
    _charge: Arc<super::page_store::Charge>,
}
impl AuthorityKnowledge {
    pub fn new(
        captured: &CapturedHead,
        pages: &super::page_store::PageStore,
    ) -> Result<Arc<Self>, ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&captured.identity, &pages.binding_identity()) {
            return Err(invalid("knowledge belongs to another store binding"));
        }
        let charge = pages.charge_output(
            std::mem::size_of::<Self>()
                + std::mem::size_of::<tokio::sync::Mutex<LineageState>>()
                + 4 * std::mem::size_of::<usize>(),
        )?;
        Ok(Arc::new(Self {
            gate: Arc::new(tokio::sync::Mutex::new(LineageState {
                floor: captured.head,
                highest_epoch: captured.head.epoch,
            })),
            identity: pages.binding_identity(),
            key_owner: captured.observed.clone(),
            _charge: charge,
        }))
    }
    pub async fn observe(
        &self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<Option<CapturedHead>, ObjectLogError> {
        if !Arc::ptr_eq(&self.identity, &pages.binding_identity())
            || self.key_owner.inner.key != key
        {
            return Err(invalid("knowledge belongs to another authority binding"));
        }
        let mut state = self.gate.lock().await;
        let observed = pages.read_head(key, HEAD_BYTES).await?;
        WriterLineage::validate(&mut state, pages, observed).await
    }
}
struct WriterLineage {
    gate: Arc<tokio::sync::Mutex<LineageState>>,
    acquired_epoch: u64,
    _knowledge: Arc<AuthorityKnowledge>,
    _charge: Arc<super::page_store::Charge>,
}
impl WriterLineage {
    async fn observe(
        &self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<Option<CapturedHead>, ObjectLogError> {
        let mut state = self.gate.lock().await;
        let observed = pages.read_head(key, HEAD_BYTES).await?;
        Self::validate(&mut state, pages, observed).await
    }
    async fn validate(
        state: &mut LineageState,
        pages: Arc<super::page_store::PageStore>,
        observed: MetadataSnapshot,
    ) -> Result<Option<CapturedHead>, ObjectLogError> {
        let scope = pages.scope();
        let head = NativeHead::decode(
            observed
                .bytes()
                .ok_or_else(|| invalid("known native head is missing"))?,
            &scope.namespace,
            &scope.incarnation,
        )?;
        // Seeing a higher valid fence revokes this lineage even if its root is
        // missing/corrupt or the attempted range cannot subsequently resolve.
        state.highest_epoch = state.highest_epoch.max(head.epoch);
        if let Some(root) = head.index_root {
            drop(pages.read(root).await?);
        }
        let floor = state.floor;
        if head.epoch < state.highest_epoch
            || head.epoch < floor.epoch
            || head.log_start != floor.log_start
            || head.next_offset < floor.next_offset
            || head.semantic_anchor != floor.semantic_anchor
        {
            return Ok(None);
        }
        if let Some(before) = floor.index_root {
            let Some(after) = head.index_root else {
                return Ok(None);
            };
            if !super::index::Index::new(pages.clone())
                .proves_extension(before, after)
                .await?
            {
                return Ok(None);
            }
        }
        state.floor = head;
        Ok(Some(CapturedHead {
            head,
            observed,
            identity: pages.binding_identity(),
        }))
    }
}

pub(super) struct AppendInput<'a> {
    pub object_id: &'a str,
    pub payload: &'a [u8],
    pub records: u32,
}
pub(super) struct AppendGroupLimits {
    pub entries: usize,
    pub framed_bytes: usize,
}
pub(super) struct ProofBudget {
    pub entries: usize,
}
struct OwnedCertificates {
    entries: Vec<super::codec::LeafEntry>,
    _charge: Arc<super::page_store::Charge>,
}

/// Private transition: callers cannot replace its original observation or root.
pub(super) struct PreparedAppend {
    original: CapturedHead,
    next: NativeHead,
    attempted: bool,
    certificates: OwnedCertificates,
    lineage: Arc<WriterLineage>,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Resolution {
    DurableContent,
    Rejected,
    Unknown,
}
impl PreparedAppend {
    pub(super) fn validate_binding(
        &self,
        pages: &super::page_store::PageStore,
        key: &str,
    ) -> Result<(), ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.original.identity, &pages.binding_identity())
            || self.original.observed.inner.key != key
        {
            return Err(invalid("append belongs to another authority binding"));
        }
        Ok(())
    }
    #[cfg(test)]
    pub async fn resolve_range(
        &self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<Resolution, ObjectLogError> {
        self.resolve_group(pages, key, ProofBudget { entries: 1 })
            .await
    }
    pub async fn resolve_group(
        &self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
        budget: ProofBudget,
    ) -> Result<Resolution, ObjectLogError> {
        if budget.entries == 0 {
            return Err(invalid("zero group proof budget"));
        }
        if !self.attempted || self.original.observed.inner.key != key {
            return Err(invalid(
                "append resolution requires original attempted authority",
            ));
        }
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.original.identity, &pages.binding_identity()) {
            return Err(invalid("append belongs to another store binding"));
        }
        let Some(current) = self.lineage.observe(pages.clone(), key).await? else {
            return Ok(Resolution::Unknown);
        };
        if current.head.epoch < self.original.head.epoch
            || current.head.next_offset < self.original.head.next_offset
            || current.head.log_start != self.original.head.log_start
            || current.head.semantic_anchor != self.original.head.semantic_anchor
        {
            return Ok(Resolution::Unknown);
        }
        let index = super::index::Index::new(pages.clone());
        if let Some(before) = self.original.head.index_root {
            let Some(after) = current.head.index_root else {
                return Ok(Resolution::Unknown);
            };
            if !index.proves_extension(before, after).await? {
                return Ok(Resolution::Unknown);
            }
        }
        if current.head.next_offset < self.next.next_offset {
            return Ok(Resolution::Unknown);
        }
        let mut cursor = self.original.head.next_offset;
        let mut ordinal = 0;
        let mut mismatch = false;
        for _ in 0..budget.entries {
            let Some(owned) = current.lookup(pages.clone(), cursor).await? else {
                return Ok(Resolution::Unknown);
            };
            let actual = *owned.entry();
            drop(owned);
            if actual.base != cursor {
                return Ok(Resolution::Unknown);
            }
            let end = actual.end()?;
            if end <= cursor {
                return Ok(Resolution::Unknown);
            }
            mismatch |= self.certificates.entries.get(ordinal) != Some(&actual);
            ordinal += 1;
            cursor = end;
            if cursor >= self.next.next_offset {
                return Ok(
                    if !mismatch
                        && ordinal == self.certificates.entries.len()
                        && cursor == self.next.next_offset
                    {
                        Resolution::DurableContent
                    } else {
                        Resolution::Rejected
                    },
                );
            }
        }
        Ok(Resolution::Unknown)
    }
    pub async fn publish_outcome(
        &mut self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<PublishOutcome, ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.original.identity, &pages.binding_identity()) {
            return Err(invalid("append belongs to another store binding"));
        }
        if self.attempted {
            return Err(invalid("append publication already attempted"));
        }
        if self.original.observed.inner.key != key {
            return Err(invalid("append literal key mismatch"));
        }
        // A publication invocation is terminal even when shared knowledge lets
        // us reject it before issuing its original conditional write.
        self.attempted = true;
        let mut lineage = self.lineage.gate.lock().await;
        if lineage.highest_epoch > self.lineage.acquired_epoch
            || lineage.floor != self.original.head
        {
            return Err(invalid(
                "writer original authority is below its shared floor",
            ));
        }
        match pages
            .swap_head(key, self.original.observed.clone(), &self.next.encode())
            .await?
        {
            MetadataSwap::Stored => {
                lineage.floor = self.next;
                Ok(PublishOutcome::Stored(self.next))
            }
            MetadataSwap::Conflict(current) => {
                WriterLineage::validate(&mut lineage, pages, current)
                    .await?
                    .ok_or_else(|| invalid("conflicting authority regressed or changed prefix"))?;
                Ok(PublishOutcome::Conflict)
            }
        }
    }
    #[cfg(test)]
    pub async fn publish(
        &mut self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<NativeHead, ObjectLogError> {
        match self.publish_outcome(pages, key).await? {
            PublishOutcome::Stored(head) => Ok(head),
            PublishOutcome::Conflict => Err(invalid("original append authority changed")),
        }
    }
    /// Exact full-head equality proves this complete original transition.
    /// Later/different authority requires resolve_range instead;
    /// numeric head advancement alone never establishes our publication.
    #[cfg(test)]
    pub async fn resolve_exact(
        &self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<Option<NativeHead>, ObjectLogError> {
        if !self.attempted {
            return Err(invalid("append publication has not been attempted"));
        }
        if !Arc::ptr_eq(&self.original.identity, &pages.binding_identity()) {
            return Err(invalid("append belongs to another store binding"));
        }
        if self.original.observed.inner.key != key {
            return Err(invalid("append literal key mismatch"));
        }
        let Some(current) = self.lineage.observe(pages, key).await? else {
            return Ok(None);
        };
        Ok((current.head == self.next).then_some(self.next))
    }
}
pub(super) struct OwnedPayload {
    observed: MetadataSnapshot,
    start: usize,
    len: usize,
}
impl OwnedPayload {
    pub fn bytes(&self) -> &[u8] {
        &self.observed.bytes().expect("validated immutable payload")
            [self.start..self.start + self.len]
    }
}
/// Writer authority is constructed only by a successful fence and exact recapture.
#[derive(Clone)]
pub(super) struct FencedHead {
    captured: CapturedHead,
    lineage: Arc<WriterLineage>,
}
impl FencedHead {
    pub(super) fn knowledge(&self) -> Arc<AuthorityKnowledge> {
        self.lineage._knowledge.clone()
    }
    pub fn snapshot(&self) -> &CapturedHead {
        &self.captured
    }
    #[cfg(test)]
    pub async fn prepare_append(
        self,
        pages: Arc<super::page_store::PageStore>,
        object_id: &str,
        raw: &[u8],
        records: u32,
    ) -> Result<PreparedAppend, ObjectLogError> {
        let framed_bytes = super::data::DataFrame::encoded_len(object_id, raw.len())?;
        self.prepare_group(
            pages,
            &[AppendInput {
                object_id,
                payload: raw,
                records,
            }],
            AppendGroupLimits {
                entries: 1,
                framed_bytes,
            },
        )
        .await
    }
    pub async fn prepare_group(
        self,
        pages: Arc<super::page_store::PageStore>,
        inputs: &[AppendInput<'_>],
        limits: AppendGroupLimits,
    ) -> Result<PreparedAppend, ObjectLogError> {
        if inputs.is_empty()
            || limits.entries == 0
            || limits.framed_bytes == 0
            || inputs.len() > limits.entries
        {
            return Err(invalid("append group limits"));
        }
        let captured = self.captured;
        let lineage = self.lineage;
        pages.require_operation()?;
        {
            let state = lineage.gate.lock().await;
            if state.highest_epoch > lineage.acquired_epoch || state.floor != captured.head {
                return Err(invalid(
                    "writer original authority is below its shared floor",
                ));
            }
        }
        if !Arc::ptr_eq(&captured.identity, &pages.binding_identity()) {
            return Err(invalid("captured head belongs to another store binding"));
        }
        if captured.head.semantic_anchor != SemanticCoverage::Uninitialized {
            return Err(invalid(
                "append requires consumer semantic publication for covered heads",
            ));
        }
        let mut next_offset = captured.head.next_offset;
        let mut framed_bytes = 0usize;
        // Validate the entire group before the first immutable write.
        for (ordinal, input) in inputs.iter().enumerate() {
            if input.records == 0 || input.records > i32::MAX as u32 {
                return Err(invalid("append record count"));
            }
            next_offset = next_offset
                .checked_add(u64::from(input.records))
                .filter(|next| *next <= i64::MAX as u64)
                .ok_or_else(|| invalid("append offset overflow"))?;
            let frame_len =
                super::data::DataFrame::encoded_len(input.object_id, input.payload.len())?;
            pages.validate_data_limit(frame_len)?;
            framed_bytes = framed_bytes
                .checked_add(frame_len)
                .filter(|bytes| *bytes <= limits.framed_bytes)
                .ok_or_else(|| invalid("append group framed byte limit"))?;
            if inputs[..ordinal]
                .iter()
                .any(|prior| prior.object_id == input.object_id)
            {
                return Err(invalid("duplicate logical identity in append group"));
            }
        }
        let retained_bytes = inputs
            .len()
            .checked_mul(std::mem::size_of::<super::codec::LeafEntry>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<OwnedCertificates>()))
            .ok_or_else(|| invalid("append certificate capacity"))?;
        let charge = pages.charge_output(retained_bytes)?;
        let mut certificates = OwnedCertificates {
            entries: Vec::with_capacity(inputs.len()),
            _charge: charge,
        };
        // Resource probes include the permanently retained group certificates.
        // They are capacity checks, not reservations against concurrent work.
        for input in inputs {
            let key = pages.data_key(input.object_id)?;
            let frame_len =
                super::data::DataFrame::encoded_len(input.object_id, input.payload.len())?;
            let frame_charge = super::data::DataFrame::charge_frame(&pages, frame_len)?;
            pages.preflight_data(key.as_str(), frame_len)?;
            drop(frame_charge);
        }
        let index = super::index::Index::new(pages.clone());
        let mut root = captured.head.index_root;
        let mut base = captured.head.next_offset;
        for input in inputs {
            let physical_key = pages.data_key(input.object_id)?;
            let frame = super::data::DataFrame::encode(&pages, input.object_id, input.payload)?;
            let view = super::data::DataFrame::decode(frame.bytes())?;
            let logical_id = index.store_literal_id(input.object_id).await?;
            let identity = index.store_object_id(physical_key.as_str()).await?;
            pages
                .upload_data(physical_key.as_str(), frame.bytes())
                .await?;
            let certificate = super::codec::LeafEntry {
                object_id: identity,
                logical_id,
                base,
                record_count: input.records,
                byte_start: view.byte_start,
                byte_len: u32::try_from(input.payload.len())
                    .map_err(|_| invalid("append physical byte length"))?,
                payload_hash: Sha256::digest(view.payload).into(),
            };
            root = Some(index.append(root, certificate).await?);
            base = certificate.end()?;
            certificates.entries.push(certificate);
        }
        let next = NativeHead {
            next_offset,
            index_root: root,
            ..captured.head
        };
        Ok(PreparedAppend {
            original: captured,
            next,
            attempted: false,
            certificates,
            lineage,
        })
    }
    pub async fn refresh(
        &self,
        pages: Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<Self, ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.captured.identity, &pages.binding_identity())
            || self.captured.observed.inner.key != key
        {
            return Err(invalid("writer belongs to another authority binding"));
        }
        let Some(current) = self.lineage.observe(pages, key).await? else {
            return Err(invalid("writer authority regressed during refresh"));
        };
        let state = self.lineage.gate.lock().await;
        if state.highest_epoch > self.lineage.acquired_epoch
            || current.head.epoch != self.lineage.acquired_epoch
            || current.head != state.floor
        {
            return Err(invalid("writer authority changed during refresh"));
        }
        Ok(Self {
            captured: current,
            lineage: self.lineage.clone(),
        })
    }
}
impl CapturedHead {
    pub(super) fn value(&self) -> NativeHead {
        self.head
    }
    pub(super) fn head_bytes(&self) -> [u8; HEAD_BYTES] {
        self.head.encode()
    }

    pub async fn read_record_bounded(
        &self,
        pages: Arc<super::page_store::PageStore>,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<Option<(u64, u32, OwnedPayload)>, ObjectLogError> {
        let Some(owned) = self.lookup(pages.clone(), offset).await? else {
            return Ok(None);
        };
        let entry = owned.entry();
        if entry.byte_len as usize > max_payload_bytes {
            return Err(ObjectLogError::BudgetExceeded(
                "native payload exceeds caller read limit".into(),
            ));
        }
        let index = super::index::Index::new(pages.clone());
        let logical = index.literal_id(entry.logical_id).await?;
        let physical = index.object_id(entry.object_id).await?;
        let expected = pages.data_key(logical.as_str())?;
        if expected.as_str() != physical.as_str() {
            return Err(invalid(
                "indexed physical key differs from full logical identity mapping",
            ));
        }
        let length = u64::from(entry.byte_start)
            .checked_add(u64::from(entry.byte_len))
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| invalid("indexed data length"))?;
        let observed = pages.read_data(physical.as_str(), length).await?;
        let raw = observed
            .bytes()
            .ok_or_else(|| invalid("indexed immutable data is missing"))?;
        let frame = super::data::DataFrame::decode(raw)?;
        if frame.identity != logical.as_str()
            || frame.byte_start != entry.byte_start
            || frame.payload.len() != entry.byte_len as usize
            || <[u8; 32]>::from(Sha256::digest(frame.payload)) != entry.payload_hash
        {
            return Err(invalid(
                "indexed data identity, range or payload digest differs",
            ));
        }
        Ok(Some((
            entry.base,
            entry.record_count,
            OwnedPayload {
                observed,
                start: entry.byte_start as usize,
                len: entry.byte_len as usize,
            },
        )))
    }
    #[cfg(test)]
    pub async fn read_payload(
        &self,
        pages: Arc<super::page_store::PageStore>,
        offset: u64,
    ) -> Result<Option<OwnedPayload>, ObjectLogError> {
        Ok(self
            .read_record_bounded(pages, offset, usize::MAX)
            .await?
            .map(|(_, _, payload)| payload))
    }
    pub async fn capture(
        pages: &super::page_store::PageStore,
        key: &str,
    ) -> Result<Self, ObjectLogError> {
        let observed = pages.read_head(key, HEAD_BYTES).await?;
        let scope = pages.scope();
        let head = NativeHead::decode(
            observed
                .bytes()
                .ok_or_else(|| invalid("known native head is missing"))?,
            &scope.namespace,
            &scope.incarnation,
        )?;
        if let Some(root) = head.index_root {
            // Envelope decoding alone does not authorize an index root.
            drop(pages.read(root).await?);
        }
        Ok(Self {
            head,
            observed,
            identity: pages.binding_identity(),
        })
    }
    /// Fence exactly the captured observation. A conflict does not substitute
    /// a fresh root or silently retry this original transition. The returned
    /// head is a fence result, not a publication token: this cut still owns
    /// the pre-acquisition observation. Publication must recapture authority
    /// and validate its epoch/root before constructing a transition.
    fn fence_transition(
        &self,
        pages: &super::page_store::PageStore,
        key: &str,
    ) -> Result<NativeHead, ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.identity, &pages.binding_identity()) {
            return Err(invalid("captured head belongs to another store binding"));
        }
        let fenced = NativeHead {
            epoch: self
                .head
                .epoch
                .checked_add(1)
                .ok_or_else(|| invalid("native epoch exhausted"))?,
            ..self.head
        };
        if self.observed.inner.key != key {
            return Err(invalid("fence literal head key mismatch"));
        }
        Ok(fenced)
    }
    #[cfg(test)]
    pub async fn acquire(
        &self,
        pages: &super::page_store::PageStore,
        key: &str,
    ) -> Result<NativeHead, ObjectLogError> {
        let fenced = self.fence_transition(pages, key)?;
        match pages
            .swap_head(key, self.observed.clone(), &fenced.encode())
            .await?
        {
            MetadataSwap::Stored => Ok(fenced),
            MetadataSwap::Conflict(_) => Err(invalid("captured head changed before acquisition")),
        }
    }
    /// Obtain the post-fence token without relabeling the old observation.
    /// Any intervening change invalidates this writer attempt.
    #[cfg(test)]
    pub async fn acquire_and_recapture(
        &self,
        pages: &Arc<super::page_store::PageStore>,
        key: &str,
    ) -> Result<FencedHead, ObjectLogError> {
        let knowledge = AuthorityKnowledge::new(self, pages)?;
        self.acquire_with_knowledge(pages, key, knowledge).await
    }
    pub(super) async fn acquire_with_knowledge(
        &self,
        pages: &Arc<super::page_store::PageStore>,
        key: &str,
        knowledge: Arc<AuthorityKnowledge>,
    ) -> Result<FencedHead, ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&knowledge.identity, &self.identity)
            || knowledge.key_owner.inner.key != key
            || self.observed.inner.key != key
        {
            return Err(invalid("fence belongs to another authority family"));
        }
        let charge = pages.charge_output(
            std::mem::size_of::<WriterLineage>() + 2 * std::mem::size_of::<usize>(),
        )?;
        let mut state = knowledge.gate.lock().await;
        if state.floor != self.head || state.highest_epoch > self.head.epoch {
            return Err(invalid(
                "fence original authority is below shared knowledge",
            ));
        }
        let fenced = self.fence_transition(pages, key)?;
        match pages
            .swap_head(key, self.observed.clone(), &fenced.encode())
            .await?
        {
            MetadataSwap::Stored => {}
            MetadataSwap::Conflict(observed) => {
                WriterLineage::validate(&mut state, pages.clone(), observed).await?;
                return Err(invalid("captured head changed before acquisition"));
            }
        }
        // Stored establishes knowledge, but does not relabel the original token.
        state.floor = fenced;
        state.highest_epoch = state.highest_epoch.max(fenced.epoch);
        let observed = pages.read_head(key, HEAD_BYTES).await?;
        let current = WriterLineage::validate(&mut state, pages.clone(), observed)
            .await?
            .ok_or_else(|| invalid("native authority regressed after acquisition"))?;
        if current.head != fenced {
            return Err(invalid("native authority changed after acquisition"));
        }
        drop(state);
        let lineage = Arc::new(WriterLineage {
            gate: knowledge.gate.clone(),
            acquired_epoch: current.head.epoch,
            _knowledge: knowledge,
            _charge: charge,
        });
        Ok(FencedHead {
            captured: current,
            lineage,
        })
    }
    pub async fn lookup(
        &self,
        pages: Arc<super::page_store::PageStore>,
        offset: u64,
    ) -> Result<Option<super::index::OwnedEntry>, ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.identity, &pages.binding_identity()) {
            return Err(invalid("captured head belongs to another store binding"));
        }
        // Retain the original exact metadata token for later conditional
        // publication; no fresh head read can silently replace this cut.
        debug_assert!(self.observed.bytes().is_some());
        super::index::Index::new(pages)
            .lookup(self.head.index_root, offset)
            .await
    }
}

fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::Sequencer(message.into())
}
#[cfg(test)]
struct Lifecycle {
    closed: bool,
    owned: usize,
}
#[cfg(test)]
struct Owner {
    store: Arc<dyn BoundedMetadataStore>,
    key: String,
    namespace: [u8; 32],
    incarnation: [u8; 32],
    gate: Arc<Semaphore>,
    lifecycle: Mutex<Lifecycle>,
    drained: Notify,
}
#[cfg(test)]
struct PhysicalOwner {
    owner: Arc<Owner>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
#[cfg(test)]
impl Drop for PhysicalOwner {
    fn drop(&mut self) {
        drop(self.permit.take());
        let mut state = self
            .owner
            .lifecycle
            .lock()
            .expect("head lifecycle poisoned");
        state.owned -= 1;
        if state.owned == 0 {
            self.owner.drained.notify_waiters();
        }
    }
}
/// Storage-internal reader/fencer for an already provisioned native head.
/// It cannot create a missing head, migrate legacy manifests, publish roots or
/// authorize Fireweed to bypass cold recovery. Operations are physically owned
/// after dispatch, serialized, and drained even when the caller drops its reply.
#[derive(Clone)]
#[cfg(test)]
pub struct NativeHeadReader {
    owner: Arc<Owner>,
}
#[cfg(test)]
impl NativeHeadReader {
    /// Bind an existing head's literal storage key and immutable scope. This
    /// performs no I/O. Missing authority always fails closed, including known
    /// catalog membership; provisioning is a separate future publication path.
    pub fn new_known(
        store: Arc<dyn BoundedMetadataStore>,
        key: String,
        namespace: [u8; 32],
        incarnation: [u8; 32],
    ) -> Result<Self, ObjectLogError> {
        if key.is_empty()
            || key.contains('\0')
            || key.split('/').any(|part| part == "..")
            || namespace == [0; 32]
            || incarnation == [0; 32]
        {
            return Err(ObjectLogError::InvalidConfig(
                "invalid native head binding".into(),
            ));
        }
        Ok(Self {
            owner: Arc::new(Owner {
                store,
                key,
                namespace,
                incarnation,
                gate: Arc::new(Semaphore::new(1)),
                lifecycle: Mutex::new(Lifecycle {
                    closed: false,
                    owned: 0,
                }),
                drained: Notify::new(),
            }),
        })
    }
    /// Read the exact durable head. Fresh missing/corrupt authority is unknown.
    pub async fn read(&self) -> Result<NativeHead, ObjectLogError> {
        self.dispatch(false).await
    }
    /// Increment only the durable epoch, preserving bounds and both roots.
    /// Competing CAS retries reread and validate the original namespace and
    /// incarnation. An ambiguous write is returned as an error without retry.
    pub async fn acquire(&self) -> Result<NativeHead, ObjectLogError> {
        self.dispatch(true).await
    }
    async fn dispatch(&self, acquire: bool) -> Result<NativeHead, ObjectLogError> {
        let permit = Arc::clone(&self.owner.gate)
            .acquire_owned()
            .await
            .map_err(|_| invalid("native head is closed"))?;
        {
            let mut state = self
                .owner
                .lifecycle
                .lock()
                .expect("head lifecycle poisoned");
            if state.closed {
                return Err(invalid("native head is closed"));
            }
            state.owned += 1;
        }
        let physical = Arc::new(PhysicalOwner {
            owner: Arc::clone(&self.owner),
            permit: Some(permit),
        });
        let (send, receive) = oneshot::channel();
        tokio::spawn(async move {
            let result = physical
                .owner
                .execute(acquire, Arc::clone(&physical) as AppendOwner)
                .await;
            let _ = send.send(result);
            drop(physical);
        });
        receive
            .await
            .map_err(|_| invalid("native head physical operation panicked"))?
    }
    /// Close admission and await adapter-owned operations and source buffers.
    /// SDK transport internals and ambiguous remote effects are not certified.
    /// Already
    /// queued pre-dispatch callers fail without I/O; all clones share closure.
    pub async fn close_and_drain(&self) {
        self.owner
            .lifecycle
            .lock()
            .expect("head lifecycle poisoned")
            .closed = true;
        self.owner.gate.close();
        loop {
            let notified = self.owner.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .owner
                .lifecycle
                .lock()
                .expect("head lifecycle poisoned")
                .owned
                == 0
            {
                return;
            }
            notified.await;
        }
    }
}
#[cfg(test)]
impl Owner {
    fn decode(&self, observed: &MetadataSnapshot) -> Result<NativeHead, ObjectLogError> {
        let bytes = observed
            .bytes()
            .ok_or_else(|| invalid("known native head is missing"))?;
        NativeHead::decode(bytes, &self.namespace, &self.incarnation)
    }
    async fn execute(
        &self,
        acquire: bool,
        physical: AppendOwner,
    ) -> Result<NativeHead, ObjectLogError> {
        let mut previous: Option<NativeHead> = None;
        let mut observed = self
            .store
            .read_metadata(
                &self.key,
                metadata_limits(HEAD_BYTES),
                metadata_owners(Arc::clone(&physical)),
            )
            .await?;
        for _ in 0..8 {
            let mut head = self.decode(&observed)?;
            if previous.is_some_and(|old| {
                head.epoch < old.epoch
                    || head.log_start < old.log_start
                    || head.next_offset < old.next_offset
                    || (head.bounds() == old.bounds()
                        && (head.index_root != old.index_root
                            || head.semantic_anchor != old.semantic_anchor))
            }) {
                return Err(invalid(
                    "native head conflict authority regressed or changed roots",
                ));
            }
            previous = Some(head);
            if !acquire {
                return Ok(head);
            }
            head.epoch = head
                .epoch
                .checked_add(1)
                .ok_or_else(|| invalid("native epoch exhausted"))?;
            match self
                .store
                .swap_metadata(
                    &self.key,
                    observed,
                    Bytes::copy_from_slice(&head.encode()),
                    metadata_limits(HEAD_BYTES),
                    metadata_owners(Arc::clone(&physical)),
                )
                .await?
            {
                MetadataSwap::Stored => return Ok(head),
                MetadataSwap::Conflict(current) => observed = current,
            }
        }
        Err(invalid("native head conditional acquisition exhausted"))
    }
}

#[cfg(test)]
mod tests;
