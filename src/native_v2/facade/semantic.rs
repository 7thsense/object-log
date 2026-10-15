//! Semantic inputs share the native facade's admission and publication lifetime.
use super::*;
use crate::native_v2::semantic_map::{Edit, WorkBudget};

/// Operational work admission. Exhaustion is an error, never absence or EOF.
#[derive(Clone, Copy, Debug)]
pub struct NativeSemanticWork {
    /// Cumulative semantic-map, manifest and seal page reads/writes.
    /// Append-index work is bounded separately by native group limits.
    pub pages: u64,
    /// Maximum cumulative map-key/value hydration and read-bound copies.
    /// Edit-input copies use input_bytes and shared byte credit separately.
    pub hydrated_bytes: u64,
}
impl NativeSemanticWork {
    pub(super) fn budget(self) -> WorkBudget {
        WorkBudget::new(self.pages, self.hydrated_bytes)
    }
}
/// Explicit bounds for one atomic semantic edit group.
#[derive(Clone, Copy, Debug)]
pub struct NativeEditLimits {
    /// Maximum number of distinct, strictly byte-ordered edits.
    pub edits: u64,
    /// Maximum aggregate borrowed key and replacement-value bytes.
    pub input_bytes: u64,
    /// Cumulative authenticated map and publication work.
    pub work: NativeSemanticWork,
}
/// Full byte keys; None deletes the key, Some(empty) stores an empty value.
pub struct NativeSemanticEdit<'a> {
    /// Complete semantic key, compared as unsigned bytes.
    pub key: &'a [u8],
    /// Replacement value or deletion.
    pub value: Option<&'a [u8]>,
}
struct OwnedEdit {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}
pub(super) struct SemanticInputs {
    entries: Vec<OwnedEdit>,
    pub(super) limits: NativeEditLimits,
    _charge: Arc<Charge>,
}
impl Drop for SemanticInputs {
    fn drop(&mut self) {}
}
impl SemanticInputs {
    pub(super) fn copy(
        pages: &PageStore,
        edits: &[NativeSemanticEdit<'_>],
        limits: NativeEditLimits,
    ) -> Result<Self, ObjectLogError> {
        if u64::try_from(edits.len()).map_err(|_| invalid("semantic edit count"))? > limits.edits {
            return Err(invalid("semantic edit count limit"));
        }
        let mut bytes = 0usize;
        for (ordinal, edit) in edits.iter().enumerate() {
            if ordinal > 0 && edits[ordinal - 1].key >= edit.key {
                return Err(invalid(
                    "semantic edits must have strictly increasing full keys",
                ));
            }
            bytes = bytes
                .checked_add(edit.key.len())
                .and_then(|n| n.checked_add(edit.value.map_or(0, <[u8]>::len)))
                .ok_or_else(|| invalid("semantic input length overflow"))?;
        }
        if u64::try_from(bytes).map_err(|_| invalid("semantic input length"))? > limits.input_bytes
        {
            return Err(invalid("semantic input byte limit"));
        }
        let allocation = edits
            .len()
            .checked_mul(std::mem::size_of::<OwnedEdit>() + std::mem::size_of::<Edit<'_>>())
            .and_then(|n| n.checked_add(bytes))
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<Self>() + std::mem::size_of::<Vec<Edit<'_>>>())
            })
            .ok_or_else(|| invalid("semantic input allocation"))?;
        let charge = pages.charge_output(allocation)?;
        let entries = edits
            .iter()
            .map(|edit| OwnedEdit {
                key: edit.key.to_vec(),
                value: edit.value.map(<[u8]>::to_vec),
            })
            .collect();
        Ok(Self {
            entries,
            limits,
            _charge: charge,
        })
    }
    pub(super) fn references(&self) -> Vec<Edit<'_>> {
        self.entries
            .iter()
            .map(|edit| Edit {
                key: &edit.key,
                value: edit.value.as_deref(),
            })
            .collect()
    }
}

impl NativeLog {
    /// Prepare supplied log records and semantic edits for one original-token CAS.
    /// Both are published together by submit; consumer planners own their meaning.
    /// expected_cut must match the writer after fencing/refresh, including its
    /// storage version; that check runs while the shared writer is locked.
    pub async fn prepare_with_edits(
        &self,
        writer: &NativeWriter,
        expected_cut: &NativeReadCut,
        inputs: &[NativeAppend<'_>],
        edits: &[NativeSemanticEdit<'_>],
        limits: NativeEditLimits,
    ) -> Result<NativePublication, ObjectLogError> {
        self.prepare_inner(writer, inputs, Some((expected_cut, edits, limits)))
            .await
    }
}

/// Independently admitted semantic value. It retains bytes, never execution.
pub struct NativeSemanticValue {
    value: crate::native_v2::semantic_map::OwnedBytes,
}
impl NativeSemanticValue {
    /// Exact complete value; an empty slice is distinct from an absent key.
    pub fn bytes(&self) -> &[u8] {
        self.value.as_slice()
    }
}
impl NativeLog {
    /// Read against the exact supplied Covered cut without refreshing authority.
    /// Missing/corrupt source or exhausted work is an error, never key absence.
    pub async fn semantic_lookup(
        &self,
        cut: &NativeReadCut,
        key: &[u8],
        limits: NativeSemanticWork,
    ) -> Result<Option<NativeSemanticValue>, ObjectLogError> {
        self.check(&cut.origin)?;
        let mut work = limits.budget();
        work.hydrate(u64::try_from(key.len()).map_err(|_| invalid("semantic lookup key length"))?)?;
        let allocation = key
            .len()
            .checked_add(std::mem::size_of::<LiteralInput>())
            .ok_or_else(|| invalid("semantic lookup key allocation"))?;
        let charge = self.owner.pages.charge_output(allocation)?;
        let key = LiteralInput {
            bytes: key.to_vec(),
            _charge: charge,
        };
        let read = RetainedRead {
            captured: cut.read.captured.clone(),
            binding: cut.read.binding.clone(),
            knowledge: cut.read.knowledge.clone(),
            charge: cut.read.charge.clone(),
        };
        let owner = self.owner.clone();
        self.run(move |physical| async move {
            let pages = owner
                .pages
                .in_operation(physical)?
                .for_binding(&read.binding)?;
            let map = read.captured.semantic_map(&pages, &mut work).await?;
            map.lookup(&pages, &key.bytes, &mut work)
                .await
                .map(|value| value.map(|value| NativeSemanticValue { value }))
        })
        .await
    }
}

mod scan;
pub use scan::{
    NativeScanLimits, NativeScanStatus, NativeSemanticContinuation, NativeSemanticRange,
    NativeSemanticRow, NativeSemanticScan,
};

#[cfg(test)]
mod tests;
