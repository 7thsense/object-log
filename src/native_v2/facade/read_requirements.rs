//! Bounds derived from the authenticated entry before record hydration.
use crate::{
    ObjectLogError,
    native_v2::codec::{FrameKind, LeafEntry, PAGE_BYTES},
};

/// Upper bounds on SDK object reads for one sealed `NativeReadPlan` hydration.
///
/// These exclude the preceding offset-index lookup (at most 17 page reads),
/// provider retries, HTTP/TLS overhead and allocation/retained-memory admission.
/// `hydration_bytes` sums supplied object-body limits, not observed wire bytes.
/// A failed hydration can consume any prefix of this work. Consumers must debit
/// their shared request budget before consuming the plan and must not refund
/// failed or cancelled attempts. Identity lengths also allow consumers to reject
/// unexpected identities before allocating or reading their overflow chains.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeReadRequirements {
    logical_identity_bytes: u64,
    physical_identity_bytes: u64,
    identity_pages: u64,
    validation_pages: u64,
    frame_bytes: u64,
    hydration_reads: u64,
    hydration_bytes: u64,
}
impl NativeReadRequirements {
    pub(super) fn from_entry(entry: &LeafEntry) -> Result<Self, ObjectLogError> {
        entry.end()?;
        let overflow =
            || ObjectLogError::BudgetExceeded("native read requirements overflow".into());
        // A valid overflow chain may have just ONE byte per page. Its sealed
        // descriptor's entries bounds calls, including malformed-chain failures.
        let identity_pages = entry
            .logical_id
            .entries
            .checked_add(entry.object_id.entries)
            .ok_or_else(overflow)?;
        let validation_pages = u64::from(entry.frame_kind == FrameKind::Coupled);
        let pages = identity_pages
            .checked_add(validation_pages)
            .ok_or_else(overflow)?;
        let frame_bytes = u64::from(entry.byte_start)
            .checked_add(u64::from(entry.byte_len))
            .ok_or_else(overflow)?;
        let hydration_reads = pages.checked_add(1).ok_or_else(overflow)?;
        let hydration_bytes = pages
            .checked_mul(PAGE_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(frame_bytes))
            .ok_or_else(overflow)?;
        Ok(Self {
            logical_identity_bytes: entry.logical_id.end,
            physical_identity_bytes: entry.object_id.end,
            identity_pages,
            validation_pages,
            frame_bytes,
            hydration_reads,
            hydration_bytes,
        })
    }
    /// Complete UTF-8 logical identity length, before its allocation.
    pub fn logical_identity_bytes(&self) -> u64 {
        self.logical_identity_bytes
    }
    /// Complete UTF-8 physical object key length, before its allocation.
    pub fn physical_identity_bytes(&self) -> u64 {
        self.physical_identity_bytes
    }
    /// Upper bound on identity overflow-page read attempts.
    pub fn identity_pages(&self) -> u64 {
        self.identity_pages
    }
    /// Upper bound on coupled-manifest validation-page read attempts (zero or one).
    pub fn validation_pages(&self) -> u64 {
        self.validation_pages
    }
    /// Complete immutable frame size, including identity/header and user payload.
    /// The returned `NativeRecord` retains this whole frame, not just its payload.
    pub fn frame_bytes(&self) -> u64 {
        self.frame_bytes
    }
    /// Identity pages + coupled validation pages + the frame read.
    pub fn hydration_reads(&self) -> u64 {
        self.hydration_reads
    }
    /// Sum of object-body read caps: each metadata page <=4096 bytes + full frame.
    pub fn hydration_bytes(&self) -> u64 {
        self.hydration_bytes
    }
}

#[cfg(test)]
mod tests;
