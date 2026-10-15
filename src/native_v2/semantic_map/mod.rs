//! Authenticated byte-ordered map, independent of queue planning policy.
mod bytes;
pub(super) use bytes::OwnedBytes;
mod codec;
mod edit;
mod manifest;
mod publication;
mod seal;
pub(super) use edit::Edit;
pub(super) use publication::{CapturedMap, proves_head_extension};
pub(super) use seal::read as read_seal;
mod range;
pub(super) use range::Step as ScanStep;
mod read;

use crate::ObjectLogError;
pub(super) struct EditBounds {
    pub edits: u64,
    pub input_bytes: u64,
}

/// Work limits are operational; exhaustion never establishes absence or EOF.
pub(super) struct WorkBudget {
    pages: u64,
    hydrated_bytes: u64,
}
impl WorkBudget {
    pub fn new(pages: u64, hydrated_bytes: u64) -> Self {
        Self {
            pages,
            hydrated_bytes,
        }
    }
    pub fn page(&mut self) -> Result<(), ObjectLogError> {
        self.pages = self
            .pages
            .checked_sub(1)
            .ok_or_else(|| ObjectLogError::BudgetExceeded("semantic map page work".into()))?;
        Ok(())
    }
    pub fn hydrate(&mut self, bytes: u64) -> Result<(), ObjectLogError> {
        self.hydrated_bytes = self.hydrated_bytes.checked_sub(bytes).ok_or_else(|| {
            ObjectLogError::BudgetExceeded("semantic map hydrated byte work".into())
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(super) use test_support::GatedStore as TestGatedStore;
#[cfg(test)]
mod tests;

/// Validate the transition identified by an authenticated indexed frame.
/// The caller must also bind this frame to its captured append-index cut.
pub(super) async fn validate_transition(
    pages: &super::page_store::PageStore,
    transition: super::data::Transition,
    work: &mut WorkBudget,
) -> Result<(), ObjectLogError> {
    if transition.scope != pages.scope() {
        return Err(ObjectLogError::InvalidBatch(
            "semantic transition scope".into(),
        ));
    }
    manifest::Manifest::validate_transition(pages, transition, work).await
}
