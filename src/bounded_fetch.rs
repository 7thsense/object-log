//! Metadata is planned before strict payload I/O. Caller admission remains owned
//! by the embedding operation; soft packing targets are not hard byte limits.
use crate::{
    BlobStore, BoundedSequencer, FetchedBatch, IndexEntry, LogEngine, ObjectLogError, PartitionKey,
    Sequencer,
};
use std::sync::{Arc, Weak};

/// Unforgeable metadata page from one sequencer/blob authority. Reserve the
/// declared metadata budget before constructing, then per-batch bytes before read.
pub struct FetchPlan<S: Sequencer> {
    entries: Vec<IndexEntry>,
    sequencer: Weak<S>,
    blob: Weak<dyn BlobStore>,
}
impl<S: Sequencer> FetchPlan<S> {
    /// Borrow the bounded metadata entries without cloning object identifiers.
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }
    /// Sum of planned payload bytes; reserve this or each batch before I/O.
    pub fn payload_bytes(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| u64::from(entry.location.byte_len))
            .sum()
    }
}
impl<S: BoundedSequencer + 'static> LogEngine<S> {
    /// Plan a count/metadata-bounded page without reading payloads. The soft
    /// target includes at least one batch; use a separate hard admitted bound.
    pub fn plan_fetch(
        &self,
        partition: &PartitionKey,
        offset: i64,
        max_entries: usize,
        max_metadata_bytes: usize,
        target_payload_bytes: u64,
    ) -> Result<FetchPlan<S>, ObjectLogError> {
        if offset < 0 || target_payload_bytes == 0 {
            return Err(ObjectLogError::InvalidConfig(
                "invalid bounded fetch offset/target".into(),
            ));
        }
        if offset < self.sequencer().log_start_offset(partition)? {
            return Err(ObjectLogError::RangeOutOfBounds(
                "fetch precedes retained history".into(),
            ));
        }
        let mut entries =
            self.sequencer()
                .lookup_page(partition, offset, max_entries, max_metadata_bytes)?;
        if entries.len() > max_entries {
            return Err(ObjectLogError::Sequencer(
                "bounded sequencer exceeded entry limit".into(),
            ));
        }
        let mut previous = None;
        for entry in &entries {
            let end = crate::bounded_lookup::entry_end(entry)?;
            if let Some(previous) = previous {
                if entry.base_offset != previous {
                    return Err(ObjectLogError::Sequencer(
                        "fetch page ranges are not contiguous".into(),
                    ));
                }
            } else if entry.base_offset > offset || end <= offset {
                return Err(ObjectLogError::Sequencer(
                    "fetch page does not cover requested offset".into(),
                ));
            }
            previous = Some(end);
        }
        let mut bytes = 0u64;
        let mut count = 0;
        for entry in &entries {
            if bytes >= target_payload_bytes && count > 0 {
                break;
            }
            crate::bounded_lookup::entry_end(entry)?;
            bytes = bytes
                .checked_add(u64::from(entry.location.byte_len))
                .ok_or_else(|| {
                    ObjectLogError::RangeOutOfBounds("fetch payload sum overflow".into())
                })?;
            count += 1;
        }
        entries.truncate(count);
        Ok(FetchPlan {
            entries,
            sequencer: Arc::downgrade(self.sequencer()),
            blob: Arc::downgrade(self.blob_store()),
        })
    }
    /// Caller admits `entry.location.byte_len` before this read. `max_bytes` is
    /// a hard batch bound; a soft target may admit an oversized first batch.
    pub async fn read_planned_batch(
        &self,
        plan: &FetchPlan<S>,
        index: usize,
        max_bytes: u64,
    ) -> Result<FetchedBatch, ObjectLogError> {
        let same_seq = plan
            .sequencer
            .upgrade()
            .is_some_and(|seq| Arc::ptr_eq(&seq, self.sequencer()));
        let same_blob = plan
            .blob
            .upgrade()
            .is_some_and(|blob| Arc::ptr_eq(&blob, self.blob_store()));
        if !same_seq || !same_blob {
            return Err(ObjectLogError::InvalidConfig(
                "fetch plan belongs to another authority".into(),
            ));
        }
        let entry = plan.entries.get(index).ok_or_else(|| {
            ObjectLogError::RangeOutOfBounds("fetch plan index outside page".into())
        })?;
        let count = u64::from(entry.location.byte_len);
        if count > max_bytes {
            return Err(ObjectLogError::BudgetExceeded(
                "batch exceeds hard admitted byte limit".into(),
            ));
        }
        let start = u64::from(entry.location.byte_start);
        let end = start
            .checked_add(count)
            .ok_or_else(|| ObjectLogError::RangeOutOfBounds("batch range overflow".into()))?;
        let payload = crate::engine::get_range_with_retries(
            self.blob_store(),
            &entry.location.object_id,
            start..end,
            crate::engine::RangeRead::Exact,
        )
        .await?
        .ok_or_else(|| ObjectLogError::MissingObject(entry.location.object_id.clone()))?;
        if payload.len() as u64 != count {
            return Err(ObjectLogError::RangeOutOfBounds(
                "strict adapter returned wrong batch length".into(),
            ));
        }
        Ok(FetchedBatch {
            base_offset: entry.base_offset,
            record_count: entry.record_count,
            payload,
        })
    }
}
