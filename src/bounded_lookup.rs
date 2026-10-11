//! Incremental lookup bounds metadata copies, not full resident index storage.
use crate::{IndexEntry, ObjectLogError, PartitionKey, Sequencer};

/// Compile-time native page port; legacy custom sequencers need not implement it.
pub trait BoundedSequencer: Sequencer {
    /// Caller reserves `max_metadata_bytes` before calling. Includes vector entry
    /// headers and cloned full object identifiers. Check retained log_start under
    /// the same lock as page copying. No unbounded lookup fallback.
    fn lookup_page(
        &self,
        partition: &PartitionKey,
        offset: i64,
        max_entries: usize,
        max_metadata_bytes: usize,
    ) -> Result<Vec<IndexEntry>, ObjectLogError>;
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::Sequencer(message.into())
}
pub(crate) fn entry_end(entry: &IndexEntry) -> Result<i64, ObjectLogError> {
    if entry.base_offset < 0 || entry.record_count <= 0 {
        return Err(invalid("invalid index offset/count"));
    }
    entry
        .base_offset
        .checked_add(i64::from(entry.record_count))
        .ok_or_else(|| invalid("index offset overflow"))
}
pub(crate) fn validate_index(
    entries: &[IndexEntry],
    log_start: i64,
    next: i64,
) -> Result<(), ObjectLogError> {
    if log_start < 0 || next < log_start {
        return Err(invalid("invalid retained index bounds"));
    }
    let mut previous = None;
    for entry in entries {
        let end = entry_end(entry)?;
        if entry.location.object_id.is_empty() {
            return Err(invalid("index has empty object identifier"));
        }
        if let Some(previous) = previous {
            if entry.base_offset != previous {
                return Err(invalid("index ranges are not ordered and contiguous"));
            }
        } else if entry.base_offset > log_start || end <= log_start {
            return Err(invalid("index does not cover retained boundary"));
        }
        previous = Some(end);
    }
    if previous.unwrap_or(log_start) != next {
        return Err(invalid("index does not cover committed tail"));
    }
    Ok(())
}
/// Installed indexes already validate; do not rescan the full suffix per page.
pub(crate) fn page(
    entries: &[IndexEntry],
    offset: i64,
    max_entries: usize,
    max_bytes: usize,
) -> Result<Vec<IndexEntry>, ObjectLogError> {
    if offset < 0 || max_entries == 0 || max_bytes == 0 {
        return Err(ObjectLogError::InvalidConfig(
            "positive page limits and nonnegative offset required".into(),
        ));
    }
    let start = entries
        .partition_point(|entry| entry.base_offset + i64::from(entry.record_count) <= offset);
    let available = &entries[start..];
    let mut count = 0;
    let mut bytes = 0usize;
    for entry in available.iter().take(max_entries) {
        let cost = std::mem::size_of::<IndexEntry>()
            .checked_add(entry.location.object_id.len())
            .and_then(|n| n.checked_add(64))
            .ok_or_else(|| invalid("index metadata size overflow"))?;
        let Some(next) = bytes.checked_add(cost).filter(|next| *next <= max_bytes) else {
            if count == 0 {
                return Err(ObjectLogError::BudgetExceeded(
                    "first index entry exceeds metadata page budget".into(),
                ));
            }
            break;
        };
        bytes = next;
        count += 1;
    }
    let mut out = Vec::with_capacity(count);
    out.extend(available[..count].iter().cloned());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BatchLocation;
    fn entry(base: i64, count: i32, name: &str) -> IndexEntry {
        IndexEntry {
            base_offset: base,
            record_count: count,
            location: BatchLocation {
                object_id: name.into(),
                byte_start: 0,
                byte_len: 1,
            },
        }
    }
    #[test]
    fn installed_ranges_are_checked_without_sorting_or_overflow() {
        let good = [entry(0, 3, "a"), entry(3, 2, "b")];
        validate_index(&good, 0, 5).unwrap();
        validate_index(&good, 2, 5).unwrap();
        for entries in [
            vec![entry(-1, 1, "a")],
            vec![entry(0, 0, "a")],
            vec![entry(i64::MAX, 1, "a")],
            vec![entry(0, 2, "a"), entry(1, 1, "b")],
            vec![entry(1, 1, "a")],
            vec![entry(0, 1, "")],
        ] {
            assert!(validate_index(&entries, 0, 2).is_err());
        }
        assert!(validate_index(&good, 0, 6).is_err());
        assert!(validate_index(&[], 0, 1).is_err());
        validate_index(&[], 5, 5).unwrap();
    }
    #[test]
    fn page_bound_includes_full_object_names_and_mid_batch_offsets() {
        let entries = [
            entry(0, 3, &"a".repeat(5000)),
            entry(3, 2, "b"),
            entry(5, 1, "c"),
        ];
        let cost = std::mem::size_of::<IndexEntry>() + 5000 + 64;
        assert!(matches!(
            page(&entries, 2, 3, cost - 1),
            Err(ObjectLogError::BudgetExceeded(_))
        ));
        let first = page(&entries, 2, 3, cost).unwrap();
        assert_eq!(first, entries[..1]);
        assert_eq!(page(&entries, 3, 1, 4096).unwrap(), entries[1..2]);
        assert!(page(&entries, 6, 1, 4096).unwrap().is_empty());
        assert!(page(&entries, -1, 1, 4096).is_err());
        assert!(page(&entries, 0, 0, 4096).is_err());
    }
}
