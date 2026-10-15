//! Immutable scan continuations retain one exact cut and original range.
use super::*;
use crate::native_v2::semantic_map::{OwnedBytes, ScanStep};
use std::ops::Bound;

/// Complete full-byte bounds. Resumption always excludes the last returned key.
#[derive(Clone, Copy, Debug)]
pub struct NativeSemanticRange<'a> {
    /// Inclusive/exclusive lower key, or unbounded.
    pub lower: Bound<&'a [u8]>,
    /// Inclusive/exclusive upper key, or unbounded.
    pub upper: Bound<&'a [u8]>,
}
/// Separate returned-output admission from cumulative traversal work.
#[derive(Clone, Copy, Debug)]
pub struct NativeScanLimits {
    /// Maximum returned rows; must be nonzero.
    pub rows: usize,
    /// Maximum aggregate returned key/value bytes, enforced before value load.
    pub output_bytes: usize,
    /// Cumulative page and hydration work for this page.
    pub work: NativeSemanticWork,
}
/// A limit proves a contiguous returned prefix, without asserting more exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeScanStatus {
    /// The requested interval was authentically exhausted.
    Complete,
    /// A row/output limit stopped traversal; completion remains unproved.
    LimitReached,
}
/// One independently admitted immutable key/value pair.
pub struct NativeSemanticRow {
    key: OwnedBytes,
    value: OwnedBytes,
}
impl NativeSemanticRow {
    /// Complete unsigned-byte key.
    pub fn key(&self) -> &[u8] {
        self.key.as_slice()
    }
    /// Complete value, including an explicitly stored empty value.
    pub fn value(&self) -> &[u8] {
        self.value.as_slice()
    }
}
struct OwnedRange {
    lower: Option<(Vec<u8>, bool)>,
    upper: Option<(Vec<u8>, bool)>,
    _charge: Arc<Charge>,
}
struct ScanState {
    read: RetainedRead,
    range: Arc<OwnedRange>,
    after: Option<OwnedBytes>,
    _charge: Arc<Charge>,
}
/// Sealed immutable cut/range continuation. Cloning or canceling a call cannot
/// advance it; replaying the same continuation repeats the same source interval.
#[derive(Clone)]
pub struct NativeSemanticContinuation {
    state: Arc<ScanState>,
    origin: Arc<()>,
}
/// A complete page or contiguous prefix and its exact immutable continuation.
pub struct NativeSemanticScan {
    rows: Vec<NativeSemanticRow>,
    continuation: Option<NativeSemanticContinuation>,
    status: NativeScanStatus,
    _charge: Arc<Charge>,
}
impl NativeSemanticScan {
    /// Rows in strictly increasing unsigned full-byte order.
    pub fn rows(&self) -> &[NativeSemanticRow] {
        &self.rows
    }
    /// Complete, or explicitly unproved completion after a limit.
    pub fn status(&self) -> NativeScanStatus {
        self.status
    }
    /// Present exactly when status is LimitReached.
    pub fn continuation(&self) -> Option<&NativeSemanticContinuation> {
        self.continuation.as_ref()
    }
}
fn bound(value: Bound<&[u8]>) -> Option<(&[u8], bool)> {
    match value {
        Bound::Unbounded => None,
        Bound::Included(key) => Some((key, true)),
        Bound::Excluded(key) => Some((key, false)),
    }
}
fn retained(read: &RetainedRead) -> RetainedRead {
    RetainedRead {
        captured: read.captured.clone(),
        binding: read.binding.clone(),
        knowledge: read.knowledge.clone(),
        charge: read.charge.clone(),
    }
}
impl NativeLog {
    /// Begin a scan against this exact Covered cut; no head refresh occurs.
    pub async fn semantic_scan(
        &self,
        cut: &NativeReadCut,
        range: NativeSemanticRange<'_>,
        limits: NativeScanLimits,
    ) -> Result<NativeSemanticScan, ObjectLogError> {
        self.check(&cut.origin)?;
        validate_limits(limits)?;
        let lower = bound(range.lower);
        let upper = bound(range.upper);
        if lower.zip(upper).is_some_and(|(lo, hi)| lo.0 > hi.0) {
            return Err(invalid("semantic scan bounds"));
        }
        let bytes = lower
            .map_or(0, |b| b.0.len())
            .checked_add(upper.map_or(0, |b| b.0.len()))
            .ok_or_else(|| invalid("semantic scan bound length"))?;
        let mut work = limits.work.budget();
        work.hydrate(u64::try_from(bytes).map_err(|_| invalid("semantic scan bound length"))?)?;
        let allocation = bytes
            .checked_add(std::mem::size_of::<OwnedRange>() + 2 * std::mem::size_of::<usize>())
            .ok_or_else(|| invalid("semantic scan range allocation"))?;
        let charge = self.owner.pages.charge_output(allocation)?;
        let range = Arc::new(OwnedRange {
            lower: lower.map(|(key, inclusive)| (key.to_vec(), inclusive)),
            upper: upper.map(|(key, inclusive)| (key.to_vec(), inclusive)),
            _charge: charge,
        });
        let charge = self
            .owner
            .pages
            .charge_output(std::mem::size_of::<ScanState>() + 2 * std::mem::size_of::<usize>())?;
        let state = Arc::new(ScanState {
            read: cut.retained(),
            range,
            after: None,
            _charge: charge,
        });
        self.scan_state(state, limits, work).await
    }
    /// Resume without changing the cut/range or mutating the input continuation.
    pub async fn semantic_scan_next(
        &self,
        continuation: &NativeSemanticContinuation,
        limits: NativeScanLimits,
    ) -> Result<NativeSemanticScan, ObjectLogError> {
        self.check(&continuation.origin)?;
        validate_limits(limits)?;
        self.scan_state(continuation.state.clone(), limits, limits.work.budget())
            .await
    }
    async fn scan_state(
        &self,
        state: Arc<ScanState>,
        limits: NativeScanLimits,
        mut work: WorkBudget,
    ) -> Result<NativeSemanticScan, ObjectLogError> {
        let owner = self.owner.clone();
        self.owner
            .resources
            .run(move |physical| async move {
                let pages = owner
                    .pages
                    .in_operation(physical)?
                    .for_binding(&state.read.binding)?;
                let charge = pages.charge_output(scan_allocation(limits.rows)?)?;
                let mut rows = Vec::with_capacity(limits.rows);
                let map = state.read.captured.semantic_map(&pages, &mut work).await?;
                let lower = state
                    .after
                    .as_ref()
                    .map(|key| (key.as_slice(), false))
                    .or_else(|| {
                        state
                            .range
                            .lower
                            .as_ref()
                            .map(|(key, inclusive)| (key.as_slice(), *inclusive))
                    });
                let upper = state
                    .range
                    .upper
                    .as_ref()
                    .map(|(key, inclusive)| (key.as_slice(), *inclusive));
                let mut cursor = map.range(&pages, lower, upper, &mut work).await?;
                let mut output = 0usize;
                let mut status = NativeScanStatus::LimitReached;
                for _ in 0..limits.rows {
                    match cursor
                        .next_bounded(&pages, &mut work, limits.output_bytes - output)
                        .await
                    {
                        Ok(ScanStep::Row(item)) => {
                            output = output
                                .checked_add(item.key.as_slice().len())
                                .and_then(|n| n.checked_add(item.value.as_slice().len()))
                                .ok_or_else(|| invalid("semantic scan output overflow"))?;
                            rows.push(NativeSemanticRow {
                                key: item.key,
                                value: item.value,
                            });
                        }
                        Ok(ScanStep::End) => {
                            status = NativeScanStatus::Complete;
                            break;
                        }
                        Ok(ScanStep::OutputLimit { .. }) if !rows.is_empty() => break,
                        Ok(ScanStep::OutputLimit { actual, limit }) => {
                            return Err(ObjectLogError::ReadLimitExceeded { actual, limit });
                        }
                        Err(error) => return Err(error),
                    }
                }
                let continuation = if status == NativeScanStatus::LimitReached {
                    let last = rows
                        .last()
                        .ok_or_else(|| invalid("semantic scan cannot make progress"))?;
                    let after = OwnedBytes::copy(&pages, last.key(), &mut work)?;
                    let charge = pages.charge_output(
                        std::mem::size_of::<ScanState>() + 2 * std::mem::size_of::<usize>(),
                    )?;
                    Some(NativeSemanticContinuation {
                        state: Arc::new(ScanState {
                            read: retained(&state.read),
                            range: state.range.clone(),
                            after: Some(after),
                            _charge: charge,
                        }),
                        origin: owner.identity.clone(),
                    })
                } else {
                    None
                };
                Ok(NativeSemanticScan {
                    rows,
                    continuation,
                    status,
                    _charge: charge,
                })
            })
            .await
    }
}
fn scan_allocation(rows: usize) -> Result<usize, ObjectLogError> {
    rows.checked_mul(std::mem::size_of::<NativeSemanticRow>())
        .and_then(|n| n.checked_add(std::mem::size_of::<NativeSemanticScan>()))
        .ok_or_else(|| invalid("semantic scan row allocation"))
}
fn validate_limits(limits: NativeScanLimits) -> Result<(), ObjectLogError> {
    if limits.rows == 0 {
        return Err(invalid("semantic scan row limit must be nonzero"));
    }
    scan_allocation(limits.rows)?;
    Ok(())
}
