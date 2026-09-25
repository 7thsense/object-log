//! The sequencing seam: offset assignment, the offset→location index, and
//! retention. The engine owns storage + buffering; a `Sequencer` owns offsets.

use crate::ObjectLogError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// Opaque, engine-visible identifier for an independent offset stream (one dense,
/// monotonic offset space). A Kafka broker maps `(topic, partition)` onto one of
/// these; a WAL maps a shard. object-log treats it as an opaque key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionKey(pub String);

impl PartitionKey {
    /// Borrow the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Where a batch's bytes live inside its object. **Authored by the engine** (it
/// owns object layout); the sequencer stores it in the index and returns it from
/// [`Sequencer::lookup`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchLocation {
    /// Key of the object holding the batch.
    pub object_id: String,
    /// Byte offset of the batch within the object.
    pub byte_start: u32,
    /// Byte length of the batch.
    pub byte_len: u32,
}

/// One batch presented to [`Sequencer::commit`]. The engine fills `partition`,
/// `record_count`, and `location` (all engine-visible); `meta` is forwarded
/// **uninterpreted** — only the sequencer reads it.
pub struct CommitBatch<'a, M> {
    /// The offset stream this batch belongs to.
    pub partition: PartitionKey,
    /// Number of records in the batch (offsets advance by this).
    pub record_count: i32,
    /// Where the batch lives in its (already-durable) object.
    pub location: BatchLocation,
    /// Sequencer-private metadata (e.g. idempotent-producer identity). Opaque to
    /// the engine.
    pub meta: &'a M,
    /// Fence epoch the caller believes owns this partition. [`ManifestSequencer`](crate::ManifestSequencer)
    /// rejects the batch when the stored index epoch differs. Other sequencers
    /// ignore it.
    pub epoch: u64,
}

/// Per-batch result of [`Sequencer::commit`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOutcome {
    /// A fresh, contiguous offset range was assigned starting at `base_offset`.
    Assigned {
        /// First offset assigned to the batch.
        base_offset: i64,
        /// Number of records committed.
        record_count: i32,
    },
    /// A retried batch was recognized as already committed (idempotent no-op);
    /// the original `base_offset` is returned.
    Duplicate {
        /// The originally assigned first offset.
        base_offset: i64,
    },
    /// This batch was not indexed. Other batches in the same call may already
    /// be durable. The caller must rewrite this batch in a new data object
    /// rather than reuse the offsets it expected.
    Rejected {
        /// Why the conditional update did not land.
        reason: String,
    },
}

/// An index entry resolving an offset range to its bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Where the batch's bytes live.
    pub location: BatchLocation,
    /// First offset in the batch.
    pub base_offset: i64,
    /// Number of records in the batch.
    pub record_count: i32,
}

/// The linearization point: assigns offsets to durably-stored batches and owns
/// the offset→location index.
///
/// **Synchronous on purpose** — a lin-point is a critical section, not async I/O.
/// The engine calls it from its flush worker (a dedicated thread / blocking
/// task), so a blocking implementation (a `Mutex`, a SQL transaction) is fine.
///
/// Contract: [`commit`](Sequencer::commit) is **atomic across the whole slice**
/// (all supplied batches commit together or not at all), and the engine
/// presents batches for any single [`PartitionKey`] in arrival order and never
/// splits one partition across concurrent `commit` calls.
pub trait Sequencer: Send + Sync {
    /// Sequencer-private per-batch metadata, forwarded uninterpreted by the
    /// engine. object-log's default sequencers use `()`; a Kafka coordinator uses
    /// its producer-identity fields.
    type Meta: Send + Sync;

    /// Opt in to atomic commits containing already-durable batches from several
    /// data objects, in creation order. Default preserves one-object calls for
    /// custom sequencers that rely on that boundary.
    fn supports_multi_object_commit(&self) -> bool {
        false
    }

    /// Assign offsets to durably uploaded batches and persist the index.
    /// By default these belong to one object; an opted-in implementation may
    /// receive several objects in one ordered call.
    /// Returns one [`CommitOutcome`] per input batch, in order.
    ///
    /// On `Err`, nothing from this call was indexed. A [`CommitOutcome::Rejected`]
    /// batch was not indexed either; earlier batches in the same call may already
    /// be durable when the sequencer commits one partition at a time.
    fn commit(
        &self,
        batches: &[CommitBatch<'_, Self::Meta>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError>;

    /// Resolve `fetch_offset` to the ordered index entries covering it onward.
    fn lookup(
        &self,
        partition: &PartitionKey,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>, ObjectLogError>;

    /// The next offset to be assigned (index-only; no object reads).
    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError>;

    /// The first readable offset (advances on [`truncate_before`](Sequencer::truncate_before)).
    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError>;

    /// Retention MECHANISM (not policy): drop index entries below `offset` and
    /// return object ids that now have **no** live references from **any**
    /// partition (objects are multiplexed and shared), for the engine to delete.
    fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError>;

    /// Move the stored fence epoch for `partition` from `expected` to `new_epoch`.
    ///
    /// The default succeeds without storing anything. [`ManifestSequencer`](crate::ManifestSequencer)
    /// reads that partition's durable index, fails when its epoch is neither
    /// `expected` nor `new_epoch`, and otherwise stores `new_epoch`, creating the
    /// index when it is missing. Once this returns, a writer that has not seen
    /// the new epoch cannot commit to the partition.
    fn fence_epoch(
        &self,
        partition: &PartitionKey,
        expected: u64,
        new_epoch: u64,
    ) -> Result<(), ObjectLogError> {
        let _ = (partition, expected, new_epoch);
        Ok(())
    }

    /// The fence epoch this handle last read for `partition`, if it tracks one.
    ///
    /// After a [`CommitOutcome::Rejected`] batch the engine reports
    /// [`ObjectLogError::Fenced`] when this epoch is past the batch's epoch. The
    /// default returns `None`, which leaves rejections as sequencer errors.
    fn partition_epoch(&self, partition: &PartitionKey) -> Option<u64> {
        let _ = partition;
        None
    }

    /// Reload `partition`'s index from durable storage, so this handle sees
    /// commits made by other writers since it opened.
    ///
    /// The default does nothing: a sequencer with a single writer is already
    /// current.
    fn refresh_partition(&self, partition: &PartitionKey) -> Result<(), ObjectLogError> {
        let _ = partition;
        Ok(())
    }
}

/// Per-partition in-memory index state.
#[derive(Default)]
struct PartState {
    next_offset: i64,
    log_start: i64,
    entries: Vec<IndexEntry>,
}

/// A single-node, in-memory [`Sequencer`] (`Meta = ()`): assigns dense offsets and
/// keeps the offset→location index in memory.
///
/// The log *bytes* live durably in the `BlobStore`, but this index does not
/// survive a restart — use it for tests and single-process work, or persist the
/// index yourself for a crash-durable standalone log.
#[derive(Default)]
pub struct InMemorySequencer {
    state: Mutex<HashMap<PartitionKey, PartState>>,
}

impl InMemorySequencer {
    /// Create an empty sequencer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Object ids referenced by any live index entry (for orphan reaping).
    pub fn live_object_ids(&self) -> HashSet<String> {
        let state = self.state.lock().expect("poisoned");
        let mut live = HashSet::new();
        for ps in state.values() {
            for e in &ps.entries {
                live.insert(e.location.object_id.clone());
            }
        }
        live
    }
}

impl Sequencer for InMemorySequencer {
    type Meta = ();

    fn supports_multi_object_commit(&self) -> bool {
        true
    }

    fn commit(
        &self,
        batches: &[CommitBatch<'_, Self::Meta>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let mut state = self.state.lock().expect("poisoned");
        let mut outcomes = Vec::with_capacity(batches.len());
        for cb in batches {
            let ps = state.entry(cb.partition.clone()).or_default();
            let base = ps.next_offset;
            ps.entries.push(IndexEntry {
                location: cb.location.clone(),
                base_offset: base,
                record_count: cb.record_count,
            });
            ps.next_offset = base + cb.record_count as i64;
            outcomes.push(CommitOutcome::Assigned {
                base_offset: base,
                record_count: cb.record_count,
            });
        }
        Ok(outcomes)
    }

    fn lookup(
        &self,
        partition: &PartitionKey,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>, ObjectLogError> {
        let state = self.state.lock().expect("poisoned");
        let Some(ps) = state.get(partition) else {
            return Ok(Vec::new());
        };
        Ok(ps
            .entries
            .iter()
            .filter(|e| e.base_offset + e.record_count as i64 > fetch_offset)
            .cloned()
            .collect())
    }

    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .state
            .lock()
            .expect("poisoned")
            .get(partition)
            .map_or(0, |ps| ps.next_offset))
    }

    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .state
            .lock()
            .expect("poisoned")
            .get(partition)
            .map_or(0, |ps| ps.log_start))
    }

    fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError> {
        let mut state = self.state.lock().expect("poisoned");
        let mut dropped: Vec<String> = Vec::new();
        match state.get_mut(partition) {
            Some(ps) => {
                for e in ps.entries.iter() {
                    if e.base_offset + e.record_count as i64 <= offset {
                        dropped.push(e.location.object_id.clone());
                    }
                }
                ps.entries
                    .retain(|e| e.base_offset + e.record_count as i64 > offset);
                if offset > ps.log_start {
                    ps.log_start = offset.min(ps.next_offset);
                }
            }
            None => return Ok(Vec::new()),
        }
        // An object is reclaimable only when no entry in ANY partition references it.
        let mut live: HashSet<String> = HashSet::new();
        for ps in state.values() {
            for e in &ps.entries {
                live.insert(e.location.object_id.clone());
            }
        }
        let mut dead: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for oid in dropped {
            if !live.contains(&oid) && seen.insert(oid.clone()) {
                dead.push(oid);
            }
        }
        Ok(dead)
    }
}
