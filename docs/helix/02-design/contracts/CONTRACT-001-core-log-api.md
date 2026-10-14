---
ddx:
  id: contract-core-log-api
  depends_on:
    - adr-object-storage-log-engine-and-sequencer-seam
    - prd
---

# Contract

**Contract ID**: CONTRACT-001  
**Type**: library  
**Version**: v3
**Status**: accepted  
**Related**: ADR-002, PRD FR-7..FR-30, TD-004  
**Supersedes**: CONTRACT-001 v1 (Kafka-shaped `LogBackend` / `AckMode` / `EpochGuard` / record model)

## Purpose

This contract defines object-log’s normative **engine and sequencer** surface. Implementations of storage and sequencing may vary, but callers of `LogEngine` and implementors of `Sequencer` MUST preserve these semantics.

## Scope and Boundaries

- **In scope**: partition keys, opaque batch payloads, durability levels, group-commit produce, fetch by offset, flush barriers, truncate_before, Sequencer commit/lookup/bounds, CommitOutcome, engine presentation invariants.
- **Out of scope**: Kafka wire protocol, record batch codecs, producer identity fields as engine types, epoch fencing APIs, consumer groups, transactions, authorization, product payload schemas.
- **Owning system**: object-log core library.

## Normative Surface

| Element | Type / Shape | Required | Rules | Notes |
|---------|---------------|----------|-------|-------|
| `PartitionKey` | non-empty string newtype | yes | Opaque ordered log identity; engine does not parse structure | Consumers may encode topic-partition or shard ids |
| `payload` | `Bytes` | yes | Opaque; engine MUST NOT inspect schema | Framing/CRC owned by consumer |
| `record_count` | `i32` | yes | MUST be &gt; 0 for produce | Offsets advance by this count |
| `Durability::Buffered` | enum | yes | MAY return before PUT; MUST NOT return offsets; MUST NOT claim durable | Fire-and-forget |
| `Durability::Durable` | enum | yes | MUST return only after successful object PUT; MUST NOT require offsets | Survives crash; unsequenced |
| `Durability::Sequenced` | enum | yes | MUST return only after successful PUT **and** `Sequencer::commit`; MUST return base/last offsets when Assigned | Strong ack |
| `AppendOutcome` | `{ base_offset?, last_offset?, durable, sequenced }` | yes | Offsets `None` unless sequenced | |
| `FetchedBatch` | `{ base_offset, record_count, payload }` | yes | Payload bytes MUST match what was produced | |
| `FlushConfig` | linger, max_bytes, max_batches, max_inflight_flushes, max_buffered_bytes, budget | yes | linger is packing control; max_bytes is high safety ceiling | See TD-004 |
| `LogEngine::produce` | async | yes | Enqueues batch; resolves at requested durability | Empty batch → `InvalidBatch` |
| `LogEngine::produce_at_epoch` | async | yes | As `produce`, carrying an opaque fence epoch to `CommitBatch::epoch` | Engine never compares epochs |
| `LogEngine::flush` | async | yes | Barrier for all work enqueued at or before the call | |
| `LogEngine::fetch` | async | yes | lookup → get_range slices → ordered batches | Size-bounded by max_bytes |
| `LogEngine::fetch_stream` | async visitor | yes (P2) | Same order as fetch; no full `Vec` materialization | Bounded-RAM replay |
| `LogEngine::truncate_before` | async | yes | Sequencer truncate then delete returned object ids | Shared objects deleted only when unreferenced |
| `reap_orphans` / `LogEngine::reap_orphans` | async | yes (P2) | Delete data-prefix keys not in live set | Quiescent writers only |
| `live_object_ids` (default sequencers) | sync | yes for shipped sequencers | Set of object ids in the index | Feeds reaper |
| `Sequencer::Meta` | associated type | yes | Engine forwards uninterpreted; `Send + Sync` | Default sequencers use `()` |
| `BatchLocation` | `{ object_id, byte_start, byte_len }` | yes | **Authored by engine** after layout | Sequencer stores in index |
| `CommitBatch` | `{ partition, record_count, location, meta, epoch }` | yes | Engine fills all but interprets only partition/count/location | `epoch` is forwarded for the sequencer's fencing policy |
| `CommitOutcome::Assigned` | `{ base_offset, record_count }` | yes | Fresh contiguous range | |
| `CommitOutcome::Duplicate` | `{ base_offset }` | yes | Idempotent retry recognized by sequencer | Visibility unchanged |
| `CommitOutcome::Rejected` | `{ reason }` | yes | Batch not indexed; others in the call may be | Engine reports `Sequencer::rejection_error` |
| `Sequencer::commit` | sync | yes | One ordered outcome per batch; atomicity follows the implementation; Manifest publishes per partition and errors may follow earlier durable publications | Lin-point |
| `Sequencer::lookup` | sync | yes | Entries covering `fetch_offset` onward | |
| `Sequencer::high_watermark` | sync | yes | Next offset to assign (index-only) | |
| `Sequencer::log_start_offset` | sync | yes | First readable offset | Advances on truncate |
| `Sequencer::truncate_before` | sync | yes | Drop entries below offset; return object ids with **no** live refs from **any** partition | Mechanism, not policy |
| `Sequencer::fence_epoch` | sync | no (default no-op) | Once it returns, no batch below `new_epoch` commits to the partition | `ManifestSequencer` fences the durable index |
| `Sequencer::refresh_partition` | sync | no (default no-op) | Reload the partition's durable index to see other writers' commits | For multi-writer sequencers |
| `Sequencer::rejection_error` | sync | no (default `Sequencer(reason)`) | Map a `Rejected` batch to the error the engine reports | Rejection policy is the sequencer's |

### Engine invariants (normative)

1. **PUT before commit.** The engine MUST durable-put the sealed object before calling `commit`. A successful `lookup` entry implies its byte range is durably readable under a crash-durable BlobStore.
2. **Unique object id, fresh on retry.** Each seal uses a unique object key; retries MUST NOT reuse a key from a failed or uncertain PUT.
3. **In-order, un-split per partition.** For any single `PartitionKey`, batches are presented to `commit` in arrival order and never split across concurrent in-flight `commit` calls. Satisfied by a single flush worker, or by sharding concurrency on `PartitionKey` only (never on opaque Meta).
4. **Publication scope.** InMemory validates and commits the whole slice under its lock. Manifest publishes each partition independently; an error can follow a durable earlier partition. The engine may acknowledge nothing for that seal, which does not prove every publication had no effects.

## Precedence and Compatibility

- **Versioning**: v2 is the 0.2.x contract. Removing durability/offset semantics requires a new major contract version.
- **Ordering**: per-partition produce arrival order is preserved through commit presentation; `Duplicate` takes precedence over assigning a new range when the sequencer recognizes a retry.
- **Backward compatibility**: v1 Kafka-shaped types are **removed**, not deprecated. Callers must migrate to v2.
- **Meta evolution**: consumers may change their `Meta` type without a contract major if engine bounds (`Send + Sync`) hold.

## Native authority and owned replay

Owned append is additive to ordinary produce: an opaque shared lifetime owner
follows the existing budget/enqueue/flush path. Pending payloads drop before their
owners; every started upload retains its sealed batch owners through terminal
I/O. Engine failure can send an error before those uploads finish. Final-owner
notification, not ACK, governs reservation release. Cleanup drops callbacks
outside the engine lock. Embedders admit all retained copies/header capacity.

Native conditional append requires Sequenced acknowledgement and original
nonnegative next-offset conditions; insertion sequence and sparse hints are not
log heads. Unsupported sequencers reject before legacy commit or upload. Ordered
siblings advance a partition's tentative head only when accepted. All-rejected
partitions do not publish. CAS conflicts reconcile exact location/base/count and
re-evaluate original conditions. Partial reconciliation, unavailable retained
identity, malformed authority and ambiguous replies remain unknown. Publication
is per partition; no cross-partition transaction is promised.

Fresh authoritative state contains epoch, retained start and next offset. Native
acquisition checked-increments epoch and returns bounds from the same successful
CAS. Neither operation falls back to cached getters or legacy fencing. State is
a point-in-time target, not exclusivity or a pinned history session; subsequent
writes still require conditional head and epoch checks. Known observed/catalog
membership and ambiguous/malformed conflict evidence forbid recreating missing
authority. A genuinely unseen namespace may report zero state. Uncertainty
markers are failure knowledge, never synthetic durable success.

`BoundedSequencer` is a separate required page port. Validate ordered contiguous
positive ranges and boundary/tail coverage before installation; budget cloned
entries, headers and complete object identifiers before allocation. Retention is
checked under the same lock as page copying. `FetchPlan` binds sequencer/blob
identity and reports metadata/selected byte lengths before payload I/O. First
batch size may exceed a soft target; a distinct admitted hard bound applies.

Strict range reads never fall back to whole-object reads. Unknown adapters reject;
Local opens and reads the checked range, Memory can share a slice, and S3 verifies
206 status, exact length and complete Content-Range before bounded collection.
Empty reads use checked EOF/HEAD. Cancellation is not proof that independently
started blocking work has ended; embedders retain ownership through completion.

These ports preserve serialized legacy batch/outcome types. Full manifest
residency/rewrite, paged source roots, total resource bounds, authenticated semantic
proofs and provider/process-loss qualification remain obligations. The bounded
metadata/fixed-head foundation is specified in CONTRACT-002; it does not activate
Fireweed's public backend or permit trusting its serving projection.

## Error Semantics

| Condition | Error / Outcome | Retry | Recovery Expectation |
|-----------|------------------|-------|----------------------|
| Empty / invalid batch | `InvalidBatch` | no | Fix produce inputs |
| Invalid config | `InvalidConfig` | no | Fix FlushConfig/BudgetConfig |
| Transient storage failure | `StorageUnavailable` | yes | Engine may retry PUT; caller may retry produce |
| Range bounds on get_range | `RangeOutOfBounds` | no | Fix range or object length assumption |
| Missing object at fetch | `MissingObject` | no | Repair storage or index |
| Sequencer commit/lookup failure | `Sequencer` | depends | Refresh consumer state; retry with same Meta if idempotent |
| Batch behind a moved fence (from a sequencer's `rejection_error`) | `Fenced` | no | Not committed; stop writing at that epoch |
| Budget admission failure | `BudgetExceeded` | yes later | Back off or raise budget / change mode |
| Commit `Duplicate` | success with original offset | yes | Treat as prior success |

## Examples

```text
produce:
  partition: "events-0"
  payload: <opaque bytes>
  record_count: 3
  meta: ()
  durability: Sequenced

result (Assigned):
  base_offset: 0
  last_offset: 2
  durable: true
  sequenced: true

fetch:
  partition: "events-0"
  offset: 0
  max_bytes: 1048576

fetched:
  - base_offset: 0
    record_count: 3
    payload: <same opaque bytes>
```

## Non-Normative Notes

- Kafka `acks=0/1/-1` map to `Buffered` / `Durable` / `Sequenced` **in the consumer**, not as object-log enum aliases.
- Idempotent producer triples live in the consumer’s `Meta` and sequencer logic.
- Checksums over payload framing are consumer-owned; object-log does not whole-object-checksum multiplexed objects (incompatible with `get_range`).

## Validation Checklist

- [x] Normative fields and rules are explicit.
- [x] Engine invariants match ADR-002.
- [x] Error handling is explicit.
- [x] Executable tests can be derived (see test plan).
- [x] Non-normative notes cannot be mistaken for requirements.

## Bounded native append facade

The `native` feature exposes `NativeLog` independently of the legacy sequencer
interface. Local and S3 adapters use the same namespace, writer, publication and
fixed-cut operations. `NativeLimits` specifies store-wide execution admission,
retained byte admission (a checked 64-bit ledger, bounded by platform allocation
size), opaque version size, namespace-binding size, append
entry count and framed append-group size. Namespace size does not constrain
payload size. Invalid page allocation limits fail before executor creation.
S3 composition calls `NativeLog::preflight_s3` before constructing its adapter
client. The same checked byte-length/page/source planner validates native
construction without copying derived paths before rejection.

`open` observes an existing namespace; `provision_new` explicitly publishes an
empty candidate. Empty candidate election is not consumer queue provisioning:
the consumer must establish its unique genesis and semantic authority.
`acquire` fences the exact retained namespace observation. `capture` and
`refresh_writer` explicitly observe fresh authority; immutable read cuts never
refresh while reading. Derived handles share monotonic authority knowledge.
This family knowledge is not a global registry or cold-restart rollback oracle.

`prepare` admits and retains the complete ordered append group and its original
conditional authority. `submit` attempts that original transition once.
`NativePublishOutcome::Stored` proves the exact conditional write succeeded;
`Conflict` requires an actual rejected storage condition and fully validated
current authority. Regressed, malformed or incompatible authority is an error.
An error or lost reply never grants permission to prepare a replacement as if
the original were absent. The retained publication supports bounded `resolve`:
`DurableContent` proves complete original ordered content, `IncompatibleCoverage`
proves complete competing content, and `Unknown` provides neither proof.
Content resolution does not establish which writer published it.

Admitted operations own their work through caller cancellation. Replies and errors
are delivered to the caller only after that operation's final physical source owner
has released its admission, including owned-task panic and detached cleanup. Namespace,
writer, publication, cut and payload allocations retain their byte credits until
the corresponding owned bytes are destroyed. `shutdown` closes admission and
drains physical work; retained byte-only handles may outlive it. Payload reads
use one authenticated captured-root lookup for range and content validation.
`read_bounded` rejects an authenticated payload length above the caller limit
before identity-overflow or data reads. The caller must separately admit its own
consumer allocation before invoking hydration. `plan_read` returns EOF or a sealed
fixed-cut entry with its exact payload length without identity/payload hydration.
The consumer admits actual bytes, then consumes that same entry with `read_planned`;
no fresh head observation or second index lookup replaces the plan. `NativeRecord::into_bytes` transfers
the SDK owner without copying; byte aliases retain admission through final drop.
The external append/reopen contract runs unchanged on Local and explicitly
configured S3, with both S3 payload-signing modes. These checks do not establish
provider durability, consumer semantic-index correctness or release throughput.
