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
**Version**: v2  
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
| `LogEngine::flush` | async | yes | Barrier for all work enqueued at or before the call | |
| `LogEngine::fetch` | async | yes | lookup → get_range slices → ordered batches | Size-bounded by max_bytes |
| `LogEngine::truncate_before` | async | yes | Sequencer truncate then delete returned object ids | Shared objects deleted only when unreferenced |
| `Sequencer::Meta` | associated type | yes | Engine forwards uninterpreted; `Send + Sync` | Default sequencers use `()` |
| `BatchLocation` | `{ object_id, byte_start, byte_len }` | yes | **Authored by engine** after layout | Sequencer stores in index |
| `CommitBatch` | `{ partition, record_count, location, meta }` | yes | Engine fills all but interprets only partition/count/location | |
| `CommitOutcome::Assigned` | `{ base_offset, record_count }` | yes | Fresh contiguous range | |
| `CommitOutcome::Duplicate` | `{ base_offset }` | yes | Idempotent retry recognized by sequencer | Visibility unchanged |
| `Sequencer::commit` | sync | yes | Atomic across entire slice; one outcome per batch in order; `Err` commits nothing | Lin-point |
| `Sequencer::lookup` | sync | yes | Entries covering `fetch_offset` onward | |
| `Sequencer::high_watermark` | sync | yes | Next offset to assign (index-only) | |
| `Sequencer::log_start_offset` | sync | yes | First readable offset | Advances on truncate |
| `Sequencer::truncate_before` | sync | yes | Drop entries below offset; return object ids with **no** live refs from **any** partition | Mechanism, not policy |

### Engine invariants (normative)

1. **PUT before commit.** The engine MUST durable-put the sealed object before calling `commit`. A successful `lookup` entry implies its byte range is durably readable under a crash-durable BlobStore.
2. **Unique object id, fresh on retry.** Each seal uses a unique object key; retries MUST NOT reuse a key from a failed or uncertain PUT.
3. **In-order, un-split per partition.** For any single `PartitionKey`, batches are presented to `commit` in arrival order and never split across concurrent in-flight `commit` calls. Satisfied by a single flush worker, or by sharding concurrency on `PartitionKey` only (never on opaque Meta).
4. **Atomic multiplex commit.** `commit` is all-or-nothing for every batch in the object; on `Err` the engine acks nothing for that seal.

## Precedence and Compatibility

- **Versioning**: v2 is the 0.2.x contract. Removing durability/offset semantics requires a new major contract version.
- **Ordering**: per-partition produce arrival order is preserved through commit presentation; `Duplicate` takes precedence over assigning a new range when the sequencer recognizes a retry.
- **Backward compatibility**: v1 Kafka-shaped types are **removed**, not deprecated. Callers must migrate to v2.
- **Meta evolution**: consumers may change their `Meta` type without a contract major if engine bounds (`Send + Sync`) hold.

## Error Semantics

| Condition | Error / Outcome | Retry | Recovery Expectation |
|-----------|------------------|-------|----------------------|
| Empty / invalid batch | `InvalidBatch` | no | Fix produce inputs |
| Invalid config | `InvalidConfig` | no | Fix FlushConfig/BudgetConfig |
| Transient storage failure | `StorageUnavailable` | yes | Engine may retry PUT; caller may retry produce |
| Range bounds on get_range | `RangeOutOfBounds` | no | Fix range or object length assumption |
| Missing object at fetch | `MissingObject` | no | Repair storage or index |
| Sequencer commit/lookup failure | `Sequencer` | depends | Refresh consumer state; retry with same Meta if idempotent |
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
