---
ddx:
  id: td-core-and-object-backend
  depends_on:
    - contract-core-log-api
    - contract-object-store-api
    - adr-object-storage-log-engine-and-sequencer-seam
    - prd
    - concerns
---

# Technical Design: TD-001 LogEngine, BlobStore, and Sequencer Seam

**Status**: accepted (rewritten for object-log 0.2.x / ADR-002)  
**Contracts**: CONTRACT-001 v2, CONTRACT-002 v2 | **ADR**: ADR-002 | **Scope**: core crate modules  
**Supersedes**: TD-001 v1 (Kafka-shaped `LogBackend`, segment codec, manifest CAS, EpochGuard)

## Scope

This design documents the buildable 0.2.x core: `BlobStore`, `LogEngine`, `Sequencer`, default sequencers, and errors.

**In scope**

- Crate modules matching `src/{blob,engine,sequencer,manifest_sequencer,error,lib}.rs`
- Memory and Local BlobStore adapters
- InMemorySequencer and ManifestSequencer
- Group-commit flush worker and durability resolution
- Conformance-oriented integration tests under `tests/`

**Out of scope**

- Durable-ops budget details (TD-004)
- S3 adapter internals (TD-002)
- Consumer Kafka/WAL codecs and coordinator logic
- Orphan reaper and `fetch_stream` (deferred)

## Technical Approach

**Strategy**: three layers inside one crate—(1) BlobStore port, (2) buffered multiplexing engine, (3) pluggable sync sequencer. The engine owns layout and durable put; the sequencer owns offsets and index.

**Key decisions**

- Unique object keys per seal (no store CAS).
- Opaque payloads; `PartitionKey` is the only log identity the engine interprets.
- `Durability::{Buffered,Durable,Sequenced}` instead of Kafka acks.
- Sync `Sequencer` called from the flush worker; async produce futures via oneshot.
- Single flush worker by default (`max_inflight_flushes = 1`) preserves per-partition order.

## Component Map

| Component | Files | Responsibility |
|-----------|-------|----------------|
| BlobStore + Memory/Local | `src/blob.rs` | Durable put path, get_range, list, delete, media stats |
| LogEngine | `src/engine.rs` | Buffer, seal, put, commit, fetch, truncate_before, flush |
| Sequencer trait + InMemory | `src/sequencer.rs` | Offset assign, index, truncate mechanism |
| ManifestSequencer | `src/manifest_sequencer.rs` | Persist index via BlobStore manifests |
| Errors | `src/error.rs` | Stable public error kinds |
| Budget (see TD-004) | `src/budget.rs` | Media-ops admission / early-flush |

## Produce / Fetch Flows

### Produce (Sequenced)

1. Validate batch (`record_count > 0`).
2. Enqueue `(partition, payload, record_count, meta, durability, responder)`.
3. Flush worker seals when linger/size/batch/early-flush/flush-barrier triggers.
4. Layout batches into one object; assign `BatchLocation`s; `put` / `put_chunks`.
5. On put success: resolve `Durable` waiters; call `sequencer.commit`.
6. On commit success: resolve `Sequenced` waiters with offsets; on failure: error all waiters; next retry uses a **fresh** object key.

### Fetch

1. `sequencer.lookup(partition, offset)`.
2. For each index entry until `max_bytes`: `get_range(object_id, byte_start..byte_start+byte_len)`.
3. Return `FetchedBatch` list (opaque payload preserved).

### Truncate

1. `dead = sequencer.truncate_before(partition, offset)`.
2. `blob.delete` each returned object id (only unreferenced across partitions).

## Data Layout

There is **no** framed OLOG segment codec in-crate. A sealed object is the concatenation of opaque batch payloads in seal order; locations are absolute byte ranges within that object. Index state lives in the sequencer (memory or manifest objects).

ManifestSequencer persists enough metadata per commit to rebuild `(partition → IndexEntry list, next offset, log start)` on `open`.

## Integration Points

| From | To | Method | Data |
|------|-----|--------|------|
| Consumer | `LogEngine::produce` | async API | opaque bytes + Meta |
| Consumer | `impl Sequencer` | sync trait | offset authority, dedupe/fencing in Meta |
| Engine | `BlobStore` | async trait | sealed object bytes |
| Engine | `Sequencer::commit` | sync | locations + Meta refs |

## Security

- No authn/authz in core.
- Key traversal rejected at BlobStore boundary.
- Payload confidentiality/integrity framing is caller-owned.

## Testing

| Area | Location | Coverage |
|------|----------|----------|
| BlobStore conformance | `tests/blob.rs` | Memory + Local port suite, durability across Local instances |
| Engine | `tests/engine.rs` | round-trip, amortization, durability, failures, atomic commit, truncate, budget |
| Manifest | `tests/manifest.rs` | restart rebuild |
| Perf | `tests/perf_*.rs` | budget + honest local throughput (release) |

## Implementation Sequence (historical → current)

1. ADR-002 re-foundation (0.2.0): remove v1 backend/codec/CAS.
2. Engine + sequencers + Memory/Local (done).
3. TD-004 budget controller (done).
4. S3 feature (TD-002, done).
5. Remaining: conformance suite extraction, integration maps, deferred P2s.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Concurrent flush reorders partition | M | H | Default max_inflight=1; FR-15 tests |
| Users pick InMemorySequencer for prod | M | M | Docs + ManifestSequencer |
| Orphan objects after commit fail | M | L | Documented; external reaper |

## Review Checklist

- [x] Matches ADR-002 and shipped modules
- [x] No Kafka-shaped core types
- [x] Test pointers are real paths
