---
ddx:
  id: prd
kind: product
depends_on:
  - product-vision
  - concerns
---

# Product Requirements Document

## Summary

object-log is a Rust embeddable **buffered, multiplexing append log** over pluggable object storage. It provides:

- a `BlobStore` port (durable-on-return put, get, get_range, list, delete) with memory, local filesystem, and optional S3 adapters;
- a `LogEngine` that group-commits many opaque batches into few objects and resolves produces at `Durability::{Buffered, Durable, Sequenced}`;
- a pluggable sync `Sequencer` seam that owns offset assignment and the offset→location index (`InMemorySequencer`, `ManifestSequencer`, or consumer-supplied);
- retention **mechanism** via `truncate_before` (policy stays with the consumer).

The library **does not** implement Kafka wire protocol, record framing, producer identity, epoch fencing, or broker coordination. Consumers (e.g. fjord/heimq, Niflheim cold tier, pqueue projections) map their semantics onto partition keys, opaque bytes, durability levels, and `Sequencer::Meta`.

This PRD supersedes the ADR-001-era Kafka-shaped core requirements. Governing architecture: **ADR-002**.

## Problem and Goals

### Problem

Brokers and ingestion systems need amortized durable appends on object storage. A one-PUT-per-produce design is economically unusable. Baking Kafka record/acks/idempotency types into a storage library forces every consumer into broker vocabulary and blocks pure cold-tier WAL reuse. Product-local write paths (fjord-log, Niflheim cold WAL) duplicated the same durability and multiplexing problems.

### Goals

1. Ship one shared Rust engine for durable group-committed append and offset-addressed fetch over object storage.
2. Keep payloads opaque and sequencing pluggable so fjord, Niflheim, and others integrate without product schemas in object-log.
3. Make PUT count independent of produce/partition count under continuous load (linger-bound packing).
4. Provide crash-durable standalone operation via `ManifestSequencer` (or equivalent) without requiring a broker.
5. Keep layer purity: zero Kafka producer/record/acks types in the object-log public API.

### Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| P0 FR coverage | 100% of P0 FRs covered by named tests | `cargo test` + traceability review |
| PUT amortization | Under continuous produce, objects ≈ flushes ≪ produces | `put_count_independent_of_partition_count`, perf_budget |
| Sequenced correctness | `Sequenced` returns only after durable PUT + successful commit | engine durability + failure tests |
| Atomic multiplex commit | One commit failure acks no co-multiplexed batch | `multiplexed_commit_is_all_or_nothing` |
| Manifest restart | Index survives process restart with same blob root | `manifest_index_survives_restart` |
| Layer purity | No Kafka identifiers in public API types | API review / grep gate |

### Non-Goals

- Kafka wire protocol, consumer groups, transactions, broker metadata, ACLs, quotas.
- Kafka record batch codec or offset stamping inside payload bytes (heimq/consumer concern).
- Epoch fencing, leader election, or cluster consensus inside object-log.
- Product-specific pqueue command envelopes or Niflheim row/WAL codecs.
- Local hot-tier fsync latency tiers (consumers that need sub-PUT local durability front their own buffer).
- Automatic background orphan reaping while writers are active (manual quiescent `reap_orphans` is in scope).

## Users and Scope

### Primary Persona: Storage-Engine Integrator

**Role**: Rust engineer embedding durable object storage under a broker, queue, or WAL  
**Goals**: Produce opaque batches, fetch by offset, amortize PUTs, plug their own sequencer  
**Pain Points**: Prior options were full brokers, one-PUT-per-batch S3 code, or Kafka-shaped libraries that leak broker types

### Secondary Persona: Sequencer Author

**Role**: Engineer implementing offset authority (in-process, SQL, or coordinator)  
**Goals**: Receive durable batch locations, assign offsets atomically, own dedupe/fencing in `Meta`  
**Pain Points**: Need a clear sync seam with in-order presentation and all-or-nothing commit

## Requirements

### Must Have (P0)

1. BlobStore port with durable-on-return put (Local/S3), get, get_range, list, delete; Memory for tests (not crash-durable).
2. LogEngine group-commit: many produces → one object; PUT then sequence.
3. Durability levels: Buffered, Durable, Sequenced with correct offset visibility.
4. Opaque payloads and opaque partition keys; no payload schema inspection.
5. Sequencer seam (sync, generic `Meta`) with atomic multi-batch commit and lookup/HWM/log-start/truncate_before.
6. In-order, un-split per-partition presentation to commit (single flush worker or equivalent).
7. InMemorySequencer and ManifestSequencer (crash-durable index) ship in-crate.
8. LocalBlobStore durable protocol and MemoryBlobStore for tests.
9. truncate_before deletes only objects with no remaining index references.
10. Production-usable flush controls (linger, max_bytes ceiling, budget) without requiring one-record-per-object rejection config.

### Should Have (P1)

1. Optional S3-compatible BlobStore (feature-gated) with multipart for large objects.
2. Durable-ops budget controller and `pipeline_snapshot` (TD-004).
3. Perf harnesses documenting local throughput honesty.
4. Documented integration maps for fjord Sequencer binding and Niflheim cold tier.

### Nice to Have (P2)

1. Streaming fetch for wide offset windows (`fetch_stream`) — **implemented**.
2. Quiescent orphan reaper (`reap_orphans` + `live_object_ids`) — **implemented**.
3. CLI tools for manifest inspection and object listing — **implemented** (`cli` feature).

## Functional Requirements

### Subsystem: BlobStore Port

- **FR-1** — The library MUST expose an async `BlobStore` with `put`, `get`, `get_range`, `list`, and `delete`.
- **FR-2** — For crash-durable adapters (Local, S3), `put` / `put_chunks` MUST be durable-on-return: `Ok` means bytes survive process/host crash under the adapter’s durability model. Memory MUST document non-crash-durability.
- **FR-3** — `get_range` MUST return a byte sub-range without requiring a full-object read; bounds errors MUST be distinct from missing keys.
- **FR-4** — Object keys MUST reject empty keys and path traversal (`..`, NUL) as invalid.
- **FR-5** — Large objects MUST be writable without requiring callers to implement multipart (S3 adapter chunks internally; Local streams via `put_chunks`).
- **FR-6** — In-memory and local filesystem adapters MUST pass the same BlobStore conformance suite for put/get/get_range/list/delete.

### Subsystem: LogEngine and Group-Commit

- **FR-7** — `produce(partition, payload, record_count, meta, durability)` MUST accept opaque payload bytes and a partition key.
- **FR-8** — Empty batches MUST be rejected.
- **FR-9** — The engine MUST multiplex batches across partitions into sealed objects and PUT each sealed object before calling `Sequencer::commit`.
- **FR-10** — Under continuous produce with default linger, PUT/object count MUST track flushes, not produce count or partition count.
- **FR-11** — `Durability::Buffered` MUST NOT claim durability or offsets; `Durable` MUST resolve after successful PUT without requiring offsets; `Sequenced` MUST resolve only after successful PUT and commit, with assigned offsets.
- **FR-12** — `flush()` MUST barrier all work enqueued at or before the call.
- **FR-13** — On PUT failure after retries, no batch in that seal MUST be acknowledged as durable or sequenced.
- **FR-14** — On commit failure, no batch in that object MUST be acknowledged as sequenced; a retry MUST use a fresh object key (no aliasing of a failed PUT).
- **FR-15** — For any single partition key, batches MUST be presented to `commit` in arrival order and MUST NOT be split across concurrent in-flight commits.
- **FR-16** — `fetch(partition, offset, max_bytes)` MUST use sequencer lookup and `get_range` to return opaque batch payloads with base offsets.
- **FR-16a** — The library SHOULD provide `fetch_stream` (or equivalent visitor) that yields batches without materializing a full `Vec` for wide replay.
- **FR-17** — `truncate_before(partition, offset)` MUST drop index entries via the sequencer and delete only object ids the sequencer reports as unreferenced across all partitions.
- **FR-17a** — The library SHOULD provide orphan reaping that deletes data-prefix objects absent from a live-object set, documented as safe only when writers on that prefix are quiescent.

### Subsystem: Sequencer Seam

- **FR-18** — `Sequencer` MUST be synchronous and generic over associated type `Meta`; the engine MUST forward `Meta` uninterpreted.
- **FR-19** — `commit` MUST be all-or-nothing across the batch slice; on `Err`, nothing is committed.
- **FR-20** — `commit` MUST return one `CommitOutcome` per input batch (`Assigned` or `Duplicate`) in order.
- **FR-21** — `lookup`, `high_watermark`, and `log_start_offset` MUST be provided for fetch and bounds without full object scans.
- **FR-22** — `InMemorySequencer` (`Meta = ()`) MUST ship for tests and single-process use and MUST document non-persistent index.
- **FR-23** — `ManifestSequencer` (`Meta = ()`) MUST persist index state to the BlobStore so standalone open/restart rebuilds the index (≤1 manifest PUT per group-commit amortization model).

### Subsystem: Operations and Cost

- **FR-24** — Default `FlushConfig` MUST use linger as the primary packing control and a high `max_bytes` safety ceiling (default 1 GiB class), not a tiny per-record object mode.
- **FR-25** — The engine MUST support a durable-ops budget controller (TD-004) with inspectable `pipeline_snapshot()`.
- **FR-26** — Optional S3 adapter (feature-gated) MUST implement `BlobStore` without requiring manifest CAS from the store.

### Subsystem: Consumer Compatibility (constraints, not product schemas)

- **FR-27** — pqueue-class consumers MUST be able to store opaque command bytes and project by offset without object-log knowing queue schemas.
- **FR-28** — Niflheim-class consumers MUST be able to use object-log as a cold-tier blob + optional sequencer/index path without object-log knowing row/WAL codecs.
- **FR-29** — Broker-class consumers MUST be able to implement `Sequencer` with their own `Meta` (e.g. producer identity) and map external ack levels onto `Durability` outside object-log.
- **FR-30** — The public API MUST NOT export Kafka record, acks, producer_id/epoch/sequence, or topic/partition record models as core types.

## Acceptance Test Sketches

| Requirement | Scenario | Input | Expected Output |
|-------------|----------|-------|-----------------|
| FR-6 | Memory and Local port suite | put/get/get_range/list/delete | identical behavioral pass |
| FR-10 | Multi-partition continuous produce | many partitions, one linger window | object count ≈ flushes ≪ produces |
| FR-11 | Sequenced produce | single batch, Sequenced | base_offset set; durable+sequenced |
| FR-11 | Buffered produce + flush | Buffered then flush() | data becomes durable after barrier |
| FR-13 | Permanent PUT failure | failing BlobStore | Err; no offset; no false ack |
| FR-14 | Commit fails then retry | failing Sequencer once | first Err; retry succeeds once; orphan object may remain |
| FR-15/FR-19 | Multiplexed atomic commit | multi-partition seal, commit errs | no batch acked |
| FR-20 | Idempotent Duplicate | Sequencer returns Duplicate | original base_offset; no double visibility |
| FR-17 | truncate_before | retire offsets; shared objects | only unreferenced objects deleted |
| FR-23 | Manifest restart | produce, drop engine, reopen | fetch returns prior payloads |
| FR-27/FR-28 | Opaque payloads | arbitrary bytes + headers-in-payload | round-trip unchanged |

## Technical Context

- **Language/Runtime**: Rust edition 2024; MSRV as declared in `Cargo.toml`
- **Key Libraries**: `tokio`, `async-trait`, `bytes`, `serde`/`serde_json`, `thiserror`; optional `aws-sdk-s3`
- **Data/Storage**: BlobStore adapters; no embedded database
- **APIs**: Rust library only in v1
- **Platform Targets**: Linux and macOS; S3-compatible stores for optional adapter

### Layering (normative product boundary)

```
Consumer formats & coordination (fjord, heimq, niflheim, pqueue)
        │  implements Sequencer / maps Durability / owns codecs
        ▼
object-log: LogEngine + BlobStore + Sequencer seam
        │
        ▼
Object storage (memory / local / S3)
```

## Constraints, Assumptions, Dependencies

### Constraints

- Object storage has higher commit latency than local disks; production packing uses linger/group-commit.
- Library handles opaque bytes only; callers own classification/encryption policy.

### Assumptions

- Consumers that need fencing or idempotent producer dedupe implement them in `Sequencer` / `Meta`.
- Consumers that need Kafka wire or record batch codecs use a separate crate (e.g. heimq).
- MemoryBlobStore is never production durability authority.

### Dependencies

- ADR-002 (accepted architecture).
- TD-004 (flush budget controller).
- Optional: AWS S3-compatible endpoint for feature `s3` tests.

## Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Consumers reintroduce Kafka types into object-log | Med | High | FR-30 purity gate; code review |
| Concurrent flush workers reorder partitions | Med | High | FR-15; single-flight default; ordering tests |
| Standalone users assume InMemorySequencer is durable | Med | Med | Docs + ship ManifestSequencer |
| Orphan objects accumulate | Med | Low | Documented deferral; external reaper via list |

## Open Questions

- [x] Is Kafka a backend *inside* object-log? — **No.** Sequencing/coordination stay in the consumer. Closed by ADR-002.
- [x] Is manifest CAS required of BlobStore? — **No.** Unique object keys + durable put; ManifestSequencer writes its own manifest objects. Closed by ADR-002.
- [ ] Should `fetch_stream` land before 1.0? — product priority; default defer to P2 unless Niflheim blocks.
- [x] Which S3-compatible providers are evidence-gated first? — **MinIO** (local path-style) recorded 2026-07-31 in TD-002; Garage/AWS/R2 re-run `tests/s3.rs` before claiming.

## Success Criteria

- `cargo test` covers all P0 FRs with named tests listed in the test plan.
- Public API matches CONTRACT-001/002 (v2) and ADR-002.
- README and crate docs describe engine + sequencer, not Kafka-shaped append.
- No production claim of Kafka drop-in or CAS-based ObjectStore.

## Review Checklist

- [x] Summary works as a standalone 1-pager
- [x] Problem statement describes a specific failure mode
- [x] Goals are outcomes, not activities
- [x] Success metrics have targets and measurement methods
- [x] Non-goals exclude reasonable false assumptions
- [x] Personas have specific pain points
- [x] P0 requirements are necessary for the current product
- [x] Functional requirements are testable and carry stable `FR-n` IDs
- [x] Requirements trace to Product Vision and ADR-002
