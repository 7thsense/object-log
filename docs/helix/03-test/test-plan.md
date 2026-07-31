---
ddx:
  id: test-plan
  depends_on:
    - prd
    - contract-core-log-api
    - contract-object-store-api
    - td-core-and-object-backend
    - td-durable-ops-budget-and-flush-controller
---

# Test Plan

## Testing Strategy

**Goals**: prove BlobStore durability semantics, LogEngine group-commit and durability levels, Sequencer atomicity/idempotency/truncate, ManifestSequencer restart, and PUT amortization.  
**Out of Scope**: Kafka wire protocol, consumer groups, live multi-cloud S3 matrix as a required gate, orphan reaper.  
**Traceability Source**: PRD FR-1..FR-30, CONTRACT-001/002 v2, ADR-002, TD-001, TD-004.  
**User-story ACs**: waived for this library (see concerns); FR→named-test mapping is the coverage floor.

### Test Levels

| Level | Coverage Target | Priority |
|-------|-----------------|----------|
| Unit / module | errors, budget helpers, key validation | P0 |
| Integration | Engine + Memory/Local + sequencers | P0 |
| Contract/port | BlobStore port_suite on Memory + Local | P0 |
| Perf | budget/group-commit; honest local throughput (**release**) | P1 |
| Optional external | S3 env-gated | P2 |

### Frameworks

| Type | Framework | Reason |
|------|-----------|--------|
| Async integration | `tokio::test` + `tempfile` | Engine and Local store |
| Perf | dedicated test bins, env-sized | Avoid CI flake; document release requirement |
| Optional S3 | env-gated tokio tests | No CI hard dependency |

## FR → Named Test Mapping (P0)

| FR | Named test(s) | File |
|----|---------------|------|
| FR-1..FR-4, FR-6 | `memory_blob_store_conforms`, `local_blob_store_conforms`, `local_blob_store_put_is_durable_and_readable_across_instances` | `tests/blob.rs` |
| FR-2 (media ops) | `local_put_reports_two_media_ops`, `memory_put_reports_zero_media_ops` | `tests/blob.rs` |
| FR-5 | `put_chunks_matches_put_without_premerge` | `tests/perf_throughput.rs` |
| FR-7..FR-9, FR-16 | `produce_fetch_round_trip` | `tests/engine.rs` |
| FR-8 | covered by produce validation paths / `InvalidBatch` | `src/engine.rs` + engine tests |
| FR-10 | `put_count_independent_of_partition_count`, `group_commit_reduces_put_count_under_linger` | `tests/engine.rs`, `tests/perf_budget.rs` |
| FR-11 | `sequenced_implies_durable`, `flush_drains_buffered_produces` | `tests/engine.rs`, `tests/perf_throughput.rs` |
| FR-12 | `flush_drains_buffered_produces` | `tests/perf_throughput.rs` |
| FR-13 | `put_failure_yields_no_ack_no_offset` | `tests/engine.rs` |
| FR-14 | `commit_failure_orphans_object_and_retry_is_exactly_once` | `tests/engine.rs` |
| FR-15, FR-19 | `multiplexed_commit_is_all_or_nothing`, `per_producer_send_order_is_contiguous_on_shared_partition` | `tests/engine.rs` |
| FR-20 | `idempotent_retry_does_not_duplicate` | `tests/engine.rs` |
| FR-17 | `truncate_before_deletes_dead_objects` | `tests/engine.rs` |
| FR-18..FR-22 | `in_memory_sequencer_conforms` + engine fakes | `tests/sequencer_conformance.rs`, `tests/engine.rs` |
| FR-23 | `manifest_index_survives_restart`, `manifest_sequencer_conforms` | `tests/manifest.rs`, `tests/sequencer_conformance.rs` |
| FR-24..FR-25 | `pipeline_snapshot_exposes_budget_defaults`, `fail_closed_rejects_when_budget_starved`, `headroom_allows_fast_single_produce`, `default_config_idle_latency_budget` | `tests/engine.rs`, `tests/perf_budget.rs` |
| FR-26 | `s3_blob_store_round_trip` (env-gated) | `tests/s3.rs` |
| FR-27..FR-28 | opaque payload round-trip in `produce_fetch_round_trip` | `tests/engine.rs` |
| FR-29..FR-30 | layer-purity review / no Kafka types in public API | API review + crate docs |

### Residual test gaps (tracked)

| Gap | Priority | Notes |
|-----|----------|-------|
| Multi-producer send-order contiguity | **Closed** | `per_producer_send_order_is_contiguous_on_shared_partition` |
| Sequencer conformance extract | **Closed** | `tests/sequencer_conformance.rs` |
| `fetch_stream` | **Closed** | `fetch_stream_visits_batches_in_order`, `fetch_stream_stops_on_visitor_error` |
| Orphan reaper | **Closed** | `reap_orphans_deletes_unreferenced_data_objects` |

## Critical Paths (P0)

1. Produce/fetch opaque bytes with sequenced offsets.
2. Group-commit amortizes PUTs under load.
3. Sequenced only after PUT + commit; Buffered + flush barrier.
4. PUT failure → no false ack; commit failure → no sequenced ack; retry fresh key.
5. Multiplexed commit all-or-nothing.
6. Duplicate commit outcome does not double-append.
7. truncate_before deletes only unreferenced objects.
8. Memory + Local BlobStore port conformance.
9. ManifestSequencer restart.

## Secondary Paths

- P1: budget modes, early-flush idle behavior, Local media op accounting.
- P1: honest throughput table in **release** mode (`cargo test --release --test perf_throughput`).
- P2: live S3.

## Infrastructure

| Requirement | Specification |
|-------------|---------------|
| Correctness gate | `cargo test` (default; excludes needing release perf floor) |
| Perf evidence | `cargo test --release --test perf_throughput honest -- --nocapture` (ratio assert is release-only unless `OBJECT_LOG_PERF_ASSERT=1`) |
| Clippy | `cargo clippy --all-targets -- -D warnings` |
| Optional S3 | document env vars in `tests/s3.rs` |

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Debug builds fail honest B2≈B0 assertion | Low | Require `--release` for that test; do not use as debug gate |
| Optional S3 skipped forever | Med | Claim S3 support only when env tests run |
| Phantom FR coverage | High | Table above is authoritative; update when tests rename |

## Build Handoff

**Commands**

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo test --release --test perf_throughput honest -- --nocapture   # perf evidence
```

**Blocking gate**: all P0 named tests pass; docs do not claim Kafka-in-core, CAS ObjectStore, or live S3 without evidence.

## Review Checklist

- [x] P0 FRs map to named tests
- [x] Framework choices match library product
- [x] Known gaps recorded
- [x] Build handoff commands are concrete
