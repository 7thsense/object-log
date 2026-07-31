---
ddx:
  id: implementation-plan
  depends_on:
    - td-core-and-object-backend
    - td-s3-adapter-retention-snapshots
    - td-conformance-kafka-backend-extraction
    - td-durable-ops-budget-and-flush-controller
    - test-plan
---

# Implementation Plan

## Build Order

object-log 0.2.x re-foundation (ADR-002) is **implemented**. Remaining work hardens conformance, documentation for consumers, and deferred P2s—not a rebuild of the Kafka-shaped 0.1 API.

## Milestones

### M0: Re-foundation (done)

- BlobStore + Memory/Local/S3
- LogEngine group-commit + Durability
- Sequencer + InMemory + Manifest
- TD-004 budget controller
- Gate: `cargo test` green for P0 suite

### M1: Conformance Hardening (done)

- `tests/sequencer_conformance.rs` for InMemory + Manifest.
- `per_producer_send_order_is_contiguous_on_shared_partition` engine ordering test.
- Honest perf ratio assert gated to release / `OBJECT_LOG_PERF_ASSERT=1`.
- Gate: `cargo test` + `cargo clippy --all-targets -- -D warnings`.

### M2: Consumer Integration Docs (FEAT-006) (done)

- README consumer integration table; TD-003 binding sketches for fjord / Niflheim / pqueue-class.
- No product schemas in object-log.

### M3: S3 Production Evidence (done for MinIO class)

- Operator runbook in CONTRIBUTING + TD-002; `OBJECT_LOG_S3_*` env names.
- Live tests: round-trip + multipart put/get_range (`tests/s3.rs`).
- Evidence recorded against local MinIO (2026-07-31) in TD-002.
- Gate: re-run suite before claiming other providers (Garage/AWS/R2).

### M4: Streaming Fetch + Orphan Reaper (done)

- `LogEngine::fetch_stream` visitor API + tests.
- `reap_orphans` free function + `LogEngine::reap_orphans`; `live_object_ids` on
  InMemory/Manifest sequencers; quiescent-only safety docs.
- Gate: engine tests for stream order/error stop and orphan delete.

### M4b: Diagnostics CLI (done)

- Feature `cli` binary `object-log`: list, inspect, orphans, fetch.
- `ManifestSequencer::snapshot` + CLI smoke test (`tests/cli_smoke.rs`).

### M5: 0.3.0 shipped; 1.0 still open

- **0.3.0** tagged/published (2026-07-31): fetch_stream, orphans, CLI, conformance.
- Hardening: CI `perf` (release ratio) + `s3-minio` live adapter tests.
- 1.0 remains an operator API-freeze decision (checklist below).

#### 1.0 readiness checklist (draft)

| Check | Status |
|-------|--------|
| P0 FR→named tests | Yes (test-plan) |
| CONTRACT-001/002 v2 match public API | Yes for produce/fetch/stream/reap/BlobStore/Sequencer |
| Layer purity (no Kafka types in public API) | Yes |
| S3 evidence (MinIO class) | Yes (TD-002) |
| MSRV + CI | Yes (1.88, `.github/workflows/ci.yml`) |
| Semver 1.0 decision | **Open** — operator call; crate remains 0.2.x until cut |

## Explicitly Not Planned (rejected)

- Restore `ObjectLogBackend` / segment codec / EpochGuard / CAS ObjectStore.
- Kafka `LogBackend` adapter inside this crate.
- Production min-records reject flag as the amortization mechanism.

## Test Plan Summary

- Unit/integration: see `docs/helix/03-test/test-plan.md`.
- Perf: release-mode honest throughput; debug may not meet B2≈B0 floor.
- Optional external: S3.

## Exit Criteria (current product)

- All P0 FRs covered by named tests in the test plan table.
- Under continuous produce, PUT count tracks flushes (amortization tests green).
- ManifestSequencer restart test green.
- Public docs match ADR-002 (engine + sequencer, not Kafka core).
- HELIX frame/contracts/TDs no longer describe ADR-001 surfaces.
