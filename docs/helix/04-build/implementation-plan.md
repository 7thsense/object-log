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

### M3: S3 Production Evidence

- Env-gated CI or operator runbook for MinIO/Garage/LocalStack.
- Confirm multipart + get_range against at least one S3-compatible target.
- Gate: recorded evidence before “S3 production supported” claims.

### M4: Deferred P2 (optional)

- `fetch_stream` if Niflheim (or other) blocks on materializing wide fetches.
- Orphan reaper design + implementation.
- Gate: design snippet + tests; does not block 0.2.x consumers.

### M5: 1.0 Readiness

- API freeze review against CONTRACT-001/002 v2.
- CHANGELOG and semver discipline.
- Gate: no open P0 FR without a test; layer-purity grep clean.

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
