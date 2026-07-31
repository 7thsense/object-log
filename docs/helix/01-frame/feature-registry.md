---
ddx:
  id: feature-registry
  depends_on:
    - prd
---

# Feature Registry

## Features

| Feature ID | Name | PRD Subsystem | Priority | Status | Notes |
|------------|------|---------------|----------|--------|-------|
| FEAT-001 | BlobStore Port | BlobStore Port | P0 | implemented | put/get/get_range/list/delete; Memory + Local; key validation |
| FEAT-002 | LogEngine Group-Commit | LogEngine and Group-Commit | P0 | implemented | produce/fetch/flush/truncate_before; Durability levels; linger packing |
| FEAT-003 | Sequencer Seam | Sequencer Seam | P0 | implemented | sync trait, Meta, atomic commit; InMemory + Manifest sequencers |
| FEAT-004 | Durable-Ops Budget | Operations and Cost | P1 | implemented | TD-004 budget modes, early-flush, pipeline_snapshot |
| FEAT-005 | S3 BlobStore Adapter | Operations and Cost | P1 | implemented | feature `s3`; multipart; no store CAS |
| FEAT-006 | Consumer Integration Maps | Consumer Compatibility | P1 | implemented | README + TD-003 binding sketches; sequencer_conformance tests |
| FEAT-007 | Streaming Fetch | LogEngine | P2 | implemented | `LogEngine::fetch_stream` visitor API |
| FEAT-008 | Orphan Reaper | Operations | P2 | implemented | `reap_orphans` + `live_object_ids` (quiescent-only) |
| FEAT-011 | Diagnostics CLI | Operations | P2 | implemented | `object-log` bin: produce/consume/roundtrip + list/inspect/orphans |
| FEAT-009 | Kafka Types in Core | — | — | rejected | Superseded by ADR-002; was ADR-001 FEAT-001 Kafka-shaped core |
| FEAT-010 | CAS ObjectStore / EpochGuard | — | — | rejected | Removed in 0.2.0; fencing/dedupe live in Sequencer Meta |

## Dependency Notes

FEAT-001 and FEAT-003 are the ports. FEAT-002 is the engine that composes them. FEAT-004/005 improve production operability without changing the core contract. FEAT-006 is documentation/integration readiness, not new core types. FEAT-007/008 are explicit deferrals. FEAT-009/010 record rejections so evolve passes do not resurrect ADR-001 surfaces.

## Review Checklist

- [x] Every PRD subsystem maps to at least one feature.
- [x] P0 features cover launch-critical behavior and match 0.2.x code.
- [x] Rejected Kafka/CAS core features are recorded to prevent regression.
