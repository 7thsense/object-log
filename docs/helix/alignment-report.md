---
ddx:
  id: alignment-report-2026-07-31
  depends_on:
    - product-vision
    - prd
    - concerns
    - feature-registry
    - adr-object-storage-log-engine-and-sequencer-seam
    - adr-kafka-compatible-core-object-storage-backend
    - contract-core-log-api
    - contract-object-store-api
    - td-core-and-object-backend
    - td-s3-adapter-retention-snapshots
    - td-conformance-kafka-backend-extraction
    - td-durable-ops-budget-and-flush-controller
    - test-plan
    - implementation-plan
---

# Alignment Report — object-log specs vs code

**Date**: 2026-07-31  
**Mode**: HELIX align → evolve (gap closure)  
**Scope**: Full HELIX stack (`docs/helix/**`) bidirectionally vs crate `object-log` 0.2.0 (`src/`, `tests/`, `README.md`, `CHANGELOG.md`)  
**Review time target**: &lt; 10 minutes  

## Closure status (same day)

| Work item | Status |
|-----------|--------|
| ALIGN-001 vision/PRD/concerns/features | **Closed** — rewritten to ADR-002 |
| ALIGN-002 CONTRACT-001/002 v2 | **Closed** — accepted; supersede v1 Kafka/CAS surface |
| ALIGN-003 TD-001/002/003 | **Closed** — rewritten for engine/BlobStore/conformance |
| ALIGN-004 test-plan + implementation-plan | **Closed** — FR→named-test table; M0 done |
| ALIGN-005 CHANGELOG Unreleased | **Closed** — removed 0.1 phantom bullets |
| ALIGN-006 library US waiver | **Closed** — concerns framing exception |
| Residual | **M1–M3 closed**. Remaining optional: M4 P2 (`fetch_stream`, orphan reaper), M5 1.0 API freeze |

## Executive summary (original diagnose)

The **implemented product and ADR-002 are aligned with each other**. At report open, nearly every higher-authority planning artifact still described the **superseded ADR-001** Kafka-shaped model. That stack-wide authority drift is **resolved in docs** by the evolve pass above. Residual work is test hardening (M1), not re-architecture.

---

## Authority reconstruction (what should govern)

| Rank | Artifact | Effective authority today | Reality check |
|------|----------|---------------------------|---------------|
| 1 | Product vision | Claims Kafka-compatible core log | **DIVERGENT** vs ADR-002 + code |
| 2 | PRD + concerns + feature-registry | Kafka record model, acks, epoch guard, segment codec, CAS | **DIVERGENT** |
| 3 | ADR-002 (Accepted) | Buffered engine, BlobStore, Sequencer, opaque bytes, zero Kafka in crate | **Governing architecture** |
| 3b | ADR-001 | Superseded (explicit banner) | Historical only |
| 4 | CONTRACT-001 / CONTRACT-002 | Still ADR-001 surface | **STALE** / **DIVERGENT** |
| 5 | TD-001 / TD-002 / TD-003 | Still ADR-001 surface | **STALE_PLAN** |
| 5b | TD-004 | Durable-ops budget + flush controller | **ALIGNED** with code |
| 6 | Test plan / implementation plan | Trace to FR-1..28 and TD-001 | **STALE_PLAN** |
| 7 | Code + README 0.2.0 | `BlobStore` / `LogEngine` / `Sequencer` | **Aligned with ADR-002** |

**Intent after ADR-002 (reconstructed from ADR-002 + code + README):**

object-log is a **generic, buffered, multiplexing object-storage log engine**. It owns durable-on-return blob storage, group-commit, and a pluggable sequencing seam. It deals only in **opaque payload bytes** and **partition keys**. Kafka producer/log semantics, record framing, idempotent-producer triples, and epoch fencing live **above** object-log (fjord/heimq or consumer control planes), not in this crate.

---

## Code surface map (material projection)

| Surface | Path | Governing artifact today | Status |
|---------|------|--------------------------|--------|
| `BlobStore` + Memory/Local | `src/blob.rs` | ADR-002 §1 (not CONTRACT-002) | Implemented |
| `S3BlobStore` | `src/s3.rs` (feature `s3`) | ADR-002; TD-002 is wrong trait | Implemented |
| `LogEngine`, `Durability`, `FlushConfig` | `src/engine.rs` | ADR-002 §2–§3; TD-004 | Implemented |
| Budget / `pipeline_snapshot` | `src/budget.rs` | TD-004 | Implemented |
| `Sequencer`, `InMemorySequencer` | `src/sequencer.rs` | ADR-002 §4 | Implemented |
| `ManifestSequencer` | `src/manifest_sequencer.rs` | ADR-002 risk mitigation | Implemented |
| `ObjectLogError` | `src/error.rs` | No current contract | Implemented (ADR-002-shaped) |
| Deleted: `ObjectLogBackend`, segment codec, `EpochGuard`, record model, `AckMode`, CAS `ObjectStore` | — | Still named in PRD/contracts/TD-001 | **Absent by design** |

**Unmapped code → no governing FR/contract** (material surfaces without current normative contract):

- `Durability::{Buffered,Durable,Sequenced}`
- `LogEngine::produce` / `fetch` / `flush` / `truncate_before` / `pipeline_snapshot`
- `Sequencer::{commit,lookup,high_watermark,log_start_offset,truncate_before}` + `Meta`
- `BlobStore::{put,put_chunks,get,get_range,list,delete,take_media_op_stats}`
- Group-commit / linger / max_inflight_flushes invariants

---

## Classification legend

| Tag | Meaning |
|-----|---------|
| `ALIGNED` | Spec and code (or higher authority) agree |
| `INCOMPLETE` | Spec or code missing required piece |
| `DIVERGENT` | Spec and code/authority contradict |
| `UNDERSPECIFIED` | Behavior real but not governed tightly enough |
| `STALE_PLAN` | Plan/design describes superseded approach |
| `BLOCKED` | Cannot proceed without a human decision |

---

## Findings (by severity)

### F1 — Frame stack still claims Kafka-shaped core  · `DIVERGENT` · Critical

**Evidence**

- Vision mission/positioning/north star: Kafka-compatible log abstraction (`docs/helix/00-discover/product-vision.md` L10–L22, L41, L50).
- PRD summary and FR-1..FR-9, FR-15..FR-18, FR-19..FR-28: topics, records, `acks`, producer triple, epoch guard, `LogBackend`, Kafka backend substitutability (`docs/helix/01-frame/prd.md` L14–L131).
- Concerns: `kafka-compatibility` as active core concern with producer/acks vocabulary (`docs/helix/01-frame/concerns.md` L17).
- Feature registry FEAT-001..005: Kafka-compatible core, segment backend, fencing, CAS-era adapters (`docs/helix/01-frame/feature-registry.md` L14–L18).
- ADR-002 explicit supersession: removes Kafka vocabulary from object-log; zero Kafka identifiers validation gate (`docs/helix/02-design/adr/ADR-002-...md` L51, L142, L198, L205–L207).
- Code crate docs and README: "knows nothing about … Kafka" (`src/lib.rs` L5–L6; `README.md` L5–L8).

**Why it matters**: Every FR ID, acceptance sketch, and P0 metric under the old model is an unreliable gate. Downstream evolve/build work will thrash until vision/PRD are restated.

| Handoff field | Value |
|---------------|--------|
| Destination | Product vision, PRD, concerns, feature-registry (and eventually user stories if introduced) |
| Deliverable shape | Restate mission around **object-storage log engine + Sequencer seam**; move Kafka producer/log compatibility to **consumer layer** (fjord/heimq/adapters). Rewrite FRs to `BlobStore`/`LogEngine`/`Sequencer`/`Durability`/`truncate_before`. Retire or relocate FR-1..FR-9 Kafka record model, FR-15..FR-17 epoch/CAS, FR-23/25/27 Kafka backend-as-LogBackend. |
| Next mode | `evolve` (vision → PRD → concerns → features) |
| Evidence | Paths above |

---

### F2 — Normative contracts describe deleted APIs  · `DIVERGENT` / `STALE_PLAN` · Critical

**Evidence**

- CONTRACT-001 normative surface: `TopicName`, `Record`, `AckMode`, `ProducerState`, `EpochGuard`, `LogBackend` (`docs/helix/02-design/contracts/CONTRACT-001-core-log-api.md` L29–L46). Status still `draft`. Related: ADR-001 only.
- CONTRACT-002: `put_if_absent`, `compare_and_set`, `capabilities()` (`docs/helix/02-design/contracts/CONTRACT-002-object-store-api.md` L29–L38). ADR-002: no conditional writes; unique keys per flush (`ADR-002` L53–L54).
- Implemented errors: `InvalidObjectKey`, `StorageUnavailable`, `RangeOutOfBounds`, `MissingObject`, `InvalidBatch`, `Sequencer`, `BudgetExceeded`, `InvalidConfig` (`src/error.rs`) — not `Fenced` / `CorruptSegment` / `Conflict` / `UnsupportedCapability`.

| Handoff field | Value |
|---------------|--------|
| Destination | New CONTRACT-001 (engine + Sequencer), new CONTRACT-002 (BlobStore); archive or supersede current drafts |
| Deliverable shape | CONTRACT-001-v2: `PartitionKey`, opaque payload, `Durability`, `produce`/`fetch`/`flush`/`truncate_before`, Sequencer atomic commit + in-order presentation invariants, `CommitOutcome`, error table. CONTRACT-002-v2: `BlobStore` durable-on-return `put`/`put_chunks`/`get`/`get_range`/`list`/`delete`/`take_media_op_stats`; explicit non-goals for CAS. |
| Next mode | `design` (contracts) after frame evolve, or `evolve` if treating contracts as threaded requirements |
| Evidence | CONTRACT-001 L29–L68; CONTRACT-002 L29–L56; `src/lib.rs`, `src/error.rs` |

---

### F3 — TD-001 / TD-002 / TD-003 are stale plans  · `STALE_PLAN` · High

**Evidence**

- TD-001: `LogBackend`, segment codec `OLOG`, manifest CAS, `EpochGuard`, files `src/model.rs` / `src/segment.rs` / `src/object_backend.rs` — none exist (`docs/helix/02-design/technical-designs/TD-001-...md` L18–L133; depends on ADR-001).
- TD-002: S3 as CAS `ObjectStore`, snapshot/retention policy types, FR-13 tiny-segment rejection (`TD-002` L20–L94). Code has `S3BlobStore` without CAS; retention is `Sequencer::truncate_before` mechanism only.
- TD-003: Kafka-backed `LogBackend`, conformance table for append/acks/epoch/manifest conflict (`TD-003` L18–L96). Contradicts ADR-002 layering (Kafka backend is not an object-log concern).

TD-004 is **ALIGNED** and should remain the model for post-ADR-002 design docs.

| Handoff field | Value |
|---------------|--------|
| Destination | Superseding TDs (or evolve TD-001 into “engine + sequencer + blobstore”; TD-002 into “S3 BlobStore + retention mechanism”; TD-003 into “Sequencer conformance + consumer integration maps”) |
| Deliverable shape | Mark TD-001/002/003 **Superseded** with pointers to ADR-002 + TD-004; write TD-005 (or rewrite) covering actual modules, flush invariants, ManifestSequencer, S3 multipart/`get_range`, and consumer integration (fjord Sequencer, niflheim cold tier) without reintroducing Kafka types into object-log. |
| Next mode | `design` (after frame + contracts) |
| Evidence | TD-001 depends_on ADR-001; tree `src/` has no `segment.rs`/`object_backend.rs`/`store.rs` |

---

### F4 — Test plan and implementation plan track deleted product  · `STALE_PLAN` · High

**Evidence**

- Test plan P0 paths: `acks=0`, manifest CAS conflict, stale epoch, segment codec corruption (`docs/helix/03-test/test-plan.md` L53–L61, L74–L78). Traceability source: FR-1..28, CONTRACT-001/002, TD-001.
- Implementation plan M1–M5: extract conformance from object-backend, retention CAS rewrite, Kafka backend adapter, “no production profile permits one-record-per-object” (`docs/helix/04-build/implementation-plan.md` L21–L74).
- Actual tests (aligned with ADR-002): produce/fetch, put amortization, durability, dense offsets, PUT/commit failure orphans, multiplexed all-or-nothing, idempotent `Duplicate` meta, truncate_before, budget, blob port suite, ManifestSequencer restart, S3 round-trip, perf harnesses (`tests/engine.rs`, `blob.rs`, `manifest.rs`, …).

| Handoff field | Value |
|---------------|--------|
| Destination | test-plan.md, implementation-plan.md |
| Deliverable shape | Rewrite P0 critical paths to ADR-002 validation table (PUT amortization, Sequenced after PUT+commit, atomic multiplex commit, in-order presentation, ManifestSequencer restart, BlobStore conformance, budget). Drop Kafka-backend-in-object-log milestones; replace with Sequencer conformance suite + optional env-gated S3. Map each P0 to named tests (and later `@covers` once stories/ACs exist). |
| Next mode | `evolve` (test plan) then `polish` work items |
| Evidence | test-plan L17, L53–L61; implementation-plan L21–L74; `tests/*.rs` |

---

### F5 — Missing user stories / feature specs / AC citation  · `INCOMPLETE` · Medium

**Evidence**

- No `user-stories` or `feature-specification` artifacts under `docs/helix/01-frame/`.
- Feature registry statuses are all `defined` / `deferred` with no vertical slices or `US-n-AC-m` IDs.
- Tests do not cite AC IDs (`@covers`) — expected given no stories, but HELIX frame coverage floor is unmet if this product stays HELIX-governed for build/verify.

| Handoff field | Value |
|---------------|--------|
| Destination | feature-specification(s) + user-stories (or a deliberate library exception recorded in concerns) |
| Deliverable shape | Either (a) thin library framing: FEAT specs for BlobStore / LogEngine / Sequencer / ManifestSequencer / S3 with Given/When/Then ACs and FR mapping, or (b) documented exception that contract tests + ADR validation metrics substitute for user stories for an embeddable library. |
| Next mode | `frame` after PRD evolve, or record exception in concerns via `evolve` |
| Evidence | `docs/helix/01-frame/` listing; no US/AC grep hits |

---

### F6 — ADR-002 optional / validation gaps vs code  · `INCOMPLETE` · Medium

| Gap | ADR-002 claim | Code/tests | Classification |
|-----|---------------|------------|----------------|
| Streaming fetch | Optional `fetch_stream` for wide replay | Not present | `INCOMPLETE` (optional; mark deferred or implement) |
| Engine ordering test | Non-Docker many-producers contiguity test | `concurrent_producers_get_dense_contiguous_offsets` covers density, not per-producer send-order under Meta-aware sequencer | `INCOMPLETE` |
| Sequencer conformance suite | Shared suite for any Sequencer | Tests exercise InMemory + ad-hoc fakes; no extracted conformance module | `INCOMPLETE` |
| Orphan reaper | Out of scope 0.2.0 | Orphan objects remain; test documents orphan | `ALIGNED` (explicit deferral) |
| Zero Kafka identifiers | Validation metric | Source has no Kafka types; rustdoc mentions Kafka as consumer example only | `ALIGNED` (soft) |

| Handoff field | Value |
|---------------|--------|
| Destination | test-plan + optional TD note; code only if product wants `fetch_stream` now |
| Deliverable shape | (1) Add engine ordering + Sequencer conformance cases to test plan and tests. (2) Record `fetch_stream` as deferred P1 with consumer owner (niflheim) or implement. |
| Next mode | `evolve` (test plan) then `build` for tests; `design` only if `fetch_stream` API needs design |
| Evidence | ADR-002 L146, L190–L201; `tests/engine.rs` |

---

### F7 — CHANGELOG Unreleased still describes 0.1 APIs  · `DIVERGENT` · Low

**Evidence**: `CHANGELOG.md` L29–L52 under `[Unreleased]` references segment codec property tests, `MemoryObjectStore`/`LocalObjectStore`, `ObjectLogBackendConfig::min_records_per_segment` — after a complete 0.2.0 breaking section.

| Handoff field | Value |
|---------------|--------|
| Destination | CHANGELOG.md (project docs, not HELIX activity required) |
| Deliverable shape | Move true unreleased 0.2.x notes under 0.2.x or a clean Unreleased; remove 0.1-only bullets or park under 0.1.0 history. |
| Next mode | `build` (docs fix) or include in evolve housekeeping |
| Evidence | `CHANGELOG.md` L7–L52 |

---

### F8 — Concern overrides still cite FR-13 one-record-per-object  · `DIVERGENT` · Medium

**Evidence**: concerns override table (`docs/helix/01-frame/concerns.md` L26) and vision batch-efficiency KPI (`product-vision.md` L52) require rejecting one-record-per-object production profiles. ADR-002 amortizes via **group-commit + linger**, not min-records config; production may still flush a single batch after idle early-flush (correct and intended).

| Handoff field | Value |
|---------------|--------|
| Destination | concerns.md + PRD success metrics + vision KPI |
| Deliverable shape | Replace “reject one-record-per-object” with **measurable PUT amortization under load** (e.g. N produces / 1 object under continuous produce with default linger) plus documented idle early-flush exception. |
| Next mode | `evolve` with F1 |
| Evidence | concerns L26; ADR-002 L99; `tests/engine.rs` `put_count_independent_of_partition_count`; `tests/perf_budget.rs` group_commit |

---

### F9 — S3 / Kafka “backend” scope confusion  · `UNDERSPECIFIED` · Medium

**Evidence**: PRD open questions and TD-003 still discuss Kafka-as-LogBackend and CAS capability matrix. ADR-002 DAG places Kafka coordination in fjord, wire format in heimq, storage in object-log. Code ships optional `S3BlobStore` without CAS or production config fail-closed for missing CAS.

| Handoff field | Value |
|---------------|--------|
| Destination | PRD open questions + TD-002 rewrite + concerns |
| Deliverable shape | Close open questions: Kafka adapter is **out of object-log**; S3 is a BlobStore adapter (multipart, range GET, durable put). Capability detection for CAS is **obsolete**. Document env-gated S3 tests as optional infra. |
| Next mode | `evolve` (PRD) + `design` (S3 TD) |
| Evidence | prd L196–L197; TD-003; `src/s3.rs`; ADR-002 L29–L47 |

---

### F10 — Local/code claims vs perf gate (observational)  · `INCOMPLETE` · Low (runtime)

`cargo test` (debug) failed `honest_local_throughput_table` with B2.flush/B0 ≈ 0.11 (harness expects near-parity). TD-004 documents release-mode evidence. Not a spec-stack authority conflict; treat as verification/harness hygiene (run `--release` or gate the test).

| Handoff field | Value |
|---------------|--------|
| Destination | test-plan / CI notes / `tests/perf_throughput.rs` |
| Deliverable shape | Require `--release` for that test or `#[ignore]` without env flag; align CI with TD-004. |
| Next mode | `build` or CI polish |
| Evidence | local `cargo test` failure; TD-004 L84–L100 |

---

## Aligned areas (do not thrash)

| Item | Why aligned |
|------|-------------|
| ADR-002 ↔ code architecture | BlobStore, LogEngine, Sequencer, Durability, ManifestSequencer, truncate_before match |
| TD-004 ↔ budget/flush | Defaults, early-flush, media ops, Local durable protocol match implementation |
| Opaque payloads | Vision/PRD intent preserved under ADR-002 (even stronger) |
| No Kafka wire in core | Consistent across old and new authority |
| Orphan objects deferred | ADR-002 explicit; tests document orphans without claiming reaper |
| ntier-proposal rejection | Documented; matches niflheim cold-tier integration story |

---

## Content migration ledger (misplaced content)

| Source | Content unit | Classification | Destination | Content to add | Follow-up |
|--------|--------------|---------------|-------------|----------------|-----------|
| vision + PRD FR-1..9 | Kafka record / acks / producer triple as **core** | `move` | Consumer-system docs / fjord+heimq governance (out of this repo) or PRD “non-goals / adapter layer above object-log” | “Kafka producer semantics are **not** object-log types; consumers map acks→Durability and carry Meta” | evolve vision/PRD |
| PRD FR-15..17, CONTRACT EpochGuard | Epoch fencing in core | `move` | Sequencer implementor responsibilities (fjord coordinator) | Fencing is Sequencer/Meta concern | evolve + contracts |
| CONTRACT-002 CAS | Manifest compare-and-set | `delete` / `move` | Superseded; ManifestSequencer uses plain put of unique manifest objects | Document unique-key + durable put | new CONTRACT-002 |
| TD-001 segment codec | OLOG framed segments | `delete` | Removed 0.2; framing is consumer-owned (heimq/niflheim) | Opaque multiplexed bytes | mark TD-001 superseded |
| TD-003 Kafka LogBackend | Kafka as object-log backend | `move` | fjord/heimq binding design | object-log is storage engine only | evolve TD-003 or archive |
| concerns FR-13 override | Reject 1-record/object | `split` | Replace with amortization KPI under load | Linger group-commit metric | evolve concerns |
| PRD FR-24..27 pqueue/Niflheim | Still valid as **integration constraints** | `keep` (reword) | PRD compatibility section | Opaque bytes + cold-tier / projection without product schemas in object-log | evolve wording only |

---

## Recommended gap-closure sequence

```text
1. evolve  — vision, PRD, concerns, feature-registry  (F1, F8, F9)
2. design  — CONTRACT-001/002 rewrite (or v2) for engine + BlobStore  (F2)
3. design  — supersede TD-001/002/003; keep TD-004; optional TD-005 integration maps  (F3, F9)
4. evolve  — test-plan + implementation-plan to ADR-002 P0s  (F4, F6)
5. frame   — optional user stories / FEAT specs OR recorded library exception  (F5)
6. build   — ordering + sequencer conformance tests; CHANGELOG hygiene; perf gate  (F6, F7, F10)
```

Do **not** implement new product features to “satisfy” old FRs (Kafka record model, CAS ObjectStore, EpochGuard). That would re-diverge from ADR-002.

---

## Suggested work items (tracker-ready)

1. **ALIGN-001** — Evolve vision + PRD + concerns + features to ADR-002 product definition.  
2. **ALIGN-002** — Author CONTRACT-001-v2 (LogEngine + Sequencer) and CONTRACT-002-v2 (BlobStore); supersede drafts.  
3. **ALIGN-003** — Supersede TD-001/002/003; document S3 as BlobStore; retention = truncate_before.  
4. **ALIGN-004** — Rewrite test-plan and implementation-plan; map P0s to existing `tests/*` names; add missing ordering/conformance cases.  
5. **ALIGN-005** — CHANGELOG Unreleased cleanup; perf test release gate.  
6. **ALIGN-006** — Decide: user stories for library vs explicit concern exception.

---

## Verdict

| Layer | At report open | After evolve pass |
|-------|----------------|-------------------|
| Architecture (ADR-002) ↔ Code | **ALIGNED** | **ALIGNED** |
| Frame ↔ Architecture/Code | **DIVERGENT** | **ALIGNED** (docs evolved) |
| Contracts + TD-001/002/003 ↔ Code | **STALE / DIVERGENT** | **ALIGNED** (v2 rewrite) |
| TD-004 ↔ Code | **ALIGNED** | **ALIGNED** |
| Test/impl plans ↔ Code | **STALE_PLAN** | **ALIGNED** (P1 residual tests remain) |

**Overall**: governing stack matches ADR-002 + 0.2.x code. Next safe HELIX action: `build` M1 conformance hardening (ordering harness, optional sequencer_conformance extract), or `polish` consumer integration docs (FEAT-006).
