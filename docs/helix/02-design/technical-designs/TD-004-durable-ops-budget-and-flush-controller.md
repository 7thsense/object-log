---
ddx:
  id: td-durable-ops-budget-and-flush-controller
  depends_on:
    - adr-object-storage-log-engine-and-sequencer-seam
---

# Technical Design: TD-004 Durable-Ops Budget and Adaptive Flush Controller

**Status**: accepted (grill-confirmed 2026-07-30 on object-log **0.2.0**)  
**Related**: ADR-002, `LogEngine`, `BlobStore`, `FlushConfig`  
**Consumers**: Fjord (first-class; S3), Fireweed/Local, standalone `ManifestSequencer` users  
**Code tip**: `bb5dd2e` / 0.2.0 engine

## 1. Purpose

Extend the **existing** ADR-002 `LogEngine` group-commit path with a
**budget-aware flush controller** so that:

1. **Media / billable durable ops** (not bulk MB/s) are the performance currency.
2. **Latency stays low when media is idle** (headroom early-flush).
3. **Under load**, the engine co-buffers within an operator max wait, reducing
   durable-ops/s (and S3 cost) without a second consumer-side flush brain.
4. **Fjord** gets this behavior **by default** (safe, unaggressive defaults).
5. Config, startup measurement (Local), ongoing EWMA, and **effective** knobs are
   all inspectable; config **caps always win**.

This design **does not** re-found the engine, reintroduce 0.1.0 CAS segment
keys, or put Kafka semantics in object-log.

## 2. Background

### 2.1 What 0.2.0 already provides

| Capability | Location |
|------------|----------|
| Multiplexed group-commit | `LogEngine` — many produces → one object PUT |
| Flush triggers | `FlushConfig`: `max_bytes`, `max_batches`, `linger` |
| Concurrent PUTs | `max_inflight_flushes` |
| Memory backpressure | `max_buffered_bytes` |
| Local durable put | temp → `sync_all` → rename → parent dir `sync_all` (~2 media ops) |
| S3 durable put | PutObject / multipart (`s3` feature) |
| Sequencer after PUT | may add another durable put (`ManifestSequencer`) |

### 2.2 Gap

- Default `linger: ZERO` flushes ASAP → high media-ops rate under sparse produce.
- No durable-op budget, probe, or adaptive linger.
- No media-op accounting on `BlobStore`.
- Fireweed-class arithmetic (`ops/s ≈ seals/s × cmds/seal` with seals limited by
  media_ops/s) needs engine-level rate control for Fjord and Local alike.

### 2.3 Grill decisions (normative)

1. Budget-aware **flush controller** in `LogEngine` (not rebuild group-commit).
2. Currency = **media / billable durable ops**.
3. Auto-drives **effective linger + headroom early-flush**. Segment size under
   load is **rate × linger**; `max_bytes` is a high physics ceiling (default
   ~1 GiB) so size does not short-circuit linger.
4. Modes: `latency_priority` (default) | `budget_priority` | `fail_closed`.
   **Latency cannot lose** for admitted work: only **backpressure** and/or
   **small flushes** (overdraft + undersize metrics).
5. Auto-tune: budget + linger curve; inspectable layers; config cap wins.
6. All media ops on the **shared** `BlobStore` (data + sequencer/manifest puts).
7. Controller **on by default**; Fjord is first-class.
8. **No S3 probe by default**; Local may probe; capacity from config / defaults /
   ongoing EWMA.
9. `BlobStore::take_media_op_stats()` defaulted; fallback 1 media_op per put.

## 3. Goals / non-goals

### Goals

1. Media-op accounting on Local, Memory, S3 adapters.
2. Store-scoped token budget consumed by flush+commit durable work on that store.
3. Adaptive effective linger within `[0, linger]` (operator `linger` = max wait).
4. Headroom early-flush when tokens abundant.
5. Budget modes with admission reservation for `fail_closed`.
6. Inspectable `PipelineSnapshot` (configured / startup / ongoing / effective).
7. Defaults suitable for Fjord S3 without a startup probe tax.
8. Tests for accounting, adaptive linger under mock capacity, mode behavior.

### Non-goals

- Treating `max_bytes` as a performance knob (it is a safety ceiling; packing is linger).
- Kafka idempotence / epoch fencing inside object-log.
- Distributed multi-process Local coordination.
- Weakening durable-on-return `put` or Sequenced semantics.

## 4. Design

### 4.1 Media op accounting

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MediaOpStats {
    pub media_ops: u64,
    pub bytes: u64,
}

// On BlobStore:
fn take_media_op_stats(&self) -> Option<MediaOpStats> { None }
```

| Adapter | media_ops per successful put |
|---------|------------------------------|
| `LocalBlobStore` | 2 (file sync + dir sync); + ancestor dir syncs if we count them later |
| `S3BlobStore` | 1 per PutObject; multipart = parts + complete (+ abort if any) |
| `MemoryBlobStore` | 0 media_ops; bytes still counted |
| Unknown impl | `None` → engine treats each successful put/put_chunks as **1** media_op |

Failed ops that still performed durable work should increment what was observed
when the adapter can know (best-effort).

### 4.2 Budget config (extends flush policy)

```rust
pub enum BudgetMode {
    /// Default. Deadline flush always runs; overdraft allowed; meter it.
    LatencyPriority,
    /// Admission may wait for tokens; admitted deadline flushes still complete.
    BudgetPriority,
    /// Reserve predicted media_ops at Durable/Sequenced admission; else error.
    FailClosed,
}

pub struct BudgetConfig {
    /// Master switch; default **true** (Fjord wants this on).
    pub enabled: bool,
    pub mode: BudgetMode,
    /// Hard cap on durable_ops/sec (None = no cap beyond capacity×fraction).
    pub budget_per_sec_cap: Option<f64>,
    pub budget_per_sec_floor: Option<f64>,
    /// Fraction of measured/default capacity when cap unset or as soft target.
    pub budget_fraction: f64, // default 0.5
    /// Used when no probe and no ongoing estimate (S3 default path).
    pub default_capacity_per_sec: f64, // Local default ~100, S3-class ~50
    pub early_flush_fill_ratio: f64,   // e.g. 0.5
    pub early_flush_cooldown: Duration,
    pub admission_timeout: Duration,  // budget_priority wait
}
```

**Effective budget** (normative):

```text
capacity = startup_probe ?? ongoing_ewma_capacity ?? default_capacity_per_sec
raw = capacity * budget_fraction
effective = raw
if cap: effective = min(effective, cap)
if floor: effective = max(effective, floor)
require floor ≤ cap when both set
```

### 4.3 Adaptive linger

Operator `FlushConfig.linger` is the **maximum** time a batch may wait in the
queue before a deadline flush (hard ceiling).

**Default change (0.2.x):** `linger` default becomes **`50ms`** (not `ZERO`), so
the controller has room to co-buffer under pressure. Headroom early-flush
restores near-zero wait when media is idle—preserving Fjord’s idle latency
while enabling batching under load.

```text
effective_linger ∈ [0, config.linger]
  high headroom  → effective_linger → 0 (early flush)
  low headroom   → effective_linger → config.linger
```

Hard triggers unchanged: `max_bytes`, `max_batches`, shutdown.

**Early-flush gate (bulk must not split seals):** early-flush only when
token headroom, `queued_bytes ≤ early_flush_max_queued_bytes` (default 4 MiB),
**and** no enqueue for `early_flush_idle` (default 10 ms). Sustained produce
keeps the idle timer fresh → full linger / `flush()` packs.

**Local put protocol:** temp → `sync_data` (fdatasync) → rename → dir `fsync`.
`put_chunks` streams chunks to the temp file (no full pre-merge copy).

**Latency cannot lose:** at `effective_linger` / operator deadline, flush **even
if undersized** and even if that **overdrafts** the budget under
`latency_priority`. Under `budget_priority` / `fail_closed`, prefer
**admission backpressure** so fewer waiters exist; never stretch past deadline
for already-admitted Durable/Sequenced produces.

### 4.4 Predicted cost and shared-store sequencing

- EWMA of media_ops observed around each flush (take stats before/after put +
  after `Sequencer::commit` if commit uses the same store).
- Bootstrap prediction: Local **2**, S3 single-put **1**, multipart measured;
  +1 if using `ManifestSequencer` on same blob (until measured).
- `fail_closed`: reserve prediction at admission; release excess after actual.

### 4.5 Probe

| Backend | Default |
|---------|---------|
| Local | Optional short durable put+delete under `.object-log-probe/` at engine start |
| S3 | **No probe**; use `default_capacity_per_sec` + ongoing EWMA |
| Memory | Synthetic unlimited / high capacity |

### 4.6 Inspectability

```rust
pub struct EffectiveKnob<T> { configured, startup_measured, ongoing_measured, effective, reason }
pub struct PipelineSnapshot {
  budget_per_sec: EffectiveKnob<f64>,
  effective_linger: EffectiveKnob<Duration>,
  // counters: flushes, media_ops, overdraft, undersized, admissions rejected, ...
  buffer: BufferStats,
}
```

`LogEngine::pipeline_snapshot()` (or `flush_stats`) exposes this.

### 4.7 API / errors

- `ObjectLogError` gains `#[non_exhaustive]` if not already, plus
  `BudgetExceeded`, `InvalidConfig` as needed.
- `FlushConfig` gains `budget: BudgetConfig` (or flattened fields).
- Env debug: extend `OLOG_DEBUG_FLUSH_CONFIG` to print effective knobs.

## 5. Implementation slices

| Slice | Work |
|-------|------|
| S1 | `MediaOpStats` + `take_media_op_stats` on trait; Local/Memory/S3 implement; unit tests |
| S2 | Token budget + modes + predicted cost; admission paths |
| S3 | Adaptive effective linger + headroom early-flush in `take_batch` / flush loop |
| S4 | Probe (Local) + ongoing EWMA + inspectable snapshot; default linger 50ms + budget on |
| S5 | Conformance / integration tests; docs (README snippet); Fjord-facing notes |

## 6. Acceptance criteria

1. Local durable put reports media_ops ≥ 2 per put (file+dir) in tests.
2. Engine snapshot shows configured/startup/ongoing/effective budget.
3. With tight budget and non-zero linger, under load effective linger stretches
   and flush rate drops vs linger=0 baseline (test with mock/slow stats).
4. With headroom, single produce at Durable completes without waiting full linger.
5. Deadline flush undersized + overdraft metered under latency_priority.
6. `fail_closed` rejects admission when tokens cannot be reserved.
7. S3 path does not require startup probe.
8. Existing engine tests pass; Sequenced still only after put+commit.
9. Default config enables budget controller (Fjord-ready).

## 7. Risks

| Risk | Mitigation |
|------|------------|
| Default linger 50ms changes Fjord tail latency | Headroom early-flush; document; allow linger=0 to force legacy ASAP |
| ManifestSequencer double-count accuracy | Shared store stats bracketing commit |
| Trait default None under-counts custom stores | Document weight fallback; encourage impl |

## 8. Summary

Ship a **default-on**, Fjord-first **media-ops budget** and **adaptive linger**
controller on top of ADR-002’s `LogEngine`, with exact store accounting, no S3
startup probe, inspectable effective knobs, and latency-safe modes that only
backpressure or write smaller objects—never stretch past the operator max wait.
