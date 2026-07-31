---
ddx:
  id: td-durable-ops-budget-and-flush-controller
  depends_on:
    - adr-object-storage-log-engine-and-sequencer-seam
---

# Technical Design: TD-004 Durable-Ops Budget and Adaptive Flush Controller

**Status**: implemented (object-log 0.2.x)  
**Related**: ADR-002, `LogEngine`, `BlobStore`, `FlushConfig`  
**Consumers**: Fjord (first-class), Fireweed/Local, `ManifestSequencer` users  

## 1. Purpose

Make `LogEngine` a **latency ↔ throughput** write path over any `BlobStore`:

1. **Linger** is the packing control (under load: segment size ≈ rate × linger).
2. **`max_bytes`** is a **high physics/safety ceiling** (default 1 GiB), not the packing knob.
3. **Durable-ops budget** meters media ops (fsync/PUT), with modes that never strand admitted waiters past max linger without seal or error.
4. **Local** durable put is the fast protocol: temp → `sync_data` → rename → dir `fsync`; `put_chunks` streams without a full pre-merge copy.
5. One inspectable surface: `pipeline_snapshot()`.

## 2. Normative behavior

### 2.1 Flush triggers

Seal when any of:

| Trigger | Condition |
|---------|-----------|
| Size | `queued_bytes ≥ max_bytes` or `batches ≥ max_batches` |
| Linger deadline | oldest buffered item age ≥ `linger` |
| Early-flush | budget on **and** headroom **and** `queued_bytes ≤ early_flush_max_queued_bytes` **and** no enqueue for `early_flush_idle` |
| `flush()` | barrier: all seq ≤ barrier sealed |
| Shutdown | drain |

**Packing rule:** under continuous produce, early-flush is **off** (idle timer keeps resetting). Seals wait full linger or size. Under sparse produce, after `early_flush_idle` quiet, seal early for latency.

### 2.2 Defaults (`FlushConfig::default`)

| Knob | Default | Role |
|------|---------|------|
| `max_bytes` | **1 GiB** | Ceiling so linger binds |
| `max_batches` | 10_000 | Secondary ceiling |
| `linger` | **50 ms** | Latency↔throughput control |
| `max_inflight_flushes` | **1** | Single-flight Local bulk; raise for parallel S3 |
| `max_buffered_bytes` | **2 GiB** | Queue + inflight RAM cap |
| `budget.enabled` | **true** | On for Fjord |
| `budget.mode` | `LatencyPriority` | Deadline seals may overdraft |
| `early_flush_max_queued_bytes` | 4 MiB | No early-flush if queue larger |
| `early_flush_idle` | 10 ms | No early-flush unless quiet that long |

### 2.3 Budget

- Currency: **media_ops** from `BlobStore::take_media_op_stats()` (fallback: 1 per put).
- Local: **2** per put (`sync_data` + dir `fsync`).
- Shared store: stats around data put **and** sequencer commit (e.g. ManifestSequencer).
- Modes: `LatencyPriority` | `BudgetPriority` | `FailClosed` (see implementation).
- Effective budget = f(config cap/floor, fraction, default capacity); **cap wins**.

### 2.4 Local durable protocol

```text
create parent dirs if needed
write temp (same dir as final key), streaming put_chunks without full concat
File::sync_data   // fdatasync on Unix
rename temp → final
fsync(parent dir)
```

### 2.5 Flush API

`LogEngine::flush()` — barrier for all work enqueued at or before the call. Primary use: drain `Durability::Buffered` pipelines.

### 2.6 Hot I/O path (no slow dual path)

| Situation | Path |
|-----------|------|
| `max_inflight_flushes == 1` (default) | Flush thread `block_on(put_chunks)`; Local uses `block_in_place` (no `spawn_blocking` queue) |
| `max_inflight_flushes > 1` | Concurrent put tasks on the flush runtime (S3 parallelism) |
| Local on current-thread Tokio | `spawn_blocking` (unit tests) |

## 3. Performance evidence (honest harness)

`tests/perf_throughput.rs` — zeros only, alloc outside timers, `--release`:

| Label | Meaning |
|-------|---------|
| dd | shell `dd conv=fdatasync` |
| B0 | Rust stream + `sync_data` |
| B0d | B0 + dir fsync |
| B1 | warm median flat-key `LocalBlobStore::put` |
| B2 | prebuilt chunks → produce + flush (split enqueue vs flush) |

Representative host result (256 MiB): **B2 ≈ 0.9× B0**, **objects = 1**, enqueue negligible vs flush.

Run:

```bash
OBJECT_LOG_PERF_BYTES=$((256*1024*1024)) \
  cargo test --release --test perf_throughput honest -- --nocapture
```

## 4. Non-goals

- Auto-tuning `max_bytes` as a performance knob (ceiling only).
- In-crate hot journal tier (optional future Local mode).
- Kafka semantics inside object-log.

## 5. Acceptance (as-built gates)

1. Bulk B2 with total ≪ `max_bytes`: **objects ≤ 2**.  
2. B2.flush / B0 ≥ **0.70** (release, honest harness).  
3. Warm B1 not systematically 3× slower than B0 (median of ≥3 runs).  
4. Idle sequenced produce completes well under full linger after idle gap.  
5. `cargo test --all-features` + clippy `-D warnings` green.  
6. Class A: no Durable/Sequenced ack before put (+ commit for Sequenced).

## 6. Implementation map

| Area | Files |
|------|--------|
| Budget + early-flush policy | `src/budget.rs` |
| Group-commit, linger, flush barrier | `src/engine.rs` |
| Local put / put_chunks | `src/blob.rs` |
| S3 media_ops | `src/s3.rs` |
| Honest perf | `tests/perf_throughput.rs` |
| Policy unit tests | `tests/perf_budget.rs`, `tests/engine.rs` |
