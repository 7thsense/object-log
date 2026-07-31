---
title: Sequencer
weight: 3
---

Synchronous linearization point: assign offsets and own the offset→location
index. The engine never interprets `Meta`.

## Trait surface

| Method | Role |
|--------|------|
| `commit` | Atomic multi-batch assign; `Assigned` or `Duplicate` per batch |
| `lookup` | Index entries from a fetch offset onward |
| `high_watermark` / `log_start_offset` | Index-only bounds |
| `truncate_before` | Drop entries; return object ids unreferenced across **all** partitions |

## Shipped implementors

| Type | Meta | Crash-durable index? |
|------|------|----------------------|
| `InMemorySequencer` | `()` | No — bytes may live in BlobStore; index is process memory |
| `ManifestSequencer` | `()` | Yes — manifest objects rebuilt on `open` |

## Consumer implementors

Brokers implement `Sequencer` with typed `Meta` (producer identity, fencing).
object-log stays free of those types. Conformance checklist lives in TD-003 and
`tests/sequencer_conformance.rs`.
