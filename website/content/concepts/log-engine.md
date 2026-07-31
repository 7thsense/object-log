---
title: LogEngine
weight: 2
---

Buffered, multiplexing append engine over a `BlobStore` and a `Sequencer`.

## Produce path

1. Enqueue `(partition, payload, record_count, meta, durability)`.
2. Flush worker seals when linger, size, batch count, early-flush, or
   `flush()` barrier fires.
3. Layout batches into one object; **PUT** via BlobStore.
4. Call `Sequencer::commit` with engine-authored `BatchLocation`s.
5. Resolve waiters per requested durability.

Invariants (contract):

- PUT before commit  
- Fresh object key on retry  
- Per-partition arrival order; no split across concurrent commits  
- Commit is all-or-nothing for the seal  

## Fetch path

`lookup` → `get_range` per index entry → `FetchedBatch { base_offset, record_count, payload }`.

`fetch_stream` visits batches without building a full `Vec` (wide replay).

## Flush controls

| Knob | Role |
|------|------|
| `linger` | Primary packing control under load (default 50 ms) |
| `max_bytes` | High safety ceiling (default 1 GiB) |
| `max_inflight_flushes` | Default 1 (ordering-friendly single-flight) |
| budget | Durable-ops controller (TD-004); inspect via `pipeline_snapshot()` |
