---
title: Durability
weight: 4
---

object-log's own vocabulary—not Kafka `acks`. Consumers map externally.

| Level | Meaning |
|-------|---------|
| `Buffered` | Accepted into the flush buffer; may be lost on crash before flush |
| `Durable` | Object PUT completed; survives crash; **no offset yet** |
| `Sequenced` | Durable **and** sequencer commit returned; stable offsets |

## Mapping example (consumer-side)

| External | Typical mapping |
|----------|-----------------|
| fire-and-forget / `acks=0` | `Buffered` (+ optional later `flush`) |
| “bytes on media” | `Durable` |
| “has a stable offset” / `acks=all` | `Sequenced` |

## Failure semantics

| Event | Visibility |
|-------|------------|
| PUT fails after retries | No durable or sequenced ack |
| Commit fails | No sequenced ack; object may be orphaned; retry uses a **fresh** key |
| `Duplicate` outcome | Prior `base_offset`; no second visibility |

Orphan cleanup: `reap_orphans` with `live_object_ids()` — only when writers on
that data prefix are **quiescent**.
