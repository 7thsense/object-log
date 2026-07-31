---
ddx:
  id: td-conformance-kafka-backend-extraction
  depends_on:
    - td-core-and-object-backend
    - adr-object-storage-log-engine-and-sequencer-seam
    - prd
---

# Technical Design: TD-003 Conformance and Consumer Integration

**Status**: accepted (rewritten for object-log 0.2.x / ADR-002)  
**Related**: CONTRACT-001/002 v2, PRD FR-27..FR-30, FEAT-006  
**Supersedes**: TD-003 v1 (Kafka-backed `LogBackend` inside object-log; wire scaffolding in this crate)

## Scope

**In scope**

- Shared behavioral expectations for BlobStore adapters and Sequencer implementations
- Integration maps for fjord, Niflheim cold tier, and pqueue-class consumers
- Explicit boundary: Kafka wire/protocol crates stay **outside** object-log

**Out of scope**

- Implementing a Kafka produce/fetch backend inside object-log
- Kafka TCP, SASL, consumer groups, transactions
- Shipping heimq or fjord code in this repository

## Conformance Suites

### BlobStore conformance

Every `BlobStore` adapter MUST pass:

| Case | Required behavior |
|------|-------------------|
| put/get round-trip | bytes preserved |
| get missing | `None` |
| get_range slice / empty / OOB | slice ok; empty range empty; OOB error |
| list prefix | returns written keys under prefix |
| delete missing | success |
| invalid key | `InvalidObjectKey` |
| Local durability | data readable after new `LocalBlobStore` on same root |

Reference: `tests/blob.rs` (`port_suite`).

### Engine + Sequencer behavioral suite

| Case | Required behavior | Reference test |
|------|-------------------|----------------|
| produce/fetch round-trip | opaque bytes + offsets | `produce_fetch_round_trip` |
| PUT amortization | objects independent of partition count under group-commit | `put_count_independent_of_partition_count` |
| Sequenced ⇒ durable | offsets only when sequenced | `sequenced_implies_durable` |
| Dense concurrent offsets | no gaps under concurrent producers | `concurrent_producers_get_dense_contiguous_offsets` |
| PUT hard fail | no ack, no offset | `put_failure_yields_no_ack_no_offset` |
| Transient PUT retry | eventually acks | `transient_put_failure_is_retried_before_ack` |
| Commit fail + retry | exactly-once visibility; fresh object key | `commit_failure_orphans_object_and_retry_is_exactly_once` |
| Atomic multiplex | commit Err acks none | `multiplexed_commit_is_all_or_nothing` |
| Duplicate outcome | prior offset, no double append | `idempotent_retry_does_not_duplicate` |
| truncate_before | deletes only dead objects | `truncate_before_deletes_dead_objects` |
| Manifest restart | index survives reopen | `manifest_index_survives_restart` |

### Sequencer implementor checklist

A custom `Sequencer` MUST:

1. Assign offsets atomically across the full `commit` slice (all-or-nothing).
2. Return one `CommitOutcome` per input batch in order.
3. Honor `Duplicate` when recognizing idempotent retries via `Meta`.
4. Ensure `lookup` only returns entries whose bytes are durable (engine guarantees put-before-commit).
5. On `truncate_before`, return only object ids with zero remaining references **across partitions**.

Reference suite: `tests/sequencer_conformance.rs` (InMemory + Manifest). Engine presentation ordering: `per_producer_send_order_is_contiguous_on_shared_partition` in `tests/engine.rs`.

## Consumer Integration Maps

### Fjord (broker)

```text
heimq: Kafka wire + record batch codec (offset stamp in format)
fjord-coordinator: impl object_log::Sequencer (Meta = producer fields; fencing; EOS)
fjord binding: map Kafka acks → Durability; LogEngine::produce/fetch
object-log: BlobStore + group-commit only
```

**Binding sketch**

| Kafka / broker concept | object-log mapping |
|------------------------|--------------------|
| `acks=0` | `Durability::Buffered` (+ optional later `flush`) |
| `acks=1` (local) | often `Durability::Durable` (bytes on object store; no offset yet) or `Sequenced` if offset required |
| `acks=all` / `-1` | `Durability::Sequenced` |
| `(topic, partition)` | `PartitionKey` (encode however the coordinator prefers) |
| record batch bytes | opaque `payload`; heimq stamps `base_offset` after assign |
| producer id/epoch/seq | fields inside `Sequencer::Meta` only |
| DeleteRecords / retention | coordinator policy → `engine.truncate_before` |

object-log MUST NOT grow Kafka types to “help” fjord (PRD FR-30).

### Niflheim (cold tier)

```text
Niflheim hot tier: local fsync path (out of object-log scope)
Niflheim cold: BlobStore put/get_range/list; durable-on-return marks cold_durable
Optional: use LogEngine or raw BlobStore depending on whether offset index is desired
Codecs/checksums: Niflheim-owned framing inside opaque bytes
```

**Binding sketch**

| Niflheim cold concern | object-log primitive |
|-----------------------|----------------------|
| Mark cold durable after upload | `BlobStore::put` Ok (durable-on-return) |
| Load one chunk from coalesced object | `get_range(key, start..end)` |
| Boot scan of cold keys | `list(prefix)` (paginates internally) |
| Large coalesced segments | multipart inside `S3BlobStore` / streaming `put_chunks` on Local |
| Offset index / publish gating | Niflheim control plane, or optional `Sequencer`/`LogEngine` |

### pqueue-class

```text
Opaque command bytes as produce payloads
Projection reads via fetch by offset
Ownership/fencing: consumer control plane or custom Sequencer Meta
```

**Binding sketch**: `produce` command envelopes as opaque bytes; project with `fetch`; snapshot high-watermark then `truncate_before`. Fencing is not a core API—use `Meta` or refuse produce in the caller before enqueue.

## Shared Kafka Protocol Boundary (outside this crate)

A future shared protocol crate (heimq or successor) may own:

- frame read/write, API versions, SASL/TLS plumbing
- record batch encode/decode and offset stamping

It must **not** own object-log storage or BlobStore adapters.

## Testing / Extraction Gates

- object-log P0 suite green on Memory + Local.
- Optional S3 env tests green before claiming S3 production support.
- Fjord hermetic parity remains fjord’s merge gate after binding to object-log (not re-run inside this repo by default).

## Review Checklist

- [x] No Kafka LogBackend inside object-log
- [x] Conformance cases name real tests
- [x] Consumer maps respect ADR-002 layering
